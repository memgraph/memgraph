// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "coordination/raft_state.hpp"
#include "utils/file.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "libnuraft/nuraft.hxx"

using memgraph::coordination::CoordinatorInstanceContext;
using memgraph::coordination::CoordinatorInstanceInitConfig;
using memgraph::coordination::CoordinatorStateManagerConfig;
using memgraph::coordination::DataInstanceConfig;
using memgraph::coordination::DataInstanceContext;
using memgraph::coordination::RaftState;
using memgraph::coordination::ReplicationClientInfo;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::replication_coordination_glue::ReplicationRole;
using memgraph::utils::UUID;
using nuraft::ptr;

// Networking is used in this test, be careful with ports used.
class RaftStateTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!std::filesystem::exists(test_folder_)) return;
    std::filesystem::remove_all(test_folder_);
  }

  void TearDown() override {
    if (!std::filesystem::exists(test_folder_)) return;
    std::filesystem::remove_all(test_folder_);
  }

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "MG_tests_unit_raft_state"};

  int32_t const coordinator_id = 21;
  uint16_t const bolt_port = 6687;
  uint16_t const coordinator_port = 40112;
  uint16_t const management_port = 21345;
};

TEST_F(RaftStateTest, RaftStateEmptyMetadata) {
  auto become_leader_cb = []() {};
  auto become_follower_cb = []() {};

  auto const instance_config =
      CoordinatorInstanceInitConfig{.coordinator_id = coordinator_id,
                                    .coordinator_port = coordinator_port,
                                    .bolt_port = bolt_port,
                                    .management_port = management_port,
                                    .durability_dir = test_folder_ / "high_availability" / "raft_state_empty_metadata",
                                    .coordinator_hostname = "localhost"};

  auto const raft_state =
      std::make_unique<RaftState>(instance_config, std::move(become_leader_cb), std::move(become_follower_cb));
  raft_state->InitRaftServer();

  ASSERT_EQ(raft_state->InstanceName(), fmt::format("coordinator_{}", coordinator_id));
  ASSERT_TRUE(raft_state->IsLeader());
  ASSERT_TRUE(raft_state->GetDataInstancesContext().empty());

  // Context for coordinators get changed only after you added coordinator
  auto const coords_ctx = raft_state->GetCoordinatorInstancesContext();
  ASSERT_EQ(coords_ctx.size(), 0);

  // Aux is updated as soon as raft server is initialized
  auto const coords_aux = raft_state->GetCoordinatorInstancesAux();
  ASSERT_EQ(coords_aux.size(), 1);
}

class RaftStateParamTest : public RaftStateTest, public ::testing::WithParamInterface<bool> {};
INSTANTIATE_TEST_SUITE_P(BoolParams, RaftStateParamTest, ::testing::Values(true, false));

TEST_P(RaftStateParamTest, GetMixedRoutingTable) {
  auto become_leader_cb = []() {};
  auto become_follower_cb = []() {};
  auto const init_config =
      CoordinatorInstanceInitConfig{.coordinator_id = coordinator_id,
                                    .coordinator_port = coordinator_port,
                                    .bolt_port = bolt_port,
                                    .management_port = management_port,
                                    .durability_dir = test_folder_ / "high_availability" / "mixed_routing",
                                    .coordinator_hostname = "localhost"};

  auto raft_state_leader =
      std::make_unique<RaftState>(init_config, std::move(become_leader_cb), std::move(become_follower_cb));

  raft_state_leader->InitRaftServer();

  std::vector<DataInstanceContext> data_instances{};
  auto const curr_uuid = UUID{};

  data_instances.emplace_back(
      DataInstanceConfig{
          .instance_name = "instance1",
          .mgt_server = Endpoint{"0.0.0.0", 10011},
          .bolt_server = Endpoint{"0.0.0.0", 7687},
          .replication_client_info = ReplicationClientInfo{.instance_name = "instance1",
                                                           .replication_mode = ReplicationMode::ASYNC,
                                                           .replication_server = Endpoint{"0.0.0.0", 10001}}},
      ReplicationRole::MAIN, curr_uuid);

  data_instances.emplace_back(
      DataInstanceConfig{
          .instance_name = "instance2",
          .mgt_server = Endpoint{"0.0.0.0", 10012},
          .bolt_server = Endpoint{"0.0.0.0", 7688},
          .replication_client_info = ReplicationClientInfo{.instance_name = "instance2",
                                                           .replication_mode = ReplicationMode::ASYNC,
                                                           .replication_server = Endpoint{"0.0.0.0", 10002}}},
      ReplicationRole::REPLICA, curr_uuid);

  data_instances.emplace_back(
      DataInstanceConfig{
          .instance_name = "instance3",
          .mgt_server = Endpoint{"0.0.0.0", 10013},
          .bolt_server = Endpoint{"0.0.0.0", 7689},
          .replication_client_info = ReplicationClientInfo{.instance_name = "instance3",
                                                           .replication_mode = ReplicationMode::ASYNC,
                                                           .replication_server = Endpoint{"0.0.0.0", 10003}}},

      ReplicationRole::REPLICA, curr_uuid);

  auto coord_instances = std::vector<CoordinatorInstanceContext>{};
  coord_instances.emplace_back(1, fmt::format("localhost:{}", bolt_port));

  bool const enabled_reads_on_main = GetParam();
  // NOLINTNEXTLINE
  memgraph::coordination::CoordinatorClusterStateDelta const delta_state{
      .data_instances_ = data_instances,
      .coordinator_instances_ = coord_instances,
      .current_main_uuid_ = curr_uuid,
      .enabled_reads_on_main_ = enabled_reads_on_main};

  ASSERT_TRUE(raft_state_leader->AppendClusterUpdate(delta_state));

  auto const routing_table = raft_state_leader->GetRoutingTable();

  ASSERT_EQ(routing_table.size(), 3);

  auto const &[main_instances, main_role] = routing_table[0];
  ASSERT_EQ(main_role, "WRITE");
  ASSERT_EQ(main_instances, std::vector<std::string>{"0.0.0.0:7687"});

  auto const &[replica_instances, replica_role] = routing_table[1];
  ASSERT_EQ(replica_role, "READ");
  if (enabled_reads_on_main) {
    auto const expected_replicas = std::vector<std::string>{"0.0.0.0:7688", "0.0.0.0:7689", "0.0.0.0:7687"};
    ASSERT_EQ(replica_instances, expected_replicas);
  } else {
    auto const expected_replicas = std::vector<std::string>{"0.0.0.0:7688", "0.0.0.0:7689"};
    ASSERT_EQ(replica_instances, expected_replicas);
  }

  auto const &[routing_instances, routing_role] = routing_table[2];
  ASSERT_EQ(routing_role, "ROUTE");
  auto const expected_routers = std::vector<std::string>{fmt::format("localhost:{}", bolt_port)};
  ASSERT_EQ(routing_instances, expected_routers);
}
