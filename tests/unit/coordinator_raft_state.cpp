// Copyright 2024 Memgraph Ltd.
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
#include "json/json.hpp"

#include "libnuraft/nuraft.hxx"

using memgraph::coordination::CoordinatorInstanceInitConfig;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::RaftState;
using memgraph::coordination::ReplicationClientInfo;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
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

  uint32_t const coordinator_id = 21;
  uint16_t const bolt_port = 6687;
  uint16_t const coordinator_port = 40112;
};

TEST_F(RaftStateTest, RaftStateEmptyMetadata) {
  auto become_leader_cb = []() {};
  auto become_follower_cb = []() {};

  auto const config = CoordinatorInstanceInitConfig{coordinator_id, coordinator_port, bolt_port,
                                                    test_folder_ / "high_availability" / "coordinator"};

  auto raft_state = RaftState::MakeRaftState(config, std::move(become_leader_cb), std::move(become_follower_cb));

  ASSERT_EQ(raft_state.InstanceName(), fmt::format("coordinator_{}", coordinator_id));
  ASSERT_EQ(raft_state.RaftSocketAddress(), fmt::format("0.0.0.0:{}", coordinator_port));
  ASSERT_TRUE(raft_state.IsLeader());
  ASSERT_TRUE(raft_state.GetReplicationInstances().empty());

  auto const coords = raft_state.GetCoordinatorInstances();
  ASSERT_EQ(coords.size(), 1);
}

TEST_F(RaftStateTest, GetSingleRouterRoutingTable) {
  auto become_leader_cb = []() {};
  auto become_follower_cb = []() {};
  auto const init_config = CoordinatorInstanceInitConfig{coordinator_id, coordinator_port, bolt_port,
                                                         test_folder_ / "high_availability" / "coordinator"};

  auto const raft_state =
      RaftState::MakeRaftState(init_config, std::move(become_leader_cb), std::move(become_follower_cb));
  auto routing_table = raft_state.GetRoutingTable();

  ASSERT_EQ(routing_table.size(), 1);

  auto const routers = routing_table[0];
  auto const expected_routers = std::vector<std::string>{fmt::format("0.0.0.0:{}", bolt_port)};
  ASSERT_EQ(routers.first, expected_routers);
  ASSERT_EQ(routers.second, "ROUTE");
}

TEST_F(RaftStateTest, GetMixedRoutingTable) {
  auto become_leader_cb = []() {};
  auto become_follower_cb = []() {};
  auto const init_config = CoordinatorInstanceInitConfig{coordinator_id, coordinator_port, bolt_port,
                                                         test_folder_ / "high_availability" / "coordinator"};
  auto leader = RaftState::MakeRaftState(init_config, std::move(become_leader_cb), std::move(become_follower_cb));

  leader.AppendRegisterReplicationInstanceLog(CoordinatorToReplicaConfig{
      .instance_name = "instance1",
      .mgt_server = Endpoint{"0.0.0.0", 10011},
      .bolt_server = Endpoint{"0.0.0.0", 7687},
      .replication_client_info = ReplicationClientInfo{.instance_name = "instance1",
                                                       .replication_mode = ReplicationMode::ASYNC,
                                                       .replication_server = Endpoint{"0.0.0.0", 10001}}});

  leader.AppendRegisterReplicationInstanceLog(CoordinatorToReplicaConfig{
      .instance_name = "instance2",
      .mgt_server = Endpoint{"0.0.0.0", 10012},
      .bolt_server = Endpoint{"0.0.0.0", 7688},
      .replication_client_info = ReplicationClientInfo{.instance_name = "instance2",
                                                       .replication_mode = ReplicationMode::ASYNC,
                                                       .replication_server = Endpoint{"0.0.0.0", 10002}}});

  leader.AppendRegisterReplicationInstanceLog(CoordinatorToReplicaConfig{
      .instance_name = "instance3",
      .mgt_server = Endpoint{"0.0.0.0", 10013},
      .bolt_server = Endpoint{"0.0.0.0", 7689},
      .replication_client_info = ReplicationClientInfo{.instance_name = "instance3",
                                                       .replication_mode = ReplicationMode::ASYNC,
                                                       .replication_server = Endpoint{"0.0.0.0", 10003}}});

  leader.AppendSetInstanceAsMainLog("instance1", UUID{});

  auto const routing_table = leader.GetRoutingTable();

  ASSERT_EQ(routing_table.size(), 3);

  auto const &mains = routing_table[0];
  ASSERT_EQ(mains.second, "WRITE");
  ASSERT_EQ(mains.first, std::vector<std::string>{"0.0.0.0:7687"});

  auto const &replicas = routing_table[1];
  ASSERT_EQ(replicas.second, "READ");
  auto const expected_replicas = std::vector<std::string>{"0.0.0.0:7688", "0.0.0.0:7689"};
  ASSERT_EQ(replicas.first, expected_replicas);

  auto const &routers = routing_table[2];
  ASSERT_EQ(routers.second, "ROUTE");
  auto const expected_routers = std::vector<std::string>{fmt::format("0.0.0.0:{}", bolt_port)};
  ASSERT_EQ(routers.first, expected_routers);
}
