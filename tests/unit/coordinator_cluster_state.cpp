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

#include "coordination/coordinator_cluster_state.hpp"
#include "io/network/endpoint.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/uuid.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "libnuraft/nuraft.hxx"

#include <vector>

using memgraph::coordination::CoordinatorClusterState;
using memgraph::coordination::CoordinatorClusterStateDelta;
using memgraph::coordination::CoordinatorInstanceContext;
using memgraph::coordination::DataInstanceConfig;
using memgraph::coordination::DataInstanceContext;
using memgraph::coordination::InstanceUUIDUpdate;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::replication_coordination_glue::ReplicationRole;
using memgraph::utils::UUID;
using nuraft::buffer;
using nuraft::ptr;

// No networking communication in this test.
class CoordinatorClusterStateTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(CoordinatorClusterStateTest, TestUpgrade) {
  std::vector<DataInstanceContext> data_instances;

  auto config = DataInstanceConfig{.instance_name = "instance2",
                                   .mgt_server = Endpoint{"127.0.0.1", 10111},
                                   .bolt_server = Endpoint{"127.0.0.1", 7688},
                                   .replication_client_info = {.instance_name = "instance_name",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10010}}};

  auto const uuid = UUID{};
  data_instances.emplace_back(config, ReplicationRole::REPLICA, uuid);

  std::vector coord_instances{
      CoordinatorInstanceContext{.id = 1, .bolt_server = "127.0.0.1:7690"},
      CoordinatorInstanceContext{.id = 2, .bolt_server = "127.0.0.1:7691"},
  };

  {
    nlohmann::json legacy_json = {{memgraph::coordination::kDataInstances.data(), data_instances},
                                  {memgraph::coordination::kMainUUID.data(), uuid},
                                  {memgraph::coordination::kCoordinatorInstances.data(), coord_instances},
                                  {memgraph::coordination::kEnabledReadsOnMain.data(), true},
                                  {memgraph::coordination::kSyncFailoverOnly.data(), false},
                                  {memgraph::coordination::kMaxFailoverLagOnReplica.data(), 20}};

    CoordinatorClusterState legacy_state;
    nlohmann::from_json(legacy_json, legacy_state);
  }

  {
    nlohmann::json legacy_json = {{memgraph::coordination::kClusterState.data(), data_instances},
                                  {memgraph::coordination::kMainUUID.data(), uuid},
                                  {memgraph::coordination::kCoordinatorInstances.data(), coord_instances},
                                  {memgraph::coordination::kEnabledReadsOnMain.data(), true},
                                  {memgraph::coordination::kSyncFailoverOnly.data(), false},
                                  {memgraph::coordination::kMaxFailoverLagOnReplica.data(), 20}

    };
    CoordinatorClusterState legacy_state;
    nlohmann::from_json(legacy_json, legacy_state);
  }
}

TEST_F(CoordinatorClusterStateTest, RegisterReplicationInstance) {
  CoordinatorClusterState cluster_state{};
  std::vector<DataInstanceContext> data_instances;

  auto config = DataInstanceConfig{.instance_name = "instance3",
                                   .mgt_server = Endpoint{"127.0.0.1", 10112},
                                   .bolt_server = Endpoint{"127.0.0.1", 7687},
                                   .replication_client_info = {.instance_name = "instance_name",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10001}}};

  auto const uuid = UUID{};
  data_instances.emplace_back(config, ReplicationRole::REPLICA, uuid);

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state{.data_instances_ = std::move(data_instances),
                                                 .current_main_uuid_ = uuid};

  cluster_state.DoAction(delta_state);

  auto const data_instances_res = cluster_state.GetDataInstancesContext();
  ASSERT_EQ(data_instances_res.size(), 1);
  ASSERT_EQ(data_instances_res[0].config, config);
  ASSERT_EQ(data_instances_res[0].status, ReplicationRole::REPLICA);
}

TEST_F(CoordinatorClusterStateTest, SetInstanceToReplica) {
  std::vector<DataInstanceContext> data_instances;
  CoordinatorClusterState cluster_state{};
  {
    auto config = DataInstanceConfig{.instance_name = "instance1",
                                     .mgt_server = Endpoint{"127.0.0.1", 10112},
                                     .bolt_server = Endpoint{"127.0.0.1", 7687},
                                     .replication_client_info = {.instance_name = "instance1",
                                                                 .replication_mode = ReplicationMode::ASYNC,
                                                                 .replication_server = Endpoint{"127.0.0.1", 10001}}};
    auto const uuid = UUID{};
    data_instances.emplace_back(config, ReplicationRole::MAIN, uuid);

    // NOLINTNEXTLINE
    CoordinatorClusterStateDelta const delta_state{.data_instances_ = data_instances, .current_main_uuid_ = uuid};
    cluster_state.DoAction(delta_state);
  }
  {
    auto config = DataInstanceConfig{.instance_name = "instance2",
                                     .mgt_server = Endpoint{"127.0.0.1", 10111},
                                     .bolt_server = Endpoint{"127.0.0.1", 7688},
                                     .replication_client_info = {.instance_name = "instance2",
                                                                 .replication_mode = ReplicationMode::ASYNC,
                                                                 .replication_server = Endpoint{"127.0.0.1", 10010}}};

    auto const uuid = UUID{};
    data_instances.emplace_back(config, ReplicationRole::REPLICA, uuid);

    // NOLINTNEXTLINE
    CoordinatorClusterStateDelta const delta_state{.data_instances_ = std::move(data_instances),
                                                   .current_main_uuid_ = uuid};
    cluster_state.DoAction(delta_state);
  }

  auto const repl_instances = cluster_state.GetDataInstancesContext();
  ASSERT_EQ(repl_instances.size(), 2);
  ASSERT_EQ(repl_instances[0].status, ReplicationRole::MAIN);
  ASSERT_EQ(repl_instances[1].status, ReplicationRole::REPLICA);
  ASSERT_TRUE(cluster_state.MainExists());
  ASSERT_TRUE(cluster_state.HasMainState("instance1"));
  ASSERT_FALSE(cluster_state.HasMainState("instance2"));
  ASSERT_FALSE(cluster_state.IsCurrentMain("instance2"));
}

TEST_F(CoordinatorClusterStateTest, Coordinators) {
  CoordinatorClusterState cluster_state;

  std::vector<CoordinatorInstanceContext> const coord_instances{
      CoordinatorInstanceContext{.id = 1, .bolt_server = "127.0.0.1:7690"},
      CoordinatorInstanceContext{.id = 2, .bolt_server = "127.0.0.1:7691"},
  };

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state{.coordinator_instances_ = std::move(coord_instances),
                                                 .current_main_uuid_ = UUID{}};
  cluster_state.DoAction(delta_state);

  auto const coord_instances_res = cluster_state.GetCoordinatorInstancesContext();
  ASSERT_EQ(coord_instances_res.size(), 2);
  ASSERT_EQ(coord_instances_res[0].bolt_server, "127.0.0.1:7690");
  ASSERT_EQ(coord_instances_res[0].id, 1);
  ASSERT_EQ(coord_instances_res[1].bolt_server, "127.0.0.1:7691");
  ASSERT_EQ(coord_instances_res[1].id, 2);
}

TEST_F(CoordinatorClusterStateTest, Marshalling) {
  CoordinatorClusterState cluster_state;
  std::vector<DataInstanceContext> data_instances;

  auto config = DataInstanceConfig{.instance_name = "instance2",
                                   .mgt_server = Endpoint{"127.0.0.1", 10111},
                                   .bolt_server = Endpoint{"127.0.0.1", 7688},
                                   .replication_client_info = {.instance_name = "instance_name",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10010}}};

  auto const uuid = UUID{};
  data_instances.emplace_back(config, ReplicationRole::REPLICA, uuid);

  std::vector<CoordinatorInstanceContext> coord_instances{
      CoordinatorInstanceContext{.id = 1, .bolt_server = "127.0.0.1:7690"},
      CoordinatorInstanceContext{.id = 2, .bolt_server = "127.0.0.1:7691"},
  };

  // by default, it should be true
  ASSERT_TRUE(cluster_state.GetSyncFailoverOnly());

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state{.data_instances_ = std::move(data_instances),
                                                 .coordinator_instances_ = coord_instances,
                                                 .current_main_uuid_ = uuid,
                                                 .enabled_reads_on_main_ = true,
                                                 .sync_failover_only_ = false,
                                                 .max_failover_replica_lag_ = 25,
                                                 .max_replica_read_lag_ = 10};
  cluster_state.DoAction(delta_state);

  ptr<buffer> data;
  cluster_state.Serialize(data);

  auto deserialized_cluster_state = CoordinatorClusterState::Deserialize(*data);
  ASSERT_EQ(cluster_state, deserialized_cluster_state);
  ASSERT_TRUE(cluster_state.GetEnabledReadsOnMain());
  ASSERT_FALSE(cluster_state.GetSyncFailoverOnly());
  ASSERT_EQ(cluster_state.GetMaxFailoverReplicaLag(), 25);
  ASSERT_EQ(cluster_state.GetMaxReplicaReadLag(), 10);
}

TEST_F(CoordinatorClusterStateTest, RoutingPoliciesSwitch) {
  CoordinatorClusterState cluster_state{};
  std::vector<DataInstanceContext> data_instances;

  auto config = DataInstanceConfig{.instance_name = "instance2",
                                   .mgt_server = Endpoint{"127.0.0.1", 10111},
                                   .bolt_server = Endpoint{"127.0.0.1", 7688},
                                   .replication_client_info = {.instance_name = "instance_name",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10010}}};

  auto const uuid = UUID{};
  data_instances.emplace_back(config, ReplicationRole::REPLICA, uuid);

  std::vector coord_instances{
      CoordinatorInstanceContext{.id = 1, .bolt_server = "127.0.0.1:7690"},
      CoordinatorInstanceContext{.id = 2, .bolt_server = "127.0.0.1:7691"},
  };

  auto json = nlohmann::json{{memgraph::coordination::kDataInstances.data(), data_instances},
                             {memgraph::coordination::kMainUUID.data(), uuid},
                             {memgraph::coordination::kCoordinatorInstances.data(), coord_instances}};

  auto const log = json.dump();
  ptr<buffer> data = buffer::alloc(sizeof(uint32_t) + log.size());
  nuraft::buffer_serializer bs(data);
  bs.put_str(log);

  auto deserialized_cluster_state = CoordinatorClusterState::Deserialize(*data);
  ASSERT_EQ(deserialized_cluster_state.GetCoordinatorInstancesContext(), coord_instances);
  ASSERT_EQ(deserialized_cluster_state.GetDataInstancesContext(), data_instances);
  ASSERT_EQ(deserialized_cluster_state.GetCurrentMainUUID(), uuid);
  // by default read false
  ASSERT_FALSE(deserialized_cluster_state.GetEnabledReadsOnMain());
  // by default read true
  ASSERT_TRUE(deserialized_cluster_state.GetSyncFailoverOnly());
  // by default read uint64_t::max()
  ASSERT_EQ(deserialized_cluster_state.GetMaxFailoverReplicaLag(), std::numeric_limits<uint64_t>::max());
  ASSERT_EQ(deserialized_cluster_state.GetMaxReplicaReadLag(), std::numeric_limits<uint64_t>::max());
}
