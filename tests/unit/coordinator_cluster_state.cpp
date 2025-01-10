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
#include "utils/file.hpp"
#include "utils/uuid.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "json/json.hpp"
#include "libnuraft/nuraft.hxx"

#include <vector>

using memgraph::coordination::CoordinatorClusterState;
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

  std::vector<CoordinatorInstanceContext> coord_instances;

  cluster_state.DoAction(data_instances, coord_instances, uuid);

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

    std::vector<CoordinatorInstanceContext> coord_instances;
    cluster_state.DoAction(data_instances, coord_instances, uuid);
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

    std::vector<CoordinatorInstanceContext> coord_instances;

    cluster_state.DoAction(data_instances, coord_instances, uuid);
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

  cluster_state.DoAction({}, coord_instances, UUID{});

  auto const coord_instances_res = cluster_state.GetCoordinatorInstancesContext();
  ASSERT_EQ(coord_instances_res.size(), 2);
  ASSERT_EQ(coord_instances_res[0].bolt_server, "127.0.0.1:7690");
  ASSERT_EQ(coord_instances_res[0].id, 1);
  ASSERT_EQ(coord_instances_res[1].bolt_server, "127.0.0.1:7691");
  ASSERT_EQ(coord_instances_res[1].id, 2);
}

TEST_F(CoordinatorClusterStateTest, Marshalling) {
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

  std::vector<CoordinatorInstanceContext> coord_instances{
      CoordinatorInstanceContext{.id = 1, .bolt_server = "127.0.0.1:7690"},
      CoordinatorInstanceContext{.id = 2, .bolt_server = "127.0.0.1:7691"},
  };

  cluster_state.DoAction(data_instances, coord_instances, uuid);

  ptr<buffer> data{};
  cluster_state.Serialize(data);

  auto deserialized_cluster_state = CoordinatorClusterState::Deserialize(*data);
  ASSERT_EQ(cluster_state, deserialized_cluster_state);
}
