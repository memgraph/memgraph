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

#include "nuraft/coordinator_cluster_state.hpp"
#include "io/network/endpoint.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/file.hpp"
#include "utils/uuid.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "json/json.hpp"

#include "libnuraft/nuraft.hxx"

using memgraph::coordination::CoordinatorClusterState;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::DataInstanceState;
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
  std::vector<DataInstanceState> data_instances;

  auto config =
      CoordinatorToReplicaConfig{.instance_name = "instance3",
                                 .mgt_server = Endpoint{"127.0.0.1", 10112},
                                 .bolt_server = Endpoint{"127.0.0.1", 7687},
                                 .replication_client_info = {.instance_name = "instance_name",
                                                             .replication_mode = ReplicationMode::ASYNC,
                                                             .replication_server = Endpoint{"127.0.0.1", 10001}},
                                 .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                 .instance_down_timeout_sec = std::chrono::seconds{5},
                                 .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                 .ssl = std::nullopt};

  auto const uuid = UUID{};
  data_instances.emplace_back(config, ReplicationRole::REPLICA, uuid);

  cluster_state.DoAction(std::make_pair(data_instances, uuid));

  auto instances = cluster_state.GetDataInstances();
  ASSERT_EQ(instances.size(), 1);
  ASSERT_EQ(instances[0].config, config);
  ASSERT_EQ(instances[0].status, ReplicationRole::REPLICA);
}

TEST_F(CoordinatorClusterStateTest, SetInstanceToReplica) {
  std::vector<DataInstanceState> data_instances;
  CoordinatorClusterState cluster_state{};
  {
    auto config =
        CoordinatorToReplicaConfig{.instance_name = "instance1",
                                   .mgt_server = Endpoint{"127.0.0.1", 10112},
                                   .bolt_server = Endpoint{"127.0.0.1", 7687},
                                   .replication_client_info = {.instance_name = "instance1",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10001}},
                                   .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                   .instance_down_timeout_sec = std::chrono::seconds{5},
                                   .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                   .ssl = std::nullopt};
    auto const uuid = UUID{};
    data_instances.emplace_back(config, ReplicationRole::MAIN, uuid);
    cluster_state.DoAction(std::make_pair(data_instances, uuid));
  }
  {
    auto config =
        CoordinatorToReplicaConfig{.instance_name = "instance2",
                                   .mgt_server = Endpoint{"127.0.0.1", 10111},
                                   .bolt_server = Endpoint{"127.0.0.1", 7688},
                                   .replication_client_info = {.instance_name = "instance2",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10010}},
                                   .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                   .instance_down_timeout_sec = std::chrono::seconds{5},
                                   .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                   .ssl = std::nullopt};

    auto const uuid = UUID{};
    data_instances.emplace_back(config, ReplicationRole::REPLICA, uuid);
    cluster_state.DoAction(std::make_pair(data_instances, uuid));
  }

  auto const repl_instances = cluster_state.GetDataInstances();
  ASSERT_EQ(repl_instances.size(), 2);
  ASSERT_EQ(repl_instances[0].status, ReplicationRole::MAIN);
  ASSERT_EQ(repl_instances[1].status, ReplicationRole::REPLICA);
  ASSERT_TRUE(cluster_state.MainExists());
  ASSERT_TRUE(cluster_state.HasMainState("instance1"));
  ASSERT_FALSE(cluster_state.HasMainState("instance2"));
  ASSERT_FALSE(cluster_state.IsCurrentMain("instance2"));
}

TEST_F(CoordinatorClusterStateTest, ReplicationInstanceStateSerialization) {
  DataInstanceState instance_state{
      CoordinatorToReplicaConfig{.instance_name = "instance3",
                                 .mgt_server = Endpoint{"127.0.0.1", 10112},
                                 .bolt_server = Endpoint{"127.0.0.1", 7687},
                                 .replication_client_info = {.instance_name = "instance_name",
                                                             .replication_mode = ReplicationMode::ASYNC,
                                                             .replication_server = Endpoint{"127.0.0.1", 10001}},
                                 .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                 .instance_down_timeout_sec = std::chrono::seconds{5},
                                 .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                 .ssl = std::nullopt},
      ReplicationRole::MAIN, .instance_uuid = UUID()};

  nlohmann::json j = instance_state;
  DataInstanceState deserialized_instance_state = j.get<DataInstanceState>();

  EXPECT_EQ(instance_state.config, deserialized_instance_state.config);
  EXPECT_EQ(instance_state.status, deserialized_instance_state.status);
}

TEST_F(CoordinatorClusterStateTest, Marshalling) {
  CoordinatorClusterState cluster_state{};
  std::vector<DataInstanceState> data_instances;

  auto config =
      CoordinatorToReplicaConfig{.instance_name = "instance2",
                                 .mgt_server = Endpoint{"127.0.0.1", 10111},
                                 .bolt_server = Endpoint{"127.0.0.1", 7688},
                                 .replication_client_info = {.instance_name = "instance_name",
                                                             .replication_mode = ReplicationMode::ASYNC,
                                                             .replication_server = Endpoint{"127.0.0.1", 10010}},
                                 .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                 .instance_down_timeout_sec = std::chrono::seconds{5},
                                 .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                 .ssl = std::nullopt};

  auto const uuid = UUID{};
  data_instances.emplace_back(config, ReplicationRole::REPLICA, uuid);
  cluster_state.DoAction(std::make_pair(data_instances, uuid));

  ptr<buffer> data{};
  cluster_state.Serialize(data);

  auto deserialized_cluster_state = CoordinatorClusterState::Deserialize(*data);
  ASSERT_EQ(cluster_state, deserialized_cluster_state);
}
