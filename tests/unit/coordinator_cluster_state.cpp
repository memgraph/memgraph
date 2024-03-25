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
using memgraph::coordination::CoordinatorInstanceInitConfig;
using memgraph::coordination::CoordinatorInstanceState;
using memgraph::coordination::CoordinatorToCoordinatorConfig;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::InstanceUUIDUpdate;
using memgraph::coordination::RaftLogAction;
using memgraph::coordination::ReplicationInstanceState;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::replication_coordination_glue::ReplicationRole;
using memgraph::utils::UUID;
using nuraft::buffer;
using nuraft::ptr;

class CoordinatorClusterStateTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() /
                                     "MG_tests_unit_coordinator_cluster_state"};
};

TEST_F(CoordinatorClusterStateTest, RegisterReplicationInstance) {
  auto const init_config1 =
      CoordinatorInstanceInitConfig{.coordinator_id = 1, .coordinator_port = 10111, .bolt_port = 7688};
  CoordinatorClusterState cluster_state{init_config1};

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

  cluster_state.DoAction(config, RaftLogAction::REGISTER_REPLICATION_INSTANCE);

  auto instances = cluster_state.GetReplicationInstances();
  ASSERT_EQ(instances.size(), 1);
  ASSERT_EQ(instances[0].config, config);
  ASSERT_EQ(instances[0].status, ReplicationRole::REPLICA);
  ASSERT_EQ(cluster_state.GetCoordinatorInstances().size(), 1);

  ASSERT_TRUE(cluster_state.HasReplicaState("instance3"));
}

TEST_F(CoordinatorClusterStateTest, UnregisterReplicationInstance) {
  auto const init_config1 =
      CoordinatorInstanceInitConfig{.coordinator_id = 1, .coordinator_port = 10111, .bolt_port = 7688};
  CoordinatorClusterState cluster_state{init_config1};

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

  cluster_state.DoAction(config, RaftLogAction::REGISTER_REPLICATION_INSTANCE);
  cluster_state.DoAction("instance3", RaftLogAction::UNREGISTER_REPLICATION_INSTANCE);

  ASSERT_EQ(cluster_state.GetReplicationInstances().size(), 0);
}

TEST_F(CoordinatorClusterStateTest, SetInstanceToMain) {
  auto const init_config1 =
      CoordinatorInstanceInitConfig{.coordinator_id = 1, .coordinator_port = 10111, .bolt_port = 7688};
  CoordinatorClusterState cluster_state{init_config1};
  {
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
    cluster_state.DoAction(config, RaftLogAction::REGISTER_REPLICATION_INSTANCE);
  }
  {
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
    cluster_state.DoAction(config, RaftLogAction::REGISTER_REPLICATION_INSTANCE);
  }

  cluster_state.DoAction(InstanceUUIDUpdate{.instance_name = "instance3", .uuid = UUID{}},
                         RaftLogAction::SET_INSTANCE_AS_MAIN);
  auto const repl_instances = cluster_state.GetReplicationInstances();
  ASSERT_EQ(repl_instances.size(), 2);
  ASSERT_EQ(repl_instances[0].status, ReplicationRole::REPLICA);
  ASSERT_EQ(repl_instances[1].status, ReplicationRole::MAIN);
  ASSERT_TRUE(cluster_state.MainExists());
  ASSERT_TRUE(cluster_state.HasMainState("instance3"));
  ASSERT_FALSE(cluster_state.HasMainState("instance2"));
  ASSERT_TRUE(cluster_state.HasReplicaState("instance2"));
  ASSERT_FALSE(cluster_state.HasReplicaState("instance3"));
}

TEST_F(CoordinatorClusterStateTest, SetInstanceToReplica) {
  auto const init_config1 =
      CoordinatorInstanceInitConfig{.coordinator_id = 1, .coordinator_port = 10111, .bolt_port = 7688};

  CoordinatorClusterState cluster_state{init_config1};
  {
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
    cluster_state.DoAction(config, RaftLogAction::REGISTER_REPLICATION_INSTANCE);
  }
  {
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
    cluster_state.DoAction(config, RaftLogAction::REGISTER_REPLICATION_INSTANCE);
  }

  cluster_state.DoAction(InstanceUUIDUpdate{.instance_name = "instance3", .uuid = UUID{}},
                         RaftLogAction::SET_INSTANCE_AS_MAIN);
  cluster_state.DoAction("instance3", RaftLogAction::SET_INSTANCE_AS_REPLICA);
  cluster_state.DoAction(InstanceUUIDUpdate{.instance_name = "instance2", .uuid = UUID{}},
                         RaftLogAction::SET_INSTANCE_AS_MAIN);
  auto const repl_instances = cluster_state.GetReplicationInstances();
  ASSERT_EQ(repl_instances.size(), 2);
  ASSERT_EQ(repl_instances[0].status, ReplicationRole::MAIN);
  ASSERT_EQ(repl_instances[1].status, ReplicationRole::REPLICA);
  ASSERT_TRUE(cluster_state.MainExists());
  ASSERT_TRUE(cluster_state.HasMainState("instance2"));
  ASSERT_FALSE(cluster_state.HasMainState("instance3"));
  ASSERT_TRUE(cluster_state.HasReplicaState("instance3"));
  ASSERT_FALSE(cluster_state.HasReplicaState("instance2"));
}

TEST_F(CoordinatorClusterStateTest, UpdateUUID) {
  auto const init_config1 =
      CoordinatorInstanceInitConfig{.coordinator_id = 1, .coordinator_port = 10111, .bolt_port = 7688};
  CoordinatorClusterState cluster_state{init_config1};
  auto uuid = UUID();
  cluster_state.DoAction(uuid, RaftLogAction::UPDATE_UUID_OF_NEW_MAIN);
  ASSERT_EQ(cluster_state.GetCurrentMainUUID(), uuid);
}

TEST_F(CoordinatorClusterStateTest, AddCoordinatorInstance) {
  auto const init_config1 =
      CoordinatorInstanceInitConfig{.coordinator_id = 1, .coordinator_port = 10111, .bolt_port = 7688};
  CoordinatorToCoordinatorConfig config{.coordinator_id = 1,
                                        .bolt_server = Endpoint{"127.0.0.1", 7687},
                                        .coordinator_server = Endpoint{"127.0.0.1", 10111}};

  CoordinatorClusterState cluster_state{init_config1};
  cluster_state.DoAction(config, RaftLogAction::ADD_COORDINATOR_INSTANCE);

  auto instances = cluster_state.GetCoordinatorInstances();
  ASSERT_EQ(instances.size(), 2);
  ASSERT_EQ(instances[1].config, config);
}

TEST_F(CoordinatorClusterStateTest, ReplicationInstanceStateSerialization) {
  ReplicationInstanceState instance_state{
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
  ReplicationInstanceState deserialized_instance_state = j.get<ReplicationInstanceState>();

  EXPECT_EQ(instance_state.config, deserialized_instance_state.config);
  EXPECT_EQ(instance_state.status, deserialized_instance_state.status);
}

TEST_F(CoordinatorClusterStateTest, CoordinatorInstanceStateSerialization) {
  CoordinatorInstanceState instance_state{
      CoordinatorToCoordinatorConfig{.coordinator_id = 1,
                                     .bolt_server = Endpoint{"127.0.0.1", 7687},
                                     .coordinator_server = Endpoint{"127.0.0.1", 10111}}};
  nlohmann::json j = instance_state;
  CoordinatorInstanceState deserialized_instance_state = j.get<CoordinatorInstanceState>();
  ASSERT_EQ(instance_state, deserialized_instance_state);
}

TEST_F(CoordinatorClusterStateTest, Marshalling) {
  auto const init_config1 =
      CoordinatorInstanceInitConfig{.coordinator_id = 1, .coordinator_port = 10111, .bolt_port = 7688};

  CoordinatorClusterState cluster_state{init_config1};
  CoordinatorToCoordinatorConfig config{.coordinator_id = 1,
                                        .bolt_server = Endpoint{"127.0.0.1", 7687},
                                        .coordinator_server = Endpoint{"127.0.0.1", 10111}};

  cluster_state.DoAction(config, RaftLogAction::ADD_COORDINATOR_INSTANCE);

  auto config2 =
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
  cluster_state.DoAction(config2, RaftLogAction::REGISTER_REPLICATION_INSTANCE);

  ptr<buffer> data{};
  cluster_state.Serialize(data);

  auto deserialized_cluster_state = CoordinatorClusterState::Deserialize(*data);
  ASSERT_EQ(cluster_state, deserialized_cluster_state);
}
