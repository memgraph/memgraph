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
#include "nuraft/coordinator_state_machine.hpp"
#include "replication_coordination_glue/role.hpp"

#include "utils/file.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "json/json.hpp"

#include "libnuraft/nuraft.hxx"

using memgraph::coordination::CoordinatorClusterState;
using memgraph::coordination::CoordinatorStateMachine;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::RaftLogAction;
using memgraph::coordination::ReplicationInstanceState;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::replication_coordination_glue::ReplicationRole;
using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::ptr;

class CoordinatorClusterStateTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() /
                                     "MG_tests_unit_coordinator_cluster_state"};
};

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
      ReplicationRole::MAIN};

  nlohmann::json j = instance_state;
  ReplicationInstanceState deserialized_instance_state = j.get<ReplicationInstanceState>();

  EXPECT_EQ(instance_state.config, deserialized_instance_state.config);
  EXPECT_EQ(instance_state.status, deserialized_instance_state.status);
}

TEST_F(CoordinatorClusterStateTest, DoActionRegisterInstances) {
  auto coordinator_cluster_state = memgraph::coordination::CoordinatorClusterState{};

  {
    auto config =
        CoordinatorToReplicaConfig{.instance_name = "instance1",
                                   .mgt_server = Endpoint{"127.0.0.1", 10111},
                                   .bolt_server = Endpoint{"127.0.0.1", 7687},
                                   .replication_client_info = {.instance_name = "instance1",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10001}},
                                   .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                   .instance_down_timeout_sec = std::chrono::seconds{5},
                                   .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                   .ssl = std::nullopt};

    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);
    auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);

    coordinator_cluster_state.DoAction(payload, action);
  }
  {
    auto config =
        CoordinatorToReplicaConfig{.instance_name = "instance2",
                                   .mgt_server = Endpoint{"127.0.0.1", 10112},
                                   .bolt_server = Endpoint{"127.0.0.1", 7688},
                                   .replication_client_info = {.instance_name = "instance2",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10002}},
                                   .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                   .instance_down_timeout_sec = std::chrono::seconds{5},
                                   .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                   .ssl = std::nullopt};

    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);
    auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);

    coordinator_cluster_state.DoAction(payload, action);
  }
  {
    auto config =
        CoordinatorToReplicaConfig{.instance_name = "instance3",
                                   .mgt_server = Endpoint{"127.0.0.1", 10113},
                                   .bolt_server = Endpoint{"127.0.0.1", 7689},
                                   .replication_client_info = {.instance_name = "instance3",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10003}},
                                   .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                   .instance_down_timeout_sec = std::chrono::seconds{5},
                                   .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                   .ssl = std::nullopt};

    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);
    auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);

    coordinator_cluster_state.DoAction(payload, action);
  }
  {
    auto config =
        CoordinatorToReplicaConfig{.instance_name = "instance4",
                                   .mgt_server = Endpoint{"127.0.0.1", 10114},
                                   .bolt_server = Endpoint{"127.0.0.1", 7690},
                                   .replication_client_info = {.instance_name = "instance4",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10004}},
                                   .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                   .instance_down_timeout_sec = std::chrono::seconds{5},
                                   .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                   .ssl = std::nullopt};

    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);
    auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);

    coordinator_cluster_state.DoAction(payload, action);
  }
  {
    auto config =
        CoordinatorToReplicaConfig{.instance_name = "instance5",
                                   .mgt_server = Endpoint{"127.0.0.1", 10115},
                                   .bolt_server = Endpoint{"127.0.0.1", 7691},
                                   .replication_client_info = {.instance_name = "instance5",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10005}},
                                   .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                   .instance_down_timeout_sec = std::chrono::seconds{5},
                                   .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                   .ssl = std::nullopt};

    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);
    auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);

    coordinator_cluster_state.DoAction(payload, action);
  }
  {
    auto config =
        CoordinatorToReplicaConfig{.instance_name = "instance6",
                                   .mgt_server = Endpoint{"127.0.0.1", 10116},
                                   .bolt_server = Endpoint{"127.0.0.1", 7692},
                                   .replication_client_info = {.instance_name = "instance6",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10006}},
                                   .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                   .instance_down_timeout_sec = std::chrono::seconds{5},
                                   .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                   .ssl = std::nullopt};

    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);
    auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);

    coordinator_cluster_state.DoAction(payload, action);
  }

  ptr<buffer> data;
  coordinator_cluster_state.Serialize(data);

  auto deserialized_coordinator_cluster_state = CoordinatorClusterState::Deserialize(*data);
  ASSERT_EQ(coordinator_cluster_state.GetReplicationInstances(),
            deserialized_coordinator_cluster_state.GetReplicationInstances());
}
