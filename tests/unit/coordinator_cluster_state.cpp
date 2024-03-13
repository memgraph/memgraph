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
      CoordinatorToReplicaConfig{"instance3",
                                 "127.0.0.1",
                                 10112,
                                 std::chrono::seconds{1},
                                 std::chrono::seconds{5},
                                 std::chrono::seconds{10},
                                 {"instance_name", ReplicationMode::ASYNC, "replication_ip_address", 10001},
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
    CoordinatorToReplicaConfig config{"instance1",
                                      "127.0.0.1",
                                      10111,
                                      std::chrono::seconds{1},
                                      std::chrono::seconds{5},
                                      std::chrono::seconds{10},
                                      {"instance_name", ReplicationMode::ASYNC, "replication_ip_address", 10001},
                                      .ssl = std::nullopt};

    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);
    auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);

    coordinator_cluster_state.DoAction(payload, action);
  }
  {
    CoordinatorToReplicaConfig config{"instance2",
                                      "127.0.0.1",
                                      10112,
                                      std::chrono::seconds{1},
                                      std::chrono::seconds{5},
                                      std::chrono::seconds{10},
                                      {"instance_name", ReplicationMode::ASYNC, "replication_ip_address", 10002},
                                      .ssl = std::nullopt};

    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);
    auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);

    coordinator_cluster_state.DoAction(payload, action);
  }
  {
    CoordinatorToReplicaConfig config{"instance3",
                                      "127.0.0.1",
                                      10113,
                                      std::chrono::seconds{1},
                                      std::chrono::seconds{5},
                                      std::chrono::seconds{10},
                                      {"instance_name", ReplicationMode::ASYNC, "replication_ip_address", 10003},
                                      .ssl = std::nullopt};

    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);
    auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);

    coordinator_cluster_state.DoAction(payload, action);
  }
  {
    CoordinatorToReplicaConfig config{"instance4",
                                      "127.0.0.1",
                                      10114,
                                      std::chrono::seconds{1},
                                      std::chrono::seconds{5},
                                      std::chrono::seconds{10},
                                      {"instance_name", ReplicationMode::ASYNC, "replication_ip_address", 10004},
                                      .ssl = std::nullopt};

    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);
    auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);

    coordinator_cluster_state.DoAction(payload, action);
  }
  {
    CoordinatorToReplicaConfig config{"instance5",
                                      "127.0.0.1",
                                      10115,
                                      std::chrono::seconds{1},
                                      std::chrono::seconds{5},
                                      std::chrono::seconds{10},
                                      {"instance_name", ReplicationMode::ASYNC, "replication_ip_address", 10005},
                                      .ssl = std::nullopt};

    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);
    auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);

    coordinator_cluster_state.DoAction(payload, action);
  }
  {
    CoordinatorToReplicaConfig config{"instance6",
                                      "127.0.0.1",
                                      10116,
                                      std::chrono::seconds{1},
                                      std::chrono::seconds{5},
                                      std::chrono::seconds{10},
                                      {"instance_name", ReplicationMode::ASYNC, "replication_ip_address", 10006},
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
