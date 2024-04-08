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

#include "coordination/coordinator_communication_config.hpp"
#include "io/network/endpoint.hpp"
#include "nuraft/coordinator_state_machine.hpp"
#include "nuraft/raft_log_action.hpp"
#include "utils/file.hpp"
#include "utils/uuid.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "json/json.hpp"

using memgraph::coordination::CoordinatorStateMachine;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::RaftLogAction;
using memgraph::coordination::ReplicationClientInfo;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::utils::UUID;

// No networking communication in this test.
class RaftLogSerialization : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "MG_tests_unit_raft_log_serialization"};
};

TEST_F(RaftLogSerialization, ReplClientInfo) {
  ReplicationClientInfo info{.instance_name = "instance_name",
                             .replication_mode = ReplicationMode::SYNC,
                             .replication_server = Endpoint{"127.0.0.1", 10111}};

  nlohmann::json j = info;
  ReplicationClientInfo info2 = j.get<memgraph::coordination::ReplicationClientInfo>();

  ASSERT_EQ(info, info2);
}

TEST_F(RaftLogSerialization, CoordinatorToReplicaConfig) {
  CoordinatorToReplicaConfig config{.instance_name = "instance3",
                                    .mgt_server = Endpoint{"127.0.0.1", 10112},
                                    .replication_client_info = {.instance_name = "instance_name",
                                                                .replication_mode = ReplicationMode::ASYNC,
                                                                .replication_server = Endpoint{"127.0.0.1", 10001}},
                                    .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                    .instance_down_timeout_sec = std::chrono::seconds{5},
                                    .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                    .ssl = std::nullopt};

  nlohmann::json j = config;
  CoordinatorToReplicaConfig config2 = j.get<memgraph::coordination::CoordinatorToReplicaConfig>();

  ASSERT_EQ(config, config2);
}

TEST_F(RaftLogSerialization, RaftLogActionRegister) {
  auto action = RaftLogAction::REGISTER_REPLICATION_INSTANCE;

  nlohmann::json j = action;
  RaftLogAction action2 = j.get<memgraph::coordination::RaftLogAction>();

  ASSERT_EQ(action, action2);
}

TEST_F(RaftLogSerialization, RaftLogActionUnregister) {
  auto action = RaftLogAction::UNREGISTER_REPLICATION_INSTANCE;

  nlohmann::json j = action;
  RaftLogAction action2 = j.get<memgraph::coordination::RaftLogAction>();

  ASSERT_EQ(action, action2);
}

TEST_F(RaftLogSerialization, RaftLogActionPromote) {
  auto action = RaftLogAction::SET_INSTANCE_AS_MAIN;

  nlohmann::json j = action;
  RaftLogAction action2 = j.get<memgraph::coordination::RaftLogAction>();

  ASSERT_EQ(action, action2);
}

TEST_F(RaftLogSerialization, RaftLogActionDemote) {
  auto action = RaftLogAction::SET_INSTANCE_AS_REPLICA;

  nlohmann::json j = action;
  RaftLogAction action2 = j.get<memgraph::coordination::RaftLogAction>();

  ASSERT_EQ(action, action2);
}

TEST_F(RaftLogSerialization, RaftLogActionUpdateUUIDForInstance) {
  auto action = RaftLogAction::UPDATE_UUID_FOR_INSTANCE;

  nlohmann::json j = action;
  RaftLogAction action2 = j.get<memgraph::coordination::RaftLogAction>();

  ASSERT_EQ(action, action2);
}

TEST_F(RaftLogSerialization, RegisterInstance) {
  CoordinatorToReplicaConfig config{.instance_name = "instance3",
                                    .mgt_server = Endpoint{"127.0.0.1", 10112},
                                    .replication_client_info = {.instance_name = "instance_name",
                                                                .replication_mode = ReplicationMode::ASYNC,
                                                                .replication_server = Endpoint{"127.0.0.1", 10001}},
                                    .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                    .instance_down_timeout_sec = std::chrono::seconds{5},
                                    .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                    .ssl = std::nullopt};

  auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(config);
  auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);
  ASSERT_EQ(action, RaftLogAction::REGISTER_REPLICATION_INSTANCE);
  ASSERT_EQ(config, std::get<CoordinatorToReplicaConfig>(payload));
}

TEST_F(RaftLogSerialization, UnregisterInstance) {
  auto buffer = CoordinatorStateMachine::SerializeUnregisterInstance("instance3");
  auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);
  ASSERT_EQ(action, RaftLogAction::UNREGISTER_REPLICATION_INSTANCE);
  ASSERT_EQ("instance3", std::get<std::string>(payload));
}

TEST_F(RaftLogSerialization, SetInstanceAsMain) {
  auto instance_uuid_update =
      memgraph::coordination::InstanceUUIDUpdate{.instance_name = "instance3", .uuid = memgraph::utils::UUID{}};
  auto buffer = CoordinatorStateMachine::SerializeSetInstanceAsMain(instance_uuid_update);
  auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);
  ASSERT_EQ(action, RaftLogAction::SET_INSTANCE_AS_MAIN);
  ASSERT_EQ(instance_uuid_update.instance_name,
            std::get<memgraph::coordination::InstanceUUIDUpdate>(payload).instance_name);
  ASSERT_EQ(instance_uuid_update.uuid, std::get<memgraph::coordination::InstanceUUIDUpdate>(payload).uuid);
}

TEST_F(RaftLogSerialization, SetInstanceAsReplica) {
  auto buffer = CoordinatorStateMachine::SerializeSetInstanceAsReplica("instance3");
  auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);
  ASSERT_EQ(action, RaftLogAction::SET_INSTANCE_AS_REPLICA);
  ASSERT_EQ("instance3", std::get<std::string>(payload));
}

TEST_F(RaftLogSerialization, UpdateUUIDForNewMain) {
  UUID uuid;
  auto buffer = CoordinatorStateMachine::SerializeUpdateUUIDForNewMain(uuid);
  auto [payload, action] = CoordinatorStateMachine::DecodeLog(*buffer);
  ASSERT_EQ(action, RaftLogAction::UPDATE_UUID_OF_NEW_MAIN);
  ASSERT_EQ(uuid, std::get<UUID>(payload));
}
