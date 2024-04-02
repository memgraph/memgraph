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

#include "nuraft/coordinator_state_machine.hpp"
#include "utils/file.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "json/json.hpp"

#include "libnuraft/nuraft.hxx"

using memgraph::coordination::CoordinatorStateMachine;
using memgraph::coordination::CoordinatorToCoordinatorConfig;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::InstanceUUIDUpdate;
using memgraph::coordination::RaftLogAction;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::utils::UUID;
using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::ptr;

class CoordinatorStateMachineTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() /
                                     "MG_tests_unit_coordinator_state_machine"};
};

TEST_F(CoordinatorStateMachineTest, SerializeRegisterReplicationInstance) {
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

  ptr<buffer> data = CoordinatorStateMachine::SerializeRegisterInstance(config);
  buffer_serializer bs(*data);
  auto const expected = nlohmann::json{{"action", RaftLogAction::REGISTER_REPLICATION_INSTANCE}, {"info", config}};
  ASSERT_EQ(bs.get_str(), expected.dump());
}

TEST_F(CoordinatorStateMachineTest, SerializeUnregisterReplicationInstance) {
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

  CoordinatorStateMachine::SerializeRegisterInstance(config);
  ptr<buffer> data = CoordinatorStateMachine::SerializeUnregisterInstance("instance3");
  buffer_serializer bs(*data);

  auto const expected =
      nlohmann::json{{"action", RaftLogAction::UNREGISTER_REPLICATION_INSTANCE}, {"info", "instance3"}};
  ASSERT_EQ(bs.get_str(), expected.dump());
}

TEST_F(CoordinatorStateMachineTest, SerializeSetInstanceToMain) {
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

  CoordinatorStateMachine::SerializeRegisterInstance(config);

  {
    auto const uuid_update_obj = InstanceUUIDUpdate{.instance_name = "instance3", .uuid = UUID{}};
    ptr<buffer> data = CoordinatorStateMachine::SerializeSetInstanceAsMain(uuid_update_obj);
    buffer_serializer bs(*data);
    auto const expected = nlohmann::json{{"action", RaftLogAction::SET_INSTANCE_AS_MAIN}, {"info", uuid_update_obj}};
    ASSERT_EQ(bs.get_str(), expected.dump());
  }

  {
    ptr<buffer> data = CoordinatorStateMachine::SerializeSetInstanceAsReplica("instance3");
    buffer_serializer bs(*data);
    auto const expected = nlohmann::json{{"action", RaftLogAction::SET_INSTANCE_AS_REPLICA}, {"info", "instance3"}};
    ASSERT_EQ(bs.get_str(), expected.dump());
  }
}

TEST_F(CoordinatorStateMachineTest, SerializeUpdateUUID) {
  auto uuid = UUID{};

  ptr<buffer> data = CoordinatorStateMachine::SerializeUpdateUUIDForNewMain(uuid);
  buffer_serializer bs(*data);
  auto const expected = nlohmann::json{{"action", RaftLogAction::UPDATE_UUID_OF_NEW_MAIN}, {"info", uuid}};
  ASSERT_EQ(bs.get_str(), expected.dump());
}
