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

#include "nuraft/coordinator_log_store.hpp"
#include "coordination/coordinator_communication_config.hpp"
#include "io/network/endpoint.hpp"
#include "nuraft/coordinator_state_machine.hpp"
#include "nuraft/raft_log_action.hpp"
#include "utils/file.hpp"
#include "utils/uuid.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "json/json.hpp"

using memgraph::coordination::CoordinatorLogStore;
using memgraph::coordination::CoordinatorStateMachine;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::RaftLogAction;
using memgraph::coordination::ReplicationClientInfo;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::utils::UUID;
;

using nuraft::log_entry;

// No networking communication in this test.
class CoordinatorLogStoreTests : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!std::filesystem::exists(test_folder_)) return;
    std::filesystem::remove_all(test_folder_);
  }

  void TearDown() override {
    if (!std::filesystem::exists(test_folder_)) return;
    std::filesystem::remove_all(test_folder_);
  }

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() /
                                     "MG_tests_unit_coordinator_log_store_tests"};
};

TEST_F(CoordinatorLogStoreTests, TestBasicSerialization) {
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

  // coordinator log store start
  {
    CoordinatorLogStore log_store{test_folder_ / "TestBasicSerialization"};
    ASSERT_EQ(log_store.next_slot(), 1);
    ASSERT_EQ(log_store.start_index(), 1);

    auto register_instance_log_entry = cs_new<log_entry>(1, buffer, nuraft::log_val_type::app_log);
    log_store.append(register_instance_log_entry);

    ASSERT_EQ(log_store.next_slot(), 2);
    ASSERT_EQ(log_store.start_index(), 1);
  }  // coordinator log store destroy

  // start again, read logs
  {
    CoordinatorLogStore log_store{test_folder_ / "TestBasicSerialization"};
    ASSERT_EQ(log_store.next_slot(), 2);
    ASSERT_EQ(log_store.start_index(), 1);

    auto entry = log_store.entry_at(1);

    auto [payload, action] = CoordinatorStateMachine::DecodeLog(entry->get_buf());

    ASSERT_EQ(log_store.next_slot(), 2);
    ASSERT_EQ(log_store.start_index(), 1);
    ASSERT_EQ(action, RaftLogAction::REGISTER_REPLICATION_INSTANCE);
    ASSERT_EQ(config, std::get<CoordinatorToReplicaConfig>(payload));
  }  // destroy again
}

TEST_F(CoordinatorLogStoreTests, TestMultipleInstancesSerialization) {
  // Define three instances
  CoordinatorToReplicaConfig config1{.instance_name = "instance1",
                                     .mgt_server = Endpoint{"127.0.0.1", 10112},
                                     .replication_client_info = {.instance_name = "instance_name1",
                                                                 .replication_mode = ReplicationMode::ASYNC,
                                                                 .replication_server = Endpoint{"127.0.0.1", 10001}},
                                     .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                     .instance_down_timeout_sec = std::chrono::seconds{5},
                                     .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                     .ssl = std::nullopt};

  CoordinatorToReplicaConfig config2{.instance_name = "instance2",
                                     .mgt_server = Endpoint{"127.0.0.1", 10113},
                                     .replication_client_info = {.instance_name = "instance_name2",
                                                                 .replication_mode = ReplicationMode::ASYNC,
                                                                 .replication_server = Endpoint{"127.0.0.1", 10002}},
                                     .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                     .instance_down_timeout_sec = std::chrono::seconds{5},
                                     .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                     .ssl = std::nullopt};

  CoordinatorToReplicaConfig config3{.instance_name = "instance3",
                                     .mgt_server = Endpoint{"127.0.0.1", 10114},
                                     .replication_client_info = {.instance_name = "instance_name3",
                                                                 .replication_mode = ReplicationMode::ASYNC,
                                                                 .replication_server = Endpoint{"127.0.0.1", 10003}},
                                     .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                     .instance_down_timeout_sec = std::chrono::seconds{5},
                                     .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                     .ssl = std::nullopt};

  // Create log store
  {
    CoordinatorLogStore log_store{test_folder_ / "TestMultipleInstancesSerialization"};
    ASSERT_EQ(log_store.next_slot(), 1);
    ASSERT_EQ(log_store.start_index(), 1);

    // Add logs for each instance
    auto register_instance_log_entry1 = cs_new<log_entry>(
        1, CoordinatorStateMachine::SerializeRegisterInstance(config1), nuraft::log_val_type::app_log);
    log_store.append(register_instance_log_entry1);

    auto register_instance_log_entry2 = cs_new<log_entry>(
        2, CoordinatorStateMachine::SerializeRegisterInstance(config2), nuraft::log_val_type::app_log);
    log_store.append(register_instance_log_entry2);

    auto register_instance_log_entry3 = cs_new<log_entry>(
        3, CoordinatorStateMachine::SerializeRegisterInstance(config3), nuraft::log_val_type::app_log);
    log_store.append(register_instance_log_entry3);

    // Add more logs to the log store
    auto set_instance_as_main_log_entry = cs_new<log_entry>(
        4,
        CoordinatorStateMachine::SerializeSetInstanceAsMain(memgraph::coordination::InstanceUUIDUpdate{
            .instance_name = config1.instance_name, .uuid = memgraph::utils::UUID{}}),
        nuraft::log_val_type::app_log);
    log_store.append(set_instance_as_main_log_entry);

    auto unregister_instance_log_entry = cs_new<log_entry>(
        5, CoordinatorStateMachine::SerializeUnregisterInstance(config2.instance_name), nuraft::log_val_type::app_log);
    log_store.append(unregister_instance_log_entry);

    // Check the log store
    ASSERT_EQ(log_store.next_slot(), 6);
    ASSERT_EQ(log_store.start_index(), 1);
  }

  CoordinatorLogStore log_store{test_folder_ / "TestMultipleInstancesSerialization"};
  ASSERT_EQ(log_store.next_slot(), 6);
  ASSERT_EQ(log_store.start_index(), 1);

  auto const get_config = [&](int const &instance_id) {
    switch (instance_id) {
      case 1:
        return config1;
      case 2:
        return config2;
      case 3:
        return config3;
      default:
        throw std::runtime_error("No instance with given id");
    };
  };

  // Check the contents of the logs
  auto const log_entries = log_store.log_entries(1, log_store.next_slot());
  for (auto &entry : *log_entries) {
    auto [payload, action] = CoordinatorStateMachine::DecodeLog(entry->get_buf());

    auto const term = entry->get_term();
    switch (entry->get_term()) {
      case 1:
        [[fallthrough]];
      case 2:
        [[fallthrough]];
      case 3: {
        ASSERT_EQ(action, RaftLogAction::REGISTER_REPLICATION_INSTANCE);
        ASSERT_EQ(get_config(term), std::get<CoordinatorToReplicaConfig>(payload));
        break;
      }
      case 4: {
        ASSERT_EQ(action, RaftLogAction::SET_INSTANCE_AS_MAIN);
        auto instance_uuid_update = std::get<memgraph::coordination::InstanceUUIDUpdate>(payload);
        ASSERT_EQ(instance_uuid_update.instance_name, "instance1");
        break;
      }
      case 5: {
        ASSERT_EQ(action, RaftLogAction::UNREGISTER_REPLICATION_INSTANCE);
        auto instance_name = std::get<std::string>(payload);
        ASSERT_EQ(instance_name, "instance2");
        break;
      }
      default:
        FAIL() << "Unexpected log entry";
    }
  }
}
