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
#include "kvstore/kvstore.hpp"
#include "nuraft/coordinator_state_machine.hpp"
#include "nuraft/raft_log_action.hpp"
#include "utils/uuid.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

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
 public:
  static auto GetLogger() -> memgraph::coordination::LoggerWrapper {
    static auto logger{memgraph::coordination::Logger("")};
    return memgraph::coordination::LoggerWrapper(&logger);
  }

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
    auto log_store_storage = std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestBasicSerialization");

    memgraph::coordination::LogStoreDurability log_store_durability{log_store_storage};

    CoordinatorLogStore log_store{GetLogger(), log_store_durability};
    ASSERT_EQ(log_store.next_slot(), 1);
    ASSERT_EQ(log_store.start_index(), 1);

    auto register_instance_log_entry = cs_new<log_entry>(1, buffer, nuraft::log_val_type::app_log);
    log_store.append(register_instance_log_entry);

    ASSERT_EQ(log_store.next_slot(), 2);
    ASSERT_EQ(log_store.start_index(), 1);
  }  // coordinator log store destroy

  // start again, read logs
  {
    auto log_store_storage = std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestBasicSerialization");

    memgraph::coordination::LogStoreDurability log_store_durability{log_store_storage};
    CoordinatorLogStore log_store{GetLogger(), log_store_durability};

    ASSERT_EQ(log_store.next_slot(), 2);
    ASSERT_EQ(log_store.start_index(), 1);

    auto entry = log_store.entry_at(1);

    auto const [payload, action] = CoordinatorStateMachine::DecodeLog(entry->get_buf());

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
    auto log_store_storage =
        std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestMultipleInstancesSerialization");

    memgraph::coordination::LogStoreDurability log_store_durability{log_store_storage};
    CoordinatorLogStore log_store{GetLogger(), log_store_durability};
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

  auto log_store_storage =
      std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestMultipleInstancesSerialization");

  memgraph::coordination::LogStoreDurability log_store_durability{log_store_storage};
  CoordinatorLogStore log_store{GetLogger(), log_store_durability};
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
    auto const [payload, action] = CoordinatorStateMachine::DecodeLog(entry->get_buf());

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

TEST_F(CoordinatorLogStoreTests, TestPackAndApplyPack) {
  {
    // Define two CoordinatorLogStore instances
    auto log_store_storage_1 = std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestPackAndApplyPack1");
    memgraph::coordination::LogStoreDurability log_store_durability_1{log_store_storage_1};
    CoordinatorLogStore log_store1{GetLogger(), log_store_durability_1};

    auto log_store_storage_2 = std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestPackAndApplyPack2");
    memgraph::coordination::LogStoreDurability log_store_durability_2{log_store_storage_2};
    CoordinatorLogStore log_store2{GetLogger(), log_store_durability_2};

    // Add the same log to both stores at index 1
    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(
        CoordinatorToReplicaConfig{.instance_name = "instance1",
                                   .mgt_server = Endpoint{"127.0.0.1", 10112},
                                   .replication_client_info = {.instance_name = "instance_name1",
                                                               .replication_mode = ReplicationMode::ASYNC,
                                                               .replication_server = Endpoint{"127.0.0.1", 10001}},
                                   .instance_health_check_frequency_sec = std::chrono::seconds{1},
                                   .instance_down_timeout_sec = std::chrono::seconds{5},
                                   .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                                   .ssl = std::nullopt});
    auto log_entry_common = cs_new<log_entry>(1, buffer, nuraft::log_val_type::app_log);
    log_store1.append(log_entry_common);
    log_store2.append(log_entry_common);

    // Add different logs to each store between indices 2 and 4
    // Add different logs to each store between indices 2 and 4
    for (uint16_t i = 2; i <= 4; ++i) {
      auto buffer1 = CoordinatorStateMachine::SerializeRegisterInstance(CoordinatorToReplicaConfig{
          .instance_name = "instance" + std::to_string(i),
          .mgt_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10112 + i)},
          .replication_client_info = {.instance_name = "instance_name" + std::to_string(i),
                                      .replication_mode = ReplicationMode::ASYNC,
                                      .replication_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10001 + i)}},
          .instance_health_check_frequency_sec = std::chrono::seconds{1},
          .instance_down_timeout_sec = std::chrono::seconds{5},
          .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
          .ssl = std::nullopt});

      auto buffer2 = CoordinatorStateMachine::SerializeRegisterInstance(CoordinatorToReplicaConfig{
          .instance_name = "instance" + std::to_string(i + 3),
          .mgt_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10112 + i + 3)},
          .replication_client_info = {.instance_name = "instance_name" + std::to_string(i + 3),
                                      .replication_mode = ReplicationMode::ASYNC,
                                      .replication_server =
                                          Endpoint{"127.0.0.1", static_cast<uint16_t>(10001 + i + 3)}},
          .instance_health_check_frequency_sec = std::chrono::seconds{1},
          .instance_down_timeout_sec = std::chrono::seconds{5},
          .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
          .ssl = std::nullopt});

      auto log_entry1 = cs_new<log_entry>(i, buffer1, nuraft::log_val_type::app_log);
      auto log_entry2 = cs_new<log_entry>(i, buffer2, nuraft::log_val_type::app_log);

      log_store1.append(log_entry1);
      log_store2.append(log_entry2);
    }

    // Use the pack method to get the logs from the first store
    auto packed_logs = log_store1.pack(2, 3);

    // Use the apply_pack method to apply these logs to the second store
    log_store2.apply_pack(2, *packed_logs);

    // Check that the logs in log_store2 are the same as those in log_store1
    for (int i = 1; i != log_store1.next_slot(); ++i) {
      auto entry1 = log_store1.entry_at(i);
      auto entry2 = log_store2.entry_at(i);

      auto const [payload1, action1] = CoordinatorStateMachine::DecodeLog(entry1->get_buf());
      auto const [payload2, action2] = CoordinatorStateMachine::DecodeLog(entry2->get_buf());

      ASSERT_EQ(std::get<memgraph::coordination::CoordinatorToReplicaConfig>(payload1),
                std::get<memgraph::coordination::CoordinatorToReplicaConfig>(payload2));
      ASSERT_EQ(entry1->get_term(), entry2->get_term());
      ASSERT_EQ(entry1->get_val_type(), entry2->get_val_type());
    }
  }

  // Check that the logs in log_store2 are the same as those in log_store1
  {
    auto log_store_storage_1 = std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestPackAndApplyPack1");
    memgraph::coordination::LogStoreDurability log_store_durability_1{log_store_storage_1};
    CoordinatorLogStore log_store1{GetLogger(), log_store_durability_1};

    auto log_store_storage_2 = std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestPackAndApplyPack2");
    memgraph::coordination::LogStoreDurability log_store_durability_2{log_store_storage_2};
    CoordinatorLogStore log_store2{GetLogger(), log_store_durability_2};
    for (int i = 1; i != log_store1.next_slot(); ++i) {
      auto entry1 = log_store1.entry_at(i);
      auto entry2 = log_store2.entry_at(i);

      auto const [payload1, action1] = CoordinatorStateMachine::DecodeLog(entry1->get_buf());
      auto const [payload2, action2] = CoordinatorStateMachine::DecodeLog(entry2->get_buf());

      ASSERT_EQ(std::get<memgraph::coordination::CoordinatorToReplicaConfig>(payload1),
                std::get<memgraph::coordination::CoordinatorToReplicaConfig>(payload2));
      ASSERT_EQ(entry1->get_term(), entry2->get_term());
      ASSERT_EQ(entry1->get_val_type(), entry2->get_val_type());
    }
  }
}

TEST_F(CoordinatorLogStoreTests, TestCompact) {
  // Define a CoordinatorLogStore instance
  auto log_store_storage = std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestCompact");
  memgraph::coordination::LogStoreDurability log_store_durability{log_store_storage};
  CoordinatorLogStore log_store{GetLogger(), log_store_durability};

  // Add 5 logs to the store
  for (int i = 1; i <= 5; ++i) {
    auto buffer = CoordinatorStateMachine::SerializeRegisterInstance(CoordinatorToReplicaConfig{
        .instance_name = "instance" + std::to_string(i),
        .mgt_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10112 + i)},
        .replication_client_info = {.instance_name = "instance_name" + std::to_string(i),
                                    .replication_mode = ReplicationMode::ASYNC,
                                    .replication_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10001 + i)}},
        .instance_health_check_frequency_sec = std::chrono::seconds{1},
        .instance_down_timeout_sec = std::chrono::seconds{5},
        .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
        .ssl = std::nullopt});
    auto log_entry_obj = cs_new<log_entry>(i, buffer, nuraft::log_val_type::app_log);
    log_store.append(log_entry_obj);
  }

  // Compact up to the log with index 3
  log_store.compact(3);

  // Check that only logs from 4 to 5 remain
  for (int i = 4; i <= 5; ++i) {
    auto entry = log_store.entry_at(i);
    ASSERT_TRUE(entry != nullptr);
    auto const [payload, action] = CoordinatorStateMachine::DecodeLog(entry->get_buf());
    ASSERT_EQ(std::get<CoordinatorToReplicaConfig>(payload).instance_name, "instance" + std::to_string(i));
  }

  // Check that logs from 1 to 3 do not exist
  for (int i = 1; i <= 3; ++i) {
    auto entry = log_store.entry_at(i);
    ASSERT_EQ(entry->get_term(), 0);  // TODO: test that entry->get_term returns nullptr
  }
}
