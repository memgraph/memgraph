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

#include "io/network/endpoint.hpp"
#include "kvstore/kvstore.hpp"
#include "replication_coordination_glue/mode.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/uuid.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

import memgraph.coordination.coordinator_communication_config;
import memgraph.coordination.coordinator_log_store;
import memgraph.coordination.coordinator_state_machine;
import memgraph.coordination.logger;

using memgraph::coordination::CoordinatorClusterStateDelta;
using memgraph::coordination::CoordinatorInstanceContext;
using memgraph::coordination::CoordinatorLogStore;
using memgraph::coordination::CoordinatorStateMachine;
using memgraph::coordination::DataInstanceConfig;
using memgraph::coordination::DataInstanceContext;
using memgraph::coordination::ReplicationClientInfo;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::replication_coordination_glue::ReplicationRole;
using memgraph::utils::UUID;

using nuraft::buffer;
using nuraft::buffer_serializer;
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
  DataInstanceConfig config{.instance_name = "instance3",
                            .mgt_server = Endpoint{"127.0.0.1", 10112},
                            .replication_client_info = {.instance_name = "instance_name",
                                                        .replication_mode = ReplicationMode::ASYNC,
                                                        .replication_server = Endpoint{"127.0.0.1", 10001}}};

  auto data_instances = std::vector<DataInstanceContext>();
  data_instances.emplace_back(config, ReplicationRole::REPLICA, UUID{});

  std::vector<CoordinatorInstanceContext> coord_instances{
      CoordinatorInstanceContext{.id = 1, .bolt_server = "127.0.0.1:7690"},
      CoordinatorInstanceContext{.id = 2, .bolt_server = "127.0.0.1:7691"},
  };

  // NOLINTNEXTLINE
  CoordinatorClusterStateDelta const delta_state{.data_instances_ = data_instances,
                                                 .coordinator_instances_ = coord_instances,
                                                 .current_main_uuid_ = UUID{},
                                                 .enabled_reads_on_main_ = false};
  auto buffer = CoordinatorStateMachine::SerializeUpdateClusterState(delta_state);

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

    CoordinatorStateMachine::DecodeLog(entry->get_buf());

    ASSERT_EQ(log_store.next_slot(), 2);
    ASSERT_EQ(log_store.start_index(), 1);
  }  // destroy again
}

TEST_F(CoordinatorLogStoreTests, TestMultipleInstancesSerialization) {
  // Define three instances
  DataInstanceConfig config1{.instance_name = "instance1",
                             .mgt_server = Endpoint{"127.0.0.1", 10112},
                             .replication_client_info = {.instance_name = "instance_name1",
                                                         .replication_mode = ReplicationMode::ASYNC,
                                                         .replication_server = Endpoint{"127.0.0.1", 10001}}};

  DataInstanceConfig config2{.instance_name = "instance2",
                             .mgt_server = Endpoint{"127.0.0.1", 10113},
                             .replication_client_info = {.instance_name = "instance_name2",
                                                         .replication_mode = ReplicationMode::ASYNC,
                                                         .replication_server = Endpoint{"127.0.0.1", 10002}}};

  DataInstanceConfig config3{.instance_name = "instance3",
                             .mgt_server = Endpoint{"127.0.0.1", 10114},
                             .replication_client_info = {.instance_name = "instance_name3",
                                                         .replication_mode = ReplicationMode::ASYNC,
                                                         .replication_server = Endpoint{"127.0.0.1", 10003}}};

  // Create log store
  {
    auto log_store_storage =
        std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestMultipleInstancesSerialization");

    memgraph::coordination::LogStoreDurability log_store_durability{log_store_storage};
    CoordinatorLogStore log_store{GetLogger(), log_store_durability};
    ASSERT_EQ(log_store.next_slot(), 1);
    ASSERT_EQ(log_store.start_index(), 1);

    auto data_instances = std::vector<DataInstanceContext>();
    data_instances.emplace_back(config1, ReplicationRole::REPLICA, UUID{});
    data_instances.emplace_back(config2, ReplicationRole::REPLICA, UUID{});
    data_instances.emplace_back(config3, ReplicationRole::REPLICA, UUID{});

    std::vector<CoordinatorInstanceContext> coord_instances{
        CoordinatorInstanceContext{.id = 1, .bolt_server = "127.0.0.1:7690"},
        CoordinatorInstanceContext{.id = 2, .bolt_server = "127.0.0.1:7691"},
    };

    // NOLINTNEXTLINE
    CoordinatorClusterStateDelta const delta_state{.data_instances_ = data_instances,
                                                   .coordinator_instances_ = coord_instances,
                                                   .current_main_uuid_ = UUID{},
                                                   .enabled_reads_on_main_ = false};

    auto log_entry_update = cs_new<log_entry>(1, CoordinatorStateMachine::SerializeUpdateClusterState(delta_state),
                                              nuraft::log_val_type::app_log);

    log_store.append(log_entry_update);

    // Check the log store
    ASSERT_EQ(log_store.next_slot(), 2);
    ASSERT_EQ(log_store.start_index(), 1);
  }

  auto log_store_storage =
      std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestMultipleInstancesSerialization");

  memgraph::coordination::LogStoreDurability log_store_durability{log_store_storage};
  CoordinatorLogStore log_store{GetLogger(), log_store_durability};
  ASSERT_EQ(log_store.next_slot(), 2);
  ASSERT_EQ(log_store.start_index(), 1);

  // Check the contents of the logs
  auto const log_entries = log_store.log_entries(1, log_store.next_slot());
  ASSERT_EQ(log_entries->size(), 1);
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

    std::vector<DataInstanceContext> data_instances{};

    auto config = DataInstanceConfig{.instance_name = "instance1",
                                     .mgt_server = Endpoint{"127.0.0.1", 10112},
                                     .replication_client_info = {.instance_name = "instance_name1",
                                                                 .replication_mode = ReplicationMode::ASYNC,
                                                                 .replication_server = Endpoint{"127.0.0.1", 10001}}};
    data_instances.emplace_back(config, ReplicationRole::REPLICA, UUID{});

    std::vector<CoordinatorInstanceContext> coord_instances{
        CoordinatorInstanceContext{.id = 1, .bolt_server = "127.0.0.1:7690"},
        CoordinatorInstanceContext{.id = 2, .bolt_server = "127.0.0.1:7691"},
    };

    // NOLINTNEXTLINE
    CoordinatorClusterStateDelta const delta_state{.data_instances_ = data_instances,
                                                   .coordinator_instances_ = coord_instances,
                                                   .current_main_uuid_ = UUID{},
                                                   .enabled_reads_on_main_ = false};

    auto buffer = CoordinatorStateMachine::SerializeUpdateClusterState(delta_state);

    auto log_entry_common = cs_new<log_entry>(1, buffer, nuraft::log_val_type::app_log);
    log_store1.append(log_entry_common);
    log_store2.append(log_entry_common);

    // Add different logs to each store between indices 2 and 4
    // Add different logs to each store between indices 2 and 4
    for (uint16_t i = 2; i <= 4; ++i) {
      auto config1 = DataInstanceConfig{
          .instance_name = "instance" + std::to_string(i),
          .mgt_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10112 + i)},
          .replication_client_info = {.instance_name = "instance_name" + std::to_string(i),
                                      .replication_mode = ReplicationMode::ASYNC,
                                      .replication_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10001 + i)}}};

      auto config2 =
          DataInstanceConfig{.instance_name = "instance" + std::to_string(i + 3),
                             .mgt_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10112 + i + 3)},
                             .replication_client_info = {
                                 .instance_name = "instance_name" + std::to_string(i + 3),
                                 .replication_mode = ReplicationMode::ASYNC,
                                 .replication_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10001 + i + 3)}}};

      data_instances.emplace_back(config1, ReplicationRole::REPLICA, UUID{});

      // NOLINTNEXTLINE
      CoordinatorClusterStateDelta const delta_state{.data_instances_ = data_instances,
                                                     .coordinator_instances_ = coord_instances,
                                                     .current_main_uuid_ = UUID{},
                                                     .enabled_reads_on_main_ = false};

      auto buffer1 = CoordinatorStateMachine::SerializeUpdateClusterState(delta_state);
      data_instances.emplace_back(config2, ReplicationRole::REPLICA, UUID{});
      // NOLINTNEXTLINE
      CoordinatorClusterStateDelta const delta_state2{.data_instances_ = data_instances};
      auto buffer2 = CoordinatorStateMachine::SerializeUpdateClusterState(delta_state2);

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

      CoordinatorStateMachine::DecodeLog(entry1->get_buf());
      CoordinatorStateMachine::DecodeLog(entry2->get_buf());

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

      CoordinatorStateMachine::DecodeLog(entry1->get_buf());
      CoordinatorStateMachine::DecodeLog(entry2->get_buf());

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

  std::vector<DataInstanceContext> data_instances{};

  // Add 5 logs to the store
  for (int i = 1; i <= 5; ++i) {
    auto config = DataInstanceConfig{
        .instance_name = "instance" + std::to_string(i),
        .mgt_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10112 + i)},
        .replication_client_info = {.instance_name = "instance_name" + std::to_string(i),
                                    .replication_mode = ReplicationMode::ASYNC,
                                    .replication_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10001 + i)}}};
    data_instances.emplace_back(config, ReplicationRole::REPLICA, UUID{});

    auto coord_instances = std::vector<CoordinatorInstanceContext>();

    // NOLINTNEXTLINE
    CoordinatorClusterStateDelta const delta_state{.data_instances_ = data_instances,
                                                   .coordinator_instances_ = coord_instances,
                                                   .current_main_uuid_ = UUID{},
                                                   .enabled_reads_on_main_ = false};
    auto buffer = CoordinatorStateMachine::SerializeUpdateClusterState(delta_state);

    auto log_entry_obj = cs_new<log_entry>(i, buffer, nuraft::log_val_type::app_log);
    log_store.append(log_entry_obj);
  }

  // Compact up to the log with index 3
  log_store.compact(3);

  // Check that only logs from 4 to 5 remain
  for (int i = 4; i <= 5; ++i) {
    auto entry = log_store.entry_at(i);
    ASSERT_TRUE(entry != nullptr);
    CoordinatorStateMachine::DecodeLog(entry->get_buf());
  }

  // Check that logs from 1 to 3 do not exist
  for (int i = 1; i <= 3; ++i) {
    auto entry = log_store.entry_at(i);
    ASSERT_EQ(entry->get_term(), 0);
  }
}

TEST_F(CoordinatorLogStoreTests, TestConf) {
  // Define a CoordinatorLogStore instance
  auto log_store_storage = std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestConf");
  memgraph::coordination::LogStoreDurability log_store_durability{log_store_storage};
  CoordinatorLogStore log_store{GetLogger(), log_store_durability};

  // conf log gets serialized as empty string, check that on restart reading from disk works correctly
  std::string data = "anything";
  auto log_term_buffer = buffer::alloc(sizeof(uint32_t) + data.size());
  buffer_serializer bs{log_term_buffer};
  bs.put_str(data);
  auto log_entry_obj = cs_new<log_entry>(1, log_term_buffer, nuraft::log_val_type::conf);
  log_store.append(log_entry_obj);

  CoordinatorLogStore log_store2{GetLogger(), log_store_durability};
  auto entry = log_store2.entry_at(1);
}
