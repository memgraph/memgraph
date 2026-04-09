// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "coordination/coordinator_log_store.hpp"
#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_state_machine.hpp"
#include "coordination/coordinator_state_manager.hpp"
#include "io/network/endpoint.hpp"
#include "kvstore/kvstore.hpp"
#include "utils/uuid.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

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
                            .mgt_server = Endpoint{"127.0.0.1", 10'112},
                            .replication_client_info = {.instance_name = "instance_name",
                                                        .replication_mode = ReplicationMode::ASYNC,
                                                        .replication_server = Endpoint{"127.0.0.1", 10'001}}};

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
                             .mgt_server = Endpoint{"127.0.0.1", 10'112},
                             .replication_client_info = {.instance_name = "instance_name1",
                                                         .replication_mode = ReplicationMode::ASYNC,
                                                         .replication_server = Endpoint{"127.0.0.1", 10'001}}};

  DataInstanceConfig config2{.instance_name = "instance2",
                             .mgt_server = Endpoint{"127.0.0.1", 10'113},
                             .replication_client_info = {.instance_name = "instance_name2",
                                                         .replication_mode = ReplicationMode::ASYNC,
                                                         .replication_server = Endpoint{"127.0.0.1", 10'002}}};

  DataInstanceConfig config3{.instance_name = "instance3",
                             .mgt_server = Endpoint{"127.0.0.1", 10'114},
                             .replication_client_info = {.instance_name = "instance_name3",
                                                         .replication_mode = ReplicationMode::ASYNC,
                                                         .replication_server = Endpoint{"127.0.0.1", 10'003}}};

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

    auto log_entry_update = cs_new<log_entry>(
        1, CoordinatorStateMachine::SerializeUpdateClusterState(delta_state), nuraft::log_val_type::app_log);

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
                                     .mgt_server = Endpoint{"127.0.0.1", 10'112},
                                     .replication_client_info = {.instance_name = "instance_name1",
                                                                 .replication_mode = ReplicationMode::ASYNC,
                                                                 .replication_server = Endpoint{"127.0.0.1", 10'001}}};
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
          .mgt_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10'112 + i)},
          .replication_client_info = {.instance_name = "instance_name" + std::to_string(i),
                                      .replication_mode = ReplicationMode::ASYNC,
                                      .replication_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10'001 + i)}}};

      auto config2 =
          DataInstanceConfig{.instance_name = "instance" + std::to_string(i + 3),
                             .mgt_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10'112 + i + 3)},
                             .replication_client_info = {
                                 .instance_name = "instance_name" + std::to_string(i + 3),
                                 .replication_mode = ReplicationMode::ASYNC,
                                 .replication_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10'001 + i + 3)}}};

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
        .mgt_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10'112 + i)},
        .replication_client_info = {.instance_name = "instance_name" + std::to_string(i),
                                    .replication_mode = ReplicationMode::ASYNC,
                                    .replication_server = Endpoint{"127.0.0.1", static_cast<uint16_t>(10'001 + i)}}};
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

  // Create a real cluster_config — conf entries always carry serialized cluster configs
  auto config = nuraft::cs_new<nuraft::cluster_config>(10, 9);
  config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(1, 0, "127.0.0.1:12000", "", false, 1));
  auto conf_buf = config->serialize();

  auto log_entry_obj = cs_new<log_entry>(1, conf_buf, nuraft::log_val_type::conf);
  log_store.append(log_entry_obj);

  CoordinatorLogStore log_store2{GetLogger(), log_store_durability};
  auto entry = log_store2.entry_at(1);
  ASSERT_EQ(entry->get_val_type(), nuraft::log_val_type::conf);
}

// Reproduces the crash-loop bug: config entries have their NuRaft buffer discarded
// during StoreEntryToDisk (stored as empty string). On restart, the reloaded buffer
// is only 4 bytes. When NuRaft's become_leader() calls cluster_config::deserialize()
// on it, it throws "not enough space" — crashing the coordinator in a loop.
TEST_F(CoordinatorLogStoreTests, TestConfEntryBufferPreservedAfterRestart) {
  // Build a realistic cluster_config with 3 servers (like a 3-coordinator HA cluster)
  auto config = nuraft::cs_new<nuraft::cluster_config>(100 /*log_idx*/, 99 /*prev_log_idx*/);
  config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(1, 0, "127.0.0.1:12000", "", false, 1));
  config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(2, 0, "127.0.0.1:12001", "", false, 1));
  config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(3, 0, "127.0.0.1:12002", "", false, 1));

  // Serialize the config — this is exactly what NuRaft does before appending a conf entry
  auto original_buf = config->serialize();
  ASSERT_GT(original_buf->size(), 4) << "Serialized cluster_config must be larger than 4 bytes";

  // Store the conf entry, then destroy the log store (simulating process shutdown)
  {
    auto log_store_storage = std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestConfBufferPreserved");
    memgraph::coordination::LogStoreDurability log_store_durability{.durability_store_ = log_store_storage};
    CoordinatorLogStore log_store{GetLogger(), log_store_durability};

    auto conf_entry = nuraft::cs_new<log_entry>(1, original_buf, nuraft::log_val_type::conf);
    log_store.append(conf_entry);

    ASSERT_EQ(log_store.next_slot(), 2);
  }

  // Recreate the log store from disk (simulating coordinator restart)
  {
    auto log_store_storage = std::make_shared<memgraph::kvstore::KVStore>(test_folder_ / "TestConfBufferPreserved");
    memgraph::coordination::LogStoreDurability log_store_durability{log_store_storage};
    CoordinatorLogStore log_store{GetLogger(), log_store_durability};

    ASSERT_EQ(log_store.next_slot(), 2);

    auto reloaded_entry = log_store.entry_at(1);
    ASSERT_NE(reloaded_entry, nullptr);
    ASSERT_EQ(reloaded_entry->get_val_type(), nuraft::log_val_type::conf);

    // This is the critical check: the reloaded buffer must be large enough for
    // cluster_config::deserialize(). With the bug, this is only 4 bytes (empty string
    // prefix) and the deserialize call throws std::overflow_error("not enough space").
    ASSERT_EQ(reloaded_entry->get_buf().size(), original_buf->size())
        << "Config entry buffer was truncated during persistence! "
           "Buffer size "
        << reloaded_entry->get_buf().size() << " bytes, expected " << original_buf->size() << " bytes";

    // This is exactly what NuRaft's become_leader() does with uncommitted config entries.
    // With the bug, this throws std::overflow_error and kills the ASIO worker thread.
    nuraft::ptr<nuraft::cluster_config> deserialized;
    EXPECT_NO_THROW(deserialized = nuraft::cluster_config::deserialize(reloaded_entry->get_buf()))
        << "cluster_config::deserialize() failed on reloaded conf entry — "
           "this is the crash-loop root cause";

    // Verify the deserialized config matches the original
    if (deserialized) {
      EXPECT_EQ(deserialized->get_log_idx(), 100);
      EXPECT_EQ(deserialized->get_prev_log_idx(), 99);
      EXPECT_EQ(deserialized->get_servers().size(), 3);
    }
  }
}

namespace {
auto MakeAppLogBuffer(std::string const &payload) -> std::shared_ptr<buffer> {
  auto buf = buffer::alloc(sizeof(uint32_t) + payload.size());
  buffer_serializer bs{buf};
  bs.put_str(payload);
  return buf;
}
}  // namespace

// Tests that new-format conf entries (JSON-serialized cluster_config) survive a full
// store → restart → reload → deserialize cycle, and that the data round-trips correctly.
TEST_F(CoordinatorLogStoreTests, TestNewFormatConfEntryRoundTrip) {
  auto const path = test_folder_ / "TestNewFormatConfEntryRoundTrip";

  auto config = nuraft::cs_new<nuraft::cluster_config>(42 /*log_idx*/, 41 /*prev_log_idx*/, true /*async_replication*/);
  config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(1, 0, "127.0.0.1:12000", "aux1", false, 1));
  config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(2, 0, "127.0.0.1:12001", "aux2", false, 1));
  config->set_user_ctx("test_user_ctx");

  auto original_buf = config->serialize();

  // Store using new code path (StoreEntryToDisk serializes conf as JSON)
  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    memgraph::coordination::LogStoreDurability durability{kv};
    CoordinatorLogStore log_store{CoordinatorLogStoreTests::GetLogger(), durability};

    auto conf_entry = nuraft::cs_new<log_entry>(3, original_buf, nuraft::log_val_type::conf);
    log_store.append(conf_entry);
    ASSERT_EQ(log_store.next_slot(), 2);
  }

  // Reload and verify full round-trip
  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    memgraph::coordination::LogStoreDurability durability{kv};
    CoordinatorLogStore log_store{CoordinatorLogStoreTests::GetLogger(), durability};

    ASSERT_EQ(log_store.next_slot(), 2);
    ASSERT_EQ(log_store.start_index(), 1);

    auto reloaded = log_store.entry_at(1);
    ASSERT_NE(reloaded, nullptr);
    ASSERT_EQ(reloaded->get_val_type(), nuraft::log_val_type::conf);
    ASSERT_EQ(reloaded->get_term(), 3);

    auto deserialized = nuraft::cluster_config::deserialize(reloaded->get_buf());
    ASSERT_NE(deserialized, nullptr);
    EXPECT_EQ(deserialized->get_log_idx(), 42);
    EXPECT_EQ(deserialized->get_prev_log_idx(), 41);
    EXPECT_TRUE(deserialized->is_async_replication());
    EXPECT_EQ(deserialized->get_user_ctx(), "test_user_ctx");
    EXPECT_EQ(deserialized->get_servers().size(), 2);

    auto it = deserialized->get_servers().begin();
    EXPECT_EQ((*it)->get_id(), 1);
    EXPECT_EQ((*it)->get_endpoint(), "127.0.0.1:12000");
    EXPECT_EQ((*it)->get_aux(), "aux1");
    ++it;
    EXPECT_EQ((*it)->get_id(), 2);
    EXPECT_EQ((*it)->get_endpoint(), "127.0.0.1:12001");
    EXPECT_EQ((*it)->get_aux(), "aux2");
  }
}

// Simulates the "snapshot exists" scenario: logs before the snapshot have been compacted,
// and the remaining logs (after the snapshot) contain a mix of app_log and new-format conf entries.
// This is the happy path after the fix — all conf entries are properly JSON-serialized.
TEST_F(CoordinatorLogStoreTests, TestLogsAfterSnapshotWithNewConfEntries) {
  auto const path = test_folder_ / "TestLogsAfterSnapshotWithNewConf";

  auto config = nuraft::cs_new<nuraft::cluster_config>(200, 199);
  config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(1, 0, "127.0.0.1:12000", "", false));
  config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(2, 0, "127.0.0.1:12001", "", false));
  config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(3, 0, "127.0.0.1:12002", "", false));

  // Store entries using the new code, then compact to simulate snapshot
  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    memgraph::coordination::LogStoreDurability durability{kv};
    CoordinatorLogStore log_store{CoordinatorLogStoreTests::GetLogger(), durability};

    // Append app_log at index 1
    auto app_buf = MakeAppLogBuffer("pre_snapshot_data");
    auto app_entry = nuraft::cs_new<log_entry>(1, app_buf, nuraft::log_val_type::app_log);
    log_store.append(app_entry);

    // Append conf at index 2
    auto conf_buf = config->serialize();
    auto conf_entry = nuraft::cs_new<log_entry>(2, conf_buf, nuraft::log_val_type::conf);
    log_store.append(conf_entry);

    // Append app_log at index 3
    auto app_buf2 = MakeAppLogBuffer("post_snapshot_data");
    auto app_entry2 = nuraft::cs_new<log_entry>(3, app_buf2, nuraft::log_val_type::app_log);
    log_store.append(app_entry2);

    ASSERT_EQ(log_store.next_slot(), 4);

    // Compact up to index 1 (simulate snapshot at index 1)
    log_store.compact(1);
    ASSERT_EQ(log_store.start_index(), 2);
  }

  // Restart: only entries 2 (conf) and 3 (app) should remain
  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    memgraph::coordination::LogStoreDurability durability{kv};
    CoordinatorLogStore log_store{CoordinatorLogStoreTests::GetLogger(), durability};

    ASSERT_EQ(log_store.start_index(), 2);
    ASSERT_EQ(log_store.next_slot(), 4);

    // Conf entry at index 2 should be properly deserialized
    auto entry2 = log_store.entry_at(2);
    ASSERT_EQ(entry2->get_val_type(), nuraft::log_val_type::conf);
    auto deserialized = nuraft::cluster_config::deserialize(entry2->get_buf());
    ASSERT_NE(deserialized, nullptr);
    EXPECT_EQ(deserialized->get_log_idx(), 200);
    EXPECT_EQ(deserialized->get_servers().size(), 3);

    // App entry at index 3 should also be correct
    auto entry3 = log_store.entry_at(3);
    ASSERT_EQ(entry3->get_val_type(), nuraft::log_val_type::app_log);
    buffer_serializer bs(entry3->get_buf());
    EXPECT_EQ(bs.get_str(), "post_snapshot_data");

    // GetAllEntriesRange should return both
    auto entries = log_store.GetAllEntriesRange(2, 4);
    ASSERT_EQ(entries.size(), 2);
    EXPECT_EQ(entries[0].first, 2);
    EXPECT_EQ(entries[0].second->get_val_type(), nuraft::log_val_type::conf);
    EXPECT_EQ(entries[1].first, 3);
    EXPECT_EQ(entries[1].second->get_val_type(), nuraft::log_val_type::app_log);
  }
}
