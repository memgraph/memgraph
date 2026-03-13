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

// Helper: write a log entry directly to KVStore in the old format (pre-fix).
// Old code stored conf entries with data="" (empty string).
namespace {
void WriteOldFormatLogEntry(memgraph::kvstore::KVStore &kv, uint64_t id, int term, std::string const &data,
                            nuraft::log_val_type val_type) {
  auto const key = fmt::format("log_entry_{}", id);
  auto const j = nlohmann::json{{"term", term}, {"data", data}, {"val_type", static_cast<int>(val_type)}};
  kv.Put(key, j.dump());
}

void WriteLogStoreMetadata(memgraph::kvstore::KVStore &kv, uint64_t start_idx, uint64_t last_log_entry) {
  std::map<std::string, std::string> batch;
  batch.emplace("start_idx", std::to_string(start_idx));
  batch.emplace("last_log_entry", std::to_string(last_log_entry));
  kv.PutMultiple(batch);
}

auto MakeAppLogBuffer(std::string const &payload) -> std::shared_ptr<buffer> {
  auto buf = buffer::alloc(sizeof(uint32_t) + payload.size());
  buffer_serializer bs{buf};
  bs.put_str(payload);
  return buf;
}
}  // namespace

// Simulates old durability where conf entries were stored with empty data.
// No snapshot exists — the log store must recover purely from logs.
// Empty conf entries should be skipped; app_log entries should be loaded correctly.
TEST_F(CoordinatorLogStoreTests, TestOldEmptyConfLogsNoSnapshot) {
  auto const path = test_folder_ / "TestOldEmptyConfLogsNoSnapshot";

  // Phase 1: Manually write old-format durability data directly to KVStore.
  // Simulates a coordinator that ran the old code and stored conf entries as empty strings.
  // Layout: [1: app_log] [2: conf (empty)] [3: app_log] [4: conf (empty)] [5: app_log]
  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    WriteLogStoreMetadata(*kv, /*start_idx=*/1, /*last_log_entry=*/5);

    // Serialize a real app_log payload (just a string wrapped in a buffer)
    WriteOldFormatLogEntry(*kv, 1, /*term=*/1, "app_payload_1", nuraft::log_val_type::app_log);
    WriteOldFormatLogEntry(*kv, 2, /*term=*/1, "", nuraft::log_val_type::conf);
    WriteOldFormatLogEntry(*kv, 3, /*term=*/1, "app_payload_3", nuraft::log_val_type::app_log);
    WriteOldFormatLogEntry(*kv, 4, /*term=*/2, "", nuraft::log_val_type::conf);
    WriteOldFormatLogEntry(*kv, 5, /*term=*/2, "app_payload_5", nuraft::log_val_type::app_log);
  }

  // Phase 2: Open the log store with the new code. Empty conf entries should be skipped.
  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    memgraph::coordination::LogStoreDurability durability{kv};
    CoordinatorLogStore log_store{CoordinatorLogStoreTests::GetLogger(), durability};

    // start_index should still be 1 (the metadata says so)
    ASSERT_EQ(log_store.start_index(), 1);

    // App log entries at indices 1, 3, 5 should be loaded.
    // Empty conf entries at indices 2 and 4 are skipped.
    // next_slot = start_idx(1) + logs_.size(3) = 4, NOT 6.
    // This is a known behavioral difference vs the old code (which would have next_slot=6
    // because it loaded empty conf entries into logs_). However, this is acceptable because
    // NuRaft will re-send missing logs or the snapshot will cover them.

    // Verify app_log entries are accessible and correct
    auto entry1 = log_store.entry_at(1);
    ASSERT_NE(entry1, nullptr);
    ASSERT_EQ(entry1->get_val_type(), nuraft::log_val_type::app_log);
    ASSERT_EQ(entry1->get_term(), 1);
    {
      buffer_serializer bs(entry1->get_buf());
      EXPECT_EQ(bs.get_str(), "app_payload_1");
    }

    auto entry3 = log_store.entry_at(3);
    ASSERT_NE(entry3, nullptr);
    ASSERT_EQ(entry3->get_val_type(), nuraft::log_val_type::app_log);
    {
      buffer_serializer bs(entry3->get_buf());
      EXPECT_EQ(bs.get_str(), "app_payload_3");
    }

    auto entry5 = log_store.entry_at(5);
    ASSERT_NE(entry5, nullptr);
    ASSERT_EQ(entry5->get_val_type(), nuraft::log_val_type::app_log);
    ASSERT_EQ(entry5->get_term(), 2);
    {
      buffer_serializer bs(entry5->get_buf());
      EXPECT_EQ(bs.get_str(), "app_payload_5");
    }

    // Conf entries at indices 2 and 4 were skipped — entry_at returns default (term=0)
    auto entry2 = log_store.entry_at(2);
    EXPECT_EQ(entry2->get_term(), 0) << "Empty conf entry should have been skipped";

    auto entry4 = log_store.entry_at(4);
    EXPECT_EQ(entry4->get_term(), 0) << "Empty conf entry should have been skipped";
  }
}

// Simulates old durability with empty conf entries, but compaction has already removed
// old entries (as would happen after a snapshot). Only entries after the compaction point remain.
// This tests that the log store works correctly when old empty conf entries have been compacted away.
TEST_F(CoordinatorLogStoreTests, TestOldEmptyConfLogsWithCompaction) {
  auto const path = test_folder_ / "TestOldEmptyConfLogsWithCompaction";

  // Phase 1: Write old-format data, simulating a state where compaction already ran.
  // Original entries were: [1: app] [2: conf(empty)] [3: app] [4: conf(empty)] [5: app]
  // After compact(3): start_idx=4, entries 1-3 removed. Remaining: [4: conf(empty)] [5: app]
  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    WriteLogStoreMetadata(*kv, /*start_idx=*/4, /*last_log_entry=*/5);

    WriteOldFormatLogEntry(*kv, 4, /*term=*/2, "", nuraft::log_val_type::conf);
    WriteOldFormatLogEntry(*kv, 5, /*term=*/2, "app_payload_5", nuraft::log_val_type::app_log);
  }

  // Phase 2: Open with new code.
  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    memgraph::coordination::LogStoreDurability durability{kv};
    CoordinatorLogStore log_store{CoordinatorLogStoreTests::GetLogger(), durability};

    ASSERT_EQ(log_store.start_index(), 4);

    // Entry at index 5 should be loaded
    auto entry5 = log_store.entry_at(5);
    ASSERT_NE(entry5, nullptr);
    ASSERT_EQ(entry5->get_val_type(), nuraft::log_val_type::app_log);
    ASSERT_EQ(entry5->get_term(), 2);
    {
      buffer_serializer bs(entry5->get_buf());
      EXPECT_EQ(bs.get_str(), "app_payload_5");
    }

    // Entry at index 4 (empty conf) should have been skipped
    auto entry4 = log_store.entry_at(4);
    EXPECT_EQ(entry4->get_term(), 0) << "Empty conf entry should have been skipped";
  }
}

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

// Tests mixed scenario: app_log entries interspersed with both old-format (empty) and
// new-format (JSON) conf entries. Simulates an upgrade scenario where some conf entries
// were written by old code and some by new code.
TEST_F(CoordinatorLogStoreTests, TestMixedOldAndNewConfEntries) {
  auto const path = test_folder_ / "TestMixedOldAndNewConfEntries";

  // Build a JSON-serialized conf entry the way the new StoreEntryToDisk does
  auto new_config = nuraft::cs_new<nuraft::cluster_config>(50, 49);
  new_config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(1, 0, "127.0.0.1:12000", "", false));
  new_config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(2, 0, "127.0.0.1:12001", "", false));
  nlohmann::json conf_json;
  memgraph::coordination::to_json(conf_json, *new_config);
  auto const new_conf_data = conf_json.dump();

  // Phase 1: Write mixed entries directly to KVStore.
  // [1: app] [2: conf (old/empty)] [3: app] [4: conf (new/JSON)] [5: app]
  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    WriteLogStoreMetadata(*kv, 1, 5);

    WriteOldFormatLogEntry(*kv, 1, 1, "payload_1", nuraft::log_val_type::app_log);
    WriteOldFormatLogEntry(*kv, 2, 1, "", nuraft::log_val_type::conf);  // old format
    WriteOldFormatLogEntry(*kv, 3, 1, "payload_3", nuraft::log_val_type::app_log);
    WriteOldFormatLogEntry(*kv, 4, 2, new_conf_data, nuraft::log_val_type::conf);  // new format
    WriteOldFormatLogEntry(*kv, 5, 2, "payload_5", nuraft::log_val_type::app_log);
  }

  // Phase 2: Load with new code.
  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    memgraph::coordination::LogStoreDurability durability{kv};
    CoordinatorLogStore log_store{CoordinatorLogStoreTests::GetLogger(), durability};

    ASSERT_EQ(log_store.start_index(), 1);

    // App entries at 1, 3, 5 should be loaded
    for (auto const &[idx, expected_payload] :
         std::vector<std::pair<uint64_t, std::string>>{{1, "payload_1"}, {3, "payload_3"}, {5, "payload_5"}}) {
      auto entry = log_store.entry_at(idx);
      ASSERT_NE(entry, nullptr);
      ASSERT_EQ(entry->get_val_type(), nuraft::log_val_type::app_log) << "Index " << idx;
      buffer_serializer bs(entry->get_buf());
      EXPECT_EQ(bs.get_str(), expected_payload) << "Index " << idx;
    }

    // Old empty conf at index 2 should be skipped
    auto entry2 = log_store.entry_at(2);
    EXPECT_EQ(entry2->get_term(), 0) << "Old empty conf entry at index 2 should have been skipped";

    // New JSON conf at index 4 should be loaded and deserializable
    auto entry4 = log_store.entry_at(4);
    ASSERT_NE(entry4, nullptr);
    ASSERT_EQ(entry4->get_val_type(), nuraft::log_val_type::conf);
    ASSERT_EQ(entry4->get_term(), 2);

    auto deserialized = nuraft::cluster_config::deserialize(entry4->get_buf());
    ASSERT_NE(deserialized, nullptr);
    EXPECT_EQ(deserialized->get_log_idx(), 50);
    EXPECT_EQ(deserialized->get_prev_log_idx(), 49);
    EXPECT_EQ(deserialized->get_servers().size(), 2);
  }
}

// Verifies that GetAllEntriesRange (used during log replay in raft_state.cpp) correctly
// skips old empty conf entries. This is the actual code path that replays logs after
// a snapshot during coordinator startup.
TEST_F(CoordinatorLogStoreTests, TestGetAllEntriesRangeSkipsOldEmptyConf) {
  auto const path = test_folder_ / "TestGetAllEntriesRangeSkipsOldEmptyConf";

  // Write old-format data: [1: app] [2: conf(empty)] [3: app] [4: app]
  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    WriteLogStoreMetadata(*kv, 1, 4);

    WriteOldFormatLogEntry(*kv, 1, 1, "payload_1", nuraft::log_val_type::app_log);
    WriteOldFormatLogEntry(*kv, 2, 1, "", nuraft::log_val_type::conf);
    WriteOldFormatLogEntry(*kv, 3, 2, "payload_3", nuraft::log_val_type::app_log);
    WriteOldFormatLogEntry(*kv, 4, 2, "payload_4", nuraft::log_val_type::app_log);
  }

  auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
  memgraph::coordination::LogStoreDurability durability{kv};
  CoordinatorLogStore log_store{CoordinatorLogStoreTests::GetLogger(), durability};

  // GetAllEntriesRange is called from raft_state.cpp to replay logs between
  // last_commit_index_snapshot and last_committed_index_state_machine.
  // Simulate replaying entries from index 1 to 5 (exclusive end).
  auto entries = log_store.GetAllEntriesRange(1, 5);

  // Only entries that were loaded into logs_ should appear.
  // Index 2 (empty conf) was skipped, so we should get entries at 1, 3, 4.
  ASSERT_EQ(entries.size(), 3);

  EXPECT_EQ(entries[0].first, 1);
  EXPECT_EQ(entries[0].second->get_val_type(), nuraft::log_val_type::app_log);

  EXPECT_EQ(entries[1].first, 3);
  EXPECT_EQ(entries[1].second->get_val_type(), nuraft::log_val_type::app_log);

  EXPECT_EQ(entries[2].first, 4);
  EXPECT_EQ(entries[2].second->get_val_type(), nuraft::log_val_type::app_log);
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

// Tests that only empty conf entries are present (no app_log entries at all) in old format.
// This could happen if a coordinator stored only configuration changes before crashing.
TEST_F(CoordinatorLogStoreTests, TestOnlyOldEmptyConfEntries) {
  auto const path = test_folder_ / "TestOnlyOldEmptyConfEntries";

  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    WriteLogStoreMetadata(*kv, 1, 3);

    WriteOldFormatLogEntry(*kv, 1, 1, "", nuraft::log_val_type::conf);
    WriteOldFormatLogEntry(*kv, 2, 1, "", nuraft::log_val_type::conf);
    WriteOldFormatLogEntry(*kv, 3, 2, "", nuraft::log_val_type::conf);
  }

  {
    auto kv = std::make_shared<memgraph::kvstore::KVStore>(path);
    memgraph::coordination::LogStoreDurability durability{kv};
    CoordinatorLogStore log_store{CoordinatorLogStoreTests::GetLogger(), durability};

    ASSERT_EQ(log_store.start_index(), 1);

    // All entries should be skipped — logs_ is empty
    // next_slot = start_idx(1) + logs_.size(0) = 1
    EXPECT_EQ(log_store.next_slot(), 1);

    // All entries should return default (term=0)
    for (uint64_t i = 1; i <= 3; ++i) {
      auto entry = log_store.entry_at(i);
      EXPECT_EQ(entry->get_term(), 0) << "All empty conf entries should have been skipped, index " << i;
    }

    // GetAllEntriesRange should return nothing
    auto entries = log_store.GetAllEntriesRange(1, 4);
    EXPECT_TRUE(entries.empty());
  }
}
