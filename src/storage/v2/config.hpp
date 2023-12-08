// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>

#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage_mode.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage {

/// Exception used to signal configuration errors.
class StorageConfigException : public utils::BasicException {
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(StorageConfigException)
};

/// Pass this class to the \ref Storage constructor to change the behavior of
/// the storage. This class also defines the default behavior.
struct Config {
  struct Gc {
    enum class Type { NONE, PERIODIC };

    Type type{Type::PERIODIC};
    std::chrono::milliseconds interval{std::chrono::milliseconds(1000)};
    friend bool operator==(const Gc &lrh, const Gc &rhs) = default;
  } gc;

  struct Items {
    bool properties_on_edges{true};
    bool enable_schema_metadata{false};
    friend bool operator==(const Items &lrh, const Items &rhs) = default;
  } items;

  struct Durability {
    enum class SnapshotWalMode { DISABLED, PERIODIC_SNAPSHOT, PERIODIC_SNAPSHOT_WITH_WAL };

    std::filesystem::path storage_directory{"storage"};

    bool recover_on_startup{false};

    SnapshotWalMode snapshot_wal_mode{SnapshotWalMode::DISABLED};

    std::chrono::milliseconds snapshot_interval{std::chrono::minutes(2)};
    uint64_t snapshot_retention_count{3};

    uint64_t wal_file_size_kibibytes{20 * 1024};
    uint64_t wal_file_flush_every_n_tx{100000};

    bool snapshot_on_exit{false};
    bool restore_replication_state_on_startup{false};

    uint64_t items_per_batch{1'000'000};
    uint64_t recovery_thread_count{8};

    // deprecated
    bool allow_parallel_index_creation{false};

    bool allow_parallel_schema_creation{false};
    friend bool operator==(const Durability &lrh, const Durability &rhs) = default;
  } durability;

  struct Transaction {
    IsolationLevel isolation_level{IsolationLevel::SNAPSHOT_ISOLATION};
    friend bool operator==(const Transaction &lrh, const Transaction &rhs) = default;
  } transaction;

  struct DiskConfig {
    std::filesystem::path main_storage_directory{"storage/rocksdb_main_storage"};
    std::filesystem::path label_index_directory{"storage/rocksdb_label_index"};
    std::filesystem::path label_property_index_directory{"storage/rocksdb_label_property_index"};
    std::filesystem::path unique_constraints_directory{"storage/rocksdb_unique_constraints"};
    std::filesystem::path name_id_mapper_directory{"storage/rocksdb_name_id_mapper"};
    std::filesystem::path id_name_mapper_directory{"storage/rocksdb_id_name_mapper"};
    std::filesystem::path durability_directory{"storage/rocksdb_durability"};
    std::filesystem::path wal_directory{"storage/rocksdb_wal"};
    friend bool operator==(const DiskConfig &lrh, const DiskConfig &rhs) = default;
  } disk;

  std::string name;
  utils::UUID uuid;
  bool force_on_disk{false};
  StorageMode storage_mode{StorageMode::IN_MEMORY_TRANSACTIONAL};

  friend bool operator==(const Config &lrh, const Config &rhs) = default;
};

inline auto ReplicationStateRootPath(memgraph::storage::Config const &config) -> std::optional<std::filesystem::path> {
  if (!config.durability.restore_replication_state_on_startup) {
    spdlog::warn(
        "Replication configuration will NOT be stored. When the server restarts, replication state will be "
        "forgotten.");
    return std::nullopt;
  }
  return {config.durability.storage_directory};
}

static inline void UpdatePaths(Config &config, const std::filesystem::path &storage_dir) {
  auto contained = [](const auto &path, const auto &base) -> std::optional<std::filesystem::path> {
    auto rel = std::filesystem::relative(path, base);
    if (!rel.empty() && rel.native()[0] != '.') {  // Contained
      return rel;
    }
    return {};
  };

  const auto old_base =
      std::filesystem::weakly_canonical(std::filesystem::absolute(config.durability.storage_directory));
  config.durability.storage_directory = std::filesystem::weakly_canonical(std::filesystem::absolute(storage_dir));

  auto UPDATE_PATH = [&](auto to_update) {
    const auto old_path = std::filesystem::weakly_canonical(std::filesystem::absolute(to_update(config.disk)));
    const auto contained_path = contained(old_path, old_base);
    if (!contained_path) {
      throw StorageConfigException("On-disk directories not contained in root.");
    }
    to_update(config.disk) = config.durability.storage_directory / *contained_path;
  };

  UPDATE_PATH(std::mem_fn(&Config::DiskConfig::main_storage_directory));
  UPDATE_PATH(std::mem_fn(&Config::DiskConfig::label_index_directory));
  UPDATE_PATH(std::mem_fn(&Config::DiskConfig::label_property_index_directory));
  UPDATE_PATH(std::mem_fn(&Config::DiskConfig::unique_constraints_directory));
  UPDATE_PATH(std::mem_fn(&Config::DiskConfig::name_id_mapper_directory));
  UPDATE_PATH(std::mem_fn(&Config::DiskConfig::id_name_mapper_directory));
  UPDATE_PATH(std::mem_fn(&Config::DiskConfig::durability_directory));
  UPDATE_PATH(std::mem_fn(&Config::DiskConfig::wal_directory));
}

}  // namespace memgraph::storage

inline void to_json(nlohmann::json &data, memgraph::storage::Config &&config) {
  data = nlohmann::json{{"gc",  // Gc
                         {{"type", config.gc.type}, {"interval_ms", config.gc.interval.count()}}},
                        {"items",  // Items
                         {{"properties_on_edges", config.items.properties_on_edges},
                          {"enable_schema_metadata", config.items.enable_schema_metadata}}},
                        {"transaction",  // Transaction
                         {{"isolation_level", config.transaction.isolation_level}}},
                        {"disk",  // Disk
                         {{"main_storage_directory", config.disk.main_storage_directory},
                          {"label_index_directory", config.disk.label_index_directory},
                          {"label_property_index_directory", config.disk.label_property_index_directory},
                          {"unique_constraints_directory", config.disk.unique_constraints_directory},
                          {"name_id_mapper_directory", config.disk.name_id_mapper_directory},
                          {"id_name_mapper_directory", config.disk.id_name_mapper_directory},
                          {"durability_directory", config.disk.durability_directory},
                          {"wal_directory", config.disk.wal_directory}}},
                        {"name", config.name},
                        {"uuid", config.uuid},
                        {"force_on_disk", config.force_on_disk},
                        {"storage_mode", config.storage_mode}};

  // Too many elements for an initialization list
  auto &durability = data["durability"];
  durability["storage_directory"] = config.durability.storage_directory;
  durability["recover_on_startup"] = config.durability.recover_on_startup;
  durability["snapshot_wal_mode"] = config.durability.snapshot_wal_mode;
  durability["snapshot_interval_min"] = config.durability.snapshot_interval.count();
  durability["snapshot_retention_count"] = config.durability.snapshot_retention_count;
  durability["wal_file_size_kibibytes"] = config.durability.wal_file_size_kibibytes;
  durability["wal_file_flush_every_n_tx"] = config.durability.wal_file_flush_every_n_tx;
  durability["snapshot_on_exit"] = config.durability.snapshot_on_exit;
  durability["restore_replication_state_on_startup"] = config.durability.restore_replication_state_on_startup;
  durability["items_per_batch"] = config.durability.items_per_batch;
  durability["recovery_thread_count"] = config.durability.recovery_thread_count;
  durability["allow_parallel_index_creation"] = config.durability.allow_parallel_index_creation;
  durability["allow_parallel_schema_creation"] = config.durability.allow_parallel_schema_creation;
}

inline void from_json(const nlohmann::json &data, memgraph::storage::Config &config) {
  // GC
  auto &gc = config.gc;
  const auto &gc_json = data["gc"];
  gc_json.at("type").get_to(gc.type);
  gc.interval = std::chrono::milliseconds(gc_json.at("interval_ms").get<long>());

  // Items
  auto &items = config.items;
  const auto &items_json = data["items"];
  items_json.at("properties_on_edges").get_to(items.properties_on_edges);        // Has to be the same
  items_json.at("enable_schema_metadata").get_to(items.enable_schema_metadata);  // ????

  // Transaction
  auto &tx = config.transaction;
  const auto &tx_json = data["transaction"];
  tx_json.at("isolation_level").get_to(tx.isolation_level);  // No?

  // Disk
  auto &disk = config.disk;
  const auto &disk_json = data["disk"];
  disk_json.at("main_storage_directory").get_to(disk.main_storage_directory);
  disk_json.at("label_index_directory").get_to(disk.label_index_directory);
  disk_json.at("label_property_index_directory").get_to(disk.label_property_index_directory);
  disk_json.at("unique_constraints_directory").get_to(disk.unique_constraints_directory);
  disk_json.at("name_id_mapper_directory").get_to(disk.name_id_mapper_directory);
  disk_json.at("id_name_mapper_directory").get_to(disk.id_name_mapper_directory);
  disk_json.at("durability_directory").get_to(disk.durability_directory);
  disk_json.at("wal_directory").get_to(disk.wal_directory);

  // Durability
  auto &durability = config.durability;
  const auto &durability_json = data["durability"];
  durability_json.at("storage_directory").get_to(durability.storage_directory);
  durability_json.at("recover_on_startup").get_to(durability.recover_on_startup);
  durability_json.at("snapshot_wal_mode").get_to(durability.snapshot_wal_mode);
  durability.snapshot_interval = std::chrono::minutes(durability_json.at("snapshot_interval_min").get<long>());
  durability_json.at("snapshot_retention_count").get_to(durability.snapshot_retention_count);
  durability_json.at("wal_file_size_kibibytes").get_to(durability.wal_file_size_kibibytes);
  durability_json.at("wal_file_flush_every_n_tx").get_to(durability.wal_file_flush_every_n_tx);
  durability_json.at("snapshot_on_exit").get_to(durability.snapshot_on_exit);
  durability_json.at("restore_replication_state_on_startup").get_to(durability.restore_replication_state_on_startup);
  durability_json.at("items_per_batch").get_to(durability.items_per_batch);
  durability_json.at("recovery_thread_count").get_to(durability.recovery_thread_count);
  durability_json.at("allow_parallel_index_creation").get_to(durability.allow_parallel_index_creation);
  durability_json.at("allow_parallel_schema_creation").get_to(durability.allow_parallel_schema_creation);

  // Other
  data.at("name").get_to(config.name);
  data.at("uuid").get_to(config.uuid);
  data.at("force_on_disk").get_to(config.force_on_disk);
  data.at("storage_mode").get_to(config.storage_mode);
}
