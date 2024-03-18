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

#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>

#include "flags/replication.hpp"
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

struct SalientConfig {
  std::string name;
  utils::UUID uuid;
  StorageMode storage_mode{StorageMode::IN_MEMORY_TRANSACTIONAL};
  struct Items {
    bool properties_on_edges{true};
    bool enable_schema_metadata{false};
    bool delta_on_identical_property_update{true};
    friend bool operator==(const Items &lrh, const Items &rhs) = default;
  } items;

  friend bool operator==(const SalientConfig &, const SalientConfig &) = default;
};

inline void to_json(nlohmann::json &data, SalientConfig::Items const &items) {
  data = nlohmann::json{{"properties_on_edges", items.properties_on_edges},
                        {"enable_schema_metadata", items.enable_schema_metadata}};
}

inline void from_json(const nlohmann::json &data, SalientConfig::Items &items) {
  data.at("properties_on_edges").get_to(items.properties_on_edges);
  data.at("enable_schema_metadata").get_to(items.enable_schema_metadata);
}

inline void to_json(nlohmann::json &data, SalientConfig const &config) {
  data = nlohmann::json{
      {"items", config.items}, {"name", config.name}, {"uuid", config.uuid}, {"storage_mode", config.storage_mode}};
}

inline void from_json(const nlohmann::json &data, SalientConfig &config) {
  data.at("items").get_to(config.items);
  data.at("name").get_to(config.name);
  data.at("uuid").get_to(config.uuid);
  data.at("storage_mode").get_to(config.storage_mode);
}

/// Pass this class to the \ref Storage constructor to change the behavior of
/// the storage. This class also defines the default behavior.
struct Config {
  struct Gc {
    enum class Type { NONE, PERIODIC };

    Type type{Type::PERIODIC};
    std::chrono::milliseconds interval{std::chrono::milliseconds(1000)};
    friend bool operator==(const Gc &lrh, const Gc &rhs) = default;
  } gc;  // SYSTEM FLAG

  struct Durability {
    enum class SnapshotWalMode { DISABLED, PERIODIC_SNAPSHOT, PERIODIC_SNAPSHOT_WITH_WAL };

    std::filesystem::path storage_directory{"storage"};  // PER INSTANCE SYSTEM FLAG-> root folder...ish

    bool recover_on_startup{false};  // PER INSTANCE SYSTEM FLAG

    SnapshotWalMode snapshot_wal_mode{SnapshotWalMode::DISABLED};  // PER DATABASE

    std::chrono::milliseconds snapshot_interval{std::chrono::minutes(2)};  // PER DATABASE
    uint64_t snapshot_retention_count{3};                                  // PER DATABASE

    uint64_t wal_file_size_kibibytes{20 * 1024};  // PER DATABASE
    uint64_t wal_file_flush_every_n_tx{100000};   // PER DATABASE

    bool snapshot_on_exit{false};                      // PER DATABASE
    bool restore_replication_state_on_startup{false};  // PER INSTANCE

    uint64_t items_per_batch{1'000'000};  // PER DATABASE
    uint64_t recovery_thread_count{8};    // PER INSTANCE SYSTEM FLAG

    // deprecated
    bool allow_parallel_index_creation{false};  // KILL

    bool allow_parallel_schema_creation{false};  // PER DATABASE
    friend bool operator==(const Durability &lrh, const Durability &rhs) = default;
  } durability;

  struct Transaction {
    IsolationLevel isolation_level{IsolationLevel::SNAPSHOT_ISOLATION};
    friend bool operator==(const Transaction &lrh, const Transaction &rhs) = default;
  } transaction;  // PER DATABASE

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

  SalientConfig salient;

  bool force_on_disk{false};  // TODO: cleanup.... remove + make the default storage_mode ON_DISK_TRANSACTIONAL if true

  friend bool operator==(const Config &lrh, const Config &rhs) = default;
};

inline auto ReplicationStateRootPath(memgraph::storage::Config const &config) -> std::optional<std::filesystem::path> {
  if (!config.durability.restore_replication_state_on_startup
#ifdef MG_ENTERPRISE
      && !FLAGS_coordinator_server_port
#endif
  ) {
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
