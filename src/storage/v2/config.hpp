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

#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>

#include "flags/coord_flag_env_handler.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage_mode.hpp"
#include "utils/compressor.hpp"
#include "utils/exceptions.hpp"
#include "utils/safe_string.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage {

/// Exception used to signal configuration errors.
class StorageConfigException : public utils::BasicException {
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(StorageConfigException)
};

struct SalientConfig {
  utils::SafeString name;
  utils::UUID uuid;
  StorageMode storage_mode{StorageMode::IN_MEMORY_TRANSACTIONAL};
  utils::CompressionLevel property_store_compression_level{utils::CompressionLevel::MID};
  struct Items {
    bool properties_on_edges{true};
    bool enable_edges_metadata{false};
    bool enable_schema_metadata{false};
    bool enable_schema_info{false};
    bool enable_label_index_auto_creation{false};
    bool enable_edge_type_index_auto_creation{false};
    bool delta_on_identical_property_update{true};
    bool property_store_compression_enabled{false};
    friend bool operator==(const Items &lrh, const Items &rhs) = default;
    friend void to_json(nlohmann::json &data, Items const &items);
    friend void from_json(const nlohmann::json &data, Items &items);
  } items;

  friend bool operator==(const SalientConfig &, const SalientConfig &) = default;
  friend void to_json(nlohmann::json &data, SalientConfig const &config);
  friend void from_json(const nlohmann::json &data, SalientConfig &config);
};

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

    std::filesystem::path storage_directory{"storage"};    // PER INSTANCE SYSTEM FLAG-> root folder...ish
    std::filesystem::path root_data_directory{"storage"};  // ROOT DATA DIR for instance not for DB

    bool recover_on_startup{false};  // PER INSTANCE SYSTEM FLAG

    SnapshotWalMode snapshot_wal_mode{
        SnapshotWalMode::DISABLED};  // PER DATABASE - as at time of initialization; can be changed by
                                     // enabling/disabling the periodic snapshot

    memgraph::utils::SchedulerInterval snapshot_interval{
        std::chrono::minutes(2)};          // PER DATABASE - as at time of initialization; can be changed by user
    uint64_t snapshot_retention_count{3};  // PER DATABASE

    uint64_t wal_file_size_kibibytes{20 * 1024};  // PER DATABASE
    uint64_t wal_file_flush_every_n_tx{100000};   // PER DATABASE

    bool snapshot_on_exit{false};                      // PER DATABASE
    bool restore_replication_state_on_startup{false};  // PER INSTANCE

    uint64_t items_per_batch{1'000'000};  // PER DATABASE
    uint64_t snapshot_thread_count{8};    // PER INSTANCE SYSTEM FLAG
    uint64_t recovery_thread_count{8};    // PER INSTANCE SYSTEM FLAG

    bool allow_parallel_snapshot_creation{false};  // PER DATABASE
    bool allow_parallel_schema_creation{false};    // PER DATABASE
    friend bool operator==(const Durability &lrh, const Durability &rhs) = default;
  } durability;

  struct Transaction {
    IsolationLevel isolation_level{IsolationLevel::SNAPSHOT_ISOLATION};
    friend bool operator==(const Transaction &lrh, const Transaction &rhs) = default;
  } transaction;  // PER DATABASE

  struct DiskConfig {
    std::filesystem::path wal_directory{"storage/rocksdb_wal"};
    friend bool operator==(const DiskConfig &lrh, const DiskConfig &rhs) = default;
  } disk;

  SalientConfig salient;

  friend bool operator==(const Config &lrh, const Config &rhs) = default;
};

inline auto ReplicationStateRootPath(memgraph::storage::Config const &config) -> std::optional<std::filesystem::path> {
  if (!config.durability.restore_replication_state_on_startup
#ifdef MG_ENTERPRISE
      && !memgraph::flags::CoordinationSetupInstance().IsDataInstanceManagedByCoordinator()
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

  UPDATE_PATH(std::mem_fn(&Config::DiskConfig::wal_directory));
}

}  // namespace memgraph::storage
