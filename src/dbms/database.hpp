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

#pragma once

#include <memory>
#include <optional>

#include "memory/db_arena_fwd.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "storage/v2/storage.hpp"
#include "utils/gatekeeper.hpp"

namespace memgraph::query {
struct TriggerStore;

namespace stream {
class Streams;
}  // namespace stream
}  // namespace memgraph::query

namespace memgraph::dbms {

struct DatabaseInfo {
  storage::StorageInfo storage_info;
  uint64_t triggers;
  uint64_t streams;
  int64_t db_memory_tracked{0};
  int64_t db_peak_memory_tracked{0};
  int64_t db_storage_memory_tracked{0};
  int64_t db_embedding_memory_tracked{0};
  int64_t db_query_memory_tracked{0};
};

static inline nlohmann::json ToJson(const DatabaseInfo &info) {
  auto res = ToJson(info.storage_info);
  res["triggers"] = info.triggers;
  res["streams"] = info.streams;
  res["db_memory_tracked"] = info.db_memory_tracked;
  res["db_peak_memory_tracked"] = info.db_peak_memory_tracked;
  res["db_storage_memory_tracked"] = info.db_storage_memory_tracked;
  res["db_embedding_memory_tracked"] = info.db_embedding_memory_tracked;
  res["db_query_memory_tracked"] = info.db_query_memory_tracked;
  return res;
}

/**
 * @brief Class containing everything associated with a single Database
 *
 */
class Database {
 public:
  /**
   * @brief Construct a new Database object
   *
   * @param config storage configuration
   * @param database_protector_factory factory function to create database protectors for async operations
   */
  explicit Database(storage::Config config,
                    std::function<storage::DatabaseProtectorPtr()> database_protector_factory = nullptr);

  ~Database();

  /**
   * @brief Returns the raw storage pointer.
   * @note Ideally everybody would be using an accessor
   * TODO: Remove
   *
   * @return storage::Storage*
   */
  storage::Storage *storage() { return storage_.get(); }

  storage::Storage const *storage() const { return storage_.get(); }

  /**
   * @brief Storage's Accessor
   *
   * @param override_isolation_level
   * @return std::unique_ptr<storage::Storage::Accessor>
   */
  std::unique_ptr<storage::Storage::Accessor> Access(
      storage::StorageAccessType rw_type = storage::StorageAccessType::WRITE,
      std::optional<storage::IsolationLevel> override_isolation_level = {},
      std::optional<std::chrono::milliseconds> timeout = std::nullopt) {
    return storage_->Access(rw_type, override_isolation_level, timeout);
  }

  std::unique_ptr<storage::Storage::Accessor> UniqueAccess(
      std::optional<storage::IsolationLevel> override_isolation_level = {},
      std::optional<std::chrono::milliseconds> timeout = std::nullopt) {
    return storage_->UniqueAccess(override_isolation_level, timeout);
  }

  std::unique_ptr<storage::Storage::Accessor> ReadOnlyAccess(
      std::optional<storage::IsolationLevel> override_isolation_level = {},
      std::optional<std::chrono::milliseconds> timeout = std::nullopt) {
    return storage_->ReadOnlyAccess(override_isolation_level, timeout);
  }

  /**
   * @brief Unique storage identified (name)
   *
   * @return std::string
   */
  std::string name() const { return storage_->name(); }

  auto name_view() const { return storage_->name_view(); }

  /**
   * @brief Unique storage identified (uuid)
   *
   * @return const utils::UUID&
   */
  const utils::UUID &uuid() const { return storage_->uuid(); }

  /**
   * @brief Returns the storage configuration
   *
   * @return const storage::Config&
   */
  const storage::Config &config() const { return storage_->config_; }

  /**
   * @brief Get the storage mode
   *
   * @return storage::StorageMode
   */
  storage::StorageMode GetStorageMode() const noexcept { return storage_->GetStorageMode(); }

  /**
   * @brief Get the storage info
   *
   * @param force_directory Use the configured directory, do not try to decipher the multi-db version
   * @return DatabaseInfo
   */
  DatabaseInfo GetInfo() const;

  /**
   * @brief Switch storage to OnDisk
   *
   */
  void SwitchToOnDisk();

  /**
   * @brief Returns the raw TriggerStore pointer
   *
   * @return query::TriggerStore*
   */
  query::TriggerStore *trigger_store() { return trigger_store_.get(); }

  /**
   * @brief Returns the raw Streams pointer
   *
   * @return query::stream::Streams*
   */
  query::stream::Streams *streams() { return streams_.get(); }

  /**
   * @brief Returns the raw ThreadPool pointer (used for after commit triggers)
   *
   * @return utils::ThreadPool*
   */
  utils::ThreadPool *thread_pool() { return &after_commit_trigger_pool_; }

  /**
   * @brief Add task to the after commit trigger thread pool
   *
   * @param new_task
   */
  void AddTask(utils::ThreadPool::TaskSignature new_task) { after_commit_trigger_pool_.AddTask(std::move(new_task)); }

  /**
   * @brief Returns the PlanCache vector raw pointer
   *
   * @return utils::Synchronized<utils::LRUCache<uint64_t, std::shared_ptr<PlanWrapper>>, utils::RWSpinLock>
   */
  query::PlanCacheLRU *plan_cache() { return &plan_cache_; }

  storage::ttl::TTL &ttl() { return storage_->ttl_; }

  /**
   * @brief Useful when trying to gracefully destroy Database.
   *
   * Tasks might have an accessor to the database, and so, might forbid it from being destroyed.
   * Call this function before attempting to destroy a database object.
   * This does not affect stream's, trigger's or ttl's durable data. We will restore to the state prior to shutdown.
   *
   */
  void StopAllBackgroundTasks();

  /// Returns the database arena pool for per-thread arena management
  memory::ArenaPool &Arena() noexcept;
  memory::ArenaPool &Arena() const noexcept;

  /// Total memory tracked for this database (storage + embeddings + query).
  /// This is the sum of all per-DB trackers and represents the tenant enforcement total.
  /// Note: Allocations from unpinned query threads may not be fully captured.
  int64_t DbMemoryUsage() const noexcept { return db_total_memory_tracker_.Amount(); }

  /// Peak of total memory tracked for this database.
  int64_t DbPeakMemoryUsage() const noexcept { return db_total_memory_tracker_.Peak(); }

  /// Storage memory only (vertices, edges, indices). Tracked via per-DB arena hooks.
  int64_t DbStorageMemoryUsage() const noexcept { return db_memory_tracker_.Amount(); }

  /// Vector index (embedding) memory only. Tracked via per-DB arena hooks.
  int64_t DbEmbeddingMemoryUsage() const noexcept { return db_embedding_memory_tracker_.Amount(); }

  /// Query execution (PMR) memory only. Tracked via TrackingMemoryResource.
  int64_t DbQueryMemoryUsage() const noexcept { return db_query_memory_tracker_.Amount(); }

  utils::MemoryTracker *DbQueryMemoryTracker() noexcept { return &db_query_memory_tracker_; }

  void SetTenantMemoryLimit(int64_t bytes) { db_total_memory_tracker_.SetHardLimit(bytes); }

  int64_t TenantMemoryLimit() const noexcept { return db_total_memory_tracker_.HardLimit(); }

 private:
  // Enforcement-only: caps total per-DB memory (tenant profile limit).
  // No parent — does not roll up to any global. Per-DB domain trackers list this
  // as their second parent so every allocation is counted here AND in the domain global.
  utils::MemoryTracker db_total_memory_tracker_;
  // Domain trackers: parent1 = global domain aggregator (for AI_PLATFORM license limits),
  //                  parent2 = db_total_memory_tracker_ (for tenant limit enforcement).
  utils::MemoryTracker db_memory_tracker_{&utils::graph_memory_tracker, &db_total_memory_tracker_};
  utils::MemoryTracker db_embedding_memory_tracker_{&utils::vector_index_memory_tracker, &db_total_memory_tracker_};
  // Query memory tracker: only enforces tenant limits. Domain aggregation happens via
  // extent hooks on the default arena (global_graph_arena_hooks → graph_memory_tracker).
  // Avoids double-counting: if this had graph_memory_tracker as parent, we'd count each
  // query PMR byte twice (once via TrackingMemoryResource::Alloc, once via arena hooks).
  utils::MemoryTracker db_query_memory_tracker_{&db_total_memory_tracker_};
  std::unique_ptr<memory::ArenaPool> db_arena_;         //!< Per-DB jemalloc arena pool with tracking hooks
  std::unique_ptr<storage::Storage> storage_;           //!< Underlying storage
  std::unique_ptr<query::TriggerStore> trigger_store_;  //!< Triggers associated with the storage
  utils::ThreadPool after_commit_trigger_pool_{1};      //!< Thread pool for after commit triggers
  std::unique_ptr<query::stream::Streams> streams_;     //!< Streams associated with the storage
  query::PlanCacheLRU plan_cache_;                      //!< Plan cache associated with the storage
};

}  // namespace memgraph::dbms
extern template struct memgraph::utils::Gatekeeper<memgraph::dbms::Database>;
