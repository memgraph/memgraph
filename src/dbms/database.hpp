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

#include <memory>
#include <optional>

#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "storage/v2/storage.hpp"
#include "utils/gatekeeper.hpp"

namespace memgraph::dbms {

struct DatabaseInfo {
  storage::StorageInfo storage_info;
  uint64_t triggers;
  uint64_t streams;
};

static inline nlohmann::json ToJson(const DatabaseInfo &info) { return ToJson(info.storage_info); }

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
   * @param repl_state replication state
   * @param database_protector_factory factory function to create database protectors for async operations
   */
  explicit Database(storage::Config config,
                    utils::Synchronized<replication::ReplicationState, utils::RWSpinLock> &repl_state,
                    std::function<storage::DatabaseProtectorPtr()> database_protector_factory = nullptr);

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
  DatabaseInfo GetInfo() const {
    DatabaseInfo info;
    info.storage_info = storage_->GetInfo();
    info.triggers = trigger_store_.GetTriggerInfo().size();
    info.streams = streams_.GetStreamInfo().size();
    return info;
  }

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
  query::TriggerStore *trigger_store() { return &trigger_store_; }

  /**
   * @brief Returns the raw Streams pointer
   *
   * @return query::stream::Streams*
   */
  query::stream::Streams *streams() { return &streams_; }

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
  void StopAllBackgroundTasks() {
    streams()->Shutdown();
    thread_pool()->ShutDown();
    storage_->StopAllBackgroundTasks();
  }

 private:
  std::unique_ptr<storage::Storage> storage_;       //!< Underlying storage
  query::TriggerStore trigger_store_;               //!< Triggers associated with the storage
  utils::ThreadPool after_commit_trigger_pool_{1};  //!< Thread pool for executing after commit triggers
  query::stream::Streams streams_;                  //!< Streams associated with the storage

  // TODO: Move to a better place
  query::PlanCacheLRU plan_cache_;  //!< Plan cache associated with the storage
};

}  // namespace memgraph::dbms
extern template struct memgraph::utils::Gatekeeper<memgraph::dbms::Database>;
