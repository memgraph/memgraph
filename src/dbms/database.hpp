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

#include <algorithm>
#include <filesystem>
#include <iterator>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "query/cypher_query_interpreter.hpp"
#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "storage/v2/storage.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/lru_cache.hpp"
#include "utils/synchronized.hpp"

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
   */
  explicit Database(storage::Config config, replication::ReplicationState &repl_state);

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
      std::optional<storage::IsolationLevel> override_isolation_level = {}) {
    return storage_->Access(repl_state_->GetRole(), override_isolation_level);
  }

  std::unique_ptr<storage::Storage::Accessor> UniqueAccess(
      std::optional<storage::IsolationLevel> override_isolation_level = {}) {
    return storage_->UniqueAccess(repl_state_->GetRole(), override_isolation_level);
  }

  /**
   * @brief Unique storage identified (name)
   *
   * @return const std::string&
   */
  const std::string &name() const { return storage_->name(); }

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
  DatabaseInfo GetInfo(bool force_directory, replication::ReplicationRole replication_role) const {
    DatabaseInfo info;
    info.storage_info = storage_->GetInfo(force_directory, replication_role);
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
  void AddTask(std::function<void()> new_task) { after_commit_trigger_pool_.AddTask(std::move(new_task)); }

  /**
   * @brief Returns the PlanCache vector raw pointer
   *
   * @return utils::Synchronized<utils::LRUCache<uint64_t, std::shared_ptr<PlanWrapper>>, utils::RWSpinLock>
   */
  query::PlanCacheLRU *plan_cache() { return &plan_cache_; }

 private:
  std::unique_ptr<storage::Storage> storage_;       //!< Underlying storage
  query::TriggerStore trigger_store_;               //!< Triggers associated with the storage
  utils::ThreadPool after_commit_trigger_pool_{1};  //!< Thread pool for executing after commit triggers
  query::stream::Streams streams_;                  //!< Streams associated with the storage

  // TODO: Move to a better place
  query::PlanCacheLRU plan_cache_;  //!< Plan cache associated with the storage

  const replication::ReplicationState *repl_state_;
};

}  // namespace memgraph::dbms

extern template struct memgraph::utils::Gatekeeper<memgraph::dbms::Database>;

namespace memgraph::dbms {
using DatabaseAccess = memgraph::utils::Gatekeeper<memgraph::dbms::Database>::Accessor;
}  // namespace memgraph::dbms
