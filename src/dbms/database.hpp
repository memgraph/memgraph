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

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "query/cypher_query_interpreter.hpp"
#include "storage/v2/storage.hpp"
#include "utils/event_counter.hpp"
#include "utils/event_histogram.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/safe_string.hpp"
#include "utils/thread_pool.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage {
class Storage;
class Accessor;

namespace ttl {
class TTL;
}  // namespace ttl
}  // namespace memgraph::storage

namespace memgraph::query {
struct TriggerStore;

namespace stream {
class Streams;
}  // namespace stream
}  // namespace memgraph::query

namespace memgraph::metrics {
class PrometheusMetrics;
struct DatabaseMetricHandles;
}  // namespace memgraph::metrics

namespace memgraph::dbms {

struct DatabaseInfo;

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
                    std::function<storage::DatabaseProtectorPtr()> database_protector_factory = nullptr,
                    metrics::PrometheusMetrics *prometheus_metrics = nullptr);

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
   * @return std::unique_ptr<storage::Accessor>
   */
  std::unique_ptr<storage::Accessor> Access(storage::StorageAccessType rw_type = storage::StorageAccessType::WRITE,
                                            std::optional<storage::IsolationLevel> override_isolation_level = {},
                                            std::optional<std::chrono::milliseconds> timeout = std::nullopt);

  std::unique_ptr<storage::Accessor> UniqueAccess(std::optional<storage::IsolationLevel> override_isolation_level = {},
                                                  std::optional<std::chrono::milliseconds> timeout = std::nullopt);

  std::unique_ptr<storage::Accessor> ReadOnlyAccess(
      std::optional<storage::IsolationLevel> override_isolation_level = {},
      std::optional<std::chrono::milliseconds> timeout = std::nullopt);

  /**
   * @brief Unique storage identified (name)
   *
   * @return std::string
   */
  std::string name() const;

  utils::SafeString::ConstSafeWrapper name_view() const;

  /**
   * @brief Unique storage identified (uuid)
   *
   * @return const utils::UUID&
   */
  const utils::UUID &uuid() const;

  /**
   * @brief Returns the storage configuration
   *
   * @return const storage::Config&
   */
  const storage::Config &config() const;

  /**
   * @brief Get the storage mode
   *
   * @return storage::StorageMode
   */
  storage::StorageMode GetStorageMode() const noexcept;

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

  storage::ttl::TTL &ttl();

  /**
   * @brief Useful when trying to gracefully destroy Database.
   *
   * Tasks might have an accessor to the database, and so, might forbid it from being destroyed.
   * Call this function before attempting to destroy a database object.
   * This does not affect stream's, trigger's or ttl's durable data. We will restore to the state prior to shutdown.
   *
   */
  void StopAllBackgroundTasks();

  metrics::DatabaseMetricHandles const *metric_handles() const { return metric_handles_.get(); }

  metrics::DatabaseMetricHandles *metric_handles() { return metric_handles_.get(); }

 private:
  std::unique_ptr<storage::Storage> storage_;           //!< Underlying storage
  std::unique_ptr<query::TriggerStore> trigger_store_;  //!< Triggers associated with the storage
  utils::ThreadPool after_commit_trigger_pool_{1};      //!< Thread pool for after commit triggers
  std::unique_ptr<query::stream::Streams> streams_;     //!< Streams associated with the storage
  query::PlanCacheLRU plan_cache_;                      //!< Plan cache associated with the storage

  std::unique_ptr<metrics::Counter[]> counters_storage_;
  std::unique_ptr<metrics::Histogram[]> histograms_storage_;

  metrics::PrometheusMetrics *prometheus_metrics_{nullptr};
  std::unique_ptr<metrics::DatabaseMetricHandles> metric_handles_;

 public:
  metrics::EventCounters counters;
  metrics::EventHistograms histograms;
};

}  // namespace memgraph::dbms
extern template struct memgraph::utils::Gatekeeper<memgraph::dbms::Database>;
