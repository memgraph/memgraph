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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

#include "flags/general.hpp"

#include "constants.hpp"
#include "dbms/database.hpp"
#include "dbms/database_info.hpp"
#include "dbms/rpc.hpp"
#include "dbms/tenant_profiles.hpp"
#include "kvstore/kvstore.hpp"
#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/storage.hpp"
#ifdef MG_ENTERPRISE
#include "dbms/database_handler.hpp"
#endif
#include "dbms/database_protector.hpp"
#include "global.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "query/interpreter_context.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/isolation_level.hpp"
#include "utils/logging.hpp"
#include "utils/rw_lock.hpp"
#include "utils/thread_pool.hpp"
#include "utils/uuid.hpp"

namespace memgraph::dbms {

struct Statistics {
  uint64_t num_vertex;                                 //!< Sum of vertexes in every database
  uint64_t num_edges;                                  //!< Sum of edges in every database
  uint64_t triggers;                                   //!< Sum of triggers in every database
  uint64_t streams;                                    //!< Sum of streams in every database
  uint64_t users;                                      //!< Number of defined users
  uint64_t roles;                                      //!< Number of defined roles
  uint64_t num_databases;                              //!< Number of isolated databases
  uint64_t num_labels;                                 //!< Number of distinct labels
  std::array<uint64_t, 7> label_node_count_histogram;  //!< Log10 histogram: [0]=1-9, [1]=10-99, ..., [6]=1M+
  uint64_t num_edge_types;                             //!< Number of distinct edge types
  uint64_t indices;                                    //!< Sum of indices in every database
  uint64_t constraints;                                //!< Sum of constraints in every database
  std::array<uint64_t, 3>
      storage_modes{};  //!< Number of databases in each storage mode [IN_MEM_TX, IN_MEM_ANA, ON_DISK_TX]
  std::array<uint64_t, 3>
      isolation_levels{};     //!< Number of databases in each isolation level [SNAPSHOT, READ_COMM, READ_UNC]
  uint64_t snapshot_enabled;  //!< Number of databases with snapshots enabled
  uint64_t wal_enabled;       //!< Number of databases with WAL enabled
  uint64_t property_store_compression_enabled;  //!< Number of databases with property store compression enabled
  std::array<uint64_t, 3>
      property_store_compression_level{};  //!< Number of databases with each compression level [LOW, MID, HIGH]
  uint64_t num_parameters;                 //!< Number of server-side parameters
  uint64_t num_descriptions;               //!< Number of server-side descriptions
};

static inline nlohmann::json ToJson(const Statistics &stats) {
  nlohmann::json res;

  res["edges"] = stats.num_edges;
  res["vertices"] = stats.num_vertex;
  res["triggers"] = stats.triggers;
  res["streams"] = stats.streams;
  res["users"] = stats.users;
  res["roles"] = stats.roles;
  res["databases"] = stats.num_databases;
  res["indices"] = stats.indices;
  res["constraints"] = stats.constraints;
  res["storage_modes"] = {{storage::StorageModeToString((storage::StorageMode)0), stats.storage_modes[0]},
                          {storage::StorageModeToString((storage::StorageMode)1), stats.storage_modes[1]},
                          {storage::StorageModeToString((storage::StorageMode)2), stats.storage_modes[2]}};
  res["isolation_levels"] = {{storage::IsolationLevelToString((storage::IsolationLevel)0), stats.isolation_levels[0]},
                             {storage::IsolationLevelToString((storage::IsolationLevel)1), stats.isolation_levels[1]},
                             {storage::IsolationLevelToString((storage::IsolationLevel)2), stats.isolation_levels[2]}};
  res["durability"] = {{"snapshot_enabled", stats.snapshot_enabled}, {"WAL_enabled", stats.wal_enabled}};
  res["property_store_compression_enabled"] = stats.property_store_compression_enabled;
  res["property_store_compression_level"] = {
      {utils::CompressionLevelToString(utils::CompressionLevel::LOW), stats.property_store_compression_level[0]},
      {utils::CompressionLevelToString(utils::CompressionLevel::MID), stats.property_store_compression_level[1]},
      {utils::CompressionLevelToString(utils::CompressionLevel::HIGH), stats.property_store_compression_level[2]}};
  res["label_node_count_histogram"] = {{"1-9", stats.label_node_count_histogram[0]},
                                       {"10-99", stats.label_node_count_histogram[1]},
                                       {"100-999", stats.label_node_count_histogram[2]},
                                       {"1K-9.99K", stats.label_node_count_histogram[3]},
                                       {"10K-99.9K", stats.label_node_count_histogram[4]},
                                       {"100K-999K", stats.label_node_count_histogram[5]},
                                       {"1M+", stats.label_node_count_histogram[6]}};
  res["num_parameters"] = stats.num_parameters;
  res["num_descriptions"] = stats.num_descriptions;

  return res;
}

/**
 * @brief Multi-database session contexts handler.
 */
class DbmsHandler {
 public:
  using LockT = utils::RWLock;
#ifdef MG_ENTERPRISE

  using NewResultT = std::expected<DatabaseAccess, NewError>;
  using DeleteResult = std::expected<void, DeleteError>;
  using RenameResult = std::expected<void, RenameError>;

  // Hot/cold suspend: reasons a tenant cannot be suspended (moved HOT -> COLD).
  enum class SuspendError : uint8_t {
    DEFAULT_DB,             //!< the default database is never suspendable
    NON_EXISTENT,           //!< no such tenant (or already cold)
    NOT_IN_MEMORY,          //!< on-disk storage mode is not suspendable
    DURABILITY_INCOMPLETE,  //!< durability mode is not {periodic snapshot + WAL}
    REPLICATING,            //!< MAIN-side replication participant (has registered replicas)
    ACTIVE_CONNECTIONS,     //!< another accessor is live; could not reach sole-accessor state
    MIN_RESIDENCY,          //!< tenant has not stayed hot long enough since last use (anti-thrash)
  };
  using SuspendResult = std::expected<void, SuspendError>;

  // Hot/cold resume: reasons a tenant cannot be resumed (moved COLD -> HOT).
  enum class ResumeError : uint8_t {
    NON_EXISTENT,     //!< no such tenant in the map / suspended registry
    RECOVERY_FAILED,  //!< recovery (or a pre-publish arm) threw; tenant stays COLD and retriable
  };
  using ResumeResult = std::expected<DatabaseAccess, ResumeError>;

  /**
   * @brief Initialize the handler.
   *
   * @param configs storage configuration
   */
  DbmsHandler(storage::Config config);
#else
  /**
   * @brief Initialize the handler. A single database is supported in community edition.
   *
   * @param configs storage configuration
   */
  DbmsHandler(storage::Config config)
      : db_gatekeeper_{[&] {
                         config.salient.name = kDefaultDB;
                         return std::move(config);
                       }(),
                       [this]() -> storage::DatabaseProtectorPtr {
                         if (auto db_acc = db_gatekeeper_.access()) {
                           return std::make_unique<DatabaseProtector>(*db_acc);
                         }
                         return nullptr;
                       }} {}
#endif

#ifdef MG_ENTERPRISE
  /**
   * @brief Create a new Database associated with the "name" database
   *
   * @param name name of the database
   * @return NewResultT context on success, error on failure
   */
  NewResultT New(const std::string &name, system::Transaction *txn = nullptr) {
    auto wr = std::lock_guard{lock_};
    const auto uuid = utils::UUID{};
    return New_(name, uuid, txn);
  }

  /**
   * @brief Create new if name/uuid do not match any database. Drop and recreate if database already present.
   * @note Default database is not dropped, only its UUID is updated and only if the database is clean.
   *
   * @param config desired salient config
   * @return NewResultT context on success, error on failure
   */
  NewResultT Update(const storage::SalientConfig &config) {
    auto wr = std::lock_guard{lock_};
    auto new_db = New_(config);
    if (new_db || new_db.error() != NewError::EXISTS) {
      // NOTE: If db already exists we retry below
      return new_db;
    }

    const auto name_view = config.name.str_view();
    spdlog::debug("Trying to create db '{}' on replica which already exists.", *name_view);

    auto db = Get_(*name_view);
    spdlog::debug("Aligning database with name {} which has UUID {}, where config UUID is {}",
                  *name_view,
                  std::string(db->uuid()),
                  std::string(config.uuid));
    if (db->uuid() == config.uuid) {  // Same db
      return db;
    }

    spdlog::debug("Different UUIDs");

    // TODO: Fix this hack
    if (*name_view == kDefaultDB) {
      const memory::DbArenaScope db_arena_scope{db.get()};
      auto *storage = db->storage();
      spdlog::debug("Last commit timestamp for DB {} is {}",
                    kDefaultDB,
                    storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_);
      // This seems correct, if database made progress
      if (storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_ !=
          storage::kTimestampInitialId) {
        spdlog::debug("Default storage is not clean, cannot update UUID...");
        return std::unexpected{NewError::GENERIC};  // Update error
      }
      spdlog::debug("Updated default db's UUID");
      // Default db cannot be deleted and remade, have to just update the UUID
      storage->config_.salient.uuid = config.uuid;
      metrics::Metrics().RebindDefaultDatabaseUUID(config.uuid);
      UpdateDurability(storage->config_, ".");
      return db;
    }

    spdlog::debug("Dropping database {} with UUID: {} and recreating with the correct UUID: {}",
                  *name_view,
                  std::string(db->uuid()),
                  std::string(config.uuid));
    // Defer drop
    (void)Delete_(db->name());
    // Second attempt
    return New_(config);
  }

  void UpdateDurability(const storage::Config &config, std::optional<std::filesystem::path> rel_dir = {});

  /**
   * @brief Get the context associated with the "name" database
   *
   * @param name
   * @return DatabaseAccess
   * @throw UnknownDatabaseException if database not found
   */
  DatabaseAccess Get(std::string_view name = kDefaultDB) {
    auto rd = std::shared_lock{lock_};
    return Get_(name);
  }

  /**
   * @brief Get the context associated with the UUID database
   *
   * @param uuid
   * @return DatabaseAccess
   * @throw UnknownDatabaseException if database not found
   */
  DatabaseAccess Get(const utils::UUID &uuid) {
    auto rd = std::shared_lock{lock_};
    return Get_(uuid);
  }

  /**
   * @brief Non-pinning existence check: returns true if the tenant exists in ANY state (HOT or COLD/suspended).
   *
   * Unlike Get(), this does NOT mint an accessor and does NOT throw. Safe to call from the query path
   * when the flag is ON to validate a db-name before storing it as the session's persistent identity.
   * GetGatekeeper covers both HOT and COLD shells; suspended_ is belt-and-suspenders for COLD rebuild
   * metadata that has not yet been written back into db_handler_ (sub-millisecond window).
   *
   * @param name tenant name to test
   * @return true if the tenant is known in any state
   */
  bool Contains(std::string_view name) const {
    auto rd = std::shared_lock{lock_};
    return db_handler_.GetGatekeeper(name) != nullptr || suspended_.contains(name);
  }

#else
  /**
   * @brief Get the context associated with the default database
   *
   * @return DatabaseAccess
   */
  DatabaseAccess Get() {
    auto acc = db_gatekeeper_.access();
    MG_ASSERT(acc, "Failed to get default database!");
    return *acc;
  }
#endif

#ifdef MG_ENTERPRISE
  /**
   * @brief Attempt to delete database.
   *
   * @param db_name database name
   * @param transaction system transaction
   * @return DeleteResult error on failure
   */
  DeleteResult TryDelete(std::string_view db_name, system::Transaction *transaction = nullptr);

  /**
   * @brief Delete or defer deletion of database.
   *
   * @param db_name database name
   * @return DeleteResult error on failure
   */
  DeleteResult Delete(std::string_view db_name);

  /**
   * @brief Delete or defer deletion of database.
   *
   * @param uuid database UUID
   * @return DeleteResult error on failure
   */
  DeleteResult Delete(utils::UUID uuid);

  /**
   * @brief Delete or defer deletion of database with a transactional scope.
   *
   * @param db_name database name
   * @param transaction system transaction
   * @return DeleteResult error on failure
   */
  DeleteResult Delete(std::string_view db_name, system::Transaction *transaction);

  /**
   * @brief Rename a database.
   *
   * @param old_name current database name
   * @param new_name new database name
   * @param txn system transaction for replication
   * @return RenameResult error on failure
   */
  RenameResult Rename(std::string_view old_name, std::string_view new_name, system::Transaction *txn = nullptr);

  // Heartbeat answer for a COLD tenant, served from suspend-time metadata (no resume).
  struct SuspendedHeartbeatInfo {
    uint64_t last_durable_timestamp;
    uint64_t num_committed_txns;
    std::string last_epoch;
  };

  /**
   * @brief Suspend (move HOT -> COLD) the named tenant, tearing down its in-memory storage.
   *
   * The on-disk durability ({snapshot + WAL}) is left intact so a later resume can rebuild.
   * Test-only at runtime in this commit; the query path lands behind the experiment flag later.
   *
   * @param name tenant name
   * @return SuspendResult — error describing why the tenant is not suspendable
   */
  SuspendResult Suspend(std::string_view name) { return Suspend_(name); }

  /**
   * @brief Set the pre-publish resume arm (runs on the recovered DatabaseAccess BEFORE the fresh
   *        gatekeeper is published into the map). Used for triggers/streams/TTL re-arm. Default empty.
   *        If it throws, the resume is aborted (RESUMING -> COLD) and the tenant stays retriable.
   */
  void SetOnResume(std::function<void(DatabaseAccess)> cb) { on_resume_ = std::move(cb); }

  /**
   * @brief Set the post-publish resume arm (runs AFTER the fresh gatekeeper is published).
   *        Used for replication wiring. Default empty.
   */
  void SetOnResumeRepl(std::function<void(DatabaseAccess)> cb) { on_resume_repl_ = std::move(cb); }

  /**
   * @brief Resume (move COLD -> HOT) the named tenant, recovering its in-memory storage inline.
   *
   * Synchronous: recovery runs on the calling thread. Single-flight via the gatekeeper — concurrent
   * callers poll until the winner publishes HOT. Test-only at runtime in this commit; the interpreter
   * query-seam caller lands in a later commit.
   *
   * @param name tenant name
   * @return ResumeResult — the HOT DatabaseAccess on success, or an error
   */
  ResumeResult Resume(std::string_view name) { return Resume_(name); }

  /**
   * @brief Resume every COLD tenant so a subsequent ForEach (e.g. epoch/timestamp update during
   *        MAIN promotion) covers them. Uses rewire_replication=false (the caller, DoToMainPromotion,
   *        holds the repl_state write lock and on_resume_repl_ would re-take it -> self-deadlock) and
   *        make_room=false (serial resume must not let make-room re-evict an already-resumed peer).
   *        Best-effort: a tenant that fails to resume is logged and left COLD.
   */
  void ResumeColdTenantsForPromotion();

  /**
   * @brief Fire-and-forget resume on the background resume executor.
   *
   * Errors are swallowed; on failure the tenant stays COLD and retriable. This is what the
   * interpreter seam will call in a later commit.
   *
   * @param name tenant name
   */
  void KickResume(std::string_view name);

  /**
   * @brief Drain + join the background resume executor. Idempotent. Call during daemon shutdown
   *        BEFORE tearing down per-tenant background tasks so no in-flight resume republishes a
   *        tenant after its tasks were stopped. (Member-destruction order also guarantees this, but
   *        an explicit early drain keeps shutdown ordering obvious.)
   */
  void ShutdownResumePool() { resume_pool_.ShutDown(); }

  /**
   * @brief Resume a COLD tenant identified by UUID (replica replication path). Scans the
   *        suspended_ map for the entry whose salient.uuid matches, then resumes it by name.
   *        Returns nullopt if no COLD tenant has that UUID (or the resume failed).
   *        Reuses the single-flight Resume_ machinery.
   *
   * @param uuid UUID of the COLD tenant to resume
   * @return DatabaseAccess on success; nullopt if not found or recovery failed
   */
  std::optional<DatabaseAccess> ResumeByUUID(const utils::UUID &uuid);

  /**
   * @brief Look up suspend-time heartbeat metadata for a COLD tenant by UUID, WITHOUT resuming it.
   *        Returns nullopt if no COLD tenant has that UUID.
   *
   * @param uuid UUID of the COLD tenant to query
   * @return SuspendedHeartbeatInfo on success; nullopt if not found
   */
  std::optional<SuspendedHeartbeatInfo> GetSuspendedHeartbeatInfo(const utils::UUID &uuid) const;

  /**
   * @brief Suspend the coldest idle HOT tenants until `bytes_to_free` bytes are freed (estimated)
   *        or `max_evictions` tenants have been suspended, whichever comes first. Coldness is ranked
   *        by LastUsedNs() (oldest = coldest). Skips the default DB and silently skips any tenant
   *        Suspend_ rejects (active connections, min-residency, replication, transitional state, ...).
   *        Returns the estimated freed bytes (sum of DbMemoryUsage() of the tenants actually
   *        suspended). CALLER MUST NOT hold lock_ (Suspend_ acquires it internally).
   *
   * @param bytes_to_free stop evicting once this many estimated bytes have been freed
   * @param max_evictions hard cap on evictions per call
   * @param exclude tenant name to skip entirely (e.g. the just-resumed tenant when called as the
   *        make-room-on-resume arm: it is held by the resume thread and would only burn a 100 ms
   *        try_begin_suspend timeout before being rejected). Empty = no exclusion.
   * @return int64_t estimated bytes freed (sum of DbMemoryUsage() of suspended tenants)
   */
  int64_t SuspendColdestIdleTenants(int64_t bytes_to_free, uint64_t max_evictions, std::string_view exclude = {});

  /**
   * @brief One memory-pressure eviction cycle: if hot/cold eviction is enabled (+ experiment + license)
   *        and memory usage is over the high watermark, suspend the coldest idle tenants down toward the
   *        low watermark. The single policy shared by the periodic HC-Evict scheduler and the
   *        make-room-on-resume path (Resume_). Self-gating: a cheap no-op (returns 0) when eviction is
   *        disabled, so it is safe to call unconditionally. CALLER MUST NOT hold lock_.
   *
   * @param exclude tenant to skip (e.g. the just-resumed tenant, held by the resume thread)
   * @return int64_t estimated bytes freed
   */
  int64_t EvictForMemoryPressure(std::string_view exclude = {});
#endif

  /**
   * @brief Return all active databases.
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> All() const {
#ifdef MG_ENTERPRISE
    auto rd = std::shared_lock{lock_};
    return db_handler_.All();
#else
    return {db_gatekeeper_.access()->get()->name()};
#endif
  }

  /**
   * @brief Return all active databases.
   *
   * @return std::vector<std::string>
   */
  auto Count() const -> std::size_t {
#ifdef MG_ENTERPRISE
    auto rd = std::shared_lock{lock_};
    return db_handler_.size();
#else
    return 1;
#endif
  }

  /**
   * @brief Return the statistics all databases.
   *
   * @return Statistics
   */
  Statistics Stats() {
    Statistics stats{};
    // TODO: Handle overflow?
#ifdef MG_ENTERPRISE
    auto rd = std::shared_lock{lock_};
    for (auto &[_, db_gk] : db_handler_) {
#else
    {
      auto &db_gk = db_gatekeeper_;
#endif
      auto db_acc_opt = db_gk.access();
      if (db_acc_opt) {
        auto &db_acc = *db_acc_opt;
        const auto &info = db_acc->GetInfo();
        const auto &storage_info = info.storage_info;
        stats.num_vertex += storage_info.vertex_count;
        stats.num_edges += storage_info.edge_count;
        stats.triggers += info.triggers;
        stats.streams += info.streams;
        ++stats.num_databases;
        stats.indices += storage_info.label_indices + storage_info.label_property_indices + storage_info.text_indices +
                         storage_info.vector_indices;
        stats.constraints += storage_info.existence_constraints + storage_info.unique_constraints;
        ++stats.storage_modes[(int)storage_info.storage_mode];
        ++stats.isolation_levels[(int)storage_info.isolation_level];
        stats.snapshot_enabled += storage_info.durability_snapshot_enabled;
        stats.wal_enabled += storage_info.durability_wal_enabled;
        stats.property_store_compression_enabled += storage_info.property_store_compression_enabled;

        ++stats.property_store_compression_level[std::to_underlying(storage_info.property_store_compression_level)];

        stats.num_descriptions += db_acc->storage()->GetDescriptionCount();

        auto const label_counts = db_acc->storage()->GetLabelCounts();

        constexpr size_t kMaxHistogramBucket = 6;
        for (auto &&[label, count] : label_counts) {
          std::size_t const bucket = std::min(kMaxHistogramBucket, static_cast<std::size_t>(std::log10(count)));
          ++stats.label_node_count_histogram[bucket];
        }
      }
    }
    return stats;
  }

  /**
   * @brief Return a vector with all database info.
   *
   * @return std::vector<DatabaseInfo>
   */
  std::vector<DatabaseInfo> Info() {
    std::vector<DatabaseInfo> res;
#ifdef MG_ENTERPRISE
    auto rd = std::shared_lock{lock_};
    res.reserve(std::distance(db_handler_.cbegin(), db_handler_.cend()));
    for (auto &[_, db_gk] : db_handler_) {
#else
    {
      auto &db_gk = db_gatekeeper_;
#endif
      auto db_acc_opt = db_gk.access();
      if (db_acc_opt) {
        auto &db_acc = *db_acc_opt;
        res.push_back(db_acc->GetInfo());
      }
    }
    return res;
  }

  /// Lightweight per-tenant runtime stats for SHOW DATABASES and hot/cold idle detection.
  /// Does NOT call the heavyweight GetInfo(); reads only the gatekeeper count and the last-used stamp.
  ///
  /// Caveat: `connections` equals Gatekeeper::count_, which counts *all* live Accessors — client
  /// sessions/transactions *plus* transient internal users (async indexer, TTL, snapshot holding a
  /// momentary DatabaseProtector). It is a useful proxy, not an exact session count. A precise
  /// per-DB session count (iterating InterpreterContext::interpreters) is a later refinement.
  /// Hot/cold lifecycle state surfaced by SHOW DATABASES. (SUSPENDING/RESUMING are transient internal
  /// phases not currently surfaced; reheat is near-instant from the client's view.)
  enum class TenantState : uint8_t { READY, COLD };

  static constexpr std::string_view TenantStateToString(TenantState s) {
    switch (s) {
      case TenantState::READY:
        return "ready";
      case TenantState::COLD:
        return "cold";
    }
    return "ready";
  }

  struct TenantRuntimeInfo {
    std::string name;
    uint64_t connections;       //!< gatekeeper accessor count (external holders + in-flight internal users); 0 if cold
    int64_t last_used_ns;       //!< steady_clock nanoseconds of last transaction setup; 0 if never used
    TenantState state;          //!< READY (hot, in active map) or COLD (suspended)
    int64_t db_memory_tracked;  //!< per-DB tracked bytes (Database::DbMemoryUsage()); 0 if cold. For eviction ranking.
  };

  std::vector<TenantRuntimeInfo> TenantRuntimeInfos() {
    std::vector<TenantRuntimeInfo> res;
#ifdef MG_ENTERPRISE
    auto rd = std::shared_lock{lock_};
    for (auto &[name, db_gk] : db_handler_) {
      // A COLD shell (value_ == nullopt) reports from the suspended_ registry; a HOT gatekeeper
      // reports live stats. Transient SUSPENDING/RESUMING gatekeepers that are not yet recorded
      // in suspended_ are a sub-millisecond window and are skipped.
      if (db_gk.state() == utils::GatekeeperState::HOT) {
        auto const connections = db_gk.use_count().value_or(0);  // read BEFORE taking an accessor
        int64_t last = 0;
        int64_t mem = 0;
        if (auto acc = db_gk.access()) {
          last = acc->get()->LastUsedNs();
          mem = acc->get()->DbMemoryUsage();
        }
        res.push_back({name, connections, last, TenantState::READY, mem});
      } else if (auto it = suspended_.find(name); it != suspended_.end()) {
        // COLD (or finished suspending and recorded): report from the rebuild metadata.
        res.push_back({name, 0, it->second.last_used_ns, TenantState::COLD, 0});
      }
      // else: transient gatekeeper not yet in suspended_ — skip.
    }
#else
    if (auto acc = db_gatekeeper_.access()) {
      res.push_back({std::string{acc->get()->name()},
                     db_gatekeeper_.use_count().value_or(0),
                     acc->get()->LastUsedNs(),
                     TenantState::READY,
                     acc->get()->DbMemoryUsage()});
    }
#endif
    return res;
  }

  /***
   * @brief Live vertex/edge/disk/memory stats for metrics.
   *
   * @param uuid
   */
  std::optional<metrics::StorageSnapshot> TryGetStorageSnapshotForMetrics(utils::UUID const &uuid);

  /**
   * @brief Restore triggers for all currently defined databases.
   * @note: Triggers can execute query procedures, so we need to reload the modules first and then the triggers
   *
   * @param ic global InterpreterContext
   */
  void RestoreTriggers(query::InterpreterContext *ic);

  /**
   * @brief Restore streams of all currently defined databases.
   * @note: Stream transformations are using modules, they have to be restored after the query modules are loaded.
   *
   * @param ic global InterpreterContext
   */
  void RestoreStreams(query::InterpreterContext *ic) {
#ifdef MG_ENTERPRISE
    auto wr = std::lock_guard{lock_};
    for (auto &[_, db_gk] : db_handler_) {
#else
    {
      auto &db_gk = db_gatekeeper_;
#endif
      auto db_acc = db_gk.access();
      if (db_acc) {
        auto *db = db_acc->get();
        spdlog::debug("Restoring streams for database \"{}\"", db->name());
        db->streams()->RestoreStreams(*db_acc, ic);
      }
    }
  }

  /**
   * @brief Iterates over all DBs
   *
   * @param f
   */
  void ForEach(std::invocable<DatabaseAccess> auto f) {
#ifdef MG_ENTERPRISE
    auto rd = std::shared_lock{lock_};
    for (auto &[_, db_gk] : db_handler_) {
#else
    {
      auto &db_gk = db_gatekeeper_;
#endif
      auto db_acc = db_gk.access();
      if (db_acc) {  // This isn't an error, just a defunct db
        f(*db_acc);
      }
    }
  }

  // Iterates over all DBs, applies the function on it but stops after
  // the result of applying a function on some DB is false
  auto AllOf(std::predicate<DatabaseAccess> auto f) -> bool {
#ifdef MG_ENTERPRISE
    auto rd = std::shared_lock{lock_};
    for (auto &[_, db_gk] : db_handler_) {
#else
    {
      auto &db_gk = db_gatekeeper_;
#endif
      auto db_acc = db_gk.access();
      // Stop when the result of the function is false
      if (db_acc && !f(*db_acc)) {
        return false;
      }
    }
    return true;
  }

  static void RecoverStorageReplication(DatabaseAccess db_acc, replication::RoleMainData &role_main_data);

#ifdef MG_ENTERPRISE
  std::expected<void, TenantProfiles::CreateError> CreateTenantProfile(std::string_view name, int64_t memory_limit,
                                                                       system::Transaction *sys_txn);
  std::expected<void, TenantProfiles::AlterError> AlterTenantProfile(std::string_view name, int64_t memory_limit,
                                                                     system::Transaction *sys_txn);
  std::expected<void, TenantProfiles::DropError> DropTenantProfile(std::string_view name, system::Transaction *sys_txn);
  std::expected<void, TenantProfiles::AttachError> SetTenantProfileOnDatabase(std::string_view profile_name,
                                                                              std::string_view db_name,
                                                                              system::Transaction *sys_txn);
  std::expected<void, TenantProfiles::DetachError> RemoveTenantProfileFromDatabase(std::string_view db_name,
                                                                                   system::Transaction *sys_txn);

  std::vector<TenantProfiles::Profile> GetAllTenantProfiles() const {
    return tenant_profiles_ ? tenant_profiles_->GetAll() : std::vector<TenantProfiles::Profile>{};
  }

  std::optional<TenantProfiles::Profile> GetTenantProfile(std::string_view name) const {
    return tenant_profiles_ ? tenant_profiles_->Get(name) : std::nullopt;
  }

  std::optional<std::string> GetTenantProfileForDatabase(std::string_view db_name) const {
    return tenant_profiles_ ? tenant_profiles_->GetProfileForDatabase(db_name) : std::nullopt;
  }
#endif

  auto default_config() const -> storage::Config const & {
#ifdef MG_ENTERPRISE
    return default_config_;
#else
    const auto acc = db_gatekeeper_.access();
    MG_ASSERT(acc, "Failed to get default database!");
    return acc->get()->config();
#endif
  }

 private:
#ifdef MG_ENTERPRISE
  /**
   * @brief return the storage directory of the associated database
   *
   * @param name Database name
   * @return std::optional<std::filesystem::path>
   */
  std::optional<std::filesystem::path> StorageDir_(std::string_view name) {
    const auto conf = db_handler_.GetConfig(name);
    if (conf) {
      return conf->durability.storage_directory;
    }
    spdlog::debug("Failed to find storage dir for database \"{}\"", name);
    return {};
  }

  /**
   * @brief Create a new Database associated with the "name" database
   *
   * @param name name of the database
   * @param uuid undelying RocksDB directory
   * @return NewResultT context on success, error on failure
   */
  NewResultT New_(std::string_view name, utils::UUID uuid, system::Transaction *txn = nullptr,
                  std::optional<std::filesystem::path> rel_dir = {}) {
    auto config_copy = default_config_;
    config_copy.salient.name = name;
    config_copy.salient.uuid = uuid;
    spdlog::debug("Creating database '{}' - '{}'", name, std::string{uuid});
    if (rel_dir) {
      storage::UpdatePaths(config_copy, default_config_.durability.storage_directory / *rel_dir);
    } else {
      storage::UpdatePaths(config_copy,
                           default_config_.durability.storage_directory / kMultiTenantDir / std::string{uuid});
    }
    return New_(std::move(config_copy), txn);
  }

  /**
   * @brief Create a new Database using the passed configuration
   *
   * @param config configuration to be used
   * @return NewResultT context on success, error on failure
   */
  NewResultT New_(const storage::SalientConfig &config, system::Transaction *txn = nullptr) {
    auto config_copy = default_config_;
    config_copy.salient = config;  // name, uuid, mode, etc
    UpdatePaths(config_copy, config_copy.durability.storage_directory / kMultiTenantDir / std::string{config.uuid});
    return New_(std::move(config_copy), txn);
  }

  /**
   * @brief Create a new Database associated with the "name" database
   *
   * @param storage_config storage configuration
   * @return NewResultT context on success, error on failure
   */
  DbmsHandler::NewResultT New_(storage::Config storage_config, system::Transaction *txn = nullptr);

  // TODO: new overload of Delete_ with DatabaseAccess
  DeleteResult Delete_(std::string_view db_name);

  /**
   * @brief Drop a COLD (suspended) tenant from disk + metadata. CALLER MUST HOLD lock_ (exclusive).
   *
   * Mirrors the HOT-drop teardown effects that actually apply to a COLD shell (no value_, no
   * threads, no streams): erase the COLD gatekeeper shell, drop the durability KVStore key, remove
   * the on-disk storage dir, detach the tenant profile, drop the suspended_ rebuild metadata, and
   * (if a system transaction is present) record the DropDatabase delta. uuid + path are sourced from
   * the suspended_ entry, since GetConfig() returns nullopt for a COLD tenant.
   */
  DeleteResult DropCold_(std::string_view db_name, system::Transaction *txn);

  /**
   * @brief Implementation of Suspend. See Suspend() for semantics.
   *
   * Three phases: (A) eligibility + capture under shared lock_, (B) freeze (no lock_),
   * (C) record metadata under lock_ then heavy teardown OUTSIDE lock_.
   */
  SuspendResult Suspend_(std::string_view name);

  /**
   * @brief Implementation of Resume. See Resume() for semantics.
   *
   * Single-flight via the gatekeeper: the winner builds a fresh HOT gatekeeper OFF the map
   * (recovery, no lock_), runs the pre-publish arm, then move-assigns it over the RESUMING shell
   * under lock_. Losers poll Get(name) until HOT (bounded). On failure: abort_resume() (RESUMING ->
   * COLD), leaving suspended_ intact so the resume is retriable.
   */
  // rewire_replication: run the post-publish on_resume_repl_ arm (re-wire MAIN-side replication
  // clients). MUST be false when called from the replica replication-apply path (ResumeByUUID):
  // that path runs on the replication RPC thread, which already holds the repl_state read lock, and
  // on_resume_repl_ takes the repl_state WRITE lock -> self-deadlock on the non-recursive RWSpinLock.
  // (On a replica the hook is a no-op anyway, so skipping it is functionally free.)
  ResumeResult Resume_(std::string_view name, bool rewire_replication = true, bool make_room = true);

  /**
   * @brief Apply `limit` to the tenant's in-memory hard limit IF it is currently HOT.
   *
   * For COLD/RESUMING tenants this is a no-op: the durable tenant_profiles_ store is the source of
   * truth and Resume_ re-applies it on the next resume. Non-throwing (unlike Get()). Takes a shared
   * lock on lock_ internally, so must NOT be called while lock_ is already held.
   */
  void ApplyTenantMemoryLimitIfHot_(std::string_view db_name, int64_t limit);

  /**
   * @brief Create a new Database associated with the default database
   *
   * @return NewResultT context on success, error on failure
   */
  void SetupDefault_() {
    try {
      Get(kDefaultDB);
    } catch (const UnknownDatabaseException &) {
      // No default DB restored, create it
      MG_ASSERT(New_(kDefaultDB, {/* random UUID */}, nullptr, ".").has_value(),
                "Failed while creating the default database");
    }

    // For back-compatibility...
    // Recreate the dbms layout for the default db and symlink to the root
    const auto dir = StorageDir_(kDefaultDB);
    MG_ASSERT(dir, "Failed to find storage path.");
    const auto main_dir = *dir / kMultiTenantDir / kDefaultDB;

    if (!std::filesystem::exists(main_dir)) {
      std::filesystem::create_directory(main_dir);
    }

    // Force link on-disk directories
    const auto conf = db_handler_.GetConfig(kDefaultDB);
    MG_ASSERT(conf, "No configuration for the default database.");
    const auto &tmp_conf = conf->disk;
    std::vector<std::filesystem::path> to_link{
        tmp_conf.main_storage_directory,
        tmp_conf.label_index_directory,
        tmp_conf.label_property_index_directory,
        tmp_conf.unique_constraints_directory,
        tmp_conf.name_id_mapper_directory,
        tmp_conf.id_name_mapper_directory,
        tmp_conf.durability_directory,
        tmp_conf.wal_directory,
    };

    // Add in-memory paths
    // Some directories are redundant (skip those)
    using namespace std::string_view_literals;
    constexpr std::array<std::string_view, 5> skip{
        "audit_log"sv, "auth"sv, "databases"sv, "internal_modules"sv, "settings"sv};
    for (auto const &item : std::filesystem::directory_iterator{*dir}) {
      const auto dir_name = std::filesystem::relative(item.path(), item.path().parent_path());
      auto const dir_name_str = dir_name.string();
      if (std::ranges::contains(skip, dir_name_str) || dir_name_str.starts_with(".")) {
        spdlog::trace("{} won't be used for symlinking.", dir_name_str);
        continue;
      }
      to_link.push_back(item.path());
    }

    // Symlink to root dir
    for (auto const &item : to_link) {
      const auto dir_name = std::filesystem::relative(item, item.parent_path());
      const auto link = main_dir / dir_name;
      const auto to = std::filesystem::relative(item, main_dir);
      if (!std::filesystem::is_symlink(link) && !std::filesystem::exists(link)) {
        std::filesystem::create_directory_symlink(to, link);
      } else {  // Check existing link
        std::error_code ec;
        const auto test_link = std::filesystem::read_symlink(link, ec);
        if (ec || test_link != to) {
          MG_ASSERT(false,
                    "Memgraph storage directory incompatible with new version.\n"
                    "Please use a clean directory or remove \"{}\" and try again.",
                    link.string());
        }
      }
    }
  }

  void RestoreTenantProfiles_() {
    for (const auto &profile : tenant_profiles_->GetAll()) {
      for (const auto &db_name : profile.databases) {
        try {
          auto db_acc = Get_(db_name);
          if (profile.memory_limit > 0) {
            db_acc.get()->SetTenantMemoryLimit(profile.memory_limit);
            spdlog::info(
                "Applied tenant profile '{}' (limit={}) to database '{}'", profile.name, profile.memory_limit, db_name);
          }
        } catch (const UnknownDatabaseException &) {
          spdlog::warn("Tenant profile '{}' references unknown database '{}' — skipping", profile.name, db_name);
        }
      }
    }
  }

  DatabaseAccess Get_(std::string_view name) {
    auto db = db_handler_.Get(name);
    if (db) {
      return *db;
    }
    throw UnknownDatabaseException("Tried to retrieve an unknown database \"{}\".", name);
  }

  /**
   * @brief Get the context associated with the UUID database
   *
   * @param uuid
   * @return DatabaseAccess
   * @throw UnknownDatabaseException if database not found
   */
  DatabaseAccess Get_(const utils::UUID &uuid) {
    // TODO Speed up
    for (auto &[_, db_gk] : db_handler_) {
      auto acc = db_gk.access();
      // A COLD shell's access() returns nullopt (state != HOT). Skip it: a COLD
      // tenant is non-replicated by construction, so it is not reachable by UUID
      // on the replication-only path that calls this overload.
      if (!acc) continue;
      if ((*acc)->uuid() == uuid) {
        return std::move(*acc);
      }
    }
    throw UnknownDatabaseException("Tried to retrieve an unknown database with UUID \"{}\".", std::string{uuid});
  }
#endif

#ifdef MG_ENTERPRISE
  // Hot/cold: rebuild metadata for a suspended (COLD) tenant. The gatekeeper stays in
  // db_handler_ as a COLD shell (value_ == nullopt); this holds what a later resume needs.
  struct SuspendedEntry {
    storage::SalientConfig salient;  //!< salient config to recreate the storage
    std::filesystem::path rel_dir;   //!< durability dir relative to the instance root
    int64_t last_used_ns;            //!< last-used stamp captured at suspend time
    // Heartbeat metadata captured at suspend time so a replica can answer a HeartbeatRpc for a
    // COLD tenant WITHOUT resuming it (resume-on-heartbeat would defeat eviction). Mirrors the
    // fields HeartbeatHandler reads from a live storage's repl_storage_state_.
    uint64_t last_durable_timestamp{0};  //!< repl_storage_state_.commit_ts_info_.ldt_ at suspend
    uint64_t num_committed_txns{0};      //!< repl_storage_state_.commit_ts_info_.num_committed_txns_
    std::string last_epoch;              //!< the "last epoch with a commit" string (see Suspend_)
  };

  mutable LockT lock_{utils::RWLock::Priority::READ};  //!< protective lock
  storage::Config default_config_;                     //!< Storage configuration used when creating new databases
  DatabaseHandler db_handler_;                         //!< multi-tenancy storage handler
  std::map<std::string, SuspendedEntry, std::less<>> suspended_;  //!< COLD tenant rebuild metadata; guarded by lock_
  std::function<void(DatabaseAccess)> on_resume_;  //!< pre-publish resume arm (triggers/streams/TTL); empty default
  std::function<void(DatabaseAccess)> on_resume_repl_;  //!< post-publish resume arm (replication); empty default
  // TODO: move to be common
  std::unique_ptr<kvstore::KVStore> durability_;     //!< list of active dbs (pointer so we can postpone its creation)
  std::unique_ptr<TenantProfiles> tenant_profiles_;  //!< per-DB resource profiles (created after durability_)
  // Background resume executor. DESTRUCTION ORDER (Finding D): declared AFTER db_handler_ so that
  // reverse-order member destruction stops+joins the pool BEFORE db_handler_ (the map) is destroyed.
  // In-flight KickResume jobs hold a raw Gatekeeper<Database>* into db_handler_; if the map died
  // first those pointers would dangle (UAF). ~ThreadPool joins all workers, so by the time
  // db_handler_ is destroyed no resume job is still touching it.
  utils::ThreadPool resume_pool_;
#endif
#ifndef MG_ENTERPRISE
  mutable utils::Gatekeeper<Database> db_gatekeeper_;  //!< Single databases gatekeeper
#endif
};  // namespace memgraph::dbms

}  // namespace memgraph::dbms
