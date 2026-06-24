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
#include <ranges>
#include <set>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

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
    ACTIVE_CONNECTIONS,     //!< another accessor is live; could not reach sole-accessor state
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

  /**
   * @brief Suspend (move HOT -> COLD) the named tenant, tearing down its in-memory storage.
   *
   * The tenant's durability dir remains intact; a later Resume() reheats it from the
   * {snapshot + WAL} artifacts. This is a blocking call (waits for sole-accessor state).
   *
   * @param name tenant database name
   * @param txn  originating system transaction; the suspend is recorded as a system action so it is
   *             ordered + replicated like CREATE/DROP DATABASE. nullptr for node-local callers.
   * @return SuspendResult — error describing why the tenant is not suspendable
   */
  SuspendResult Suspend(std::string_view name, system::Transaction *txn = nullptr) { return Suspend_(name, txn); }

  /**
   * @brief Set the pre-publish resume arm (runs on the recovered DatabaseAccess BEFORE the fresh
   *        gatekeeper is published into the map). Used for triggers/streams/TTL re-arm. Default empty.
   *        If it throws, the resume is aborted (RESUMING -> COLD) and the tenant stays retriable.
   *
   * INVARIANT: the arm must operate on the supplied DatabaseAccess and must NOT synchronously
   * re-acquire the SAME tenant's accessor (Get/GetDatabaseAccessor by name or UUID) on this thread.
   * The tenant is mid-RESUMING, so a re-entrant resume on the same thread would lose the single-flight
   * race against itself and block. The current arms honour this (they use the passed accessor; stream
   * consumers run on their own threads); a future arm must too.
   */
  void SetOnResume(std::function<void(DatabaseAccess)> cb) { on_resume_ = std::move(cb); }

  /**
   * @brief Set the pre-teardown suspend arm: stop the per-database features that pin the tenant HOT
   *        (Kafka/Pulsar stream consumers each hold a DatabaseAccess via their captured Interpreter, so
   *        the suspend freeze could never reach sole-accessor while a stream exists). Runs OFF-lock in
   *        Suspend_ (it joins consumer threads) and must NOT delete durable stream metadata, so the
   *        resume arm can rebuild the consumers. Default empty (no-op when hot/cold is not wired).
   */
  void SetOnSuspend(std::function<void(DatabaseAccess)> cb) { on_suspend_ = std::move(cb); }

  /**
   * @brief Set the streams-only restore arm, used to UNDO SetOnSuspend's stream shutdown when a suspend
   *        does not commit (e.g. a foreign connection keeps the tenant busy -> ACTIVE_CONNECTIONS, or a
   *        post-freeze step throws). Restores from durable metadata, preserving each stream's persisted
   *        running/stopped state. Triggers are NOT restored here (suspend never stops them). Default empty.
   */
  void SetRestoreStreams(std::function<void(DatabaseAccess)> cb) { restore_streams_ = std::move(cb); }

  /**
   * @brief Set the post-publish resume arm (runs AFTER the fresh gatekeeper is published).
   *        Used for replication wiring. Default empty. Wired in a later (replication) commit.
   */
  void SetOnResumeRepl(std::function<void(DatabaseAccess)> cb) { on_resume_repl_ = std::move(cb); }

  /**
   * @brief Resume (move COLD -> HOT) the named tenant, recovering its in-memory storage inline.
   *
   * Synchronous: recovery runs on the calling thread. Single-flight via the gatekeeper — concurrent
   * callers poll until the winner publishes HOT, then share the published accessor. No make-room /
   * no budget: the caller blocks for the full recovery.
   *
   * TODO(hot-cold): for the MVP this is intentionally synchronous — the bolt worker that runs
   * `RESUME DATABASE` (or first touches a COLD tenant) blocks for the entire WAL/snapshot rebuild,
   * which can be minutes for a large tenant and consumes a finite bolt worker. Move recovery onto a
   * dedicated resume thread pool and return immediately: access() already returns nullopt during the
   * RESUMING state (a clean retriable "database not available"), and the single-flight loser-poll loop
   * in Resume_ is the scaffolding a non-blocking caller would reuse to surface "still resuming, retry".
   *
   * @param name tenant database name
   * @param txn  originating system transaction; the resume is recorded as a system action so it is
   *             ordered + replicated like CREATE/DROP DATABASE. nullptr for node-local callers.
   * @return ResumeResult — the HOT DatabaseAccess on success, or an error
   */
  ResumeResult Resume(std::string_view name, system::Transaction *txn = nullptr) {
    return Resume_(name, /*rewire_replication=*/true, txn);
  }

  /**
   * @brief Suspend a tenant identified by UUID (replica-apply path for SuspendDatabaseRpc).
   *
   * Resolves the UUID to a name (the apply handler only has the UUID from the wire), then runs the
   * same node-local Suspend_ as MAIN. Suspend never gates on replication state, so the replica path
   * needs no special handling; the bounded drain is the gatekeeper's try_begin_suspend() count==1 wait.
   *
   * @return SuspendResult — NON_EXISTENT if no HOT tenant has this UUID.
   */
  SuspendResult SuspendByUUID(utils::UUID uuid, system::Transaction *txn = nullptr);

  /**
   * @brief Resume a tenant identified by UUID (replica-apply path for ResumeDatabaseRpc and the
   *        inline self-resume barrier in GetDatabaseAccessor).
   *
   * Resolves the UUID to a name from the suspended-set, then runs Resume_ with
   * rewire_replication=false (the caller holds the repl_state read lock; the post-publish
   * replication arm would re-take it as a write lock -> self-deadlock).
   *
   * @return ResumeResult — the HOT DatabaseAccess on success, NON_EXISTENT if no COLD tenant has
   *         this UUID (e.g. it was never suspended, or was already resumed by a racing caller).
   */
  ResumeResult ResumeByUUID(utils::UUID uuid, system::Transaction *txn = nullptr);

  /**
   * @brief True iff @p name currently holds a COLD shell (is in the suspended-set).
   *
   * Replica SystemRecovery reconcile uses this to branch: a name MAIN lists HOT that this replica
   * holds COLD must be resumed before Update() can proceed; a name MAIN lists COLD that is already
   * suspended here is already converged.
   */
  bool IsSuspended(std::string_view name) const {
    auto rd = std::shared_lock{lock_};
    return suspended_.contains(name);
  }

  /**
   * @brief Is @p name suspended here AND under this exact @p uuid? Distinguishes a tenant that is
   * still the same COLD tenant (refresh its metadata) from one MAIN drop+recreated under the same
   * name while this replica kept the OLD one COLD (a stale shell that must be dropped and rebuilt —
   * otherwise per-uuid Suspend/Resume RPC for the new uuid misses forever and the replica stays
   * permanently BEHIND).
   */
  bool IsSuspendedWithUuid(std::string_view name, const utils::UUID &uuid) const {
    auto rd = std::shared_lock{lock_};
    auto it = suspended_.find(name);
    return it != suspended_.end() && it->second.salient.uuid == uuid;
  }

  /**
   * @brief Does this node know @p uuid AT ALL — HOT (db_handler_) or COLD (suspended_)?
   *
   * The replica suspend/resume apply handlers resolve a UUID via SuspendByUUID/ResumeByUUID, which return
   * NON_EXISTENT both when the tenant is ALREADY in the target state (idempotent re-apply -> NO_NEED) and
   * when it is missing ENTIRELY (genuine divergence). Mapping both to NO_NEED would silently score the
   * divergent case as success, so MAIN never latches this replica BEHIND and SystemRecovery never fires to
   * supply the missing tenant. This predicate lets the handler tell the two apart: known -> NO_NEED;
   * unknown -> leave the apply FAILURE so the replica reconciles via SystemRecovery.
   */
  // NOT const: probing a HOT tenant's uuid mints (and immediately drops) a gatekeeper accessor, exactly
  // as SuspendByUUID does — Gatekeeper::access() is non-const because it transiently bumps the count.
  bool IsKnownTenant(const utils::UUID &uuid) {
    auto rd = std::shared_lock{lock_};
    for (auto &[n, db_gk] : db_handler_) {
      auto acc = db_gk.access();  // nullopt for a COLD shell — those are matched via suspended_ below
      if (acc && acc->get()->uuid() == uuid) return true;
    }
    for (const auto &[n, entry] : suspended_) {
      if (entry.salient.uuid == uuid) return true;
    }
    return false;
  }

  /**
   * @brief Atomic, de-duplicated HOT ∪ COLD tenant set for cold-aware SHOW DATABASES.
   *
   * All()/ForEach skip COLD shells, so a suspended tenant would otherwise vanish from SHOW DATABASES.
   * This reads db_handler_ (HOT) and suspended_ (COLD) under a SINGLE shared_lock and returns each
   * tenant once as (name, is_cold). De-dup matters: during the SUSPENDING transient a tenant is briefly
   * in BOTH db_handler_ (value_ still live -> passes Handler::All()) and suspended_, because
   * Suspend_ inserts into suspended_ under lock_ but calls finish_suspend() (which nulls value_) AFTER
   * releasing the lock. suspended_ takes precedence (the tenant is on its way COLD), so it is listed
   * once, as COLD — never duplicated.
   */
  std::vector<std::pair<std::string, std::string>> AllWithHotColdStatus() const {
    auto rd = std::shared_lock{lock_};
    std::vector<std::pair<std::string, std::string>> out;
    out.reserve(suspended_.size() + db_handler_.size());
    // The COLD status string distinguishes a user-suspended tenant ("COLD") from one left cold by
    // a failed boot recovery ("COLD (recovery failed[: out of memory])") — the degraded-but-alive
    // marker so the instance never silently drops a tenant.
    for (const auto &[name, entry] : suspended_) {
      // Capture-less IIFE (reason passed by value): a [&]-capturing lambda over the structured
      // binding trips a clang-analyzer NullDereference false positive on `entry`.
      const std::string_view st = [](SuspendedEntry::ColdReason reason) -> std::string_view {
        switch (reason) {
          case SuspendedEntry::ColdReason::kRecoveryFailed:
            return "COLD (recovery failed)";
          case SuspendedEntry::ColdReason::kRecoveryFailedOom:
            return "COLD (recovery failed: out of memory)";
          case SuspendedEntry::ColdReason::kSuspended:
            return "COLD";
        }
        return "COLD";  // unreachable; silences -Wreturn-type
      }(entry.cold_reason);
      out.emplace_back(name, std::string{st});
    }
    for (auto &name : db_handler_.All()) {  // HOT names only (Handler::All() skips no-value shells)
      if (!suspended_.contains(name)) out.emplace_back(std::move(name), "HOT");
    }
    return out;
  }

  /// Minimal projection of a COLD tenant for SHOW STORAGE INFO ON <cold> (previously this errored).
  struct ColdShowInfo {
    utils::UUID uuid;
    storage::StorageInfo stats;  //!< as-of-suspend snapshot (on MAIN); physical fields are MAIN-relative
    std::string status;          //!< "COLD (as-of-suspend snapshot)" or "COLD (recovery failed[...])"
  };

  /**
   * @brief Fetch a COLD tenant's as-of-suspend snapshot by name (nullopt if not suspended).
   *
   * Lets SHOW STORAGE INFO ON <cold> serve the durable cold_stats instead of tripping the Get_ cold
   * seam. The numbers are MAIN's as-of-suspend snapshot, labeled as such by the caller.
   * For a tenant left cold by a failed recovery the stats are defaults (no snapshot was captured),
   * so `status` reports the failure instead of an "as-of-suspend" label.
   */
  std::optional<ColdShowInfo> GetColdShowInfo(std::string_view name) const {
    auto rd = std::shared_lock{lock_};
    if (auto it = suspended_.find(name); it != suspended_.end()) {
      const std::string_view st = [](SuspendedEntry::ColdReason reason) -> std::string_view {
        switch (reason) {
          case SuspendedEntry::ColdReason::kRecoveryFailed:
            return "COLD (recovery failed); stats unavailable";
          case SuspendedEntry::ColdReason::kRecoveryFailedOom:
            return "COLD (recovery failed: out of memory); stats unavailable";
          case SuspendedEntry::ColdReason::kSuspended:
            return "COLD (as-of-suspend snapshot)";
        }
        return "COLD (as-of-suspend snapshot)";  // unreachable
      }(it->second.cold_reason);
      return ColdShowInfo{.uuid = it->second.salient.uuid, .stats = it->second.cold_stats, .status = std::string{st}};
    }
    return std::nullopt;
  }

  /**
   * @brief Resume a COLD tenant during replica SystemRecovery (rewire_replication=false).
   *
   * Called during SystemRecovery when MAIN's incoming HOT config names a tenant this replica holds
   * COLD: Update() would throw on the COLD shell, so it must be resumed first. rewire=false because
   * recovery runs in the replica apply context (on_resume_repl_ would re-take the repl_state lock).
   */
  ResumeResult ResumeForRecovery(std::string_view name) { return Resume_(name, /*rewire_replication=*/false); }

  /**
   * @brief Force-suspend a tenant during replica recovery, bypassing the durability-complete gate.
   * The gate protects a USER-initiated SUSPEND; in recovery the tenant is already COLD on MAIN
   * (authoritative) and suspend's consolidating snapshot is written unconditionally, so it stays
   * recoverable. Bypassing avoids a BEHIND retry loop on a replica whose durability config differs
   * from MAIN's.
   */
  SuspendResult SuspendForRecovery(std::string_view name) {
    return Suspend_(name, /*txn=*/nullptr, /*for_recovery=*/true);
  }

  /**
   * @brief Apply MAIN's authoritative cold metadata to a suspended tenant during recovery.
   *
   * Overwrites cold_stats with MAIN's as-of-suspend snapshot AND, when MAIN recorded epoch
   * metadata (has_epoch_meta — a post-epoch-capture cold entry), the current_epoch + epoch_history too. The epoch
   * matters because a replica that converges a tenant via SystemRecovery (rather than the ordered
   * SuspendDatabaseRpc) and is LATER promoted must append the correct continuous-history boundary in
   * PromoteColdTenants; without MAIN's epoch it would keep its disk-recovered one and emit a phantom
   * boundary -> spurious DIVERGED downstream. has_epoch_meta=false leaves the local epoch
   * intact. No-op if @p name is not suspended.
   */
  void ApplyColdRecoveryMeta(std::string_view name, const storage::ColdTenantRecovery &meta) {
    auto wr = std::lock_guard{lock_};
    auto it = suspended_.find(name);
    if (it == suspended_.end()) return;
    // Strong exception guarantee: do all potentially-throwing copies into locals first, then
    // commit them into it->second with noexcept moves. A bad_alloc during any copy leaves
    // it->second untouched (no partial update).
    auto stats = meta.stats;
    if (meta.has_epoch_meta) {
      auto epoch = meta.current_epoch;
      auto hist = meta.epoch_history;  // deque copy — can throw bad_alloc
      // All copies succeeded; commit with noexcept moves.
      it->second.cold_stats = std::move(stats);
      it->second.current_epoch = std::move(epoch);
      it->second.epoch_history = std::move(hist);
      it->second.has_epoch_meta = true;
      // Also adopt MAIN's as-of-suspend LDT. PromoteColdTenants emplaces the
      // promotion boundary as (current_epoch, last_durable_timestamp); without MAIN's LDT a converged
      // replica would pair MAIN's epoch with its OWN local LDT -> a phantom/garbled boundary ts that a
      // downstream replica's continuous-history check can reject (spurious DIVERGED).
      it->second.last_durable_timestamp = meta.last_durable_timestamp;
    } else {
      it->second.cold_stats = std::move(stats);
    }
  }

  /**
   * @brief Snapshot the COLD set for the SystemRecovery payload: one ColdTenantRecovery per suspended
   *        tenant (salient + cold_stats + epoch metadata), built from suspended_. Called on MAIN inside
   *        the system-transaction guard so the COLD set is coherent with the HOT ForEach as-of
   *        forced_group_timestamp, ensuring the snapshot is coherent with the HOT ForEach.
   */
  std::vector<storage::ColdTenantRecovery> SuspendedConfigsForRecovery() const {
    auto rd = std::shared_lock{lock_};
    std::vector<storage::ColdTenantRecovery> out;
    out.reserve(suspended_.size());
    std::ranges::transform(suspended_, std::back_inserter(out), [](const auto &kv) {
      const auto &entry = kv.second;
      return storage::ColdTenantRecovery{.salient = entry.salient,
                                         .stats = entry.cold_stats,
                                         .has_epoch_meta = entry.has_epoch_meta,
                                         .last_durable_timestamp = entry.last_durable_timestamp,
                                         .current_epoch = entry.current_epoch,
                                         .epoch_history = entry.epoch_history};
    });
    return out;
  }

  /**
   * @brief Eager-epoch rewrite for COLD tenants on MAIN promotion.
   *
   * ForEach/All() skip COLD tenants (no-value gatekeeper -> access() nullopt), so the promotion epoch
   * loop never reaches them and a resumed cold tenant would otherwise keep its pre-promotion (disk)
   * epoch -> the replica continuous-history check fails -> spurious DIVERGED after failover. This
   * iterates suspended_ DIRECTLY, appending the promotion boundary (current_epoch, last_durable_ts) to
   * each entry's epoch_history and setting current_epoch = @p new_epoch_id, then rewriting the durable
   * COLD marker. Resume_ later restores history+epoch verbatim. Metadata-only: no force-resume.
   *
   * Lock discipline: takes ONLY lock_ + KVStore Put, never repl_state — the caller
   * (DoToMainPromotion) holds the repl_state write lock, so acquiring it here would deadlock. The
   * reverse edge (lock_ -> repl_state via on_resume_repl_) lives only in the resume path, which this
   * does not use. Older V2 entries (has_epoch_meta == false) are left untouched.
   *
   * @param new_epoch_id the single new epoch id generated for all tenants at promotion
   */
  void PromoteColdTenants(std::string_view new_epoch_id);
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
   * @brief Per-database variants of RestoreTriggers/RestoreStreams + the suspend-time stream stop, used by
   *        the hot/cold suspend/resume arms (SetOnResume / SetOnSuspend / SetRestoreStreams). Unlike the
   *        bulk Restore* above, these operate on ONE already-held DatabaseAccess and take no lock_, so they
   *        are safe to run inside Resume_/Suspend_ (which already hold the gatekeeper freeze / publish).
   */
  static void RestoreTriggersFor(DatabaseAccess db_acc, query::InterpreterContext *ic);

  void RestoreStreamsFor(DatabaseAccess db_acc, query::InterpreterContext *ic) {
    db_acc->streams()->RestoreStreams(db_acc, ic);
  }

  static void StopStreamsFor(DatabaseAccess db_acc) { db_acc->streams()->Shutdown(); }

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
  // Hot/cold: rebuild metadata for a suspended (COLD) tenant. The gatekeeper stays in
  // db_handler_ as a COLD shell (value_ == nullopt); this holds what a later resume needs.
  struct SuspendedEntry {
    storage::SalientConfig salient;  //!< salient config to recreate the storage
    std::filesystem::path rel_dir;   //!< durability dir relative to the instance root
    // Heartbeat metadata captured at suspend time, for replication.
    uint64_t last_durable_timestamp{0};  //!< repl_storage_state_.commit_ts_info_.ldt_ at suspend
    uint64_t num_committed_txns{0};      //!< repl_storage_state_.commit_ts_info_.num_committed_txns_
    // The FULL replication epoch state captured at suspend, so a promotion can rewrite it
    // and a resume can restore it verbatim (a single epoch string is insufficient — the replica
    // continuous-history check needs the whole deque; see PromoteColdTenants + Resume_).
    // Cached here because a COLD tenant has no live storage and therefore no live repl_storage_state_
    // to read from; these values must survive in the durable cold entry so Resume_ can restore epoch
    // continuity and PromoteColdTenants can append the correct boundary while the tenant remains cold.
    std::string current_epoch;            //!< repl_storage_state_.epoch_.id() at suspend (post-promotion: new epoch)
    storage::EpochHistory epoch_history;  //!< copy of repl_storage_state_.history at suspend
    bool has_epoch_meta{false};           //!< false for older V2 cold entries (no epoch fields) — skip override
    storage::StorageInfo cold_stats;      //!< last-hot stats snapshot
    // Why this tenant is COLD. RUNTIME-ONLY (never serialized): a tenant left cold by a failed boot
    // recovery keeps its durable entry HOT, so the next restart retries; this only drives the SHOW
    // marker + log for the current (degraded) process lifetime. A user SUSPEND / restored cold entry is
    // kSuspended.
    enum class ColdReason : uint8_t { kSuspended, kRecoveryFailed, kRecoveryFailedOom };
    ColdReason cold_reason{ColdReason::kSuspended};
  };

  /// @brief Implementation of Suspend. See Suspend() for semantics.
  /// On success records a SuspendDatabase system action on @p txn (if non-null) for ordered replication.
  SuspendResult Suspend_(std::string_view name, system::Transaction *txn = nullptr, bool for_recovery = false);

  /**
   * @brief Implementation of Resume. See Resume() for semantics.
   *
   * rewire_replication: run the post-publish on_resume_repl_ arm (re-wire MAIN-side replication).
   * MUST be false when called from the replica replication-apply path (added in a later commit):
   * that caller already holds the repl_state read lock and on_resume_repl_ would re-take it as a
   * write lock -> self-deadlock on the non-recursive RWSpinLock. Default true keeps the node-local
   * (test + query-seam) callers unchanged.
   *
   * txn: originating system transaction; on success records a ResumeDatabase system action (if
   * non-null) for ordered replication. Default nullptr keeps node-local callers unchanged.
   */
  ResumeResult Resume_(std::string_view name, bool rewire_replication = true, system::Transaction *txn = nullptr);

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

  // Drop a COLD (suspended) tenant: erases suspended_ entry, durable cold marker, on-disk data dir,
  // cold shell, and tenant-profile attachment. Returns the dropped UUID on success, or DeleteError
  // on failure. Possible errors:
  //   NON_EXISTENT — `name` is not in suspended_ (defensive; callers usually guard on .contains()).
  //   USING        — the tenant's gatekeeper is not strictly COLD (RESUMING or SUSPENDING mid-
  //                  transition). A concurrent Resume_ holds the COLD->RESUMING token; erasing the
  //                  gatekeeper now would block ~Gatekeeper waiting for a terminal state while the
  //                  caller holds lock_ -> deadlock. The DROP is rejected as retriable — the resume
  //                  will complete (HOT) or abort (COLD), and a retry or the user can re-issue DROP.
  // Caller must hold lock_ (write).
  std::expected<utils::UUID, DeleteError> DeleteCold_(std::string_view name);

  // Refresh the global cold-databases gauge from the live suspended_ size. Caller MUST hold lock_
  // (every call site already does, or runs before any concurrent reader exists).
  void UpdateColdGauge_() const noexcept {
    metrics::Metrics().global.cold_databases->Set(static_cast<double>(suspended_.size()));
  }

  // Find the suspended (COLD) entry whose salient uuid matches @p uuid, or suspended_.end().
  // Caller MUST hold lock_.
  auto FindSuspendedByUuid_(utils::UUID uuid) {
    return std::ranges::find_if(suspended_, [&](auto const &kv) { return kv.second.salient.uuid == uuid; });
  }

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
    // Cold-access query seam: a suspended (COLD) tenant still has an in-map gatekeeper, but
    // access() refuses (it is not HOT), so db_handler_.Get returned nullopt. Distinguish "exists
    // but suspended" from "truly unknown" with a clear, actionable message. We reuse
    // UnknownDatabaseException so existing fallback catch sites (SetupDefault_, RestoreTenantProfiles_)
    // keep treating a COLD tenant as not-currently-usable; the message is what the user sees.
    if (suspended_.contains(name)) {
      throw SuspendedDatabaseException(
          "Database \"{}\" is suspended (cold); run RESUME DATABASE {} before using it.", name, name);
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
      // Skip non-HOT shells: a COLD (suspended) tenant keeps an in-map gatekeeper, but access()
      // returns nullopt (it is not HOT). Dereferencing it would be UB. A COLD tenant is correctly
      // "not found by UUID" here — the caller falls back to ResumeByUUID, which reheats it from disk.
      if (acc && acc->get()->uuid() == uuid) {
        return std::move(*acc);
      }
    }
    throw UnknownDatabaseException("Tried to retrieve an unknown database with UUID \"{}\".", std::string{uuid});
  }
#endif

#ifdef MG_ENTERPRISE
  mutable LockT lock_{utils::RWLock::Priority::READ};  //!< protective lock
  storage::Config default_config_;                     //!< Storage configuration used when creating new databases
  DatabaseHandler db_handler_;                         //!< multi-tenancy storage handler
  // COLD tenant rebuild metadata; guarded by lock_. The transparent std::less<> comparator is LOAD-BEARING
  // for the Resume_ publish block's noexcept guarantee: that block does suspended_.find(name) (string_view,
  // no temporary std::string -> no allocation) AFTER the gatekeeper has been committed HOT, so it must not
  // throw. Do NOT change this to std::less<std::string> — find(string_view) would then construct a
  // std::string key and could throw bad_alloc inside the noexcept window.
  std::map<std::string, SuspendedEntry, std::less<>> suspended_;
  // TODO: move to be common
  std::unique_ptr<kvstore::KVStore> durability_;     //!< list of active dbs (pointer so we can postpone its creation)
  std::unique_ptr<TenantProfiles> tenant_profiles_;  //!< per-DB resource profiles (created after durability_)
  std::function<void(DatabaseAccess)> on_resume_;    //!< pre-publish resume arm (triggers/streams/TTL); empty default
  std::function<void(DatabaseAccess)> on_suspend_;   //!< pre-teardown suspend arm (stop streams); empty default
  std::function<void(DatabaseAccess)>
      restore_streams_;  //!< streams-only restore (undo a stopped suspend); empty default
  std::function<void(DatabaseAccess)> on_resume_repl_;  //!< post-publish resume arm (replication); empty default
#endif
#ifndef MG_ENTERPRISE
  mutable utils::Gatekeeper<Database> db_gatekeeper_;  //!< Single databases gatekeeper
#endif
};  // namespace memgraph::dbms

}  // namespace memgraph::dbms
