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

// Retry/timeout knobs for Resume_'s single-flight loser loop. Defaults are the production values;
// a test can shrink them (via the DbmsHandler constructor or SetResumeRetryPolicy) to exercise the
// liveness-window / absolute-ceiling paths in milliseconds instead of the ~minutes the production
// constants require.
struct ResumeRetryPolicy {
  std::chrono::milliseconds winner_liveness_window = std::chrono::seconds(30);
  std::chrono::milliseconds max_wait = std::chrono::minutes(10);
};

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
   * @brief Outcome of the user-facing Resume() wrapper: the live accessor plus whether the tenant
   *        was ALREADY hot when Resume() was called (a no-op share of the existing accessor, see the
   *        Phase A early-exit in Resume_) versus an actual COLD -> HOT rebuild.
   *
   * Kept separate from the plain ResumeResult (still used by Resume_/ResumeByUUID/ResumeForRecovery)
   * so the RPC-apply and recovery paths — which only care about has_value()/error() — are untouched;
   * only the query-facing Resume() needs to distinguish the two success shapes for the UX (#18).
   */
  struct ResumeOutcome {
    DatabaseAccess db;
    bool already_hot;  //!< true: tenant was already HOT before this call (idempotent no-op)
  };

  using ResumeOutcomeResult = std::expected<ResumeOutcome, ResumeError>;

  /**
   * @brief Initialize the handler.
   *
   * @param config storage configuration
   * @param resume_retry_policy Resume_'s retry/timeout knobs; defaults to production values. Exposed
   *        here so a test can construct a handler with tightened knobs directly; SetResumeRetryPolicy
   *        remains available to reconfigure a live handler mid-flight.
   */
  DbmsHandler(storage::Config config, ResumeRetryPolicy resume_retry_policy = {});
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
    auto wr = std::unique_lock{lock_};
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
    // Defer drop. `db` (this function's own live DatabaseAccess) stays held across this call, so the
    // gatekeeper's count_ is >= 1 throughout every one of Delete_'s three phases -- its Phase 3
    // DeferDelete -> try_delete() (which needs count_ == 1, only its own accessor) deterministically
    // takes the defer branch, exactly the outcome this call site relied on before the phases existed.
    (void)Delete_(db->name(), wr);
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
  /// Per-call cooperative-cancel hook for a DROP. Invoked from Delete_'s Phase 2, OFF-lock (`lock_` is
  /// not held), so it may take locks of its own -- this is precisely why it cannot live in Phase 1 or
  /// Phase 3, both of which run under `lock_`. Its job is to *ask* current holders of the tenant's
  /// DatabaseAccess to release it (e.g. terminate the sessions/transactions pinning it); it must never
  /// revoke one itself -- a trigger cursor or a Bolt session mid-Prepare/Pull would use-after-free if
  /// its accessor were released out from under it. Today Delete_ calls it exactly once; it must
  /// nonetheless be idempotent, because a bounded-wait loop that re-asks each iteration is the intended
  /// next step and because a retried drop calls it again on the same tenant. May throw: a throw unwinds
  /// through Delete_'s `rollback_drain` guard, which makes the drop retriable, so this must run before
  /// anything about the drop is latched.
  ///
  /// This is a per-call parameter, not a SetOnSuspend-style member hook, because the query layer's
  /// sweep is scoped to the dropping user's privileges (it needs a QueryUserOrRole* and a
  /// TRANSACTION_MANAGEMENT privilege checker for THIS call) -- a process-wide member hook cannot carry
  /// per-call session state without either making termination unconditional or storing a
  /// session-owned pointer process-wide.
  using CooperativeCancelFn = std::function<void()>;

  // Outcome of Delete_'s Phase 2 bounded drain wait (see DrainRequest below). NOT_REQUESTED is the
  // report's default-constructed value, distinguishing "no DrainRequest was passed" from an actual
  // wait outcome for a caller that always reads a DrainReport regardless of whether it supplied one.
  enum class DrainOutcome : uint8_t { NOT_REQUESTED, CONVERGED, EXPIRED };

  // Counts the query layer can attribute at expiry. Numbers only: the operator-facing wording lives in
  // the query layer, which owns user-visible text.
  struct DrainBlockers {
    uint64_t transactions_asked_to_abort{0};
    bool probe_ran{false};
  };

  struct DrainReport {  // caller-owned; Delete_ only writes it
    DrainOutcome outcome{DrainOutcome::NOT_REQUESTED};
    std::chrono::milliseconds waited{0};
    uint64_t holders_remaining{0};
    DrainBlockers blockers{};
  };

  // Diagnostic-only holder breakdown, sampled once at expiry (see AwaitDrain_'s catch(...) around this
  // call -- an escaping throw here must not fail an otherwise-honoured, merely-expired drop).
  using HolderProbeFn = std::function<DrainBlockers()>;

  // Opt-in bounded wait for Delete_'s Phase 2 (off-lock) window: after asking outside holders to
  // release (cooperative_cancel, looped), wait up to `deadline` for the tenant's own drop accessor to
  // become the sole holder before proceeding to Phase 3's DeferDelete. A null `drain` (the default on
  // every Delete/Delete_ overload) skips the wait entirely -- this is how the replica-apply path
  // (Delete(uuid) -> Delete_) stays excluded from the deadline by construction.
  struct DrainRequest {
    std::chrono::milliseconds deadline{};
    HolderProbeFn probe{};
    DrainReport *report{nullptr};
  };

  static constexpr std::chrono::milliseconds kDrainDeadline{10'000};
  static constexpr std::chrono::milliseconds kDrainPollSlice{50};

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
   * Replica-apply path (no session/user context available here), so it always runs with an empty
   * cooperative-cancel callback -- deliberate, not an oversight; see CooperativeCancelFn's doc above.
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
   * @param cooperative_cancel see CooperativeCancelFn's doc above
   * @param drain optional bounded Phase-2 drain wait (see DrainRequest's doc above); nullptr (default)
   *        skips the wait, exactly like every call site before this parameter existed.
   * @return DeleteResult error on failure
   */
  DeleteResult Delete(std::string_view db_name, system::Transaction *transaction,
                      CooperativeCancelFn cooperative_cancel = {}, DrainRequest const *drain = nullptr);

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
   * @brief Reconfigure Resume_'s retry/timeout knobs (see ResumeRetryPolicy) on a live handler. The
   *        constructor also accepts a ResumeRetryPolicy for construction-time injection; this setter
   *        exists for callers that need to mutate the policy mid-flight (e.g. a test that shrinks the
   *        knobs to exercise the bounded single-flight loser-retry path deterministically in
   *        milliseconds instead of minutes).
   */
  void SetResumeRetryPolicy(ResumeRetryPolicy policy) { resume_retry_policy_ = policy; }

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
   * @return ResumeOutcomeResult — the HOT DatabaseAccess plus an already_hot flag on success (true
   *         iff the tenant was already HOT — the Phase A early-exit in Resume_ — rather than an
   *         actual COLD -> HOT rebuild), or an error. Distinguishing the two lets the query-facing
   *         RESUME DATABASE surface an "already resumed" notification instead of a plain success (#18).
   */
  ResumeOutcomeResult Resume(std::string_view name, system::Transaction *txn = nullptr) {
    bool already_hot = false;
    auto result = Resume_(name, txn, &already_hot);
    if (!result) return std::unexpected{result.error()};
    return ResumeOutcome{.db = std::move(*result), .already_hot = already_hot};
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
   * @brief Resume a tenant identified by UUID (replica-apply path for ResumeDatabaseRpc).
   *
   * Resolves the UUID to a name from the suspended-set, then delegates to Resume_.
   *
   * @return ResumeResult — the HOT DatabaseAccess on success, NON_EXISTENT if no COLD tenant has
   *         this UUID (e.g. it was never suspended, or was already resumed by a racing caller).
   */
  ResumeResult ResumeByUUID(utils::UUID uuid, system::Transaction *txn = nullptr);

  /**
   * @brief The UUID of the COLD shell currently held under @p name, or nullopt if @p name is not
   * suspended here.
   *
   * suspended_ is keyed by name (most lookups — USE/SHOW/Drop/Resume — arrive with a name; the
   * durable cold marker is name-keyed too), so this is the single name->cold primitive. Returning the
   * uuid (rather than a bare bool) lets a caller distinguish, in ONE lookup, the same COLD tenant
   * (uuid matches -> refresh its metadata) from one MAIN drop+recreated under the same name while this
   * replica kept the OLD shell COLD (uuid differs -> stale shell, must be dropped and rebuilt;
   * otherwise per-uuid Suspend/Resume RPC for the new uuid misses forever and the replica stays
   * permanently BEHIND). The replica SystemRecovery reconcile branches on exactly that.
   */
  std::optional<utils::UUID> GetColdUuid(std::string_view name) const {
    auto rd = std::shared_lock{lock_};
    auto it = suspended_.find(name);
    if (it == suspended_.end()) return std::nullopt;
    return it->second.salient.uuid;
  }

  /**
   * @brief True iff @p name currently holds a COLD shell (is in the suspended-set). Thin predicate
   * over GetColdUuid for call sites that only need existence (e.g. tests, SHOW branching).
   */
  bool IsSuspended(std::string_view name) const { return GetColdUuid(name).has_value(); }

 private:
  // Find the suspended (COLD) entry whose salient uuid matches @p uuid, or suspended_.end().
  // Caller MUST hold lock_. (Defined here, ahead of IsKnownTenant, because the deduced return type
  // must be seen before the call site.)
  auto FindSuspendedByUuid_(utils::UUID uuid) {
    return std::ranges::find_if(suspended_, [&](auto const &kv) { return kv.second.salient.uuid == uuid; });
  }

  // Find the HOT (in-memory, live) gatekeeper whose uuid matches @p uuid, or db_handler_.end(). The
  // HOT-by-uuid partner to FindSuspendedByUuid_: a COLD (suspended) tenant keeps an in-map gatekeeper
  // whose access() is nullopt, so it is correctly skipped. Caller MUST hold lock_ (shared or exclusive).
  auto FindHotByUuid_(utils::UUID uuid) {
    return std::ranges::find_if(db_handler_, [&](auto &kv) {
      auto acc = kv.second.access();
      return acc && acc->get()->uuid() == uuid;
    });
  }

 public:
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
    return FindHotByUuid_(uuid) != db_handler_.end() || FindSuspendedByUuid_(uuid) != suspended_.end();
  }

  /**
   * @brief Atomic, de-duplicated HOT ∪ COLD ∪ DETACHED tenant set for cold-aware SHOW DATABASES.
   *
   * All()/ForEach skip COLD shells, so a suspended tenant would otherwise vanish from SHOW DATABASES.
   * This reads db_handler_ (HOT) and suspended_ (COLD) under a SINGLE shared_lock and returns each
   * tenant once as (name, state). De-dup matters: during the SUSPENDING transient a tenant is briefly
   * in BOTH db_handler_ (value_ still live -> passes Handler::All()) and suspended_, because
   * Suspend_ inserts into suspended_ under lock_ but calls finish_suspend() (which nulls value_) AFTER
   * releasing the lock. suspended_ takes precedence (the tenant is on its way COLD), so it is listed
   * once, as COLD — never duplicated.
   *
   * Also appends one DETACHED row per detached_ name not yet in out — out already holds everything
   * appended above (HOT/COLD and earlier DETACHED rows), so a HOT/COLD row always wins and, among
   * several detached_ entries sharing a name (DROP x -> CREATE x -> DROP x again; detached_ is
   * uuid-keyed), only the first is listed: a duplicate name would just repeat in this name-keyed
   * listing. TenantMemorySum()/AllDetached() still count every one, keyed by uuid, so completeness
   * isn't lost. DETACHED is deliberate, not a failure — a live accessor delayed teardown — so it
   * never triggers a WARN/health-downgrade (see interpreter.cpp).
   */
  std::vector<std::pair<std::string, std::string>> AllWithHotColdStatus() const {
    auto rd = std::shared_lock{lock_};
    std::vector<std::pair<std::string, std::string>> out;
    out.reserve(suspended_.size() + db_handler_.size());
    // Every suspended tenant is a user-initiated (or restored) COLD shell: a HOT recovery that fails at
    // boot aborts the process, so a degraded "recovery failed" tenant can never appear here.
    for (const auto &[name, entry] : suspended_) {
      out.emplace_back(name, "COLD");
    }
    for (auto &name : db_handler_.All()) {  // HOT names only (Handler::All() skips no-value shells)
      if (!suspended_.contains(name)) out.emplace_back(std::move(name), "HOT");
    }
    auto dg = std::lock_guard{detached_lock_};
    for (auto const &d : detached_) {
      if (std::ranges::none_of(out, [&](auto const &kv) { return kv.first == d.name; })) {
        // A DRAINING row's gatekeeper is still IN db_handler_, but prepare_for_deletion() already
        // marked it, so the HOT loop above never listed it (db_handler_.All() skips a marked-for-
        // deletion entry) -- this dedup still resolves to exactly one row, just with the phase-
        // appropriate label instead of always "DETACHED".
        out.emplace_back(d.name, d.phase == TenantPhase::DRAINING ? "DRAINING" : "DETACHED");
      }
    }
    return out;
  }

  // Sole enumerator today: DROP is the only caller of RecordDetached_, firing when DeferDelete hands
  // a live Accessor's Gatekeeper to a drain thread instead of deleting it inline.
  enum class DetachReason : uint8_t { DROP };

  // Lifecycle phase of a deferred-destruction row (see DetachedTenant below). Delete_ publishes a row
  // as DRAINING in its Phase 1 (still under lock_, before the off-lock teardown window) and promotes
  // it to DETACHED in Phase 3, via PromoteDetachedPhase_, once DeferDelete has actually erased the
  // gatekeeper from db_handler_.
  //   DRAINING — accepted for deletion, still IN db_handler_ (is_marked_for_deletion + draining_ both
  //              set, so it's unaddressable for new work but not yet unaddressable by name/iteration).
  //   DETACHED — handed to a drain thread (or already destroyed inline); erased from db_handler_.
  enum class TenantPhase : uint8_t { DRAINING, DETACHED };

  /**
   * @brief Metadata for a tenant whose destruction is deferred.
   *
   * Not every row is unaddressable by name: a DRAINING row's gatekeeper is still IN db_handler_ (see
   * TenantPhase above) — only once it reaches DETACHED has db_handler_ actually erased it (see
   * Handler<T>::DeferDelete), the point after which its Database — and its bytes in
   * utils::graph_memory_tracker — stays alive purely via this row until the drain thread's last
   * accessor releases it.
   *
   * memory_at_detach is an AS-OF-DETACH snapshot, not a live figure: a live read would need a raw
   * Database* that the drain thread destroys with no lock held, the UAF class this registry avoids.
   * holders_at_detach is diagnostic only — the count observed just before the drop attempt, not
   * necessarily the count that forced the defer, since an Accessor can be minted/released between that
   * read and DeferDelete's own try_delete() check.
   */
  struct DetachedTenant {
    std::string name;  //!< name at detach; may be re-taken by a new tenant while this one drains
    utils::UUID uuid;  //!< identity; unique, survives name reuse
    std::chrono::system_clock::time_point detached_at;
    DetachReason reason;
    TenantPhase phase;  //!< DRAINING until PromoteDetachedPhase_ runs in Delete_'s Phase 3
    uint64_t holders_at_detach;
    int64_t memory_at_detach;
  };

  /// Metadata for every tenant whose destruction is still deferred. Mints NO accessor.
  std::vector<DetachedTenant> AllDetached() const {
    auto rd = std::shared_lock{lock_};
    auto dg = std::lock_guard{detached_lock_};
    return detached_;
  }

  struct TenantMemorySums {
    int64_t hot;
    int64_t detached;
  };

  /**
   * @brief Sigma tenant memory, split into the addressable (HOT) and detached halves.
   *
   * db_handler_ alone under-reports: a force-dropped tenant leaves every by-name surface immediately
   * (Handler<T>::DeferDelete's unconditional items_.erase), while its bytes stay parented into
   * utils::graph_memory_tracker until the last accessor is released. The HOT half deliberately walks
   * db_handler_ the same access()-gated way ForEach does (a COLD shell's access() is nullopt and
   * contributes 0 — a COLD tenant has no in-memory storage, so that is correct, not an omission).
   *
   * A DRAINING tenant (still IN db_handler_, see TenantPhase) is likewise not double-counted: its
   * gatekeeper's plain access() is refused while draining_ is set (Gatekeeper::access()'s hard
   * refusal), so the HOT half contributes 0 for it and its bytes come solely from its detached_ row —
   * exactly one count, whether the row is DRAINING or DETACHED.
   */
  TenantMemorySums TenantMemorySum() {  // NOT const: the HOT half mints (and drops) accessors
    auto rd = std::shared_lock{lock_};
    TenantMemorySums sums{};
    for (auto &[_, db_gk] : db_handler_) {
      if (auto db_acc = db_gk.access()) sums.hot += (*db_acc)->DbMemoryUsage();
    }
    auto dg = std::lock_guard{detached_lock_};
    for (auto const &d : detached_) sums.detached += d.memory_at_detach;
    return sums;
  }

  /// Minimal projection of a COLD tenant for SHOW STORAGE INFO ON <cold> (previously this errored).
  struct ColdShowInfo {
    utils::UUID uuid;
    storage::StorageInfo stats;  //!< as-of-suspend snapshot (on MAIN); physical fields are MAIN-relative
    std::string state;           //!< HOT/COLD state string surfaced by SHOW STORAGE INFO ON (always "COLD" here)
  };

  /**
   * @brief Fetch a COLD tenant's as-of-suspend snapshot by name (nullopt if not suspended).
   *
   * Lets SHOW STORAGE INFO ON <cold> serve the durable cold_stats instead of tripping the Get_ cold
   * seam. The numbers are MAIN's as-of-suspend snapshot, labeled as such by the caller.
   */
  std::optional<ColdShowInfo> GetColdShowInfo(std::string_view name) const {
    auto rd = std::shared_lock{lock_};
    if (auto it = suspended_.find(name); it != suspended_.end()) {
      // A suspended tenant always carries a captured as-of-suspend snapshot (a failed HOT recovery
      // aborts the boot rather than leaving a snapshot-less COLD shell behind).
      return ColdShowInfo{.uuid = it->second.salient.uuid, .stats = it->second.cold_stats, .state = "COLD"};
    }
    return std::nullopt;
  }

  /**
   * @brief Resume a COLD tenant during replica SystemRecovery.
   *
   * Called during SystemRecovery when MAIN's incoming HOT config names a tenant this replica holds
   * COLD: Update() would throw on the COLD shell, so it must be resumed first.
   */
  ResumeResult ResumeForRecovery(std::string_view name) { return Resume_(name); }

  /**
   * @brief Local uuid of a HOT tenant by name; nullopt if absent or COLD.
   *
   * COLD shells return nullopt (GetConfig → access() refuses non-HOT). The SystemRecovery handler uses
   * this to abort a stale cached 2PC commit accessor (keyed by the LOCAL uuid) BEFORE a drop frees the
   * tenant's storage — only a HOT tenant has live storage a cached accessor could dangle on, so the
   * HOT-only answer is exactly the right scope.
   */
  std::optional<utils::UUID> GetHotUuid(std::string_view name) {
    auto rd = std::shared_lock{lock_};
    if (auto conf = db_handler_.GetConfig(name)) return conf->salient.uuid;
    return std::nullopt;
  }

  /**
   * @brief Force-suspend a tenant during replica recovery, bypassing the durability-complete gate.
   * The gate protects a USER-initiated SUSPEND; in recovery MAIN is authoritative for the cold set
   * and this replica converges to it regardless. Recoverability of the resulting local cold shell
   * then depends entirely on this replica's own periodic-snapshot+WAL durability state, not on a
   * suspend-time snapshot (suspend does not take one). Bypassing avoids a BEHIND retry loop on a
   * replica whose durability config differs from MAIN's.
   */
  SuspendResult SuspendForRecovery(std::string_view name) {
    return Suspend_(name, /*txn=*/nullptr, /*for_recovery=*/true);
  }

  /**
   * @brief Apply MAIN's authoritative cold stats to a suspended tenant during recovery.
   *
   * Overwrites cold_stats with MAIN's as-of-suspend snapshot so a SystemRecovery-converged cold
   * tenant serves the same SHOW STORAGE INFO / SHOW DATABASES numbers as MAIN. No-op if @p name is
   * not suspended. After mutating the in-memory entry it rewrites the durable COLD marker (best-effort,
   * mirroring Suspend_) so the refreshed stats survive a restart. Defined out-of-line because the
   * durable-write helpers live in the .cpp.
   */
  void ApplyColdRecoveryMeta(std::string_view name, const storage::ColdTenantRecovery &meta);

  /**
   * @brief Snapshot the COLD set for the SystemRecovery payload: one ColdTenantRecovery per suspended
   *        tenant (salient + cold_stats), built from suspended_. Called on MAIN inside the
   *        system-transaction guard so the COLD set is coherent with the HOT ForEach as-of
   *        forced_group_timestamp.
   */
  std::vector<storage::ColdTenantRecovery> SuspendedConfigsForRecovery() const {
    auto rd = std::shared_lock{lock_};
    std::vector<storage::ColdTenantRecovery> out;
    out.reserve(suspended_.size());
    std::ranges::transform(suspended_, std::back_inserter(out), [](const auto &kv) {
      const auto &entry = kv.second;
      return storage::ColdTenantRecovery{.salient = entry.salient, .stats = entry.cold_stats};
    });
    return out;
  }
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
    storage::SalientConfig salient;   //!< salient config to recreate the storage
    std::filesystem::path rel_dir;    //!< durability dir relative to the instance root
    storage::StorageInfo cold_stats;  //!< last-hot stats snapshot (cold SHOW STORAGE INFO display cache)
  };

  /// @brief Implementation of Suspend. See Suspend() for semantics.
  /// On success records a SuspendDatabase system action on @p txn (if non-null) for ordered replication.
  SuspendResult Suspend_(std::string_view name, system::Transaction *txn = nullptr, bool for_recovery = false);

  /**
   * @brief Implementation of Resume. See Resume() for semantics.
   *
   * txn: originating system transaction; on success records a ResumeDatabase system action (if
   * non-null) for ordered replication. Default nullptr keeps node-local callers unchanged.
   *
   * already_hot: optional out-param, set on every success return — true iff the Phase A early-exit
   * fired (tenant was already HOT, idempotent no-op share of the existing accessor), false for an
   * actual COLD -> HOT rebuild (either as the winner or as a loser sharing the winner's publish).
   * Left untouched on an error return. Default nullptr for callers that don't care (ResumeByUUID,
   * ResumeForRecovery, and Resume_'s own internal restarts).
   */
  ResumeResult Resume_(std::string_view name, system::Transaction *txn = nullptr, bool *already_hot = nullptr);

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
  //
  // Three-phase drop, split around a deliberate off-lock window for the two unbounded thread joins
  // it runs (StopAllBackgroundTasks / streams()->DropAll()):
  //   Phase 1 (under `lock`)     — validate, own the name, begin_drain(), publish a DRAINING row.
  //   Phase 2 (`lock` released) — run the joins; nothing reachable here may take lock_.
  //   Phase 3 (under `lock`)    — re-validate, promote DRAINING -> DETACHED, hand off via DeferDelete.
  // CONTRACT: `lock` is held EXCLUSIVE on entry and on every return path, including every error
  // return. Delete_ unlocks/relocks it internally for Phase 2; the caller's lock object is left in
  // the SAME (locked) state either way, as if this were one uninterrupted critical section.
  // `cooperative_cancel`: see CooperativeCancelFn's doc at the Delete() overload that takes it. Called
  // exactly once, from Phase 2 -- see RequestCooperativeCancel_ -- plus once per AwaitDrain_ poll
  // iteration when `drain` is non-null (see AwaitDrain_'s doc).
  // `drain`: nullptr (default) skips the bounded Phase-2 wait entirely -- see DrainRequest's doc.
  DeleteResult Delete_(std::string_view db_name, std::unique_lock<LockT> &lock,
                       CooperativeCancelFn const &cooperative_cancel = {}, DrainRequest const *drain = nullptr);

  // Phase-2 helper: ask `cooperative_cancel` (if any) to release outside holders, then latch this
  // tenant's after-commit triggers stopped. Order is load-bearing: step 1 can throw, and at the point
  // it runs nothing about the drop is latched yet, so Delete_'s `rollback_drain` guard can still make
  // the drop retriable; step 2 is noexcept and one-way, so it must run last. May be called more than
  // once for the same `database` (see CooperativeCancelFn's idempotence requirement).
  static void RequestCooperativeCancel_(Database *database, CooperativeCancelFn const &cooperative_cancel);

  // Phase-2 helper: bounded wait for `drain.deadline`, re-sweeping `cooperative_cancel` every
  // kDrainPollSlice, until `gk` reports only the drop's own drain_bypass accessor still live (or the
  // deadline expires). Writes the outcome to `drain.report` if non-null. Runs entirely off `lock_` --
  // see AwaitDrain_'s definition doc for why holder_count()'s "diagnostics only" caveat does not apply
  // here, and why the wait must fail (expire) rather than hang.
  void AwaitDrain_(utils::Gatekeeper<Database> *gk, Database *database, CooperativeCancelFn const &cooperative_cancel,
                   DrainRequest const &drain);

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

  // Cold-tenant fast path shared by every Delete/TryDelete overload: if `name` is currently in
  // suspended_, drop it via DeleteCold_ (bypassing the HOT gatekeeper path, which would otherwise
  // return NON_EXISTENT for a no-value shell) and return the DeleteResult. Records a DropDatabase
  // system action only when `transaction` is non-null, matching each call site's prior behaviour.
  // Returns std::nullopt when `name` is NOT suspended — the caller should fall through to the HOT
  // path in that case. Caller must hold lock_ (write).
  std::optional<DeleteResult> TryDeleteColdFastPath_(std::string_view name, system::Transaction *transaction);

  /// Publish a deferred-destruction row. Caller MUST hold lock_ exclusive (Delete_ does).
  void RecordDetached_(DetachedTenant row) {
    auto dg = std::lock_guard{detached_lock_};
    detached_.push_back(std::move(row));
  }

  /// Retire the row once the destruction has actually completed. Runs on the drain thread with NO
  /// lock held, or inline on the Delete_ thread with lock_ still held exclusive — so it may take
  /// ONLY detached_lock_.
  void ForgetDetached_(const utils::UUID &uuid) {
    auto dg = std::lock_guard{detached_lock_};
    std::erase_if(detached_, [&](DetachedTenant const &d) { return d.uuid == uuid; });
  }

  /// DRAINING -> DETACHED. Caller MUST hold lock_ exclusive (Delete_'s Phase 3 does, right after
  /// re-validating the drain is still live); takes ONLY detached_lock_ itself, same contract as
  /// RecordDetached_/ForgetDetached_. No-op if the row is already gone.
  void PromoteDetachedPhase_(const utils::UUID &uuid) {
    auto dg = std::lock_guard{detached_lock_};
    auto it = std::ranges::find_if(detached_, [&](DetachedTenant const &d) { return d.uuid == uuid; });
    if (it != detached_.end()) it->phase = TenantPhase::DETACHED;
  }

  // Refresh the global cold-databases gauge from the live suspended_ size. Caller MUST hold lock_
  // (every call site already does, or runs before any concurrent reader exists).
  void UpdateColdGauge_() const noexcept {
    metrics::Metrics().global.cold_databases->Set(static_cast<double>(suspended_.size()));
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
    // A COLD (suspended) tenant keeps an in-map gatekeeper, but access() returns nullopt (it is not
    // HOT), so FindHotByUuid_ skips it: a COLD tenant is correctly "not found by UUID" here, and a
    // data delta for it raises UnknownDatabaseException so the replica fails that delta for MAIN to
    // recover (it is NOT reheated inline — see GetDatabaseAccessor).
    auto it = FindHotByUuid_(uuid);  // TODO Speed up (linear scan)
    if (it == db_handler_.end()) {
      throw UnknownDatabaseException("Tried to retrieve an unknown database with UUID \"{}\".", std::string{uuid});
    }
    // PRE-EXISTING hazard, not introduced here: FindHotByUuid_'s own access() call (above, to test the
    // uuid match) is a SEPARATE mint from this one, and Get(uuid) only takes lock_ SHARED -- a
    // concurrent Suspend_ can run try_begin_suspend() (also under only a shared lock_, see
    // dbms_handler.cpp) and flip HOT -> SUSPENDING in the gap between the two, so this second access()
    // can legitimately return nullopt. Guard it like the sibling Get_(name) overload instead of
    // dereferencing an empty optional.
    auto db = it->second.access();
    if (!db) {
      throw UnknownDatabaseException("Tried to retrieve an unknown database with UUID \"{}\".", std::string{uuid});
    }
    return std::move(*db);
  }
#endif

#ifdef MG_ENTERPRISE
  mutable LockT lock_{utils::RWLock::Priority::READ};  //!< protective lock
  storage::Config default_config_;                     //!< Storage configuration used when creating new databases
  // LOAD-BEARING placement, four reasons, all tied to destruction order:
  //  a) Declared BEFORE db_handler_ so they destruct AFTER it (reverse declaration order): ~Handler
  //     (inside db_handler_'s own destruction) JOINS the drain threads, whose post-delete callback
  //     calls ForgetDetached_ and needs detached_/detached_lock_ still alive. Mirrors the
  //     items_-before-deferred_ note in handler.hpp. Don't reorder, and don't insert another
  //     callback-owning member between them.
  //  b) Separate mutex, not lock_: the callback runs INLINE on Delete_'s thread when
  //     Gatekeeper::Accessor::try_delete() succeeds, and Delete_ already holds lock_ EXCLUSIVE — a
  //     non-recursive pthread rwlock, so reusing it would self-deadlock. detached_lock_ works on both
  //     the inline and the drain-thread path.
  //  c) Lock order is lock_ -> detached_lock_, never taken the other way round. Readers hold both
  //     (lock_ shared, outermost) so a row can't be observed for a tenant the inline path already
  //     destroyed synchronously. Don't "optimize" a reader to take detached_lock_ alone.
  //  d) The callback captures a raw DbmsHandler* `this`, safe only because ~Handler's join runs as
  //     part of ~DbmsHandler, so `this` stays a live (mid-destruction) object throughout the join.
  mutable std::mutex detached_lock_;      //!< guards detached_; see lock-order note above
  std::vector<DetachedTenant> detached_;  //!< metadata-only registry of deferred-destruction tenants
  DatabaseHandler db_handler_;            //!< multi-tenancy storage handler
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
      restore_streams_;                      //!< streams-only restore (undo a stopped suspend); empty default
  ResumeRetryPolicy resume_retry_policy_{};  //!< Resume_ retry/timeout knobs; test-overridable, production defaults
#endif
#ifndef MG_ENTERPRISE
  mutable utils::Gatekeeper<Database> db_gatekeeper_;  //!< Single databases gatekeeper
#endif
};  // namespace memgraph::dbms

}  // namespace memgraph::dbms
