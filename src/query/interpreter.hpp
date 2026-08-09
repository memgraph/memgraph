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

#include <gflags/gflags.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

#include "dbms/database.hpp"
#include "dbms/database_protector.hpp"
#include "flags/run_time_configurable.hpp"
#include "memory/db_arena_fwd.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/plan_v2/frontend/query_planner_context.hpp"
#include "query/stream.hpp"
#include "query/trigger_context.hpp"
#include "system/transaction.hpp"
#include "utils/event_trigger.hpp"
#include "utils/memory.hpp"
#include "utils/priorities.hpp"
#include "utils/session_context.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

#ifdef MG_ENTERPRISE
#include "coordination/instance_status.hpp"
#include "coordination/replication_lag_info.hpp"
#include "coordination/utils.hpp"
#include "metrics/scoped_gauge.hpp"
#include "utils/resource_monitoring.hpp"
#endif

namespace memgraph::query {

class FineGrainedAuthChecker;
struct CachedFineGrainedAuth;

struct QueryAllocator {
  explicit QueryAllocator(utils::MemoryTracker *db_query_tracker = nullptr)
      : tracked_memory_{db_query_tracker}, upstream_{&tracked_memory_} {}

  QueryAllocator(QueryAllocator const &) = delete;
  QueryAllocator &operator=(QueryAllocator const &) = delete;

  // No move addresses to pool & monotonic fields must be stable
  QueryAllocator(QueryAllocator &&) = delete;
  QueryAllocator &operator=(QueryAllocator &&) = delete;

  auto resource() -> utils::MemoryResource * {
#ifndef MG_MEMORY_PROFILE
    return &pool;
#else
    return &upstream_;
#endif
  }

  auto resource_without_pool() -> utils::MemoryResource * {
#ifndef MG_MEMORY_PROFILE
    return &monotonic;
#else
    return &upstream_;
#endif
  }

  auto resource_without_pool_or_mono() -> utils::MemoryResource * { return &upstream_; }

 private:
  // At least one page to ensure not sharing page with other subsystems
  static constexpr auto kMonotonicInitialSize = 4UL * 1024UL;
  // TODO: need to profile to check for good defaults, also maybe PoolResource
  //  needs to be smarter. We expect more reuse of smaller objects than larger
  //  objects. 64*1024B is maybe wasteful, whereas 256*32B maybe sensible.
  //  Depends on number of small objects expected.
  static constexpr auto kPoolBlockPerChunk = 64UL;
  static constexpr auto kPoolMaxBlockSize = 1024UL;

  utils::TrackingMemoryResource tracked_memory_{nullptr};
  utils::ResourceWithOutOfMemoryException upstream_{&tracked_memory_};
#ifndef MG_MEMORY_PROFILE
  memgraph::utils::MonotonicBufferResource monotonic{kMonotonicInitialSize, &upstream_};
  memgraph::utils::PoolResource<> pool{kPoolBlockPerChunk, &monotonic, &upstream_};
#endif
};

struct ThreadSafeQueryAllocator {
  explicit ThreadSafeQueryAllocator(utils::MemoryTracker *db_query_tracker = nullptr)
      : tracked_memory_{db_query_tracker}, upstream_{&tracked_memory_}, monotonic{kMonotonicInitialSize, &upstream_} {}

  ~ThreadSafeQueryAllocator() = default;

  ThreadSafeQueryAllocator(ThreadSafeQueryAllocator const &) = delete;
  ThreadSafeQueryAllocator &operator=(ThreadSafeQueryAllocator const &) = delete;

  // No move addresses to pool & monotonic fields must be stable
  ThreadSafeQueryAllocator(ThreadSafeQueryAllocator &&) = delete;
  ThreadSafeQueryAllocator &operator=(ThreadSafeQueryAllocator &&) = delete;

  auto resource() -> utils::MemoryResource * { return &pool; }

 private:
  static constexpr auto kMonotonicInitialSize = 4UL * 1024UL * 1024UL;
  static constexpr auto kPoolBlockPerChunk = 255;
  static constexpr auto kPoolMaxBlockSize = 1024UL;

  utils::TrackingMemoryResource tracked_memory_{nullptr};
  utils::ResourceWithOutOfMemoryException upstream_{&tracked_memory_};
  memgraph::utils::ThreadSafeMonotonicBufferResource monotonic;
  memgraph::utils::PoolResource<utils::impl::ThreadSafePool> pool{kPoolBlockPerChunk, &monotonic, &upstream_};
};

struct InterpreterContext;

inline constexpr size_t kExecutionMemoryBlockSize = 1UL * 1024UL * 1024UL;
inline constexpr size_t kExecutionPoolMaxBlockSize = 1024UL;  // 2 ^ 10

enum class QueryHandlerResult { COMMIT, ABORT, NOTHING };

#ifdef MG_ENTERPRISE
class CoordinatorQueryHandler {
 public:
  CoordinatorQueryHandler() = default;
  virtual ~CoordinatorQueryHandler() = default;

  CoordinatorQueryHandler(const CoordinatorQueryHandler &) = default;
  CoordinatorQueryHandler &operator=(const CoordinatorQueryHandler &) = default;

  CoordinatorQueryHandler(CoordinatorQueryHandler &&) = default;
  CoordinatorQueryHandler &operator=(CoordinatorQueryHandler &&) = default;

  struct MainReplicaStatus {
    std::string_view name;
    std::string_view socket_address;
    bool alive;
    bool is_main;

    MainReplicaStatus(std::string_view name, std::string_view socket_address, bool alive, bool is_main)
        : name{name}, socket_address{socket_address}, alive{alive}, is_main{is_main} {}
  };

  /// @throw QueryRuntimeException if an error occurred.
  virtual void RegisterReplicationInstance(std::string_view bolt_server, std::string_view management_server,
                                           std::string_view replication_server, std::string_view instance_name,
                                           CoordinatorQuery::SyncMode sync_mode) = 0;

  /// @throw QueryRuntimeException if an error occurred.
  virtual void UnregisterInstance(std::string_view instance_name) = 0;

  /// @throw QueryRuntimeException if an error occurred.
  virtual void SetReplicationInstanceToMain(std::string_view instance_name) = 0;

  /// @throw QueryRuntimeException if an error occurred.
  virtual coordination::InstanceStatus ShowInstance() const = 0;

  /// nullopt if the leader couldn't be reached.
  /// @throw QueryRuntimeException if an error occurred.
  virtual std::optional<std::vector<coordination::InstanceStatus>> ShowInstances() const = 0;

  /// @throw QueryRuntimeException if an error occurred.
  virtual void AddCoordinatorInstance(int32_t coordinator_id, std::string_view bolt_server,
                                      std::string_view coordinator_server, std::string_view management_server) = 0;

  virtual void RemoveCoordinatorInstance(int32_t coordinator_id) = 0;

  virtual void UpdateConfig(std::variant<int32_t, std::string> instance, io::network::Endpoint bolt_endpoint) = 0;

  virtual void DemoteInstanceToReplica(std::string_view instance_name) = 0;

  virtual void ForceResetClusterState() = 0;

  virtual void YieldLeadership() = 0;

  virtual void SetCoordinatorSetting(std::string_view setting_name, std::string_view setting_value) = 0;

  /// Both return nullopt if the leader couldn't be reached.
  virtual std::optional<std::vector<std::pair<std::string, std::string>>> ShowCoordinatorSettings() = 0;

  /// @throw QueryRuntimeException if an error occurred.
  virtual void CreateRole(std::string_view role_name, bool if_not_exists) = 0;

  /// @throw QueryRuntimeException if an error occurred.
  virtual void DropRole(std::string_view role_name) = 0;

  /// @throw QueryRuntimeException if an error occurred.
  virtual std::vector<std::string> ShowRoles() = 0;

  /// @throw QueryRuntimeException if an error occurred.
  virtual void GrantCoordinatorPrivilege(std::string_view role_name, uint64_t privileges) = 0;

  /// @throw QueryRuntimeException if an error occurred.
  virtual void RevokeCoordinatorPrivilege(std::string_view role_name, uint64_t privileges) = 0;

  /// @throw QueryRuntimeException if an error occurred. Returns the role's coordinator permission mask.
  virtual uint64_t ShowRolePrivileges(std::string_view role_name) = 0;

  virtual std::optional<coordination::ReplicationLagResult> ShowReplicationLag() = 0;

  virtual coordination::RoutingTable GetRoutingTable(std::string_view db_name) = 0;
};
#endif

class AnalyzeGraphQueryHandler {
 public:
  AnalyzeGraphQueryHandler() = default;
  virtual ~AnalyzeGraphQueryHandler() = default;

  AnalyzeGraphQueryHandler(const AnalyzeGraphQueryHandler &) = default;
  AnalyzeGraphQueryHandler &operator=(const AnalyzeGraphQueryHandler &) = default;

  AnalyzeGraphQueryHandler(AnalyzeGraphQueryHandler &&) = default;
  AnalyzeGraphQueryHandler &operator=(AnalyzeGraphQueryHandler &&) = default;

  static std::vector<std::vector<TypedValue>> AnalyzeGraphCreateStatistics(const std::span<std::string> labels,
                                                                           DbAccessor *execution_db_accessor);

  static std::vector<std::vector<TypedValue>> AnalyzeGraphDeleteStatistics(const std::span<std::string> labels,
                                                                           DbAccessor *execution_db_accessor);
};

/**
 * A container for data related to the preparation of a query.
 */
struct PreparedQuery {
  std::vector<std::string> header;
  std::vector<AuthQuery::Privilege> privileges;
  std::move_only_function<std::optional<QueryHandlerResult>(AnyStream *stream, std::optional<int> n)> query_handler;
  plan::ReadWriteTypeChecker::RWType rw_type;
  std::optional<std::string> db{};
  utils::Priority priority{utils::Priority::LOW};
  // Lazily renders the EXPLAIN plan for the slow-query log; empty unless slow logging may
  // apply. Pull invokes it past the duration gate, while the plan's DbAccessor is alive.
  std::function<std::string()> slow_query_plan_renderer{};
};

/**
 * Holds data for the Query which is extra
 * NOTE: maybe need to parse more in the future, ATM we ignore some parts from BOLT
 */
struct QueryExtras {
  storage::ExternalPropertyValue::map_t metadata_pv{};
  std::optional<int64_t> tx_timeout{};
  bool is_read{false};
};

struct CurrentDB {
  CurrentDB() = default;  // TODO: remove, we should always have an implicit default obtainable from somewhere
                          //       ATM: it is provided by the DatabaseAccess
                          //       future: should be a name + ptr to dbms_handler, lazy fetch when needed

  // No lock needed: db_acc_ is set via the member-init list, before this CurrentDB becomes reachable
  // (e.g. via InterpreterContext::interpreters), so no other thread can observe it mid-construction.
  explicit CurrentDB(memgraph::dbms::DatabaseAccess db_acc) : db_acc_{std::move(db_acc)} {}

  CurrentDB(CurrentDB const &) = delete;
  CurrentDB &operator=(CurrentDB const &) = delete;

  void SetupDatabaseTransaction(std::optional<storage::IsolationLevel> override_isolation_level, bool could_commit,
                                storage::StorageAccessType acc_type = storage::StorageAccessType::WRITE);
  void CleanupDBTransaction(bool abort);

  void SetCurrentDB(memgraph::dbms::DatabaseAccess new_db, bool in_explicit_db) {
    // Move the outgoing Accessor out of db_acc_ under the lock, then let it destruct AFTER the lock is
    // released (see db_acc_mutex_ for why: its dtor can block on a foreign GKInternals::mutex_).
    std::optional<memgraph::dbms::DatabaseAccess> old_db;
    {
      std::lock_guard lock{db_acc_mutex_};
      old_db = std::exchange(db_acc_, std::move(new_db));
      in_explicit_db_ = in_explicit_db;
    }
  }

  void ResetDB() {
    // Narrowed to db_acc_ only: db_transactional_accessor_'s dtor can abort a txn and take storage locks,
    // which would stall a concurrent foreign_db_view() if held under the same lock. old_db is swapped out
    // under the lock and destructed below, outside it (see db_acc_mutex_).
    std::optional<memgraph::dbms::DatabaseAccess> old_db;
    {
      std::lock_guard lock{db_acc_mutex_};
      old_db.swap(db_acc_);
    }
    old_db.reset();  // release db access before the accessors below, as before
    db_transactional_accessor_.reset();
    execution_db_accessor_.reset();
    trigger_context_collector_.reset();
  }

  // Releases db_acc_ only if held and marked for deletion; db_transactional_accessor_/execution_db_accessor_/
  // trigger_context_collector_ are untouched -- that's ResetDB()'s job. is_marked_for_deletion() only reads
  // an atomic_bool (no GKInternals::mutex_), so it's safe to call under db_acc_mutex_; the swapped-out
  // Accessor itself is destructed after the lock is released (see db_acc_mutex_).
  void ReleaseDbIfMarked() {
    std::optional<memgraph::dbms::DatabaseAccess> old_db;
    {
      std::lock_guard lock{db_acc_mutex_};
      if (db_acc_ && db_acc_->is_marked_for_deletion()) {
        old_db.swap(db_acc_);
      }
    }
  }

  // Owning-thread-only (or under the verifier's ACTIVE->VERIFYING CAS). Reads db_acc_ with no synchronization,
  // safe because a session's queries are serialized -- Bolt's worker pool never runs two for one session at
  // once, so a writer and this read never overlap regardless of which thread runs each -- adding a concurrent
  // writer invalidates this contract and every unlocked read in interpreter.cpp. A foreign thread -- including
  // one observing an IDLE session -- must use foreign_db_view() instead.
  std::string name() const { return db_acc_ ? db_acc_->get()->name() : ""; }

  // Safe from any thread: unlike name(), it needs no verifier CAS, which can never succeed on IDLE anyway.
  // Reads db_acc_ live, not cached -- DbmsHandler::Rename mutates storage's name in place, not db_acc_.
  struct ForeignDbView {
    std::string name;                 // "" when the session holds no database
    bool marked_for_deletion{false};  // false when there is no database
  };

  [[nodiscard]] ForeignDbView foreign_db_view() const {
    std::lock_guard lock{db_acc_mutex_};
    if (!db_acc_) return {};
    return {db_acc_->get()->name(), db_acc_->is_marked_for_deletion()};
  }

  // TODO: don't provide explicitly via constructor, instead have a lazy way of getting the current/default
  // DatabaseAccess
  //       hence, explict bolt "use DB" in metadata wouldn't necessarily get access unless query required it.
  std::optional<memgraph::dbms::DatabaseAccess> db_acc_;  // Current db (TODO: expand to support multiple)
  std::unique_ptr<storage::Storage::Accessor> db_transactional_accessor_;
  std::optional<DbAccessor> execution_db_accessor_;
  std::optional<TriggerContextCollector> trigger_context_collector_;
  bool in_explicit_db_{false};
  metrics::ScopedGauge transaction_gauge_;

  // Guards mutation of db_acc_ only; owning-thread reads (name(), ~100 direct db_acc_ reads in interpreter.cpp) skip
  // it because a session's queries are serialized, so a reader and writer for the same session never overlap.
  // Must NOT be a spinlock: foreign_db_view() calls Storage::name(), which blocks on a shared_mutex inside
  // utils::SafeString.
  //
  // LEAF LOCK: every writer above swaps the outgoing Accessor out under this lock and destroys it only
  // after releasing it -- no Accessor may be destroyed while this lock is held. An Accessor's dtor takes
  // its GKInternals::mutex_, which finish_suspend() holds across the whole ~Database + WAL finalization,
  // and InterpreterContext::TerminateSessions takes this lock (via foreign_db_view()) while holding the interpreters
  // SpinLock -- so nesting the two would put a busy-wait spinlock over the entire session table behind a
  // tenant suspend. That pile-up is NOT reachable today: try_begin_suspend() waits for count_ == 1 before
  // entering SUSPENDING, so a session holding this accessor keeps its tenant out of the suspend path
  // entirely. Keeping this a leaf lock means correctness here does not depend on that remote invariant
  // holding -- note finish_suspend()'s own count_ precondition is only a DMG_ASSERT, which compiles out
  // under NDEBUG.
  mutable std::mutex db_acc_mutex_;
};

using UserParameters_fn = std::function<UserParameters(storage::Storage const *)>;
constexpr auto no_params_fn = [](storage::Storage const *) -> UserParameters { return {}; };

class Interpreter final {
 public:
  explicit Interpreter(InterpreterContext *interpreter_context);
  Interpreter(InterpreterContext *interpreter_context, memgraph::dbms::DatabaseAccess db);
  Interpreter(const Interpreter &) = delete;
  Interpreter &operator=(const Interpreter &) = delete;
  Interpreter(Interpreter &&) = delete;
  Interpreter &operator=(Interpreter &&) = delete;

  ~Interpreter();

  void ResetCachedFga();
  FineGrainedAuthChecker const *GetCachedFga() const;

  struct PrepareResult {
    std::vector<std::string> headers;
    std::vector<query::AuthQuery::Privilege> privileges;
    std::optional<int> qid;
    std::optional<std::string> db;
  };

#ifdef MG_ENTERPRISE
  struct RouteResult {
    int ttl{300};
    std::string db{};  // Currently not used since we don't have any specific replication groups etc.
    coordination::RoutingTable servers{};
  };
#endif

  struct SessionInfo {
    std::string uuid;
    std::string username;
    std::string login_timestamp;
  };

  // Owning-thread state: written only by this session's own Bolt thread (SetUser / ResetUser /
  // SetSessionInfo) and read directly only by that same thread. Roughly forty read sites in
  // interpreter.cpp rely on that and are correct as they stand.
  //
  // A FOREIGN THREAD MUST NOT READ EITHER FIELD DIRECTLY -- it must load the published snapshot
  // below. Reading these directly across threads is a data race on a non-atomic shared_ptr/string,
  // and for user_or_role_ a use-after-free: the reader binds a reference without touching the
  // refcount, so a concurrent ResetUser can free the pointee mid-comparison.
  std::shared_ptr<QueryUserOrRole> user_or_role_{};
#ifdef MG_ENTERPRISE
  // Coordinator privilege mask captured at login (auth::Permission bits). Consulted directly only for role-less
  // (basic-auth passthrough) sessions, which carry full WRITE; sessions with coordinator roles recompute their mask
  // per check via EffectiveCoordinatorPermissions. Zero denies everything, so an interpreter that never authenticated
  // grants nothing: every privileged path must call SetCoordinatorPrivileges explicitly.
  uint64_t coordinator_permissions_{0};
  // Role names the session authenticated with on a coordinator (empty for a basic-auth passthrough session). A claim
  // captured at login, not a fact: both the privilege mask (EffectiveCoordinatorPermissions) and SHOW CURRENT ROLE
  // re-check these names against the leader's committed role set on every use, so a dropped role stops counting and
  // stops being reported without waiting for a reconnect.
  std::vector<std::string> coordinator_roles_;
  std::shared_ptr<utils::UserResources> user_resource_;
#endif
  std::unique_ptr<CachedFineGrainedAuth> cached_fga_;
  SessionInfo session_info_;
  // Published snapshots of the two fields above, for foreign readers: ShowTransactions and
  // TerminateTransactions (src/query/interpreter.cpp, src/query/interpreter_context.cpp),
  // TerminateSessions (interpreter_context.cpp), SHOW SESSIONS and GetActiveUsersInfo
  // (interpreter.cpp). Written by the owning thread only, so a store never races another store.
  //
  // std::atomic<std::shared_ptr<T>>::is_always_lock_free is false, and that is the REASON THIS
  // WORKS -- not a caveat to optimise away. libstdc++ steals the low bit of the control-block
  // pointer as a per-instance spinlock and performs the refcount increment while holding it, which
  // is exactly what makes the copy returned by load() safe against a concurrent decrement to zero.
  // Destruction of the replaced value is deferred until after that lock is released, so no
  // destructor ever runs inside it. Replacing this with a raw pointer, a relaxed atomic, or a
  // hand-rolled seqlock reintroduces the use-after-free.
  //
  // Keep each snapshot WHOLE. Do not split foreign_user_view_ into separate username/rolenames
  // atomics: QueryUserOrRole::operator== compares the pair jointly, so a reader that observed an
  // old username beside new rolenames would silently decide identity wrongly -- an authorization
  // bug, not merely a memory-safety one.
  std::atomic<std::shared_ptr<QueryUserOrRole>> foreign_user_view_{};
  std::atomic<std::shared_ptr<const SessionInfo>> foreign_session_view_{};
  bool in_explicit_transaction_{false};
  CurrentDB current_db_;

  bool expect_rollback_{false};
  std::shared_ptr<utils::AsyncTimer> current_timeout_timer_{};
  std::optional<storage::ExternalPropertyValue::map_t> metadata_{};  //!< User defined transaction metadata

#ifdef MG_ENTERPRISE
  void SetCurrentDB(std::string_view db_name, bool explicit_db);

  void ResetDB() { current_db_.ResetDB(); }

  void OnChangeCB(auto cb) { on_change_.emplace(cb); }
#else
  void SetCurrentDB();
#endif

  utils::Priority GetQueryPriority(std::optional<int> qid) const {
    const int qid_value = qid ? *qid : static_cast<int>(query_executions_.size() - 1);
    if (qid_value < 0 || qid_value >= query_executions_.size()) {
      throw InvalidArgumentsException("qid", "Query with specified ID does not exist!");
    }
    return query_executions_[qid_value]->prepared_query->priority;
  }

  utils::Priority ApproximateNextQueryPriority() const {
    // If in transaction => low, we are for sure in a cypher query situation
    // If not in transaction, we have to check the last query priority <- there can't be qid, so just check the last
    return in_explicit_transaction_    ? utils::Priority::LOW
           : query_executions_.empty() ? utils::Priority::HIGH
                                       : query_executions_.back()->prepared_query->priority;
  }

  struct ParseInfo {
    ParsedQuery parsed_query;
    double parsing_time;
  };

  enum class TransactionQuery : uint8_t { BEGIN, COMMIT, ROLLBACK };

  using ParseRes = std::variant<ParseInfo, TransactionQuery>;

  Interpreter::ParseRes Parse(const std::string &query, UserParameters_fn params_getter, QueryExtras const &extras);

  Interpreter::PrepareResult Prepare(ParseRes parse_res, UserParameters_fn params_getter, QueryExtras const &extras);

  /**
   * Prepare a query for execution.
   *
   * Preparing a query means to preprocess the query and save it for
   * future calls of `Pull`.
   *
   * @throw query::QueryException
   */
  Interpreter::PrepareResult Prepare(const std::string &query, UserParameters_fn params_getter,
                                     QueryExtras const &extras) {
    // Split Prepare in two (Parse and Prepare)
    // This allows us to parse, deduce priority and schedule accordingly
    // Leaving this one-shot version for back-compatiblity
    return Prepare(Parse(query, params_getter, extras), params_getter, extras);
  }

  /**
   * Checks if the user has the required privileges to execute the query.
   *
   * @throw query::QueryException
   */
  void CheckAuthorized(std::vector<AuthQuery::Privilege> const &privileges, std::optional<std::string> db = {});

#ifdef MG_ENTERPRISE
  auto Route(std::optional<std::string> const &db) -> RouteResult;
#endif

  /**
   * Execute the last prepared query and stream *all* of the results into the
   * given stream.
   *
   * It is not possible to prepare a query once and execute it multiple times,
   * i.e. `Prepare` has to be called before *every* call to `PullAll`.
   *
   * TStream should be a type implementing the `Stream` concept, i.e. it should
   * contain the member function `void Result(const std::vector<TypedValue> &)`.
   * The provided vector argument is valid only for the duration of the call to
   * `Result`. The stream should make an explicit copy if it wants to use it
   * further.
   *
   * @throw utils::BasicException
   * @throw query::QueryException
   */
  template <typename TStream>
  std::map<std::string, TypedValue> PullAll(TStream *result_stream) {
    return Pull(result_stream);
  }

  /**
   * Execute a prepared query and stream result into the given stream.
   *
   * TStream should be a type implementing the `Stream` concept, i.e. it should
   * contain the member function `void Result(const std::vector<TypedValue> &)`.
   * The provided vector argument is valid only for the duration of the call to
   * `Result`. The stream should make an explicit copy if it wants to use it
   * further.
   *
   * @param n If set, amount of rows to be pulled from result,
   * otherwise all the rows are pulled.
   * @param qid If set, id of the query from which the result should be pulled,
   * otherwise the last query should be used.
   *
   * @throw utils::BasicException
   * @throw query::QueryException
   */
  template <typename TStream>
  std::map<std::string, TypedValue> Pull(TStream *result_stream, std::optional<int> n = {},
                                         std::optional<int> qid = {});

  void BeginTransaction(QueryExtras const &extras = {});

  std::optional<uint64_t> GetTransactionId() const;

  // Returns the notification produced by the commit, if any. A SYNC replication failure does not abort the
  // transaction, so it is reported as a notification instead of an exception.
  std::optional<Notification> CommitTransaction();

  void RollbackTransaction();

  void SetNextTransactionIsolationLevel(storage::IsolationLevel isolation_level);
  void SetSessionIsolationLevel(storage::IsolationLevel isolation_level);

  std::vector<TypedValue> GetQueries();

  /**
   * Abort the current multicommand transaction.
   */
  void Abort();

  struct TxVerifier {
    TxVerifier(TransactionStatus original_status, std::atomic<TransactionStatus> &transaction_status)
        : original_status_(original_status), transaction_status_(transaction_status) {}

    ~TxVerifier() {
      TransactionStatus expected = TransactionStatus::VERIFYING;
      transaction_status_.compare_exchange_strong(
          expected, original_status_, std::memory_order_release, std::memory_order_relaxed);
    }

    TxVerifier(const TxVerifier &) = delete;
    TxVerifier(TxVerifier &&) = delete;
    TxVerifier &operator=(const TxVerifier &) = delete;
    TxVerifier &operator=(TxVerifier &&) = delete;

    TransactionStatus status() const { return original_status_; }

   private:
    TransactionStatus original_status_;
    std::atomic<TransactionStatus> &transaction_status_;
  };

  /**
   * Attempt to CAS the transaction status to VERIFYING so its state can be
   * safely read by another thread. Returns a TxVerifier RAII guard that
   * restores the original status on destruction, or nullopt if the CAS failed.
   */
  std::optional<TxVerifier> TryAcquireForVerification();

  std::atomic<TransactionStatus> transaction_status_{TransactionStatus::IDLE};
  // current_transaction_ is protected by the transaction_status_ atomic.
  // When transaction_status_ is VERIFYING, current_transaction_ is stable.
  // When transaction_status_ is IDLE, current_transaction_ is nullopt.
  std::optional<uint64_t> current_transaction_{std::nullopt};
  // Set in SetupInterpreterTransaction; published via the release-store on transaction_status_
  // and read by ShowTransactions under a verifier (acquire on the same atomic). system_clock
  // is used for start_time; steady_clock for elapsed_ms (immune to NTP / manual clock jumps).
  std::chrono::system_clock::time_point transaction_start_time_{};
  std::chrono::steady_clock::time_point transaction_start_steady_{};

  void ResetUser();

#ifdef MG_ENTERPRISE
  void SetUser(std::shared_ptr<QueryUserOrRole> user, std::shared_ptr<utils::UserResources> user_resource = nullptr);

  // Sets the session's effective coordinator privilege mask (auth::Permission bits). Called at authentication time on
  // coordinators; a basic-auth passthrough session is granted full WRITE.
  void SetCoordinatorPrivileges(uint64_t privileges) { coordinator_permissions_ = privileges; }

  // Sets the role names the session authenticated with on a coordinator (reported by SHOW CURRENT ROLE).
  void SetCoordinatorRoles(std::vector<std::string> roles) { coordinator_roles_ = std::move(roles); }

  // The role names the session authenticated with on a coordinator (empty for a basic-auth passthrough session).
  const std::vector<std::string> &GetCoordinatorRoles() const { return coordinator_roles_; }

  // The session's effective coordinator privilege mask. A role-less (basic-auth passthrough) session uses the mask
  // fixed at login; an SSO session recomputes it from its role names against the leader's committed role set on
  // every check, so REVOKE/DROP ROLE downgrades long-lived sessions without a reconnect.
  uint64_t EffectiveCoordinatorPermissions() const;
#else
  void SetUser(std::shared_ptr<QueryUserOrRole> user);
#endif

  void SetSessionInfo(std::string uuid, std::string username, std::string login_timestamp);

  std::optional<memgraph::system::Transaction> system_transaction_{};

  memgraph::system::Transaction *system_transaction_ptr() {
    return system_transaction_ ? &*system_transaction_ : nullptr;
  }

  memgraph::logging::SessionLogContext *GetLogContext() noexcept { return &session_log_ctx_; }

  // Reused across queries so the planner-v2 extraction buffers (frontier map,
  // selection, in-degree, topo order) keep their allocated capacity instead of
  // being freed and re-grown each query.
  plan::v2::QueryPlannerContext &query_planner_context() { return query_planner_context_; }

 private:
  void MaybeEmitFailedQueryLog(std::string_view query, std::string_view error) const {
    // TLS guard absent => no bolt message is in flight (worker/GC/NuRaft thread); never emit.
    if (memgraph::logging::ScopedSessionLog::Current() == nullptr) return;
    if (!flags::run_time::GetEffectiveLogFailedQueries(session_log_ctx_)) return;
    const auto db_name = CurrentDbLogName();
    memgraph::logging::EmitFailedQueryLog(session_log_ctx_.user(), db_name, query, error);
  }

  // db= field for the slow-/failed-query log: the current DB name, or "<none>".
  std::string CurrentDbLogName() const {
    auto name = current_db_.name();
    return name.empty() ? std::string{"<none>"} : name;
  }

  memgraph::logging::SessionLogContext session_log_ctx_{};

  void ResetInterpreter() {
    query_executions_.clear();
    system_transaction_.reset();
    transaction_queries_->clear();
    commit_notification_.reset();
    current_db_.ReleaseDbIfMarked();
  }

  struct QueryExecution {
    static constexpr struct ThreadSafe {
    } thread_safe_;

    // QueryExecution memory is charged to the DB whose query/trigger is being
    // prepared. System-only executions may pass nullptr because they do not run
    // inside a DB query-memory budget.
    explicit QueryExecution(utils::MemoryTracker *db_query_tracker = nullptr)
        : execution_memory{std::in_place_type<QueryAllocator>, db_query_tracker}, memory_tracker{db_query_tracker} {}

    QueryExecution(ThreadSafe /*marker*/, utils::MemoryTracker *db_query_tracker)
        : execution_memory{std::in_place_type<ThreadSafeQueryAllocator>, db_query_tracker},
          memory_tracker{db_query_tracker} {}

    QueryExecution(const QueryExecution &) = delete;
    QueryExecution(QueryExecution &&) = delete;
    QueryExecution &operator=(const QueryExecution &) = delete;
    QueryExecution &operator=(QueryExecution &&) = delete;

    ~QueryExecution() = default;

    std::variant<QueryAllocator, ThreadSafeQueryAllocator>
        execution_memory;  // NOTE: before all other fields which uses this memory

    /// Tracks this query's allocations when there is no storage transaction to do it. A transaction
    /// carries one of these for the same purpose; nothing about it needs the transaction, only the
    /// database's tracker to report to, and that may be absent too.
    /// NOTE: before `prepared_query`, whose plan holds a pointer to this.
    utils::QueryMemoryTracker memory_tracker;

    std::optional<PreparedQuery> prepared_query;
    std::map<std::string, TypedValue> summary;
    std::vector<Notification> notifications;
    // Original query text, kept so log emits can quote it after the lambda chain
    // owning parsed_query is torn down.
    // TODO: avoidable allocation. Already copied into transaction_queries_; could be moved
    // from parsed_query (after its EXPLAIN/PROFILE uses) or skipped when slow+failed logging
    // are both off — but then the save-gate must stay a superset of the emit-gate.
    std::string query_string;

    static auto Create(utils::MemoryTracker *db_query_tracker = nullptr) -> std::unique_ptr<QueryExecution> {
      return std::make_unique<QueryExecution>(db_query_tracker);
    }

    static auto CreateThreadSafe(utils::MemoryTracker *db_query_tracker = nullptr) -> std::unique_ptr<QueryExecution> {
      return std::make_unique<QueryExecution>(thread_safe_, db_query_tracker);
    }

    utils::MemoryResource *resource() {
      return std::visit([](auto &mem) { return mem.resource(); }, execution_memory);
    }

    void CleanRuntimeData() {
      prepared_query.reset();
      notifications.clear();
    }
  };

  // Query text for the failed-query log: prefer Pull's captured copy; before Pull moves
  // it out, captured is empty and the text still lives on the QueryExecution.
  static std::string_view FailedQueryText(const std::string &captured,
                                          const std::unique_ptr<QueryExecution> &query_execution) {
    if (!captured.empty()) return captured;
    if (query_execution) return query_execution->query_string;
    return {};
  }

  // Interpreter supports multiple prepared queries at the same time.
  // The client can reference a specific query for pull using an arbitrary qid
  // which is in our case the index of the query in the vector.
  // To simplify the handling of the qid we avoid modifying the vector if it
  // affects the position of the currently running queries in any way.
  // For example, we cannot delete the prepared query from the vector because
  // every prepared query after the deleted one will be moved by one place
  // making their qid not equal to the their index inside the vector.
  // To avoid this, we use unique_ptr with which we manualy control construction
  // and deletion of a single query execution, i.e. when a query finishes,
  // we reset the corresponding unique_ptr.
  // TODO Figure out how this would work for multi-database
  // SubqueryExpression only during a single transaction (for now should be okay as is)
  std::vector<std::unique_ptr<QueryExecution>> query_executions_;

  // all queries that are run as part of the current transaction
  utils::Synchronized<std::vector<std::string>, utils::SpinLock> transaction_queries_;

  InterpreterContext *interpreter_context_;

  std::optional<FrameChangeCollector> frame_change_collector_;

  plan::v2::QueryPlannerContext query_planner_context_;

  std::optional<storage::IsolationLevel> interpreter_isolation_level;
  std::optional<storage::IsolationLevel> next_transaction_isolation_level;

  // Notification produced by the last Commit(); set when the transaction committed on main but could not be
  // replicated to every SYNC replica. Consumed by whoever drove the commit (Pull or CommitTransaction).
  std::optional<Notification> commit_notification_;

  static void AppendNotificationToSummary(const Notification &notification, std::map<std::string, TypedValue> &summary);

  PreparedQuery PrepareTransactionQuery(Interpreter::TransactionQuery tx_query_enum, QueryExtras const &extras = {});
  void Commit();
  // Resets tx-tracking left ACTIVE by SetupInterpreterTransaction when NOTHING skips Commit()/Abort()'s cleanup.
  void FinishAutocommitNothing();
  void AdvanceCommand();
  void AbortCommand(std::unique_ptr<QueryExecution> *query_execution);
  std::optional<storage::IsolationLevel> GetIsolationLevelOverride();

  size_t ActiveQueryExecutions() {
    return std::ranges::count_if(query_executions_,
                                 [](const auto &execution) { return execution && execution->prepared_query; });
  }

  std::optional<std::function<void(std::string_view)>> on_change_{};
  void SetupInterpreterTransaction(const QueryExtras &extras);
  void SetupDatabaseTransaction(bool couldCommit,
                                storage::StorageAccessType acc_type = storage::StorageAccessType::WRITE);
};

template <typename TStream>
std::map<std::string, TypedValue> Interpreter::Pull(TStream *result_stream, std::optional<int> n,
                                                    std::optional<int> qid) {
  // Update the TLS arena index used to route allocations to the correct database arena.
  // The previous arena is restored on scope exit so pool threads are unaffected.
  std::optional<memory::DbArenaScope> plan_cache_db_arena_scope;
  if (current_db_.db_acc_) {
    plan_cache_db_arena_scope.emplace(current_db_.db_acc_->get());
  }
  MG_ASSERT(in_explicit_transaction_ || !qid, "qid can be only used in explicit transaction!");

  const int qid_value = qid ? *qid : static_cast<int>(query_executions_.size() - 1);
  if (qid_value < 0 || qid_value >= query_executions_.size()) {
    throw InvalidArgumentsException("qid", "Query with specified ID does not exist!");
  }

  if (n && n < 0) {
    throw InvalidArgumentsException("n", "Cannot fetch negative number of results!");
  }

  auto &query_execution = query_executions_[qid_value];

  MG_ASSERT(query_execution && query_execution->prepared_query, "Query already finished executing!");

  // Each prepared query has its own summary so we need to somehow preserve
  // it after it finishes executing because it gets destroyed alongside
  // the prepared query and its execution memory.
  std::optional<std::map<std::string, TypedValue>> maybe_summary;
  // Stash before query_execution can be invalidated by ResetInterpreter / reset.
  std::string captured_query_string;
  std::optional<std::string> captured_plan_text;
  // Slow-query gate, evaluated while the plan renderer's DbAccessor is still alive
  // and emitted below once this statement finishes. Each statement is logged on its
  // own completion: for autocommit that is right after Commit() (so a failing commit
  // logs as failed, not slow); inside an explicit transaction every statement is its
  // own slow-query unit, independent of the later COMMIT/ROLLBACK.
  bool emit_slow_query = false;
  int64_t slow_query_duration_ms = 0;
  try {
    // Wrap the (statically polymorphic) stream type into a common type which
    // the handler knows.
    AnyStream stream{result_stream, query_execution->resource()};
    const auto maybe_res = query_execution->prepared_query->query_handler(&stream, n);
    // Stream is using execution memory of the query_execution which
    // can be deleted after its execution so the stream should be cleared
    // first.
    stream.~AnyStream();

    // If the query finished executing, we have received a value which tells
    // us what to do after.
    if (maybe_res) {
      // Save its summary
      maybe_summary.emplace(std::move(query_execution->summary));
      captured_query_string = std::move(query_execution->query_string);

      // Evaluate the gate now, while the renderer's DbAccessor is alive (Commit /
      // ResetInterpreter tear it down). For autocommit, emit is deferred to after
      // Commit() so a commit failure is logged as failed, not slow.
      {
        const auto threshold_ms = flags::run_time::GetEffectiveLogMinDurationMs(session_log_ctx_);
        if (threshold_ms >= 0) {
          auto duration_seconds = [&](const char *key) -> double {
            auto it = maybe_summary->find(key);
            if (it == maybe_summary->end() || !it->second.IsDouble()) return 0.0;
            return it->second.ValueDouble();
          };
          const double total_sec = duration_seconds("parsing_time") + duration_seconds("planning_time") +
                                   duration_seconds("plan_execution_time");
          slow_query_duration_ms = static_cast<int64_t>(total_sec * 1000.0);
          if (slow_query_duration_ms >= threshold_ms) {
            emit_slow_query = true;
            auto &renderer = query_execution->prepared_query->slow_query_plan_renderer;
            if (renderer && flags::run_time::GetEffectiveLogQueryPlan(session_log_ctx_)) {
              captured_plan_text = renderer();
            }
          }
        }
      }

      // NOTE: must happen before Commit(), which clears the runtime data of every query execution.
      if (!query_execution->notifications.empty()) {
        std::vector<TypedValue> notifications;
        notifications.reserve(query_execution->notifications.size());
        for (const auto &notification : query_execution->notifications) {
          notifications.emplace_back(notification.ConvertToMap());
        }
        maybe_summary->insert_or_assign("notifications", std::move(notifications));
      }
      if (!in_explicit_transaction_) {
        switch (*maybe_res) {
          case QueryHandlerResult::COMMIT:
            Commit();
            break;
          case QueryHandlerResult::ABORT:
            Abort();
            break;
          case QueryHandlerResult::NOTHING: {
            // NOTHING means no storage transaction was opened on `Prepare()` (this switch only runs
            // for autocommit queries -- it is inside `if (!in_explicit_transaction_)`).
            MG_ASSERT(!current_db_.db_transactional_accessor_);
            // Unlike COMMIT/ABORT, NOTHING must dispose the ACTIVE state itself or the session stays active.
            FinishAutocommitNothing();
            break;
          }
        }
        // The commit itself can report a SYNC replication failure. This also covers the explicit COMMIT
        // query, whose handler already cleared in_explicit_transaction_ by the time we get here.
        if (commit_notification_) {
          AppendNotificationToSummary(*commit_notification_, *maybe_summary);
          commit_notification_.reset();
        }
        // As the transaction is done we can clear all the executions
        // NOTE: we cannot clear query_execution inside the Abort and Commit
        // methods as we will delete summary contained in them which we need
        // after our query finished executing.
        ResetInterpreter();
      } else {
        // We can only clear this execution as some of the queries
        // in the transaction can be in unfinished state
        query_execution.reset(nullptr);
      }
    }
  } catch (const ExplicitTransactionUsageException &e) {
    memgraph::logging::EmitSessionTraceEvent(e.what());
    MaybeEmitFailedQueryLog(FailedQueryText(captured_query_string, query_execution), e.what());
    query_execution.reset(nullptr);
    throw;
  } catch (const utils::BasicException &e) {
    memgraph::logging::EmitSessionTraceEvent(e.what());
    MaybeEmitFailedQueryLog(FailedQueryText(captured_query_string, query_execution), e.what());
    metrics::FirstFailedQuery();
    if (auto *mh = current_db_.db_acc_ ? (*current_db_.db_acc_)->metric_handles() : nullptr) {
      mh->failed_query.Increment();
      mh->failed_pull.Increment();
    } else {
      metrics::Metrics().global.failed_query->Increment();
      metrics::Metrics().global.failed_pull->Increment();
    }
    // PeriodicCommitException means the storage layer already aborted the transaction internally.
    // Null the accessor first so AbortCommand does not call Abort() a second time.
    if (dynamic_cast<const PeriodicCommitException *>(&e)) {
      current_db_.CleanupDBTransaction(false);
    }
    AbortCommand(&query_execution);
    throw;
  }

  if (maybe_summary) {
    // Toggle first successfully completed query
    metrics::FirstSuccessfulQuery();
    if (auto *mh = current_db_.db_acc_ ? (*current_db_.db_acc_)->metric_handles() : nullptr) {
      mh->successful_query.Increment();
    } else {
      metrics::Metrics().global.successful_query->Increment();
    }

    // Emit the slow-query line now that the commit succeeded (gate evaluated pre-commit).
    if (emit_slow_query) {
      std::optional<std::string_view> plan_view;
      if (captured_plan_text.has_value()) plan_view = *captured_plan_text;
      const auto db_name = CurrentDbLogName();
      memgraph::logging::EmitSlowQueryLog(
          session_log_ctx_.user(), db_name, captured_query_string, slow_query_duration_ms, plan_view);
    }

    // return the execution summary
    maybe_summary->insert_or_assign("has_more", false);
    return std::move(*maybe_summary);
  }

  // don't return the execution summary as it's not finished
  return {{"has_more", TypedValue(true)}};
}

}  // namespace memgraph::query
