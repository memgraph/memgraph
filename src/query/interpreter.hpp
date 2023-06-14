// Copyright 2023 Memgraph Ltd.
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

#include <unordered_set>

#include <gflags/gflags.h>

#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/context.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/stripped.hpp"
#include "query/interpret/frame.hpp"
#include "query/metadata.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/read_write_type_checker.hpp"
#include "query/stream.hpp"
#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/isolation_level.hpp"
#include "utils/event_counter.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/settings.hpp"
#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/thread_pool.hpp"
#include "utils/timer.hpp"
#include "utils/tsc.hpp"

namespace memgraph::metrics {
extern const Event FailedQuery;
}  // namespace memgraph::metrics

namespace memgraph::query {

inline constexpr size_t kExecutionMemoryBlockSize = 1UL * 1024UL * 1024UL;
inline constexpr size_t kExecutionPoolMaxBlockSize = 1024UL;  // 2 ^ 10

class AuthQueryHandler {
 public:
  AuthQueryHandler() = default;
  virtual ~AuthQueryHandler() = default;

  AuthQueryHandler(const AuthQueryHandler &) = delete;
  AuthQueryHandler(AuthQueryHandler &&) = delete;
  AuthQueryHandler &operator=(const AuthQueryHandler &) = delete;
  AuthQueryHandler &operator=(AuthQueryHandler &&) = delete;

  /// Return false if the user already exists.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool CreateUser(const std::string &username, const std::optional<std::string> &password) = 0;

  /// Return false if the user does not exist.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool DropUser(const std::string &username) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void SetPassword(const std::string &username, const std::optional<std::string> &password) = 0;

  /// Return false if the role already exists.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool CreateRole(const std::string &rolename) = 0;

  /// Return false if the role does not exist.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool DropRole(const std::string &rolename) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<TypedValue> GetUsernames() = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<TypedValue> GetRolenames() = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::optional<std::string> GetRolenameForUser(const std::string &username) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<TypedValue> GetUsernamesForRole(const std::string &rolename) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void SetRole(const std::string &username, const std::string &rolename) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void ClearRole(const std::string &username) = 0;

  virtual std::vector<std::vector<TypedValue>> GetPrivileges(const std::string &user_or_role) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void GrantPrivilege(
      const std::string &user_or_role, const std::vector<AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
      ,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,

      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges
#endif
      ) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void DenyPrivilege(const std::string &user_or_role, const std::vector<AuthQuery::Privilege> &privileges) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void RevokePrivilege(
      const std::string &user_or_role, const std::vector<AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
      ,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,

      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges
#endif
      ) = 0;
};

enum class QueryHandlerResult { COMMIT, ABORT, NOTHING };

class ReplicationQueryHandler {
 public:
  ReplicationQueryHandler() = default;
  virtual ~ReplicationQueryHandler() = default;

  ReplicationQueryHandler(const ReplicationQueryHandler &) = default;
  ReplicationQueryHandler &operator=(const ReplicationQueryHandler &) = default;

  ReplicationQueryHandler(ReplicationQueryHandler &&) = default;
  ReplicationQueryHandler &operator=(ReplicationQueryHandler &&) = default;

  struct Replica {
    std::string name;
    std::string socket_address;
    ReplicationQuery::SyncMode sync_mode;
    std::optional<double> timeout;
    uint64_t current_timestamp_of_replica;
    uint64_t current_number_of_timestamp_behind_master;
    ReplicationQuery::ReplicaState state;
  };

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void SetReplicationRole(ReplicationQuery::ReplicationRole replication_role, std::optional<int64_t> port) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual ReplicationQuery::ReplicationRole ShowReplicationRole() const = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void RegisterReplica(const std::string &name, const std::string &socket_address,
                               ReplicationQuery::SyncMode sync_mode,
                               const std::chrono::seconds replica_check_frequency) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void DropReplica(const std::string &replica_name) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<Replica> ShowReplicas() const = 0;
};

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
  std::function<std::optional<QueryHandlerResult>(AnyStream *stream, std::optional<int> n)> query_handler;
  plan::ReadWriteTypeChecker::RWType rw_type;
};

class Interpreter;

/**
 * Holds data shared between multiple `Interpreter` instances (which might be
 * running concurrently).
 *
 */
struct InterpreterContext {
  explicit InterpreterContext(storage::Storage *db, InterpreterConfig config,
                              const std::filesystem::path &data_directory);

  storage::Storage *db;

  // ANTLR has singleton instance that is shared between threads. It is
  // protected by locks inside of ANTLR. Unfortunately, they are not protected
  // in a very good way. Once we have ANTLR version without race conditions we
  // can remove this lock. This will probably never happen since ANTLR
  // developers introduce more bugs in each version. Fortunately, we have
  // cache so this lock probably won't impact performance much...
  utils::SpinLock antlr_lock;
  std::optional<double> tsc_frequency{utils::GetTSCFrequency()};
  std::atomic<bool> is_shutting_down{false};

  AuthQueryHandler *auth{nullptr};
  AuthChecker *auth_checker{nullptr};

  utils::SkipList<QueryCacheEntry> ast_cache;
  utils::SkipList<PlanCacheEntry> plan_cache;

  TriggerStore trigger_store;
  utils::ThreadPool after_commit_trigger_pool{1};

  const InterpreterConfig config;

  query::stream::Streams streams;
  utils::Synchronized<std::unordered_set<Interpreter *>, utils::SpinLock> interpreters;
};

/// Function that is used to tell all active interpreters that they should stop
/// their ongoing execution.
inline void Shutdown(InterpreterContext *context) { context->is_shutting_down.store(true, std::memory_order_release); }

class Interpreter final {
 public:
  explicit Interpreter(InterpreterContext *interpreter_context);
  Interpreter(const Interpreter &) = delete;
  Interpreter &operator=(const Interpreter &) = delete;
  Interpreter(Interpreter &&) = delete;
  Interpreter &operator=(Interpreter &&) = delete;
  ~Interpreter() { Abort(); }

  struct PrepareResult {
    std::vector<std::string> headers;
    std::vector<query::AuthQuery::Privilege> privileges;
    std::optional<int> qid;
  };

  std::optional<std::string> username_;
  bool in_explicit_transaction_{false};
  bool expect_rollback_{false};
  std::optional<std::map<std::string, storage::PropertyValue>> metadata_{};  //!< User defined transaction metadata

  /**
   * Prepare a query for execution.
   *
   * Preparing a query means to preprocess the query and save it for
   * future calls of `Pull`.
   *
   * @throw query::QueryException
   */
  PrepareResult Prepare(const std::string &query, const std::map<std::string, storage::PropertyValue> &params,
                        const std::string *username,
                        const std::map<std::string, storage::PropertyValue> &metadata = {});

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

  void BeginTransaction(const std::map<std::string, storage::PropertyValue> &metadata = {});

  /*
  Returns transaction id or empty if the db_accessor is not initialized.
  */
  std::optional<uint64_t> GetTransactionId() const;

  void CommitTransaction();

  void RollbackTransaction();

  void SetNextTransactionIsolationLevel(storage::IsolationLevel isolation_level);
  void SetSessionIsolationLevel(storage::IsolationLevel isolation_level);

  std::vector<TypedValue> GetQueries();

  /**
   * Abort the current multicommand transaction.
   */
  void Abort();

  std::atomic<TransactionStatus> transaction_status_{TransactionStatus::IDLE};

 private:
  struct QueryExecution {
    std::optional<PreparedQuery> prepared_query;
    std::variant<utils::MonotonicBufferResource, utils::PoolResource> execution_memory;
    utils::ResourceWithOutOfMemoryException execution_memory_with_exception;

    std::map<std::string, TypedValue> summary;
    std::vector<Notification> notifications;

    explicit QueryExecution(utils::MonotonicBufferResource monotonic_memory)
        : execution_memory(std::move(monotonic_memory)) {
      std::visit(
          [&](auto &memory_resource) {
            execution_memory_with_exception = utils::ResourceWithOutOfMemoryException(&memory_resource);
          },
          execution_memory);
    };

    explicit QueryExecution(utils::PoolResource pool_resource) : execution_memory(std::move(pool_resource)) {
      std::visit(
          [&](auto &memory_resource) {
            execution_memory_with_exception = utils::ResourceWithOutOfMemoryException(&memory_resource);
          },
          execution_memory);
    };

    QueryExecution(const QueryExecution &) = delete;
    QueryExecution(QueryExecution &&) = default;
    QueryExecution &operator=(const QueryExecution &) = delete;
    QueryExecution &operator=(QueryExecution &&) = default;

    ~QueryExecution() {
      // We should always release the execution memory AFTER we
      // destroy the prepared query which is using that instance
      // of execution memory.
      prepared_query.reset();
      std::visit([](auto &memory_resource) { memory_resource.Release(); }, execution_memory);
    }
  };

  // Interpreter supports multiple prepared queries at the same time.
  // The client can reference a specific query for pull using an arbitrary qid
  // which is in our case the index of the query in the vector.
  // To simplify the handling of the qid we avoid modifying the vector if it
  // affects the position of the currently running queries in any way.
  // For example, we cannot delete the prepared query from the vector because
  // every prepared query after the deleted one will be moved by one place
  // making their qid not equal to the their index inside the vector.
  // To avoid this, we use unique_ptr with which we manually control construction
  // and deletion of a single query execution, i.e. when a query finishes,
  // we reset the corresponding unique_ptr.
  std::vector<std::unique_ptr<QueryExecution>> query_executions_;
  // all queries that are run as part of the current transaction
  utils::Synchronized<std::vector<std::string>, utils::SpinLock> transaction_queries_;

  InterpreterContext *interpreter_context_;

  // This cannot be std::optional because we need to move this accessor later on into a lambda capture
  // which is assigned to std::function. std::function requires every object to be copyable, so we
  // move this unique_ptr into a shared_ptr.
  std::unique_ptr<storage::Storage::Accessor> db_accessor_;
  std::optional<DbAccessor> execution_db_accessor_;
  std::optional<TriggerContextCollector> trigger_context_collector_;
  std::optional<FrameChangeCollector> frame_change_collector_;

  std::optional<storage::IsolationLevel> interpreter_isolation_level;
  std::optional<storage::IsolationLevel> next_transaction_isolation_level;

  PreparedQuery PrepareTransactionQuery(std::string_view query_upper,
                                        const std::map<std::string, storage::PropertyValue> &metadata = {});
  void Commit();
  void AdvanceCommand();
  void AbortCommand(std::unique_ptr<QueryExecution> *query_execution);
  std::optional<storage::IsolationLevel> GetIsolationLevelOverride();

  size_t ActiveQueryExecutions() {
    return std::count_if(query_executions_.begin(), query_executions_.end(),
                         [](const auto &execution) { return execution && execution->prepared_query; });
  }
};

class TransactionQueueQueryHandler {
 public:
  TransactionQueueQueryHandler() = default;
  virtual ~TransactionQueueQueryHandler() = default;

  TransactionQueueQueryHandler(const TransactionQueueQueryHandler &) = default;
  TransactionQueueQueryHandler &operator=(const TransactionQueueQueryHandler &) = default;

  TransactionQueueQueryHandler(TransactionQueueQueryHandler &&) = default;
  TransactionQueueQueryHandler &operator=(TransactionQueueQueryHandler &&) = default;

  static std::vector<std::vector<TypedValue>> ShowTransactions(const std::unordered_set<Interpreter *> &interpreters,
                                                               const std::optional<std::string> &username,
                                                               bool hasTransactionManagementPrivilege);

  static std::vector<std::vector<TypedValue>> KillTransactions(
      InterpreterContext *interpreter_context, const std::vector<std::string> &maybe_kill_transaction_ids,
      const std::optional<std::string> &username, bool hasTransactionManagementPrivilege);
};

template <typename TStream>
std::map<std::string, TypedValue> Interpreter::Pull(TStream *result_stream, std::optional<int> n,
                                                    std::optional<int> qid) {
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
  try {
    // Wrap the (statically polymorphic) stream type into a common type which
    // the handler knows.
    AnyStream stream{result_stream,
                     std::visit([](auto &execution_memory) -> utils::MemoryResource * { return &execution_memory; },
                                query_execution->execution_memory)};
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
          case QueryHandlerResult::NOTHING:
            // The only cases in which we have nothing to do are those where
            // we're either in an explicit transaction or the query is such that
            // a transaction wasn't started on a call to `Prepare()`.
            MG_ASSERT(in_explicit_transaction_ || !db_accessor_);
            break;
        }
        // As the transaction is done we can clear all the executions
        // NOTE: we cannot clear query_execution inside the Abort and Commit
        // methods as we will delete summary contained in them which we need
        // after our query finished executing.
        query_executions_.clear();
        transaction_queries_->clear();
      } else {
        // We can only clear this execution as some of the queries
        // in the transaction can be in unfinished state
        query_execution.reset(nullptr);
      }
    }
  } catch (const ExplicitTransactionUsageException &) {
    query_execution.reset(nullptr);
    throw;
  } catch (const utils::BasicException &) {
    memgraph::metrics::IncrementCounter(memgraph::metrics::FailedQuery);
    AbortCommand(&query_execution);
    throw;
  }

  if (maybe_summary) {
    // return the execution summary
    maybe_summary->insert_or_assign("has_more", false);
    return std::move(*maybe_summary);
  }

  // don't return the execution summary as it's not finished
  return {{"has_more", TypedValue(true)}};
}
}  // namespace memgraph::query
