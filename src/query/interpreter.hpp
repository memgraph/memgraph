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

#include "dbms/database.hpp"
#include "memory/query_memory_control.hpp"
#include "query/auth_checker.hpp"
#include "query/auth_query_handler.hpp"
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
#include "spdlog/spdlog.h"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"
#include "utils/event_counter.hpp"
#include "utils/event_trigger.hpp"
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
extern const Event FailedPrepare;
extern const Event FailedPull;
extern const Event SuccessfulQuery;
}  // namespace memgraph::metrics

namespace memgraph::query {

struct InterpreterContext;

inline constexpr size_t kExecutionMemoryBlockSize = 1UL * 1024UL * 1024UL;
inline constexpr size_t kExecutionPoolMaxBlockSize = 1024UL;  // 2 ^ 10

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
  virtual void DropReplica(std::string_view replica_name) = 0;

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
  std::optional<std::string> db{};
};

/**
 * Holds data for the Query which is extra
 * NOTE: maybe need to parse more in the future, ATM we ignore some parts from BOLT
 */
struct QueryExtras {
  std::map<std::string, memgraph::storage::PropertyValue> metadata_pv;
  std::optional<int64_t> tx_timeout;
};

struct CurrentDB {
  CurrentDB() = default;  // TODO: remove, we should always have an implicit default obtainable from somewhere
                          //       ATM: it is provided by the DatabaseAccess
                          //       future: should be a name + ptr to dbms_handler, lazy fetch when needed
  explicit CurrentDB(memgraph::dbms::DatabaseAccess db_acc) : db_acc_{std::move(db_acc)} {}

  CurrentDB(CurrentDB const &) = delete;
  CurrentDB &operator=(CurrentDB const &) = delete;

  void SetupDatabaseTransaction(std::optional<storage::IsolationLevel> override_isolation_level, bool could_commit,
                                bool unique = false);
  void CleanupDBTransaction(bool abort);
  void SetCurrentDB(memgraph::dbms::DatabaseAccess new_db, bool in_explicit_db) {
    // do we lock here?
    db_acc_ = std::move(new_db);
    in_explicit_db_ = in_explicit_db;
  }

  // TODO: don't provide explicitly via constructor, instead have a lazy way of getting the current/default
  // DatabaseAccess
  //       hence, explict bolt "use DB" in metadata wouldn't necessarily get access unless query required it.
  std::optional<memgraph::dbms::DatabaseAccess> db_acc_;  // Current db (TODO: expand to support multiple)
  std::unique_ptr<storage::Storage::Accessor> db_transactional_accessor_;
  std::optional<DbAccessor> execution_db_accessor_;
  std::optional<TriggerContextCollector> trigger_context_collector_;
  bool in_explicit_db_{false};
};

class Interpreter final {
 public:
  explicit Interpreter(InterpreterContext *interpreter_context);
  Interpreter(InterpreterContext *interpreter_context, memgraph::dbms::DatabaseAccess db);
  Interpreter(const Interpreter &) = delete;
  Interpreter &operator=(const Interpreter &) = delete;
  Interpreter(Interpreter &&) = delete;
  Interpreter &operator=(Interpreter &&) = delete;
  ~Interpreter() { Abort(); }

  struct PrepareResult {
    std::vector<std::string> headers;
    std::vector<query::AuthQuery::Privilege> privileges;
    std::optional<int> qid;
    std::optional<std::string> db;
  };

  std::optional<std::string> username_;
  bool in_explicit_transaction_{false};
  CurrentDB current_db_;

  bool expect_rollback_{false};
  std::shared_ptr<utils::AsyncTimer> current_timeout_timer_{};
  std::optional<std::map<std::string, storage::PropertyValue>> metadata_{};  //!< User defined transaction metadata

#ifdef MG_ENTERPRISE
  void SetCurrentDB(std::string_view db_name, bool explicit_db);
  void OnChangeCB(auto cb) { on_change_.emplace(cb); }
#endif

  /**
   * Prepare a query for execution.
   *
   * Preparing a query means to preprocess the query and save it for
   * future calls of `Pull`.
   *
   * @throw query::QueryException
   */
  Interpreter::PrepareResult Prepare(const std::string &query,
                                     const std::map<std::string, storage::PropertyValue> &params,
                                     QueryExtras const &extras);

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

  void CommitTransaction();

  void RollbackTransaction();

  void SetNextTransactionIsolationLevel(storage::IsolationLevel isolation_level);
  void SetSessionIsolationLevel(storage::IsolationLevel isolation_level);

  std::vector<TypedValue> GetQueries();

  /**
   * Abort the current multicommand transaction.
   */
  void Abort();

  std::atomic<TransactionStatus> transaction_status_{TransactionStatus::IDLE};  // Tie to current_transaction_
  std::optional<uint64_t> current_transaction_;

  void ResetUser();

  void SetUser(std::string_view username);

 private:
  struct QueryExecution {
    std::variant<utils::MonotonicBufferResource, utils::PoolResource> execution_memory;
    utils::ResourceWithOutOfMemoryException execution_memory_with_exception;
    std::optional<PreparedQuery> prepared_query;

    std::map<std::string, TypedValue> summary;
    std::vector<Notification> notifications;

    static auto Create(std::variant<utils::MonotonicBufferResource, utils::PoolResource> memory_resource,
                       std::optional<PreparedQuery> prepared_query = std::nullopt) -> std::unique_ptr<QueryExecution> {
      return std::make_unique<QueryExecution>(std::move(memory_resource), std::move(prepared_query));
    }

    explicit QueryExecution(std::variant<utils::MonotonicBufferResource, utils::PoolResource> memory_resource,
                            std::optional<PreparedQuery> prepared_query)
        : execution_memory(std::move(memory_resource)), prepared_query{std::move(prepared_query)} {
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

    void CleanRuntimeData() {
      if (prepared_query.has_value()) {
        prepared_query.reset();
      }
      notifications.clear();
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
  // To avoid this, we use unique_ptr with which we manualy control construction
  // and deletion of a single query execution, i.e. when a query finishes,
  // we reset the corresponding unique_ptr.
  // TODO Figure out how this would work for multi-database
  // Exists only during a single transaction (for now should be okay as is)
  std::vector<std::unique_ptr<QueryExecution>> query_executions_;
  // all queries that are run as part of the current transaction
  utils::Synchronized<std::vector<std::string>, utils::SpinLock> transaction_queries_;

  InterpreterContext *interpreter_context_;

  std::optional<FrameChangeCollector> frame_change_collector_;

  std::optional<storage::IsolationLevel> interpreter_isolation_level;
  std::optional<storage::IsolationLevel> next_transaction_isolation_level;

  PreparedQuery PrepareTransactionQuery(std::string_view query_upper, QueryExtras const &extras = {});
  void Commit();
  void AdvanceCommand();
  void AbortCommand(std::unique_ptr<QueryExecution> *query_execution);
  std::optional<storage::IsolationLevel> GetIsolationLevelOverride();

  size_t ActiveQueryExecutions() {
    return std::count_if(query_executions_.begin(), query_executions_.end(),
                         [](const auto &execution) { return execution && execution->prepared_query; });
  }

  std::optional<std::function<void(std::string_view)>> on_change_{};
  void SetupInterpreterTransaction(const QueryExtras &extras);
  void SetupDatabaseTransaction(bool couldCommit, bool unique = false);
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
      if (current_transaction_) {
        memgraph::memory::TryStopTrackingOnTransaction(*current_transaction_);
      }
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
            MG_ASSERT(in_explicit_transaction_ || !current_db_.db_transactional_accessor_);
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
    if (current_transaction_) {
      memgraph::memory::TryStopTrackingOnTransaction(*current_transaction_);
    }
    query_execution.reset(nullptr);
    throw;
  } catch (const utils::BasicException &) {
    if (current_transaction_) {
      memgraph::memory::TryStopTrackingOnTransaction(*current_transaction_);
    }
    // Trigger first failed query
    metrics::FirstFailedQuery();
    memgraph::metrics::IncrementCounter(memgraph::metrics::FailedQuery);
    memgraph::metrics::IncrementCounter(memgraph::metrics::FailedPull);
    AbortCommand(&query_execution);
    throw;
  }

  if (maybe_summary) {
    // Toggle first successfully completed query
    metrics::FirstSuccessfulQuery();
    memgraph::metrics::IncrementCounter(memgraph::metrics::SuccessfulQuery);
    // return the execution summary
    maybe_summary->insert_or_assign("has_more", false);
    return std::move(*maybe_summary);
  }

  // don't return the execution summary as it's not finished
  return {{"has_more", TypedValue(true)}};
}

}  // namespace memgraph::query
