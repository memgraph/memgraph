#pragma once

#include <gflags/gflags.h>

#include "query/constants.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/stripped.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "query/stream.hpp"
#include "query/typed_value.hpp"
#include "utils/memory.hpp"
#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"
#include "utils/timer.hpp"
#include "utils/tsc.hpp"

DECLARE_bool(query_cost_planner);
DECLARE_int32(query_plan_cache_ttl);

namespace query {

static constexpr size_t kExecutionMemoryBlockSize = 1U * 1024U * 1024U;

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
  virtual bool CreateUser(const std::string &username,
                          const std::optional<std::string> &password) = 0;

  /// Return false if the user does not exist.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool DropUser(const std::string &username) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void SetPassword(const std::string &username,
                           const std::optional<std::string> &password) = 0;

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
  virtual std::optional<std::string> GetRolenameForUser(
      const std::string &username) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<TypedValue> GetUsernamesForRole(
      const std::string &rolename) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void SetRole(const std::string &username,
                       const std::string &rolename) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void ClearRole(const std::string &username) = 0;

  virtual std::vector<std::vector<TypedValue>> GetPrivileges(
      const std::string &user_or_role) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void GrantPrivilege(
      const std::string &user_or_role,
      const std::vector<AuthQuery::Privilege> &privileges) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void DenyPrivilege(
      const std::string &user_or_role,
      const std::vector<AuthQuery::Privilege> &privileges) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void RevokePrivilege(
      const std::string &user_or_role,
      const std::vector<AuthQuery::Privilege> &privileges) = 0;
};

enum class QueryHandlerResult { COMMIT, ABORT, NOTHING };

/**
 * A container for data related to the preparation of a query.
 */
struct PreparedQuery {
  std::vector<std::string> header;
  std::vector<AuthQuery::Privilege> privileges;
  std::function<std::optional<QueryHandlerResult>(AnyStream *stream,
                                                  std::optional<int> n)>
      query_handler;
};

// TODO: Maybe this should move to query/plan/planner.
/// Interface for accessing the root operator of a logical plan.
class LogicalPlan {
 public:
  virtual ~LogicalPlan() {}

  virtual const plan::LogicalOperator &GetRoot() const = 0;
  virtual double GetCost() const = 0;
  virtual const SymbolTable &GetSymbolTable() const = 0;
  virtual const AstStorage &GetAstStorage() const = 0;
};

class CachedPlan {
 public:
  explicit CachedPlan(std::unique_ptr<LogicalPlan> plan);

  const auto &plan() const { return plan_->GetRoot(); }
  double cost() const { return plan_->GetCost(); }
  const auto &symbol_table() const { return plan_->GetSymbolTable(); }
  const auto &ast_storage() const { return plan_->GetAstStorage(); }

  bool IsExpired() const {
    return cache_timer_.Elapsed() >
           std::chrono::seconds(FLAGS_query_plan_cache_ttl);
  };

 private:
  std::unique_ptr<LogicalPlan> plan_;
  utils::Timer cache_timer_;
};

struct CachedQuery {
  AstStorage ast_storage;
  Query *query;
  std::vector<AuthQuery::Privilege> required_privileges;
};

struct QueryCacheEntry {
  bool operator==(const QueryCacheEntry &other) const {
    return first == other.first;
  }
  bool operator<(const QueryCacheEntry &other) const {
    return first < other.first;
  }
  bool operator==(const uint64_t &other) const { return first == other; }
  bool operator<(const uint64_t &other) const { return first < other; }

  uint64_t first;
  // TODO: Maybe store the query string here and use it as a key with the hash
  // so that we eliminate the risk of hash collisions.
  CachedQuery second;
};

struct PlanCacheEntry {
  bool operator==(const PlanCacheEntry &other) const {
    return first == other.first;
  }
  bool operator<(const PlanCacheEntry &other) const {
    return first < other.first;
  }
  bool operator==(const uint64_t &other) const { return first == other; }
  bool operator<(const uint64_t &other) const { return first < other; }

  uint64_t first;
  // TODO: Maybe store the query string here and use it as a key with the hash
  // so that we eliminate the risk of hash collisions.
  std::shared_ptr<CachedPlan> second;
};

/**
 * Holds data shared between multiple `Interpreter` instances (which might be
 * running concurrently).
 *
 * Users should initialize the context but should not modify it after it has
 * been passed to an `Interpreter` instance.
 */
struct InterpreterContext {
  explicit InterpreterContext(storage::Storage *db) : db(db) {
    CHECK(db) << "Storage must not be NULL";
  }

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
  // The default execution timeout is 3 minutes.
  double execution_timeout_sec{180.0};

  AuthQueryHandler *auth{nullptr};

  utils::SkipList<QueryCacheEntry> ast_cache;
  utils::SkipList<PlanCacheEntry> plan_cache;
};

/// Function that is used to tell all active interpreters that they should stop
/// their ongoing execution.
inline void Shutdown(InterpreterContext *context) {
  context->is_shutting_down.store(true, std::memory_order_release);
}

/// Function used to set the maximum execution timeout in seconds.
inline void SetExecutionTimeout(InterpreterContext *context, double timeout) {
  context->execution_timeout_sec = timeout;
}

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

  /**
   * Prepare a query for execution.
   *
   * To prepare a query for execution means to preprocess the query and adjust
   * the state of the `Interpreter` in such a way so that the next call to
   * `PullAll` executes the query.
   *
   * @throw query::QueryException
   */
  PrepareResult Prepare(
      const std::string &query,
      const std::map<std::string, storage::PropertyValue> &params);

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

  template <typename TStream>
  std::map<std::string, TypedValue> Pull(TStream *result_stream,
                                         std::optional<int> n = {},
                                         std::optional<int> qid = {});

  void BeginTransaction();

  void CommitTransaction();

  void RollbackTransaction();

  /**
   * Abort the current multicommand transaction.
   */
  void Abort();

 private:
  struct QueryExecution {
    std::optional<PreparedQuery> prepared_query;
    utils::MonotonicBufferResource execution_memory{kExecutionMemoryBlockSize};
    std::map<std::string, TypedValue> summary;

    explicit QueryExecution() = default;
    QueryExecution(const QueryExecution &) = delete;
    QueryExecution(QueryExecution &&) = default;
    QueryExecution &operator=(const QueryExecution &) = delete;
    QueryExecution &operator=(QueryExecution &&) = default;

    ~QueryExecution() {
      prepared_query.reset();
      execution_memory.Release();
    }
  };

  std::vector<std::optional<QueryExecution>> query_executions_;

  InterpreterContext *interpreter_context_;

  std::optional<storage::Storage::Accessor> db_accessor_;
  std::optional<DbAccessor> execution_db_accessor_;
  bool in_explicit_transaction_{false};
  bool expect_rollback_{false};

  PreparedQuery PrepareTransactionQuery(std::string_view query_upper);
  void Commit();
  void AdvanceCommand();
  void AbortCommand(std::optional<QueryExecution> *query_execution);

  size_t ActiveQueryExecutions() {
    return std::count_if(query_executions_.begin(), query_executions_.end(),
                         [](const auto &execution) {
                           return execution && execution->prepared_query;
                         });
  }
};

template <typename TStream>
std::map<std::string, TypedValue> Interpreter::Pull(TStream *result_stream,
                                                    std::optional<int> n,
                                                    std::optional<int> qid) {
  CHECK(in_explicit_transaction_ || !qid)
      << "qid can be only used in explicit transaction!";
  const int qid_value =
      qid ? *qid : static_cast<int>(query_executions_.size() - 1);

  if (qid_value < 0 || qid_value >= query_executions_.size()) {
    throw InvalidArgumentsException("qid",
                                    "Query with specified ID does not exist!");
  }

  if (n && n < 0) {
    throw InvalidArgumentsException("n",
                                    "Cannot fetch negative number of results!");
  }

  auto &query_execution = query_executions_[qid_value];

  CHECK(query_execution && query_execution->prepared_query)
      << "Query already finished executing!";

  std::optional<std::map<std::string, TypedValue>> maybe_summary;
  try {
    // Wrap the (statically polymorphic) stream type into a common type which
    // the handler knows.
    AnyStream stream{result_stream, &query_execution->execution_memory};
    const auto maybe_res =
        query_execution->prepared_query->query_handler(&stream, n);

    if (maybe_res) {
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
            CHECK(in_explicit_transaction_ || !db_accessor_);
            break;
        }
      }
      maybe_summary.emplace(std::move(query_execution->summary));
      query_execution.reset();
    }
  } catch (const ExplicitTransactionUsageException &) {
    // Just let the exception propagate for error reporting purposes, but don't
    // abort the current command.
    throw;
  } catch (const utils::BasicException &) {
    AbortCommand(&query_execution);
    throw;
  }

  while (!query_executions_.empty() && !query_executions_.back()) {
    query_executions_.pop_back();
  }

  if (maybe_summary) {
    maybe_summary->insert_or_assign("has_more", false);
    return std::move(*maybe_summary);
  }

  return {{"has_more", TypedValue(true)}};
}
}  // namespace query
