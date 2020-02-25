#pragma once

#include <gflags/gflags.h>

#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/stripped.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "query/stream.hpp"
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
  std::function<QueryHandlerResult(AnyStream *stream)> query_handler;
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
  explicit InterpreterContext(storage::Storage *db)
      : db(db) {
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

  /**
   * Prepare a query for execution.
   *
   * To prepare a query for execution means to preprocess the query and adjust
   * the state of the `Interpreter` in such a way so that the next call to
   * `PullAll` executes the query.
   *
   * @throw raft::CantExecuteQueries if the Memgraph instance is not a Raft
   * leader and a query other than an Info Raft query was given
   * @throw query::QueryException
   */
  std::pair<std::vector<std::string>, std::vector<query::AuthQuery::Privilege>>
  Prepare(const std::string &query,
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
  std::map<std::string, TypedValue> PullAll(TStream *result_stream);

  /**
   * Abort the current multicommand transaction.
   */
  void Abort();

 private:
  InterpreterContext *interpreter_context_;
  std::optional<PreparedQuery> prepared_query_;
  std::map<std::string, TypedValue> summary_;

  std::optional<storage::Storage::Accessor> db_accessor_;
  std::optional<DbAccessor> execution_db_accessor_;
  bool in_explicit_transaction_{false};
  bool expect_rollback_{false};
  utils::MonotonicBufferResource execution_memory_{kExecutionMemoryBlockSize};

  PreparedQuery PrepareTransactionQuery(std::string_view query_upper);
  void Commit();
  void AdvanceCommand();
  void AbortCommand();
};

template <typename TStream>
std::map<std::string, TypedValue> Interpreter::PullAll(TStream *result_stream) {
  CHECK(prepared_query_) << "Trying to call PullAll without a prepared query";

  try {
    // Wrap the (statically polymorphic) stream type into a common type which
    // the handler knows.
    AnyStream stream{result_stream, &execution_memory_};
    QueryHandlerResult res = prepared_query_->query_handler(&stream);
    // Erase the prepared query in order to enforce that every call to `PullAll`
    // must be preceded by a call to `Prepare`.
    prepared_query_ = std::nullopt;

    if (!in_explicit_transaction_) {
      switch (res) {
        case QueryHandlerResult::COMMIT:
          Commit();
          break;
        case QueryHandlerResult::ABORT:
          Abort();
          break;
        case QueryHandlerResult::NOTHING:
          // The only cases in which we have nothing to do are those where we're
          // either in an explicit transaction or the query is such that a
          // transaction wasn't started on a call to `Prepare()`.
          CHECK(in_explicit_transaction_ || !db_accessor_);
          break;
      }
    }
  } catch (const ExplicitTransactionUsageException &) {
    // Just let the exception propagate for error reporting purposes, but don't
    // abort the current command.
    throw;
#ifdef MG_SINGLE_NODE_HA
  } catch (const query::HintedAbortError &) {
    AbortCommand();
    throw utils::BasicException("Transaction was asked to abort.");
#endif
  } catch (const utils::BasicException &) {
    AbortCommand();
    throw;
  }

  return summary_;
}

}  // namespace query
