#pragma once

#include <gflags/gflags.h>

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/stripped.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "utils/memory.hpp"
#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"
#include "utils/timer.hpp"
#include "utils/tsc.hpp"

DECLARE_bool(query_cost_planner);
DECLARE_int32(query_plan_cache_ttl);

namespace auth {
class Auth;
}  // namespace auth

namespace query {

static constexpr size_t kExecutionMemoryBlockSize = 1U * 1024U * 1024U;

/**
 * `AnyStream` can wrap *any* type implementing the `Stream` concept into a
 * single type.
 *
 * The type erasure technique is used. The original type which an `AnyStream`
 * was constructed from is "erased", as `AnyStream` is not a class template and
 * doesn't use the type in any way. Client code can then program just for
 * `AnyStream`, rather than using static polymorphism to handle any type
 * implementing the `Stream` concept.
 */
class AnyStream final {
 public:
  template <class TStream>
  AnyStream(TStream *stream, utils::MemoryResource *memory_resource)
      : content_{utils::Allocator<GenericWrapper<TStream>>{memory_resource}
                     .template new_object<GenericWrapper<TStream>>(stream),
                 [memory_resource](Wrapper *ptr) {
                   utils::Allocator<GenericWrapper<TStream>>{memory_resource}
                       .template delete_object<GenericWrapper<TStream>>(
                           static_cast<GenericWrapper<TStream> *>(ptr));
                 }} {}

  void Result(const std::vector<TypedValue> &values) {
    content_->Result(values);
  }

 private:
  struct Wrapper {
    virtual void Result(const std::vector<TypedValue> &values) = 0;
  };

  template <class TStream>
  struct GenericWrapper final : public Wrapper {
    explicit GenericWrapper(TStream *stream) : stream_{stream} {}

    void Result(const std::vector<TypedValue> &values) override {
      stream_->Result(values);
    }

    TStream *stream_;
  };

  std::unique_ptr<Wrapper, std::function<void(Wrapper *)>> content_;
};

/**
 * A container for data related to the preparation of a query.
 */
struct PreparedQuery {
  std::vector<std::string> header;
  std::vector<AuthQuery::Privilege> privileges;
  std::function<bool(AnyStream *stream)> query_handler;
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
  bool operator==(const HashType &other) const { return first == other; }
  bool operator<(const HashType &other) const { return first < other; }

  HashType first;
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
  bool operator==(const HashType &other) const { return first == other; }
  bool operator<(const HashType &other) const { return first < other; }

  HashType first;
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
#ifdef MG_SINGLE_NODE_V2
  explicit InterpreterContext(storage::Storage *db)
#else
  explicit InterpreterContext(database::GraphDb *db)
#endif
      : db(db) {
    CHECK(db) << "Storage must not be NULL";
  }

#ifdef MG_SINGLE_NODE_V2
  storage::Storage *db;
#else
  database::GraphDb *db;
#endif

  // ANTLR has singleton instance that is shared between threads. It is
  // protected by locks inside of ANTLR. Unfortunately, they are not protected
  // in a very good way. Once we have ANTLR version without race conditions we
  // can remove this lock. This will probably never happen since ANTLR
  // developers introduce more bugs in each version. Fortunately, we have
  // cache so this lock probably won't impact performance much...
  utils::SpinLock antlr_lock;
  bool is_tsc_available{utils::CheckAvailableTSC()};

  auth::Auth *auth{nullptr};

  utils::SkipList<QueryCacheEntry> ast_cache;
  utils::SkipList<PlanCacheEntry> plan_cache;
};

class Interpreter final {
 public:
  explicit Interpreter(InterpreterContext *interpreter_context);
  Interpreter(const Interpreter &) = delete;
  Interpreter &operator=(const Interpreter &) = delete;
  Interpreter(Interpreter &&) = delete;
  Interpreter &operator=(Interpreter &&) = delete;
  ~Interpreter() { Abort(); }

  std::pair<std::vector<std::string>, std::vector<query::AuthQuery::Privilege>>
  Interpret(const std::string &query,
            const std::map<std::string, PropertyValue> &params);

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
  void Prepare(const std::string &query,
               const std::map<std::string, PropertyValue> &params);

  /**
   * Execute the last prepared query and stream *all* of the results into the
   * given stream.
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

#ifdef MG_SINGLE_NODE_V2
  std::optional<storage::Storage::Accessor> db_accessor_;
#else
  std::optional<database::GraphDbAccessor> db_accessor_;
#endif
  std::optional<DbAccessor> execution_db_accessor_;
  bool in_explicit_transaction_{false};
  bool expect_rollback_{false};
  utils::MonotonicBufferResource execution_memory_{kExecutionMemoryBlockSize};

  void Commit();
  void AdvanceCommand();
  void AbortCommand();
};

template <typename TStream>
std::map<std::string, TypedValue> Interpreter::PullAll(TStream *result_stream) {
  // If we don't have any results (eg. a transaction command preceeded),
  // return an empty summary.
  if (!prepared_query_) return {};

  try {
    // Wrap the (statically polymorphic) stream type into a common type which
    // the handler knows.
    AnyStream stream{result_stream, &execution_memory_};
    bool commit = prepared_query_->query_handler(&stream);

    if (!in_explicit_transaction_) {
      if (commit) {
        Commit();
      } else {
        Abort();
      }
    }

    return summary_;
#ifdef MG_SINGLE_NODE_HA
  } catch (const query::HintedAbortError &) {
    AbortCommand();
    throw utils::BasicException("Transaction was asked to abort.");
#endif
  } catch (const utils::BasicException &) {
    AbortCommand();
    throw;
  }
}

}  // namespace query
