#pragma once

#include <gflags/gflags.h>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "query/context.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/stripped.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "utils/thread/sync.hpp"
#include "utils/timer.hpp"

DECLARE_bool(query_cost_planner);
DECLARE_int32(query_plan_cache_ttl);

namespace auth {
class Auth;
}  // namespace auth

namespace integrations::kafka {
class Streams;
}  // namespace integrations::kafka

namespace query {

// TODO: Maybe this should move to query/plan/planner.
/// Interface for accessing the root operator of a logical plan.
class LogicalPlan {
 public:
  virtual ~LogicalPlan() {}

  virtual const plan::LogicalOperator &GetRoot() const = 0;
  virtual double GetCost() const = 0;
  virtual const SymbolTable &GetSymbolTable() const = 0;
};

class Interpreter {
 private:
  class CachedPlan {
   public:
    CachedPlan(std::unique_ptr<LogicalPlan> plan);

    const auto &plan() const { return plan_->GetRoot(); }
    double cost() const { return plan_->GetCost(); }
    const auto &symbol_table() const { return plan_->GetSymbolTable(); }

    bool IsExpired() const {
      return cache_timer_.Elapsed() >
             std::chrono::seconds(FLAGS_query_plan_cache_ttl);
    };

   private:
    std::unique_ptr<LogicalPlan> plan_;
    utils::Timer cache_timer_;
  };

  using PlanCacheT = ConcurrentMap<HashType, std::shared_ptr<CachedPlan>>;

 public:
  /**
   * Encapsulates all what's necessary for the interpretation of a query
   * into a single object that can be pulled (into the given Stream).
   */
  class Results {
    friend Interpreter;
    Results(Context ctx, std::shared_ptr<CachedPlan> plan,
            std::unique_ptr<query::plan::Cursor> cursor,
            std::vector<Symbol> output_symbols, std::vector<std::string> header,
            std::map<std::string, TypedValue> summary, PlanCacheT &plan_cache,
            std::vector<AuthQuery::Privilege> privileges)
        : ctx_(std::move(ctx)),
          plan_(plan),
          cursor_(std::move(cursor)),
          frame_(ctx_.symbol_table_.max_position()),
          output_symbols_(output_symbols),
          header_(header),
          summary_(summary),
          plan_cache_(plan_cache),
          privileges_(std::move(privileges)) {}

   public:
    Results(const Results &) = delete;
    Results(Results &&) = default;
    Results &operator=(const Results &) = delete;
    Results &operator=(Results &&) = default;

    /**
     * Make the interpreter perform a single Pull. Results (if they exists) are
     * pushed into the given stream. On first Pull the header is written to the
     * stream, on last the summary.
     *
     * @param stream - The stream to push the header, results and summary into.
     * @return - If this Results is eligible for another Pull. If Pulling
     * after `false` has been returned, the behavior is undefined.
     * @tparam TStream - Stream type.
     */
    template <typename TStream>
    bool Pull(TStream &stream) {
      utils::Timer timer;
      bool return_value = cursor_->Pull(frame_, ctx_);
      if (return_value && !output_symbols_.empty()) {
        std::vector<TypedValue> values;
        values.reserve(output_symbols_.size());
        for (const auto &symbol : output_symbols_) {
          values.emplace_back(frame_[symbol]);
        }
        stream.Result(values);
      }
      execution_time_ += timer.Elapsed().count();

      if (!return_value) {
        summary_["plan_execution_time"] = execution_time_;

        if (ctx_.is_index_created_) {
          auto access = plan_cache_.access();
          for (auto &kv : access) {
            access.remove(kv.first);
          }
        }
      }

      return return_value;
    }

    /** Calls Pull() until exhausted. */
    template <typename TStream>
    void PullAll(TStream &stream) {
      while (Pull(stream)) continue;
    }

    const std::vector<std::string> &header() { return header_; }
    const std::map<std::string, TypedValue> &summary() { return summary_; }

    const std::vector<AuthQuery::Privilege> &privileges() {
      return privileges_;
    }

   private:
    Context ctx_;
    std::shared_ptr<CachedPlan> plan_;
    std::unique_ptr<query::plan::Cursor> cursor_;
    Frame frame_;
    std::vector<Symbol> output_symbols_;

    std::vector<std::string> header_;
    std::map<std::string, TypedValue> summary_;

    double execution_time_{0};
    // Gets invalidated after if an index has been built.
    PlanCacheT &plan_cache_;

    std::vector<AuthQuery::Privilege> privileges_;
  };

  Interpreter() = default;
  Interpreter(const Interpreter &) = delete;
  Interpreter &operator=(const Interpreter &) = delete;
  Interpreter(Interpreter &&) = delete;
  Interpreter &operator=(Interpreter &&) = delete;

  virtual ~Interpreter() {}

  /**
   * Generates an Results object for the parameters. The resulting object
   * can be Pulled with its results written to an arbitrary stream.
   */
  Results operator()(const std::string &query,
                     database::GraphDbAccessor &db_accessor,
                     const std::map<std::string, TypedValue> &params,
                     bool in_explicit_transaction);

  auth::Auth *auth_ = nullptr;
  integrations::kafka::Streams *kafka_streams_ = nullptr;

 protected:
  // high level tree -> logical plan
  // AstStorage and SymbolTable may be modified during planning. The created
  // LogicalPlan must take ownership of AstStorage and SymbolTable.
  virtual std::unique_ptr<LogicalPlan> MakeLogicalPlan(AstStorage, Context *);

 private:
  ConcurrentMap<HashType, AstStorage> ast_cache_;
  PlanCacheT plan_cache_;
  // Antlr has singleton instance that is shared between threads. It is
  // protected by locks inside of antlr. Unfortunately, they are not protected
  // in a very good way. Once we have antlr version without race conditions we
  // can remove this lock. This will probably never happen since antlr
  // developers introduce more bugs in each version. Fortunately, we have cache
  // so this lock probably won't impact performance much...
  utils::SpinLock antlr_lock_;

  // high level tree -> CachedPlan
  std::shared_ptr<CachedPlan> AstToPlan(AstStorage ast_storage, Context *ctx);
  // stripped query -> high level tree
  AstStorage QueryToAst(const StrippedQuery &stripped, Context &ctx);
};

}  // namespace query
