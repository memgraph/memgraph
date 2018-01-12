#pragma once

#include <ctime>
#include <limits>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db_accessor.hpp"
#include "query/context.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/stripped.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/timer.hpp"

DECLARE_int32(query_plan_cache_ttl);

namespace query {

class Interpreter {
 private:
  class CachedPlan {
   public:
    CachedPlan(std::unique_ptr<plan::LogicalOperator> plan, double cost,
               SymbolTable symbol_table, AstTreeStorage storage)
        : plan_(std::move(plan)),
          cost_(cost),
          symbol_table_(symbol_table),
          ast_storage_(std::move(storage)) {}

    const auto &plan() const { return *plan_; }
    double cost() const { return cost_; }
    const auto &symbol_table() const { return symbol_table_; }

    bool IsExpired() const {
      auto elapsed = cache_timer_.Elapsed();
      return std::chrono::duration_cast<std::chrono::seconds>(elapsed) >
             std::chrono::seconds(FLAGS_query_plan_cache_ttl);
    };

   private:
    std::unique_ptr<plan::LogicalOperator> plan_;
    double cost_;
    SymbolTable symbol_table_;
    AstTreeStorage ast_storage_;
    utils::Timer cache_timer_;
  };

 public:
  /**
   * Encapsulates all what's necessary for the interpretation of a query into a
   * single object that can be pulled (into the given Stream).
   */
  class Results {
    friend Interpreter;
    Results(Context ctx, std::shared_ptr<CachedPlan> plan,
            std::unique_ptr<query::plan::Cursor> cursor,
            std::vector<Symbol> output_symbols, std::vector<std::string> header,
            std::map<std::string, TypedValue> summary,
            ConcurrentMap<HashType, std::shared_ptr<CachedPlan>> &plan_cache)
        : ctx_(std::move(ctx)),
          plan_(plan),
          cursor_(std::move(cursor)),
          frame_(ctx_.symbol_table_.max_position()),
          output_symbols_(output_symbols),
          header_(header),
          summary_(summary),
          plan_cache_(plan_cache) {}

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
      if (!header_written_) {
        stream.Header(header_);
        header_written_ = true;
      }

      bool return_value = cursor_->Pull(frame_, ctx_);

      if (return_value && !output_symbols_.empty()) {
        std::vector<TypedValue> values;
        values.reserve(output_symbols_.size());
        for (const auto &symbol : output_symbols_) {
          values.emplace_back(frame_[symbol]);
        }
        stream.Result(values);
      }

      if (!return_value) {
        auto execution_time = execution_timer_.Elapsed();
        summary_["plan_execution_time"] = execution_time.count();
        stream.Summary(summary_);

        if (ctx_.is_index_created_) {
          // If index is created we invalidate cache so that we can try to
          // generate better plan with that cache.
          auto accessor = plan_cache_.access();
          for (const auto &cached_plan : accessor) {
            accessor.remove(cached_plan.first);
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

   private:
    Context ctx_;
    std::shared_ptr<CachedPlan> plan_;
    std::unique_ptr<query::plan::Cursor> cursor_;
    Frame frame_;
    std::vector<Symbol> output_symbols_;

    bool header_written_{false};
    std::vector<std::string> header_;
    std::map<std::string, TypedValue> summary_;

    utils::Timer execution_timer_;
    // Gets invalidated after if an index has been built.
    ConcurrentMap<HashType, std::shared_ptr<CachedPlan>> &plan_cache_;
  };

  Interpreter() = default;
  Interpreter(const Interpreter &) = delete;
  Interpreter &operator=(const Interpreter &) = delete;
  Interpreter(Interpreter &&) = delete;
  Interpreter &operator=(Interpreter &&) = delete;

  /**
   * Generates an Results object for the parameters. The resulting object
   * can the be Pulled with it's results written to an arbitrary stream.
   */
  Results operator()(const std::string &query,
                     database::GraphDbAccessor &db_accessor,
                     const std::map<std::string, TypedValue> &params,
                     bool in_explicit_transaction);

 private:
  // stripped query -> high level tree
  AstTreeStorage QueryToAst(const StrippedQuery &stripped, Context &ctx);

  // high level tree -> (logical plan, plan cost)
  // AstTreeStorage and SymbolTable may be modified during planning.
  std::pair<std::unique_ptr<plan::LogicalOperator>, double> MakeLogicalPlan(
      AstTreeStorage &, const database::GraphDbAccessor &, Context &);

  ConcurrentMap<HashType, AstTreeStorage> ast_cache_;
  ConcurrentMap<HashType, std::shared_ptr<CachedPlan>> plan_cache_;
  // Antlr has singleton instance that is shared between threads. It is
  // protected by locks inside of antlr. Unfortunately, they are not protected
  // in a very good way. Once we have antlr version without race conditions we
  // can remove this lock. This will probably never happen since antlr
  // developers introduce more bugs in each version. Fortunately, we have cache
  // so this lock probably won't impact performance much...
  SpinLock antlr_lock_;
};

}  // namespace query
