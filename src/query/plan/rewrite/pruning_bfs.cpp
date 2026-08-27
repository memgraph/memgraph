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

#include "query/plan/rewrite/pruning_bfs.hpp"

#include <algorithm>
#include <cstdint>
#include <unordered_set>
#include <vector>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"
#include "utils/on_scope_exit.hpp"

namespace memgraph::query::plan {

namespace {

/// True when the cursor can settle the bound's value and OperatorName can name
/// the walk it calls for: null (absent), a literal, or a parameter lookup.
/// Anything else (a function call, an arithmetic expression) is out of
/// ConstExternalPropertyValue's reach, which would leave OperatorName unable to
/// agree with the cursor on which walk runs.
bool BoundIsResolvable(Expression *bound) {
  if (!bound) return true;
  return utils::Downcast<PrimitiveLiteral>(bound) || utils::Downcast<ParameterLookup>(bound);
}

class PruningBFSRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  explicit PruningBFSRewriter(SymbolTable const &symbol_table) : symbol_table_(symbol_table) {}

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &) override { return true; }

  bool PreVisit(Produce &op) override {
    for (auto *ne : op.named_expressions_) {
      CollectSymbolsFromExpression(ne->expression_);
    }
    return true;
  }

  bool PreVisit(Filter &op) override {
    // The whole predicate is in expression_, which is what the cursor evaluates.
    // all_filters_ describes it for the planner and is empty for some filters.
    CollectSymbolsFromExpression(op.expression_);
    // A pattern filter reads what the main pipeline binds, and Filter::Accept
    // reaches it only after the input. Left to that order, the input would be
    // analysed against a symbol set the branch had not yet contributed to.
    for (auto const &pattern_filter : op.pattern_filters_) {
      VisitSubquery(*pattern_filter);
    }
    op.input_->Accept(*this);
    return false;
  }

  bool PostVisit(Filter &) override { return true; }

  bool PreVisit(ConstructNamedPath &op) override {
    used_symbols_.insert(op.path_symbol_.position());
    for (auto const &sym : op.path_elements_) {
      used_symbols_.insert(sym.position());
    }
    return true;
  }

  bool PreVisit(Distinct &op) override {
    // Which rows are duplicates of which is read off these, so collapsing rows
    // below is only invisible where none of them come from there.
    for (auto const &sym : op.value_symbols_) {
      used_symbols_.insert(sym.position());
    }
    dedup_stack_.push_back(deduplicates_);
    deduplicates_ = true;
    return true;
  }

  bool PostVisit(Distinct &) override {
    deduplicates_ = dedup_stack_.back();
    dedup_stack_.pop_back();
    return true;
  }

  bool PreVisit(Aggregate &op) override {
    dedup_stack_.push_back(deduplicates_);
    deduplicates_ = !op.aggregations_.empty() &&
                    std::ranges::all_of(op.aggregations_, [](auto const &elem) { return elem.distinct; });
    for (auto const &elem : op.aggregations_) {
      CollectSymbolsFromExpression(elem.arg1);
      CollectSymbolsFromExpression(elem.arg2);
    }
    for (auto *expr : op.group_by_) {
      CollectSymbolsFromExpression(expr);
    }
    for (auto const &sym : op.remember_) {
      used_symbols_.insert(sym.position());
    }
    return true;
  }

  bool PostVisit(Aggregate &) override {
    deduplicates_ = dedup_stack_.back();
    dedup_stack_.pop_back();
    return true;
  }

  bool PreVisit(EdgeUniquenessFilter &op) override {
    used_symbols_.insert(op.expand_symbol_.position());
    for (auto const &sym : op.previous_symbols_) {
      used_symbols_.insert(sym.position());
    }
    return true;
  }

  bool PreVisit(Unwind &op) override {
    CollectSymbolsFromExpression(op.input_expression_);
    return true;
  }

  bool PreVisit(OrderBy &op) override {
    CollectSymbolsFromExpressions(op.order_by_);
    for (auto const &sym : op.output_symbols_) {
      used_symbols_.insert(sym.position());
    }
    return true;
  }

  bool PreVisit(Skip &op) override {
    CollectSymbolsFromExpression(op.expression_);
    deduplicates_ = false;
    return true;
  }

  bool PreVisit(Limit &op) override {
    CollectSymbolsFromExpression(op.expression_);
    deduplicates_ = false;
    return true;
  }

  bool PreVisit(Accumulate &op) override {
    for (auto const &sym : op.symbols_) {
      used_symbols_.insert(sym.position());
    }
    return true;
  }

  bool PreVisit(CallProcedure &op) override {
    CollectSymbolsFromExpressions(op.arguments_);
    // A write procedure runs once per row, so how many rows reach it is part of
    // what it does. A read procedure's results are collapsed along with the rows.
    if (op.graph_access_ == GraphAccess::Write) {
      deduplicates_ = false;
    }
    return true;
  }

  bool PreVisit(EmptyResult &) override { return true; }

  bool PreVisit(Apply &op) override {
    VisitSubquery(*op.subquery_);
    op.input_->Accept(*this);
    return false;
  }

  bool PostVisit(Apply &) override { return true; }

  bool PreVisit(Optional &op) override {
    for (auto const &sym : op.optional_symbols_) {
      used_symbols_.insert(sym.position());
    }
    VisitSubquery(*op.optional_);
    op.input_->Accept(*this);
    return false;
  }

  bool PostVisit(Optional &) override { return true; }

  bool PreVisit(Cartesian &op) override {
    // Both branches' frames are read to build the joined row, so a symbol named
    // here is live in the branch that binds it. VisitBranch restores the set it
    // finds on entry, which is why these go in first.
    for (auto const &sym : op.left_symbols_) {
      used_symbols_.insert(sym.position());
    }
    for (auto const &sym : op.right_symbols_) {
      used_symbols_.insert(sym.position());
    }
    VisitBranch(*op.left_op_);
    VisitBranch(*op.right_op_);
    return false;
  }

  bool PostVisit(Cartesian &) override { return true; }

  bool PreVisit(Union &op) override {
    for (auto const *symbols : {&op.union_symbols_, &op.left_symbols_, &op.right_symbols_}) {
      for (auto const &sym : *symbols) {
        used_symbols_.insert(sym.position());
      }
    }
    VisitBranch(*op.left_op_);
    VisitBranch(*op.right_op_);
    return false;
  }

  bool PostVisit(Union &) override { return true; }

  bool PreVisit(Merge &op) override {
    VisitSubquery(*op.merge_match_);
    VisitSubquery(*op.merge_create_);
    op.input_->Accept(*this);
    return false;
  }

  bool PostVisit(Merge &) override { return true; }

  bool PreVisit(RollUpApply &op) override {
    used_symbols_.insert(op.list_collection_symbol_.position());
    VisitSubquery(*op.list_collection_branch_);
    op.input_->Accept(*this);
    return false;
  }

  bool PostVisit(RollUpApply &) override { return true; }

  bool PreVisit(EvaluatePatternFilter &) override { return true; }

  bool PreVisit(Expand &op) override {
    RecordExpansionInputs(op.input_symbol_, op.common_);
    return true;
  }

  bool PreVisit(ExpandVariable &op) override {
    CollectSymbolsFromExpression(op.lower_bound_);
    CollectSymbolsFromExpression(op.upper_bound_);
    CollectSymbolsFromExpression(op.filter_lambda_.expression);
    if (op.weight_lambda_) {
      CollectSymbolsFromExpression(op.weight_lambda_->expression);
    }
    CollectSymbolsFromExpression(op.limit_);

    // What this expansion reads of its own input is recorded once the expansion
    // has been judged, so that it does not count against itself.
    utils::OnScopeExit const record_inputs{[&] { RecordExpansionInputs(op.input_symbol_, op.common_); }};

    if (op.type_ != EdgeAtom::Type::DEPTH_FIRST) return true;
    if (op.common_.existing_node) return true;
    if (op.filter_lambda_.accumulated_path_symbol) return true;
    if (op.weight_lambda_) return true;
    if (!deduplicates_ || rewrite_blocked_) return true;
    if (used_symbols_.contains(op.common_.edge_symbol.position())) return true;

    // The cursor settles the bound, but OperatorName must be able to agree on
    // which walk it calls for. Only literals and parameter lookups are in
    // ConstExternalPropertyValue's reach; anything else is left as depth-first.
    if (!BoundIsResolvable(op.lower_bound_)) return true;

    op.type_ = EdgeAtom::Type::PRUNING_BFS;
    op.group_sources_ = MayShareOneSearch(op);
    return true;
  }

 private:
  /// Records what an expansion reads of the row it starts from: the vertex it
  /// expands out of, and the one it is pinned to where it matches against a
  /// vertex already on the frame.
  void RecordExpansionInputs(Symbol const &input_symbol, ExpandCommon const &common) {
    used_symbols_.insert(input_symbol.position());
    if (common.existing_node) used_symbols_.insert(common.node_symbol.position());
  }

  /// Whether one search may be shared across every row this expansion is given,
  /// each vertex reached being expanded and emitted once no matter how many
  /// sources lead into it.
  ///
  /// Sharing drops the rows a per-source search would repeat, so it needs
  /// nothing above to be able to tell those rows from the ones left standing:
  /// every symbol the input binds must be dead above the expansion, leaving the
  /// vertex written here as the whole of what the row says. Nor may what the
  /// search admits vary by row, or a vertex let through under one row's filter
  /// would be passed over under the next one's.
  bool MayShareOneSearch(ExpandVariable const &op) const {
    if (!op.input()) return false;
    // Crossing edges either way, a walk arriving back at a source may be
    // retreading the edge it left by. Ruling that out takes the branch the walk
    // was found on, and branches belong to the source they leave.
    if (op.common_.direction == EdgeAtom::Direction::BOTH) return false;
    // Under an upper bound, how far a vertex was reached decides how much of the
    // graph behind it is still in range. A vertex first reached deep would keep a
    // later source that reaches it early from walking what that source still has
    // the bound to walk.
    if (op.upper_bound_) return false;
    if (!LambdaReadsOnlyItsOwnRow(op.filter_lambda_)) return false;

    // On a subquery's branch the source is bound outside the branch, where what
    // the input binds says nothing about it, so it is asked after separately.
    if (used_symbols_.contains(op.input_symbol_.position())) return false;

    auto const bound_by_input = op.input()->ModifiedSymbols(symbol_table_);
    return std::ranges::none_of(bound_by_input,
                                [this](Symbol const &sym) { return used_symbols_.contains(sym.position()); });
  }

  /// Whether the filter reads nothing beyond the edge and vertex it is being
  /// asked about, which is what makes its verdict the same for every row.
  bool LambdaReadsOnlyItsOwnRow(ExpansionLambda const &lambda) const {
    if (!lambda.expression) return true;
    UsedSymbolsCollector collector(symbol_table_);
    lambda.expression->Accept(collector);
    return std::ranges::all_of(collector.symbols_, [&lambda](Symbol const &sym) {
      return sym.position() == lambda.inner_edge_symbol.position() ||
             sym.position() == lambda.inner_node_symbol.position();
    });
  }

  void CollectSymbolsFromExpression(Expression *expr) {
    if (!expr) return;
    UsedSymbolsCollector collector(symbol_table_);
    expr->Accept(collector);
    for (auto const &sym : collector.symbols_) {
      used_symbols_.insert(sym.position());
    }
  }

  template <typename Container>
  void CollectSymbolsFromExpressions(Container const &exprs) {
    for (auto *expr : exprs) {
      CollectSymbolsFromExpression(expr);
    }
  }

  // Fully isolated: used for Cartesian/Union where branches are independent.
  void VisitBranch(LogicalOperator &branch) {
    auto saved_symbols = used_symbols_;
    auto saved_dedup = deduplicates_;
    auto saved_blocked = rewrite_blocked_;
    auto saved_stack = dedup_stack_;

    branch.Accept(*this);

    used_symbols_ = std::move(saved_symbols);
    deduplicates_ = saved_dedup;
    rewrite_blocked_ = saved_blocked;
    dedup_stack_ = std::move(saved_stack);
  }

  // Subquery variant: merges used_symbols_ back into the parent so that edge
  // symbols referenced inside the subquery are visible to the main pipeline.
  // A block raised inside the branch merges out for the same reason: it means an
  // operator there was never analysed, so the symbols it reads were never
  // recorded either, and the branch can read symbols the main pipeline binds.
  void VisitSubquery(LogicalOperator &branch) {
    auto saved_dedup = deduplicates_;
    auto saved_stack = dedup_stack_;

    branch.Accept(*this);

    deduplicates_ = saved_dedup;
    dedup_stack_ = std::move(saved_stack);
  }

 protected:
  bool DefaultPreVisit() override {
    rewrite_blocked_ = true;
    return true;
  }

 private:
  SymbolTable const &symbol_table_;
  std::unordered_set<int64_t> used_symbols_;
  bool deduplicates_{false};
  bool rewrite_blocked_{false};
  std::vector<bool> dedup_stack_;
};

}  // namespace

std::unique_ptr<LogicalOperator> RewriteWithPruningBFS(std::unique_ptr<LogicalOperator> root_op,
                                                       SymbolTable const *symbol_table) {
  auto rewriter = PruningBFSRewriter(*symbol_table);
  root_op->Accept(rewriter);
  return root_op;
}

}  // namespace memgraph::query::plan
