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
#include <unordered_set>
#include <vector>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"

namespace memgraph::query::plan {

namespace impl {

/// Rewrites ExpandVariable(DEPTH_FIRST) to ExpandVariable(PRUNING_BFS) when the edge list
/// symbol is not used by any operator above the expand in the plan tree.
///
/// The visitor collects used symbols on PreVisit (top-down), so by the time we reach
/// an ExpandVariable, we know all symbols referenced by operators above it.
class PruningBFSRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  explicit PruningBFSRewriter(SymbolTable const &symbol_table) : symbol_table_(symbol_table) {}

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool DefaultPreVisit() override {
    rewrite_blocked_ = true;
    return true;
  }

  bool Visit(Once &) override { return true; }

  bool PreVisit(Produce &op) override {
    for (auto *ne : op.named_expressions_) {
      CollectSymbolsFromExpression(ne->expression_);
    }
    return true;
  }

  bool PreVisit(Filter &op) override {
    for (auto const &f : op.all_filters_) {
      CollectSymbolsFromExpression(f.expression);
    }
    return true;
  }

  bool PreVisit(ConstructNamedPath &op) override {
    used_symbols_.insert(op.path_symbol_.position());
    for (auto const &sym : op.path_elements_) {
      used_symbols_.insert(sym.position());
    }
    return true;
  }

  bool PreVisit(Distinct &) override {
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
    if (!op.aggregations_.empty() &&
        std::all_of(op.aggregations_.begin(), op.aggregations_.end(), [](auto const &elem) { return elem.distinct; })) {
      deduplicates_ = true;
    }
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
    return true;
  }

  bool PreVisit(Skip &op) override {
    CollectSymbolsFromExpression(op.expression_);
    return true;
  }

  bool PreVisit(Limit &op) override {
    CollectSymbolsFromExpression(op.expression_);
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
    return true;
  }

  bool PreVisit(EmptyResult &) override { return true; }

  bool PreVisit(Apply &) override { return true; }

  bool PreVisit(Optional &) override { return true; }

  bool PreVisit(Cartesian &) override { return true; }

  bool PreVisit(Union &) override { return true; }

  bool PreVisit(Merge &) override { return true; }

  bool PreVisit(RollUpApply &) override { return true; }

  bool PreVisit(EvaluatePatternFilter &) override { return true; }

  bool PreVisit(Expand &) override { return true; }

  bool PreVisit(ExpandVariable &op) override {
    if (op.type_ != EdgeAtom::Type::DEPTH_FIRST) return true;
    if (op.common_.existing_node) return true;
    if (op.filter_lambda_.accumulated_path_symbol) return true;
    if (op.weight_lambda_) return true;

    if (deduplicates_ && !rewrite_blocked_ && !used_symbols_.contains(op.common_.edge_symbol.position())) {
      op.type_ = EdgeAtom::Type::PRUNING_BFS;
    }
    return true;
  }

 private:
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

  SymbolTable const &symbol_table_;
  std::unordered_set<int> used_symbols_;
  bool deduplicates_{false};
  bool rewrite_blocked_{false};
  std::vector<bool> dedup_stack_;
};

}  // namespace impl

inline std::unique_ptr<LogicalOperator> RewriteWithPruningBFS(std::unique_ptr<LogicalOperator> root_op,
                                                              SymbolTable const &symbol_table) {
  auto rewriter = impl::PruningBFSRewriter(symbol_table);
  root_op->Accept(rewriter);
  return root_op;
}

}  // namespace memgraph::query::plan
