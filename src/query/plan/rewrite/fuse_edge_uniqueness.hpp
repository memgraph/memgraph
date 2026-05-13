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

/// @file
/// Post-construction rewrite that fuses Expand+EdgeUniquenessFilter pairs
/// when the produced edge symbol has no consumer in the plan other than the
/// EUF itself. Stage 1 fusion (inline in EnsureCyphermorphism) handles the
/// anonymous case; this pass picks up the named-but-unused case.

#pragma once

#include <unordered_set>

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"

namespace memgraph::query::plan {

namespace impl {

/// Walks the plan tree and accumulates every symbol referenced via:
///   - Any Expression* field on any operator (via UsedSymbolsCollector).
///   - ConstructNamedPath::path_elements_ (direct Symbol references).
///   - Distinct::value_symbols_ (direct Symbol references).
/// Operator-level Symbol fields that are *bindings* (output symbols) are not
/// collected - the produce side flows through expressions on consumers above.
class CollectPlanUsedSymbols final : public HierarchicalLogicalOperatorVisitor {
 public:
  explicit CollectPlanUsedSymbols(const SymbolTable &symbol_table) : collector_(symbol_table) {}

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  std::unordered_set<Symbol> take_used_symbols() && { return std::move(collector_.symbols_); }

  bool Visit(Once &) override { return true; }

  bool PreVisit(Filter &op) override {
    AcceptIfNonNull(op.expression_);
    return true;
  }

  bool PreVisit(Produce &op) override {
    for (auto *ne : op.named_expressions_) {
      if (ne) ne->Accept(collector_);
    }
    return true;
  }

  bool PreVisit(Aggregate &op) override {
    for (auto &el : op.aggregations_) {
      AcceptIfNonNull(el.arg1);
      AcceptIfNonNull(el.arg2);
    }
    for (auto *e : op.group_by_) AcceptIfNonNull(e);
    return true;
  }

  bool PreVisit(OrderBy &op) override {
    for (auto *e : op.order_by_) AcceptIfNonNull(e);
    return true;
  }

  bool PreVisit(Skip &op) override {
    AcceptIfNonNull(op.expression_);
    return true;
  }

  bool PreVisit(Limit &op) override {
    AcceptIfNonNull(op.expression_);
    return true;
  }

  bool PreVisit(Unwind &op) override {
    AcceptIfNonNull(op.input_expression_);
    return true;
  }

  bool PreVisit(Delete &op) override {
    for (auto *e : op.expressions_) AcceptIfNonNull(e);
    AcceptIfNonNull(op.buffer_size_);
    return true;
  }

  bool PreVisit(SetProperty &op) override {
    AcceptIfNonNull(op.lhs_);
    AcceptIfNonNull(op.rhs_);
    return true;
  }

  bool PreVisit(SetNestedProperty &op) override {
    AcceptIfNonNull(op.lhs_);
    AcceptIfNonNull(op.rhs_);
    return true;
  }

  bool PreVisit(SetProperties &op) override {
    AcceptIfNonNull(op.rhs_);
    return true;
  }

  bool PreVisit(RemoveProperty &op) override {
    AcceptIfNonNull(op.lhs_);
    return true;
  }

  bool PreVisit(RemoveNestedProperty &op) override {
    AcceptIfNonNull(op.lhs_);
    return true;
  }

  bool PreVisit(ExpandVariable &op) override {
    AcceptIfNonNull(op.lower_bound_);
    AcceptIfNonNull(op.upper_bound_);
    AcceptIfNonNull(op.limit_);
    AcceptIfNonNull(op.filter_lambda_.expression);
    if (op.weight_lambda_) AcceptIfNonNull(op.weight_lambda_->expression);
    return true;
  }

  bool PreVisit(ScanAllById &op) override {
    AcceptIfNonNull(op.expression_);
    return true;
  }

  bool PreVisit(ScanAllByEdgeId &op) override {
    AcceptIfNonNull(op.expression_);
    return true;
  }

  bool PreVisit(Foreach &op) override {
    AcceptIfNonNull(op.expression_);
    return true;
  }

  bool PreVisit(HashJoin &op) override {
    AcceptIfNonNull(op.hash_join_condition_);
    return true;
  }

  bool PreVisit(ConstructNamedPath &op) override {
    for (const auto &sym : op.path_elements_) collector_.symbols_.insert(sym);
    return true;
  }

  bool PreVisit(Distinct &op) override {
    for (const auto &sym : op.value_symbols_) collector_.symbols_.insert(sym);
    return true;
  }

 private:
  void AcceptIfNonNull(memgraph::query::Expression *e) {
    if (e) e->Accept(collector_);
  }

  UsedSymbolsCollector collector_;
};

/// Try to fuse `op`: if it is an EdgeUniquenessFilter directly above a plain
/// `Expand` and the edge symbol is not in @p used_symbols, stamp the Expand
/// with the EUF's state and return the Expand as the replacement. Otherwise
/// return `op` unchanged.
inline std::shared_ptr<LogicalOperator> TryFuse(std::shared_ptr<LogicalOperator> op,
                                                const std::unordered_set<Symbol> &used_symbols) {
  auto *euf = dynamic_cast<EdgeUniquenessFilter *>(op.get());
  if (!euf) return op;
  if (used_symbols.contains(euf->expand_symbol_)) return op;

  // Plain single-hop Expand: SymbolKind::Edge.
  if (euf->candidate_kind_ == EdgeUniquenessFilter::SymbolKind::Edge) {
    auto *expand = dynamic_cast<Expand *>(euf->input_.get());
    if (!expand || expand->common_.edge_symbol != euf->expand_symbol_ || expand->unique_pattern_id_ != -1) {
      return op;
    }
    expand->unique_pattern_id_ = euf->pattern_id_;
    expand->unique_previous_symbols_ = std::move(euf->previous_symbols_);
    expand->unique_is_topmost_ = euf->is_topmost_;
    return euf->input_;
  }

  // Var-length Expand: SymbolKind::EdgeList.
  if (euf->candidate_kind_ == EdgeUniquenessFilter::SymbolKind::EdgeList) {
    auto *evar = dynamic_cast<ExpandVariable *>(euf->input_.get());
    // Only fuse for plain DFS-style var-length; BFS/Dijkstra/AllShortestPaths
    // share the ExpandVariable type but do their own uniqueness internally
    // and never go through EUF on the hot path. Be defensive and require the
    // default DEPTH_FIRST type.
    if (!evar || evar->common_.edge_symbol != euf->expand_symbol_ || evar->unique_pattern_id_ != -1 ||
        evar->type_ != EdgeAtom::Type::DEPTH_FIRST) {
      return op;
    }
    evar->unique_pattern_id_ = euf->pattern_id_;
    evar->unique_previous_symbols_ = std::move(euf->previous_symbols_);
    evar->unique_is_topmost_ = euf->is_topmost_;
    return euf->input_;
  }

  return op;
}

/// Recursively walks the plan tree, fusing each Expand+EUF pair whose edge
/// symbol is not present in @p used_symbols. Mutates sub-plans in place.
/// Returns the (possibly replaced) `op` for the parent to wire up.
inline std::shared_ptr<LogicalOperator> FuseRecursive(std::shared_ptr<LogicalOperator> op,
                                                      const std::unordered_set<Symbol> &used_symbols) {
  if (!op) return op;

  auto recurse_field = [&](std::shared_ptr<LogicalOperator> &slot) {
    if (slot) slot = FuseRecursive(slot, used_symbols);
  };

  if (op->HasSingleInput()) {
    if (auto child = op->input()) {
      auto new_child = FuseRecursive(child, used_symbols);
      if (new_child != child) op->set_input(new_child);
    }
  }

  // Secondary / multi-input sub-plans.
  if (auto *opt = dynamic_cast<Optional *>(op.get())) {
    recurse_field(opt->optional_);
  } else if (auto *apply = dynamic_cast<Apply *>(op.get())) {
    recurse_field(apply->subquery_);
  } else if (auto *rollup = dynamic_cast<RollUpApply *>(op.get())) {
    recurse_field(rollup->list_collection_branch_);
  } else if (auto *pst = dynamic_cast<PeriodicSubquery *>(op.get())) {
    recurse_field(pst->subquery_);
  } else if (auto *merge = dynamic_cast<Merge *>(op.get())) {
    recurse_field(merge->merge_match_);
    recurse_field(merge->merge_create_);
  } else if (auto *fe = dynamic_cast<Foreach *>(op.get())) {
    recurse_field(fe->update_clauses_);
  } else if (auto *flt = dynamic_cast<Filter *>(op.get())) {
    for (auto &pf : flt->pattern_filters_) recurse_field(pf);
  } else if (auto *cart = dynamic_cast<Cartesian *>(op.get())) {
    recurse_field(cart->left_op_);
    recurse_field(cart->right_op_);
  } else if (auto *un = dynamic_cast<Union *>(op.get())) {
    recurse_field(un->left_op_);
    recurse_field(un->right_op_);
  } else if (auto *ij = dynamic_cast<IndexedJoin *>(op.get())) {
    recurse_field(ij->main_branch_);
    recurse_field(ij->sub_branch_);
  } else if (auto *hj = dynamic_cast<HashJoin *>(op.get())) {
    recurse_field(hj->left_op_);
    recurse_field(hj->right_op_);
  }

  return TryFuse(std::move(op), used_symbols);
}

}  // namespace impl

inline std::unique_ptr<LogicalOperator> RewriteWithFuseEdgeUniquenessFilter(std::unique_ptr<LogicalOperator> root_op,
                                                                            SymbolTable *symbol_table) {
  if (!root_op) return root_op;

  impl::CollectPlanUsedSymbols collector{*symbol_table};
  root_op->Accept(collector);
  auto used = std::move(collector).take_used_symbols();

  // We rewrite children of the root in-place. Trying to replace the root
  // itself would require converting a shared_ptr back to unique_ptr, which is
  // not natively supported - and the root is never an EUF in practice (always
  // Produce/CallProcedure/Distinct/etc.).
  auto recurse_field = [&](std::shared_ptr<LogicalOperator> &slot) {
    if (slot) slot = impl::FuseRecursive(slot, used);
  };

  if (root_op->HasSingleInput()) {
    if (auto child = root_op->input()) {
      auto new_child = impl::FuseRecursive(child, used);
      if (new_child != child) root_op->set_input(new_child);
    }
  }

  if (auto *opt = dynamic_cast<Optional *>(root_op.get())) {
    recurse_field(opt->optional_);
  } else if (auto *apply = dynamic_cast<Apply *>(root_op.get())) {
    recurse_field(apply->subquery_);
  } else if (auto *rollup = dynamic_cast<RollUpApply *>(root_op.get())) {
    recurse_field(rollup->list_collection_branch_);
  } else if (auto *pst = dynamic_cast<PeriodicSubquery *>(root_op.get())) {
    recurse_field(pst->subquery_);
  } else if (auto *merge = dynamic_cast<Merge *>(root_op.get())) {
    recurse_field(merge->merge_match_);
    recurse_field(merge->merge_create_);
  } else if (auto *fe = dynamic_cast<Foreach *>(root_op.get())) {
    recurse_field(fe->update_clauses_);
  } else if (auto *flt = dynamic_cast<Filter *>(root_op.get())) {
    for (auto &pf : flt->pattern_filters_) recurse_field(pf);
  } else if (auto *cart = dynamic_cast<Cartesian *>(root_op.get())) {
    recurse_field(cart->left_op_);
    recurse_field(cart->right_op_);
  } else if (auto *un = dynamic_cast<Union *>(root_op.get())) {
    recurse_field(un->left_op_);
    recurse_field(un->right_op_);
  } else if (auto *ij = dynamic_cast<IndexedJoin *>(root_op.get())) {
    recurse_field(ij->main_branch_);
    recurse_field(ij->sub_branch_);
  } else if (auto *hj = dynamic_cast<HashJoin *>(root_op.get())) {
    recurse_field(hj->left_op_);
    recurse_field(hj->right_op_);
  }

  return root_op;
}

}  // namespace memgraph::query::plan
