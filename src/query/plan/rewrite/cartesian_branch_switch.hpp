// Copyright 2024 Memgraph Ltd.
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
/// This file provides a plan rewriter which replaces `ScanAll` and `Expand`
/// operations with `ScanAllByEdgeType` if possible. The public entrypoint is
/// `RewriteWithEdgeTypeIndexRewriter`.

#pragma once

#include "query/plan/operator.hpp"
#include "utils/algorithm.hpp"

namespace memgraph::query::plan {

namespace impl {

template <class TDbAccessor>
class CartesianBranchSwitchRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  CartesianBranchSwitchRewriter(SymbolTable *symbol_table, AstStorage *ast_storage, TDbAccessor *db,
                                const Parameters &parameters, const IndexHints &index_hints)
      : symbol_table_(symbol_table),
        ast_storage_(ast_storage),
        db_(db),
        parameters_(parameters),
        index_hints_(index_hints) {}

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &) override { return true; }

  bool PreVisit(Filter &) override { return true; }

  bool PostVisit(Filter &) override { return true; }

  bool PreVisit(ScanAll &) override { return true; }

  bool PostVisit(ScanAll &) override { return true; }

  bool PreVisit(Expand &) override { return true; }

  bool PostVisit(Expand &) override { return true; }

  bool PreVisit(ExpandVariable &) override { return true; }

  bool PostVisit(ExpandVariable &) override { return true; }

  bool PreVisit(Merge &op) override {
    op.input()->Accept(*this);
    RewriteBranch(&op.merge_match_);
    return false;
  }

  bool PostVisit(Merge &) override { return true; }

  bool PreVisit(Optional &op) override {
    op.input()->Accept(*this);
    RewriteBranch(&op.optional_);
    return false;
  }

  bool PostVisit(Optional &) override { return true; }

  bool PreVisit(Cartesian &) override { return true; }

  bool PostVisit(Cartesian &op) override {
    auto left_cost = query::plan::EstimatePlanCost(db_, *symbol_table_, parameters_, *op.left_op_, index_hints_);
    auto right_cost = query::plan::EstimatePlanCost(db_, *symbol_table_, parameters_, *op.right_op_, index_hints_);

    if (left_cost.cardinality > right_cost.cardinality) {
      std::swap(op.left_op_, op.right_op_);
      std::swap(op.left_symbols_, op.right_symbols_);
    }

    return true;
  }

  bool PreVisit(IndexedJoin &op) override {
    RewriteBranch(&op.main_branch_);
    RewriteBranch(&op.sub_branch_);
    return false;
  }

  bool PostVisit(IndexedJoin &) override { return true; }

  bool PreVisit(HashJoin &) override { return true; }

  bool PostVisit(HashJoin &) override { return true; }

  bool PreVisit(Union &op) override {
    RewriteBranch(&op.left_op_);
    RewriteBranch(&op.right_op_);
    return false;
  }

  bool PostVisit(Union &) override { return true; }

  bool PreVisit(CreateNode &) override { return true; }
  bool PostVisit(CreateNode &) override { return true; }

  bool PreVisit(CreateExpand &) override { return true; }
  bool PostVisit(CreateExpand &) override { return true; }

  bool PreVisit(ScanAllByLabel &) override { return true; }
  bool PostVisit(ScanAllByLabel &) override { return true; }

  bool PreVisit(ScanAllByLabelPropertyRange &) override { return true; }
  bool PostVisit(ScanAllByLabelPropertyRange &) override { return true; }

  bool PreVisit(ScanAllByLabelPropertyValue &) override { return true; }
  bool PostVisit(ScanAllByLabelPropertyValue &) override { return true; }

  bool PreVisit(ScanAllByLabelProperty &) override { return true; }
  bool PostVisit(ScanAllByLabelProperty &) override { return true; }

  bool PreVisit(ScanAllById &) override { return true; }
  bool PostVisit(ScanAllById &) override { return true; }

  bool PreVisit(ScanAllByEdgeType &) override { return true; }
  bool PostVisit(ScanAllByEdgeType &) override { return true; }

  bool PreVisit(ScanAllByEdgeId &) override { return true; }
  bool PostVisit(ScanAllByEdgeId &) override { return true; }

  bool PreVisit(ConstructNamedPath &) override { return true; }
  bool PostVisit(ConstructNamedPath &) override { return true; }

  bool PreVisit(Produce &) override { return true; }
  bool PostVisit(Produce &) override { return true; }

  bool PreVisit(EmptyResult &) override { return true; }
  bool PostVisit(EmptyResult &) override { return true; }

  bool PreVisit(Delete &) override { return true; }
  bool PostVisit(Delete &) override { return true; }

  bool PreVisit(SetProperty &) override { return true; }
  bool PostVisit(SetProperty &) override { return true; }

  bool PreVisit(SetProperties &) override { return true; }
  bool PostVisit(SetProperties &) override { return true; }

  bool PreVisit(SetLabels &) override { return true; }
  bool PostVisit(SetLabels &) override { return true; }

  bool PreVisit(RemoveProperty &) override { return true; }
  bool PostVisit(RemoveProperty &) override { return true; }

  bool PreVisit(RemoveLabels &) override { return true; }
  bool PostVisit(RemoveLabels &) override { return true; }

  bool PreVisit(EdgeUniquenessFilter &) override { return true; }
  bool PostVisit(EdgeUniquenessFilter &) override { return true; }

  bool PreVisit(Accumulate &) override { return true; }
  bool PostVisit(Accumulate &) override { return true; }

  bool PreVisit(Aggregate &) override { return true; }
  bool PostVisit(Aggregate &) override { return true; }

  bool PreVisit(Skip &) override { return true; }
  bool PostVisit(Skip &) override { return true; }

  bool PreVisit(Limit &) override { return true; }
  bool PostVisit(Limit &) override { return true; }

  bool PreVisit(OrderBy &) override { return true; }
  bool PostVisit(OrderBy &) override { return true; }

  bool PreVisit(Unwind &) override { return true; }
  bool PostVisit(Unwind &) override { return true; }

  bool PreVisit(Distinct &) override { return true; }
  bool PostVisit(Distinct &) override { return true; }

  bool PreVisit(CallProcedure &) override { return true; }
  bool PostVisit(CallProcedure &) override { return true; }

  bool PreVisit(Foreach &op) override {
    op.input()->Accept(*this);
    RewriteBranch(&op.update_clauses_);
    return false;
  }

  bool PostVisit(Foreach &) override { return true; }

  bool PreVisit(EvaluatePatternFilter &) override { return true; }

  bool PostVisit(EvaluatePatternFilter & /*op*/) override { return true; }

  bool PreVisit(Apply &op) override {
    op.input()->Accept(*this);
    RewriteBranch(&op.subquery_);
    return false;
  }

  bool PostVisit(Apply & /*op*/) override { return true; }

  bool PreVisit(LoadCsv &) override { return true; }

  bool PostVisit(LoadCsv & /*op*/) override { return true; }

  bool PreVisit(RollUpApply &op) override {
    op.input()->Accept(*this);
    RewriteBranch(&op.list_collection_branch_);
    return false;
  }

  bool PostVisit(RollUpApply &) override { return true; }

  SymbolTable *symbol_table_;
  AstStorage *ast_storage_;
  TDbAccessor *db_;
  const Parameters &parameters_;
  const IndexHints &index_hints_;
  std::shared_ptr<LogicalOperator> new_root_;

  bool DefaultPreVisit() override {
    throw utils::NotYetImplemented("Operator not yet covered by EdgeTypeIndexRewriter");
  }

  void RewriteBranch(std::shared_ptr<LogicalOperator> *branch) {
    CartesianBranchSwitchRewriter<TDbAccessor> rewriter(symbol_table_, ast_storage_, db_, parameters_, index_hints_);
    (*branch)->Accept(rewriter);
    if (rewriter.new_root_) {
      *branch = rewriter.new_root_;
    }
  }
};

}  // namespace impl

template <class TDbAccessor>
std::unique_ptr<LogicalOperator> RewriteWithCartesianBranchSwitchRewriter(std::unique_ptr<LogicalOperator> root_op,
                                                                          SymbolTable *symbol_table,
                                                                          AstStorage *ast_storage, TDbAccessor *db,
                                                                          const Parameters &parameters,
                                                                          const IndexHints &index_hints) {
  impl::CartesianBranchSwitchRewriter<TDbAccessor> rewriter(symbol_table, ast_storage, db, parameters, index_hints);
  root_op->Accept(rewriter);
  return root_op;
}

}  // namespace memgraph::query::plan
