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

#include <algorithm>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>

#include "query/frontend/ast/ast.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"
#include "query/plan/rewrite/index_lookup.hpp"
#include "storage/v2/id_types.hpp"
#include "utils/algorithm.hpp"

namespace memgraph::query::plan {

namespace impl {

template <class TDbAccessor>
class EdgeTypeIndexRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  EdgeTypeIndexRewriter(SymbolTable *symbol_table, AstStorage *ast_storage, TDbAccessor *db)
      : symbol_table_(symbol_table), ast_storage_(ast_storage), db_(db) {}

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &) override { return true; }

  bool PreVisit(Filter &op) override {
    prev_ops_.push_back(&op);
    filters_.CollectFilterExpression(op.expression_, *symbol_table_);

    return true;
  }

  bool PostVisit(Filter &op) override {
    prev_ops_.pop_back();

    ExpressionRemovalResult removal = RemoveExpressions(op.expression_, filter_exprs_for_removal_);
    op.expression_ = removal.trimmed_expression;
    if (op.expression_) {
      Filters leftover_filters;
      leftover_filters.CollectFilterExpression(op.expression_, *symbol_table_);
      op.all_filters_ = std::move(leftover_filters);
    }

    if (!op.expression_) {
      SetOnParent(op.input());
    }

    return true;
  }

  bool PreVisit(ScanAll &op) override {
    prev_ops_.push_back(&op);

    if (op.input()->GetTypeInfo() == Once::kType) {
      const bool is_node_anon = op.output_symbol_.IsSymbolAnonym();
      once_under_scanall_ = is_node_anon;
    }

    return true;
  }

  bool PostVisit(ScanAll &op) override {
    prev_ops_.pop_back();

    if (EdgeTypePropertyIndexingPossible() || EdgeTypeIndexingPossible() || maybe_id_lookup_value_) {
      SetOnParent(op.input());
    }

    return true;
  }

  bool PreVisit(Expand &op) override {
    prev_ops_.push_back(&op);

    if (op.input()->GetTypeInfo() == ScanAll::kType) {
      const bool only_one_edge_type = (op.common_.edge_types.size() == 1U);
      const bool expansion_is_named = !(op.common_.edge_symbol.IsSymbolAnonym());
      const bool expdanded_node_not_named = op.common_.node_symbol.IsSymbolAnonym();

      edge_symbol_ = op.common_.edge_symbol;

      if (only_one_edge_type) {
        edge_type_ = op.common_.edge_types.front();
        edge_type_index_exist_ = db_->EdgeTypeIndexExists(edge_type_);
      }

      scanall_under_expand_ = only_one_edge_type && expansion_is_named && expdanded_node_not_named;

      const auto &modified_symbols = op.ModifiedSymbols(*symbol_table_);
      std::unordered_set<Symbol> bound_symbols(modified_symbols.begin(), modified_symbols.end());
      auto are_bound = [&bound_symbols](const auto &used_symbols) {
        for (const auto &used_symbol : used_symbols) {
          if (!utils::Contains(bound_symbols, used_symbol)) {
            return false;
          }
        }
        return true;
      };

      // Check if we filter based on the id of the edge.
      for (const auto &filter : filters_.IdFilters(edge_symbol_)) {
        if (filter.id_filter->is_symbol_in_value_ || !are_bound(filter.used_symbols)) continue;
        maybe_id_lookup_value_ = filter.id_filter->value_;
        filter_exprs_for_removal_.insert(filter.expression);
        filters_.EraseFilter(filter);
      }

      if (!scanall_under_expand_) {
        return true;
      }

      // Check if we filter based on a property of the edge.
      storage::PropertyId maybe_property;

      for (const auto &filter : filters_.PropertyFilters(edge_symbol_)) {
        if (filter.property_filter->is_symbol_in_value_ || !are_bound(filter.used_symbols)) continue;

        const auto &property = filter.property_filter->property_;
        maybe_property = GetProperty(property);
        if (db_->EdgeTypePropertyIndexExists(edge_type_, maybe_property)) {
          property_ = maybe_property;

          maybe_id_lookup_value_ = filter.property_filter->value_;
          filter_exprs_for_removal_.insert(filter.expression);
          filters_.EraseFilter(filter);

          break;
        }
      }
    }

    return true;
  }

  bool PostVisit(Expand &op) override {
    prev_ops_.pop_back();

    auto indexed_scan = GenEdgeTypeScan(op);
    if (indexed_scan) {
      SetOnParent(std::move(indexed_scan));
    }
    return true;
  }

  bool PreVisit(ExpandVariable &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(ExpandVariable &expand) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Merge &op) override {
    prev_ops_.push_back(&op);
    op.input()->Accept(*this);
    RewriteBranch(&op.merge_match_);
    return false;
  }

  bool PostVisit(Merge &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Optional &op) override {
    prev_ops_.push_back(&op);
    op.input()->Accept(*this);
    RewriteBranch(&op.optional_);
    return false;
  }

  bool PostVisit(Optional &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Cartesian &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(Cartesian &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(IndexedJoin &op) override {
    prev_ops_.push_back(&op);
    RewriteBranch(&op.main_branch_);
    RewriteBranch(&op.sub_branch_);
    return false;
  }

  bool PostVisit(IndexedJoin &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(HashJoin &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(HashJoin &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Union &op) override {
    prev_ops_.push_back(&op);
    RewriteBranch(&op.left_op_);
    RewriteBranch(&op.right_op_);
    return false;
  }

  bool PostVisit(Union &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(CreateNode &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(CreateNode &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(CreateExpand &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(CreateExpand &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByLabel &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByLabel &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByLabelPropertyRange &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByLabelPropertyRange &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByLabelPropertyValue &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByLabelPropertyValue &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByLabelProperty &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByLabelProperty &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllById &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllById &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgeType &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByEdgeType &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgeId &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByEdgeId &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ConstructNamedPath &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ConstructNamedPath &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Produce &op) override {
    prev_ops_.push_back(&op);

    if (op.input()->GetTypeInfo() == Expand::kType) {
      expand_under_produce_ = true;
    }

    return true;
  }
  bool PostVisit(Produce &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(EmptyResult &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(EmptyResult &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Delete &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(Delete &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(SetProperty &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(SetProperty &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(SetProperties &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(SetProperties &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(SetLabels &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(SetLabels &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(RemoveProperty &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(RemoveProperty &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(RemoveLabels &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(RemoveLabels &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(EdgeUniquenessFilter &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(EdgeUniquenessFilter &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Accumulate &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(Accumulate &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Aggregate &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(Aggregate &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Skip &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(Skip &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Limit &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(Limit &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(OrderBy &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(OrderBy &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Unwind &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(Unwind &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Distinct &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(Distinct &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(CallProcedure &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(CallProcedure &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Foreach &op) override {
    prev_ops_.push_back(&op);
    op.input()->Accept(*this);
    RewriteBranch(&op.update_clauses_);
    return false;
  }

  bool PostVisit(Foreach &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(EvaluatePatternFilter &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(EvaluatePatternFilter & /*op*/) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Apply &op) override {
    prev_ops_.push_back(&op);
    op.input()->Accept(*this);
    RewriteBranch(&op.subquery_);
    return false;
  }

  bool PostVisit(Apply & /*op*/) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(LoadCsv &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(LoadCsv & /*op*/) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(RollUpApply &op) override {
    prev_ops_.push_back(&op);
    op.input()->Accept(*this);
    RewriteBranch(&op.list_collection_branch_);
    return false;
  }

  bool PostVisit(RollUpApply &) override {
    prev_ops_.pop_back();
    return true;
  }

  std::shared_ptr<LogicalOperator> new_root_;

 private:
  SymbolTable *symbol_table_;
  AstStorage *ast_storage_;
  TDbAccessor *db_;
  // Collected filters, pending for examination if they can be used for advanced
  // lookup operations (by index, node ID, ...).
  Filters filters_;
  // Expressions which no longer need a plain Filter operator.
  std::unordered_set<Expression *> filter_exprs_for_removal_;
  std::vector<LogicalOperator *> prev_ops_;
  std::unordered_set<Symbol> cartesian_symbols_;
  storage::EdgeTypeId edge_type_;
  std::optional<storage::PropertyId> property_;

  Symbol edge_symbol_;
  memgraph::query::Expression *maybe_id_lookup_value_ = nullptr;

  storage::LabelId GetEdgeType(const EdgeTypeIx &edge_type) { return db_->NameToLabel(edge_type.name); }

  storage::PropertyId GetProperty(const PropertyIx &prop) { return db_->NameToProperty(prop.name); }

  // std::optional<storage::PropertyId> EdgeTypePropertyIndexingPossible(const std::unordered_set<Symbol>
  // &bound_symbols) {
  //   // Determine if the current query is filtering on a property of a given edge.
  //   auto are_bound = [&bound_symbols](const auto &used_symbols) {
  //     for (const auto &used_symbol : used_symbols) {
  //       if (!utils::Contains(bound_symbols, used_symbol)) {
  //         return false;
  //       }
  //     }
  //     return true;
  //   };

  //   storage::PropertyId ret;
  //   const bool edge_type_property_indexing_possible =
  //       once_under_scanall_ && edge_type_index_exist_ && property_filter_on_edge_;

  //   for (const auto &filter : filters_.PropertyFilters(edge_symbol_)) {
  //     if (filter.property_filter->is_symbol_in_value_ || !are_bound(filter.used_symbols)) continue;

  //     const auto &property = filter.property_filter->property_;
  //     ret = GetProperty(property);
  //     if (edge_type_property_indexing_possible && db_->EdgeTypePropertyIndexExists(edge_type_, ret)) {
  //       return ret;
  //     }

  //     maybe_id_lookup_value_ = filter.property_filter->value_;
  //     filter_exprs_for_removal_.insert(filter.expression);
  //     filters_.EraseFilter(filter);
  //   }

  //   return {};
  // }

  bool EdgeTypeIndexingPossible() const {
    return expand_under_produce_ && scanall_under_expand_ && once_under_scanall_ && edge_type_index_exist_;
  }

  bool EdgeTypePropertyIndexingPossible() const { return scanall_under_expand_ && once_under_scanall_ && property_; }

  bool expand_under_produce_ = false;
  bool scanall_under_expand_ = false;
  bool once_under_scanall_ = false;
  bool edge_type_index_exist_ = false;
  // bool property_filter_on_edge_ = false;

  bool DefaultPreVisit() override {
    throw utils::NotYetImplemented("Operator not yet covered by EdgeTypeIndexRewriter");
  }

  std::unique_ptr<ScanAll> GenEdgeTypeScan(const Expand &expand) {
    const auto &input = expand.input();
    const auto &output_symbol = expand.common_.edge_symbol;
    const auto &view = expand.view_;

    if (maybe_id_lookup_value_) {
      return std::make_unique<ScanAllByEdgeId>(input, output_symbol, maybe_id_lookup_value_, view);
    }

    // const auto &modified_symbols = expand.ModifiedSymbols(*symbol_table_);
    // std::unordered_set<Symbol> bound_symbols(modified_symbols.begin(), modified_symbols.end());

    if (EdgeTypePropertyIndexingPossible()) {
      return std::make_unique<ScanAllByEdgeTypeProperty>(input, output_symbol, edge_type_, *property_, view);
    }

    if (EdgeTypeIndexingPossible()) {
      return std::make_unique<ScanAllByEdgeType>(input, output_symbol, edge_type_, view);
    }

    // LOG_FATAL("Fatal error while rewriting query plan.");
    return nullptr;
  }

  void SetOnParent(const std::shared_ptr<LogicalOperator> &input) {
    MG_ASSERT(input);
    if (prev_ops_.empty()) {
      MG_ASSERT(!new_root_);
      new_root_ = input;
      return;
    }
    prev_ops_.back()->set_input(input);
  }

  void RewriteBranch(std::shared_ptr<LogicalOperator> *branch) {
    EdgeTypeIndexRewriter<TDbAccessor> rewriter(symbol_table_, ast_storage_, db_);
    (*branch)->Accept(rewriter);
    if (rewriter.new_root_) {
      *branch = rewriter.new_root_;
    }
  }
};

}  // namespace impl

template <class TDbAccessor>
std::unique_ptr<LogicalOperator> RewriteWithEdgeTypeIndexRewriter(std::unique_ptr<LogicalOperator> root_op,
                                                                  SymbolTable *symbol_table, AstStorage *ast_storage,
                                                                  TDbAccessor *db) {
  impl::EdgeTypeIndexRewriter<TDbAccessor> rewriter(symbol_table, ast_storage, db);
  root_op->Accept(rewriter);
  return root_op;
}

}  // namespace memgraph::query::plan
