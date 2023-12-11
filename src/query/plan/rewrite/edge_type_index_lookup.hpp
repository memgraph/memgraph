// Copyright 2023 Memgraph Ltd.
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

#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"
#include "query/plan/rewrite/index_lookup.hpp"
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

  // Remove no longer needed Filter in PostVisit, this should be the last thing
  // Filter::Accept does, so it should be safe to remove the last reference and
  // free the memory.
  bool PostVisit(Filter &op) override {
    prev_ops_.pop_back();
    ExpressionRemovalResult removal = RemoveExpressions(op.expression_, filter_exprs_for_removal_);
    op.expression_ = removal.trimmed_expression;
    if (op.expression_) {
      Filters leftover_filters;
      leftover_filters.CollectFilterExpression(op.expression_, *symbol_table_);
      op.all_filters_ = std::move(leftover_filters);
    }

    if (!op.expression_ || utils::Contains(filter_exprs_for_removal_, op.expression_)) {
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

    if (EdgeTypeIndexingPossible()) {
      SetOnParent(op.input());
    }

    return true;
  }

  bool PreVisit(Expand &op) override {
    prev_ops_.push_back(&op);

    if (op.input()->GetTypeInfo() == ScanAll::kType) {
      // Rethink how to structure this predicates.
      const bool only_one_edge_type = (op.common_.edge_types.size() == 1U);
      const bool expansion_is_named = !(op.common_.edge_symbol.IsSymbolAnonym());
      const bool expdanded_node_not_named = op.common_.node_symbol.IsSymbolAnonym();

      edge_type_index_exist = only_one_edge_type ? db_->EdgeTypeIndexExists(op.common_.edge_types.front()) : false;

      scanall_under_expand_ = only_one_edge_type && expansion_is_named && expdanded_node_not_named;
    }

    return true;
  }

  bool PostVisit(Expand &op) override {
    prev_ops_.pop_back();

    if (EdgeTypeIndexingPossible()) {
      auto indexed_scan = GenEdgeTypeScan(op);
      SetOnParent(std::move(indexed_scan));
    }

    // 0. EdgeTypeIndexing is allowed/exists.
    // 1. If the previous logical operator is a bare ScanAll with anon nexpr
    // 2. and this expand has only one named expr and a specified EdgeType,
    // 2.5.(?) and if the relationships are directionless? -> probably this is not neded
    // 3. and the destination node of this expand is anon as well,
    // 4. and if the parent of this Expand is Produce
    // 5. then remove the bare ScanAll and the Expand and replace them with ScanAllByEdgeType.

    // use TypeInfo to typematch instead?
    // if(auto *asd = dynamic_cast<ScanAll *>(prev_operator))
    // {
    //   asd->output_symbol_;
    // }

    // if(op.input().get())

    // if we emptied all the expressions from the filter, then we don't need this operator anymore
    // SetOnParent(op.input());

    return true;
  }

  bool PreVisit(ExpandVariable &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  // See if it might be better to do ScanAllBy<Index> of the destination and
  // then do ExpandVariable to existing.
  bool PostVisit(ExpandVariable &expand) override {
    prev_ops_.pop_back();
    return true;
  }

  // The following operators may only use index lookup in filters inside of
  // their own branches. So we handle them all the same.
  //  * Input operator is visited with the current visitor.
  //  * Custom operator branches are visited with a new visitor.

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
    // TODO
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

  // State to confirm EdgeType-indexing
  bool EdgeTypeIndexingPossible() const {
    return expand_under_produce_ && scanall_under_expand_ && once_under_scanall_ && edge_type_index_exist;
  }
  bool expand_under_produce_ = false;
  bool scanall_under_expand_ = false;
  bool once_under_scanall_ = false;
  bool edge_type_index_exist = false;

  bool DefaultPreVisit() override {
    throw utils::NotYetImplemented("Operator not yet covered by EdgeTypeIndexRewriter");
  }

  std::unique_ptr<ScanAllByEdgeType> GenEdgeTypeScan(const Expand &expand) {
    const auto &input = expand.input();
    // We have to make a new output symbol? maybe not?
    // Won't this bee freed? -> Probably not we do the same thing with index_lookup
    const auto &output_symbol = expand.common_.edge_symbol;
    const auto &view = expand.view_;

    // Extract edge_type from symbol
    auto edge_type = expand.common_.edge_types.front();
    // There can be only one

    // const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, storage::EdgeTypeId label, storage::View
    // view = storage::View::OLD)
    return std::make_unique<ScanAllByEdgeType>(input, output_symbol, edge_type, view);
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

  /*
    std::unique_ptr<HashJoin> GenHashJoin(const Cartesian &cartesian) {
      const auto &left_op = cartesian.left_op_;
      const auto &left_symbols = cartesian.left_symbols_;
      const auto &right_op = cartesian.right_op_;
      const auto &right_symbols = cartesian.right_symbols_;

      auto modified_symbols = cartesian.ModifiedSymbols(*symbol_table_);
      modified_symbols.insert(modified_symbols.end(), left_symbols.begin(), left_symbols.end());

      std::unordered_set<Symbol> bound_symbols(modified_symbols.begin(), modified_symbols.end());
      auto are_bound = [&bound_symbols](const auto &used_symbols) {
        for (const auto &used_symbol : used_symbols) {
          if (!utils::Contains(bound_symbols, used_symbol)) {
            return false;
          }
        }
        return true;
      };

      for (const auto &filter : filters_) {
        if (filter.type != FilterInfo::Type::Property) {
          continue;
        }

        if (filter.property_filter->is_symbol_in_value_ || !are_bound(filter.used_symbols)) {
          continue;
        }

        if (filter.property_filter->type_ != PropertyFilter::Type::EQUAL) {
          continue;
        }

        if (filter.property_filter->value_->GetTypeInfo() != PropertyLookup::kType) {
          continue;
        }
        auto *rhs_lookup = static_cast<PropertyLookup *>(filter.property_filter->value_);

        auto *join_condition = static_cast<EqualOperator *>(filter.expression);
        auto lhs_symbol = filter.property_filter->symbol_;
        auto lhs_property = filter.property_filter->property_;
        auto rhs_symbol = symbol_table_->at(*static_cast<Identifier *>(rhs_lookup->expression_));
        auto rhs_property = rhs_lookup->property_;
        filter_exprs_for_removal_.insert(filter.expression);
        filters_.EraseFilter(filter);

        if (utils::Contains(right_symbols, lhs_symbol) && utils::Contains(left_symbols, rhs_symbol)) {
          // We need to duplicate this because expressions are shared between plans
          join_condition = join_condition->Clone(ast_storage_);
          std::swap(join_condition->expression1_, join_condition->expression2_);
        }

        return std::make_unique<HashJoin>(left_op, left_symbols, right_op, right_symbols, join_condition);
      }

      return nullptr;
    }
    */
};

}  // namespace impl

template <class TDbAccessor>
std::unique_ptr<LogicalOperator> RewriteWithEdgeTypeIndexRewriter(std::unique_ptr<LogicalOperator> root_op,
                                                                  SymbolTable *symbol_table, AstStorage *ast_storage,
                                                                  TDbAccessor *db) {
  impl::EdgeTypeIndexRewriter<TDbAccessor> rewriter(symbol_table, ast_storage, db);
  root_op->Accept(rewriter);
  if (rewriter.new_root_) {
    // This shouldn't happen in real use cases because, as JoinRewriter removes Filter operations, they cannot be the
    // root operator. In case we somehow missed this, raise NotYetImplemented instead of a MG_ASSERT crashing the
    // application.
    throw utils::NotYetImplemented("A Filter operator cannot be EdgeTypeIndexRewriter's root");
  }
  return root_op;
}

}  // namespace memgraph::query::plan
