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
/// `RewriteWithEdgeIndexRewriter`.

#pragma once

#include <algorithm>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>

#include "query/frontend/ast/ast.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"
#include "query/plan/rewrite/general.hpp"
#include "storage/v2/id_types.hpp"
#include "utils/algorithm.hpp"

namespace memgraph::query::plan {

namespace impl {

template <class TDbAccessor>
class EdgeIndexRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  EdgeIndexRewriter(SymbolTable *symbol_table, AstStorage *ast_storage, TDbAccessor *db)
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

    // Filters are pushed down as far as they can go.
    // If there is a Cartesian after, that means that the filter is working on data from both branches.
    // In that case, we need to convert the Cartesian into a Join
    if (removal.did_remove) {
      LogicalOperator *input = op.input().get();
      LogicalOperator *parent = &op;

      // Find first possible branching point
      while (input->HasSingleInput()) {
        parent = input;
        input = input->input().get();
      }

      const bool is_child_cartesian = input->GetTypeInfo() == Cartesian::kType;
      if (is_child_cartesian) {
        std::vector<Symbol> modified_symbols;
        for (const auto &filter : op.all_filters_) {
          if (filter.property_filter) {
            modified_symbols.push_back(filter.property_filter->symbol_);
          }
        }
        auto does_modify = [&]() {
          // Number of symbols is small
          for (const auto &sym_in : input->ModifiedSymbols(*symbol_table_)) {
            if (std::find(modified_symbols.begin(), modified_symbols.end(), sym_in) != modified_symbols.end()) {
              return true;
            }
          }
          return false;
        };
        if (does_modify()) {
          // if we removed something from filter in front of a Cartesian, then we are doing a join from
          // 2 different branches
          auto *cartesian = dynamic_cast<Cartesian *>(input);
          auto indexed_join = std::make_shared<IndexedJoin>(cartesian->left_op_, cartesian->right_op_);
          parent->set_input(indexed_join);
        }
      }
    }

    if (!op.expression_) {
      // if we emptied all the expressions from the filter, then we don't need this operator anymore
      SetOnParent(op.input());
    }

    return true;
  }

  bool PreVisit(ScanAll &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(ScanAll &op) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Expand &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(Expand &op) override {
    prev_ops_.pop_back();
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

  bool PreVisit(ScanAllByEdge &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByEdge &scan) override {
    prev_ops_.pop_back();
    // auto indexed_scan = GenScanByEdgeIndex(scan);
    // if (indexed_scan) {
    //   SetOnParent(std::move(indexed_scan));
    // }
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

  bool PreVisit(PeriodicCommit &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(PeriodicCommit & /*op*/) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(PeriodicSubquery &op) override {
    prev_ops_.push_back(&op);
    op.input()->Accept(*this);
    RewriteBranch(&op.subquery_);
    return false;
  }

  bool PostVisit(PeriodicSubquery & /*op*/) override {
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
  std::optional<FilterInfo> property_filter_;

  struct EdgeTypePropertyIndex {
    EdgeTypeIx edge_type;
    // FilterInfo with PropertyFilter.
    FilterInfo filter;
    int64_t edge_count;
  };

  storage::EdgeTypeId GetEdgeType(const EdgeTypeIx &edge_type) { return db_->NameToLabel(edge_type.name); }
  storage::EdgeTypeId GetEdgeType(const LabelIx &edge_type) { return db_->NameToLabel(edge_type.name); }

  storage::PropertyId GetProperty(const PropertyIx &prop) { return db_->NameToProperty(prop.name); }

  struct CandidateIndices {
    std::unordered_map<std::pair<EdgeTypeIx, PropertyIx>, FilterInfo, HashPair> candidate_index_lookup_{};
  };

  CandidateIndices GetCandidateIndices(const Symbol &symbol) {
    std::unordered_map<std::pair<LabelIx, PropertyIx>, FilterInfo, HashPair> candidate_index_lookup{};
    for (const auto &edge_type : filters_.FilteredLabels(symbol)) {
      for (const auto &filter : filters_.PropertyFilters(symbol)) {
        if (filter.property_filter->is_symbol_in_value_) {
          // Skip filter expressions which use the symbol whose property we are
          // looking up or aren't bound. We cannot scan by such expressions. For
          // example, in `n.a = 2 + n.b` both sides of `=` refer to `n`, so we
          // cannot scan `n` by property index.
          continue;
        }

        const auto &property = filter.property_filter->property_;
        if (!db_->EdgeTypePropertyIndexExists(GetEdgeType(edge_type), GetProperty(property))) {
          continue;
        }
        candidate_index_lookup.insert({std::make_pair(edge_type, property), filter});
      }
    }

    return CandidateIndices{.candidate_index_lookup_ = candidate_index_lookup};
  }

  std::optional<EdgeTypeIx> FindBestEdgeTypeIndex(const std::unordered_set<EdgeTypeIx> &edge_types) {
    MG_ASSERT(!edge_types.empty(), "Trying to find the best edge type without any edge types.");

    std::optional<EdgeTypeIx> best_edge_type;
    for (const auto &edge_type : edge_types) {
      if (!db_->LabelIndexExists(GetEdgeType(edge_type))) continue;
      if (!best_edge_type) {
        best_edge_type = edge_type;
        continue;
      }
      if (db_->EdgesCount(GetEdgeType(edge_type)) < db_->EdgesCount(GetEdgeType(*best_edge_type)))
        best_edge_type = edge_type;
    }
    return best_edge_type;
  }

  std::optional<EdgeTypePropertyIndex> FindBestEdgeTypePropertyIndex(const Symbol &symbol) {
    auto candidate_indices = GetCandidateIndices(symbol);

    std::optional<EdgeTypePropertyIndex> found;
    for (const auto &[edge_property_pair, filter] : candidate_indices) {
      const auto &[edge_type, maybe_property] = edge_property_pair;
      auto property = *maybe_property;

      int64_t edge_count = db_->EdgesCount(GetEdgeType(edge_type), GetProperty(property));
      if (!found || edge_count < found->edge_count) {
        found = EdgeTypePropertyIndex{edge_type, filter, edge_count};
        continue;
      }

      if (found->edge_count > edge_count) {
        found = EdgeTypePropertyIndex{edge_type, filter, edge_count};
      }
    }
    return found;
  }

  bool DefaultPreVisit() override { throw utils::NotYetImplemented("Operator not yet covered by EdgeIndexRewriter"); }

  std::unique_ptr<ScanAll> GenScanByEdgeIndex(const ScanAllByEdge &scan) {
    const auto &input = scan.input();
    const auto &edge_symbol = scan.output_symbol_;
    const auto &view = scan.view_;

    // Now try to see if we can use label+property index. If not, try to use
    // just the label index.
    const auto edge_types = filters_.FilteredLabels(edge_symbol);
    if (edge_types.empty()) {
      // Without labels, we cannot generate any indexed ScanAll.
      return nullptr;
    }

    auto found_index = FindBestEdgeTypePropertyIndex(edge_symbol);
    if (found_index) {
      // Copy the property filter and then erase it from filters.
      const auto prop_filter = *found_index->filter.property_filter;
      filters_.EraseFilter(found_index->filter);
      std::vector<Expression *> removed_expressions;
      // filters_.EraseLabelFilter(edge_symbol, found_index->edge_type, &removed_expressions);
      filter_exprs_for_removal_.insert(removed_expressions.begin(), removed_expressions.end());
      if (prop_filter.type_ == PropertyFilter::Type::IS_NOT_NULL) {
        return std::make_unique<ScanAllByEdgeTypeProperty>(input, edge_symbol, GetEdgeType(found_index->edge_type),
                                                           GetProperty(prop_filter.property_),
                                                           prop_filter.property_.name, view);
      }
      MG_ASSERT(prop_filter.value_, "Property filter should either have bounds or a value expression.");
      return std::make_unique<ScanAllByEdgeTypePropertyValue>(input, edge_symbol, GetEdgeType(found_index->edge_type),
                                                              GetProperty(prop_filter.property_),
                                                              prop_filter.property_.name, prop_filter.value_, view);
    }

    auto maybe_edge_type = FindBestEdgeTypeIndex(edge_types);
    if (!maybe_edge_type) return nullptr;
    const auto &edge_type = *maybe_edge_type;

    std::vector<Expression *> removed_expressions;
    // filters_.EraseLabelFilter(edge_symbol, edge_type, &removed_expressions);
    filter_exprs_for_removal_.insert(removed_expressions.begin(), removed_expressions.end());
    return std::make_unique<ScanAllByEdgeType>(input, edge_symbol, GetEdgeType(edge_type), view);
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
    EdgeIndexRewriter<TDbAccessor> rewriter(symbol_table_, ast_storage_, db_);
    (*branch)->Accept(rewriter);
    if (rewriter.new_root_) {
      *branch = rewriter.new_root_;
    }
  }
};

}  // namespace impl

template <class TDbAccessor>
std::unique_ptr<LogicalOperator> RewriteWithEdgeIndexRewriter(std::unique_ptr<LogicalOperator> root_op,
                                                              SymbolTable *symbol_table, AstStorage *ast_storage,
                                                              TDbAccessor *db) {
  impl::EdgeIndexRewriter<TDbAccessor> rewriter(symbol_table, ast_storage, db);
  root_op->Accept(rewriter);
  return root_op;
}

}  // namespace memgraph::query::plan
