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
    auto indexed_scan = GenScanByEdgeIndex(scan);
    if (indexed_scan) {
      SetOnParent(std::move(indexed_scan));
    }
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

  struct EdgeTypePropertyIndexInfo {
    std::optional<LabelIx> edge_type_from_filter{};
    std::optional<storage::EdgeTypeId> edge_type_from_relationship{};
    // FilterInfo with PropertyFilter.
    FilterInfo filter;
    int64_t edge_count;
  };

  struct CandidateIndex {
    std::optional<LabelIx> edge_type_from_filter{};
    std::optional<storage::EdgeTypeId> edge_type_from_relationship{};
    PropertyIx property;
    FilterInfo filter;
  };

  storage::EdgeTypeId GetEdgeType(const EdgeTypeIx &edge_type) { return db_->NameToEdgeType(edge_type.name); }
  storage::EdgeTypeId GetEdgeType(const LabelIx &edge_type) {
    return storage::EdgeTypeId::FromUint(db_->NameToLabel(edge_type.name).AsUint());
  }
  storage::EdgeTypeId GetEdgeType(const EdgeTypePropertyIndexInfo &info) {
    return info.edge_type_from_filter.has_value() ? GetEdgeType(info.edge_type_from_filter.value())
                                                  : info.edge_type_from_relationship.value();
  }
  storage::EdgeTypeId GetEdgeType(const CandidateIndex &candidate) {
    return candidate.edge_type_from_filter.has_value() ? GetEdgeType(candidate.edge_type_from_filter.value())
                                                       : candidate.edge_type_from_relationship.value();
  }

  bool FoundIndexWithFilteredLabel(const EdgeTypePropertyIndexInfo &info) {
    return info.edge_type_from_filter.has_value();
  }

  EdgeTypePropertyIndexInfo ConstructEdgeTypePropertyIndexInfo(CandidateIndex candidate, int64_t edge_count) {
    if (candidate.edge_type_from_relationship.has_value()) {
      return EdgeTypePropertyIndexInfo{.edge_type_from_relationship = candidate.edge_type_from_relationship,
                                       .filter = candidate.filter,
                                       .edge_count = edge_count};
    }

    return EdgeTypePropertyIndexInfo{
        .edge_type_from_filter = candidate.edge_type_from_filter, .filter = candidate.filter, .edge_count = edge_count};
  }

  storage::PropertyId GetProperty(const PropertyIx &prop) { return db_->NameToProperty(prop.name); }

  std::vector<CandidateIndex> GetCandidateIndicesFromFilter(const Symbol &symbol) {
    std::vector<CandidateIndex> candidate_indices{};
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
        candidate_indices.push_back({.edge_type_from_filter = edge_type, .property = property, .filter = filter});
      }
    }

    return candidate_indices;
  }

  std::vector<CandidateIndex> GetCandidateIndicesFromRelationship(
      const Symbol &symbol, const std::optional<storage::EdgeTypeId> edge_type_from_relationship) {
    std::vector<CandidateIndex> candidate_indices{};
    for (const auto &filter : filters_.PropertyFilters(symbol)) {
      if (filter.property_filter->is_symbol_in_value_) {
        // Skip filter expressions which use the symbol whose property we are
        // looking up or aren't bound. We cannot scan by such expressions. For
        // example, in `n.a = 2 + n.b` both sides of `=` refer to `n`, so we
        // cannot scan `n` by property index.
        continue;
      }

      const auto &property = filter.property_filter->property_;
      if (!db_->EdgeTypePropertyIndexExists(edge_type_from_relationship.value(), GetProperty(property))) {
        continue;
      }
      candidate_indices.push_back(
          {.edge_type_from_relationship = edge_type_from_relationship.value(), .property = property, .filter = filter});
    }

    return candidate_indices;
  }

  std::vector<CandidateIndex> GetCandidateIndices(
      const Symbol &symbol, const std::optional<storage::EdgeTypeId> edge_type_from_relationship) {
    if (edge_type_from_relationship.has_value()) {
      return GetCandidateIndicesFromRelationship(symbol, edge_type_from_relationship);
    } else {
      return GetCandidateIndicesFromFilter(symbol);
    }
  }

  std::optional<LabelIx> FindBestEdgeTypeIndex(const std::unordered_set<LabelIx> &edge_types) {
    MG_ASSERT(!edge_types.empty(), "Trying to find the best edge type without any edge types.");

    std::optional<LabelIx> best_edge_type;
    for (const auto &edge_type : edge_types) {
      if (!db_->EdgeTypeIndexExists(GetEdgeType(edge_type))) continue;
      if (!best_edge_type) {
        best_edge_type = edge_type;
        continue;
      }
      if (db_->EdgesCount(GetEdgeType(edge_type)) < db_->EdgesCount(GetEdgeType(*best_edge_type)))
        best_edge_type = edge_type;
    }
    return best_edge_type;
  }

  std::optional<EdgeTypePropertyIndexInfo> FindBestEdgeTypePropertyIndex(
      const Symbol &symbol, const std::optional<storage::EdgeTypeId> edge_type_from_relationship) {
    auto candidate_indices = GetCandidateIndices(symbol, edge_type_from_relationship);

    std::optional<EdgeTypePropertyIndexInfo> found;
    for (const auto &candidate_index : candidate_indices) {
      int64_t edge_count = db_->EdgesCount(GetEdgeType(candidate_index), GetProperty(candidate_index.property));
      if (!found || edge_count < found->edge_count) {
        found = ConstructEdgeTypePropertyIndexInfo(candidate_index, edge_count);
      }
    }
    return found;
  }

  bool DefaultPreVisit() override { throw utils::NotYetImplemented("Operator not yet covered by EdgeIndexRewriter"); }

  std::unique_ptr<ScanAll> GenScanByEdgeIndex(const ScanAllByEdge &scan) {
    const auto &input = scan.input();
    const auto &edge_symbol = scan.output_symbol_;
    const auto &view = scan.view_;

    if (scan.edge_types_.size() > 1) {
      // we don't know how to resolve if there can be either of multiple edge types
      return nullptr;
    }

    std::optional<storage::EdgeTypeId> edge_type_from_relationship{};
    if (!scan.edge_types_.empty()) {
      // if there is already something in a relationship, we will consider that one first, rather
      // than the filters
      edge_type_from_relationship.emplace(scan.edge_types_[0]);
    }

    const auto filter_edge_types = filters_.FilteredLabels(edge_symbol);
    if (scan.edge_types_.empty() && filter_edge_types.empty()) {
      // nothing to replace with
      return nullptr;
    }

    auto found_index = FindBestEdgeTypePropertyIndex(edge_symbol, edge_type_from_relationship);
    if (found_index) {
      // Copy the property filter and then erase it from filters.
      const auto prop_filter = *found_index->filter.property_filter;
      if (prop_filter.type_ != PropertyFilter::Type::REGEX_MATCH) {
        // Remove the original expression from Filter operation only if it's not
        // a regex match. In such a case we need to perform the matching even
        // after we've scanned the index.
        filter_exprs_for_removal_.insert(found_index->filter.expression);
      }
      filters_.EraseFilter(found_index->filter);
      if (FoundIndexWithFilteredLabel(found_index.value())) {
        std::vector<Expression *> removed_expressions;
        filters_.EraseLabelFilter(edge_symbol, found_index->edge_type_from_filter.value(), &removed_expressions);
        filter_exprs_for_removal_.insert(removed_expressions.begin(), removed_expressions.end());
      }
      if (prop_filter.type_ == PropertyFilter::Type::IS_NOT_NULL) {
        return std::make_unique<ScanAllByEdgeTypeProperty>(input, edge_symbol, scan.output_from_symbol_,
                                                           scan.output_to_symbol_, GetEdgeType(found_index.value()),
                                                           GetProperty(prop_filter.property_), view);
      }
      MG_ASSERT(prop_filter.value_, "Property filter should either have bounds or a value expression.");
      return std::make_unique<ScanAllByEdgeTypePropertyValue>(
          input, edge_symbol, scan.output_from_symbol_, scan.output_to_symbol_, GetEdgeType(*found_index),
          GetProperty(prop_filter.property_), prop_filter.value_, view);
    }

    // if no edge type property index found, we try to see if we can add an index from the relationship
    if (edge_type_from_relationship.has_value() && db_->EdgeTypeIndexExists(edge_type_from_relationship.value())) {
      return std::make_unique<ScanAllByEdgeType>(input, edge_symbol, scan.output_from_symbol_, scan.output_to_symbol_,
                                                 edge_type_from_relationship.value(), view);
    }

    // if there was no edge type found in the relationship, then see in the filters if any
    auto maybe_edge_type = FindBestEdgeTypeIndex(filter_edge_types);
    if (!maybe_edge_type) return nullptr;
    const auto &edge_type = *maybe_edge_type;

    std::vector<Expression *> removed_expressions;
    filters_.EraseLabelFilter(edge_symbol, edge_type, &removed_expressions);
    filter_exprs_for_removal_.insert(removed_expressions.begin(), removed_expressions.end());
    return std::make_unique<ScanAllByEdgeType>(input, edge_symbol, scan.output_from_symbol_, scan.output_to_symbol_,
                                               GetEdgeType(edge_type), view);
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
