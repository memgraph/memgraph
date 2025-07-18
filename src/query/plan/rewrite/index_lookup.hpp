// Copyright 2025 Memgraph Ltd.
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
/// This file provides a plan rewriter which replaces `Filter` and `ScanAll`
/// operations with `ScanAllBy<Index>` if possible. The public entrypoint is
/// `RewriteWithIndexLookup`.

#pragma once

#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "frontend/ast/ast.hpp"
#include "frontend/ast/ast_storage.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"
#include "query/plan/rewrite/general.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"

DECLARE_int64(query_vertex_count_to_expand_existing);

namespace memgraph::query::plan {

namespace {
template <class TDbAccessor>
auto property_path_converter(TDbAccessor *db) {
  return [=](PropertyIxPath const &property_path) -> storage::PropertyPath {
    return property_path.path |
           ranges::views::transform([&](auto const &prop_ix) { return db->NameToProperty(prop_ix.name); }) |
           ranges::to_vector;
  };
}
}  // namespace

/// Holds a given query's index hints after sorting them by type
struct IndexHints {
  IndexHints() = default;

  template <class TDbAccessor>
  IndexHints(std::vector<IndexHint> index_hints, TDbAccessor *db) {
    for (const auto &index_hint : index_hints) {
      const auto index_type = index_hint.index_type_;
      const auto label_name = index_hint.label_ix_.name;
      if (index_type == IndexHint::IndexType::LABEL) {
        if (!db->LabelIndexReady(db->NameToLabel(label_name))) {
          spdlog::debug("Index for label {} doesn't exist", label_name);
          continue;
        }
        label_index_hints_.emplace_back(index_hint);
      } else if (index_type == IndexHint::IndexType::LABEL_PROPERTIES) {
        auto properties =
            index_hint.property_ixs_ | ranges::views::transform(property_path_converter(db)) | ranges::to_vector;

        // Fetching the corresponding index to the hint
        if (!db->LabelPropertyIndexReady(db->NameToLabel(label_name), properties)) {
          auto property_names = index_hint.property_ixs_ |
                                ranges::views::transform([&](auto &&path) { return fmt::format("{}", path); }) |
                                ranges::views::join(", ") | ranges::to<std::string>;
          spdlog::debug("Index for label doesn't exist: {} with properties {}", label_name, property_names);
          continue;
        }
        label_property_index_hints_.emplace_back(index_hint);
      } else if (index_type == IndexHint::IndexType::POINT) {
        auto property_name = index_hint.property_ixs_[0].path[0].name;
        if (!db->PointIndexExists(db->NameToLabel(label_name), db->NameToProperty(property_name))) {
          spdlog::debug("Point index for label {} and property {} doesn't exist", label_name, property_name);
          continue;
        }
        point_index_hints_.emplace_back(index_hint);
      }
    }
  }

  template <class TDbAccessor>
  bool HasLabelIndex(TDbAccessor *db, storage::LabelId label) const {
    for (const auto &[index_type, label_hint, _] : label_index_hints_) {
      auto label_id = db->NameToLabel(label_hint.name);
      if (label_id == label) {
        return true;
      }
    }
    return false;
  }

  template <class TDbAccessor>
  bool HasLabelPropertiesIndex(TDbAccessor *db, storage::LabelId label,
                               std::span<storage::PropertyPath const> property_paths) const {
    for (const auto &[index_type, label_hint, properties_prefix] : label_property_index_hints_) {
      auto label_id = db->NameToLabel(label_hint.name);
      if (label_id != label) continue;

      auto property_ids = properties_prefix | ranges::views::transform(property_path_converter(db)) | ranges::to_vector;
      // Check if paths are the same
      for (const auto &path : property_paths) {
        if (std::ranges::find(property_ids, path) != property_ids.end()) {
          return true;
        }
      }
    }
    return false;
  }

  // TODO: look into making index hints work for point indexes
  template <class TDbAccessor>
  bool HasPointIndex(TDbAccessor *db, storage::LabelId label, storage::PropertyId property) const {
    for (const auto &[index_type, label_hint, property_hint] : point_index_hints_) {
      auto label_id = db->NameToLabel(label_hint.name);
      auto property_id = db->NameToProperty(property_hint[0].path[0].name);
      if (label_id == label && property_id == property) {
        return true;
      }
    }
    return false;
  }

  std::vector<IndexHint> label_index_hints_{};
  std::vector<IndexHint> label_property_index_hints_{};
  std::vector<IndexHint> point_index_hints_{};  // TODO: check this is used somewhere
};

namespace impl {

struct HashPair {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &pair) const {
    return utils::HashCombine<T1, T2>{}(pair.first, pair.second);
  }
};

template <class TDbAccessor>
class IndexLookupRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  IndexLookupRewriter(SymbolTable *symbol_table, AstStorage *ast_storage, TDbAccessor *db, IndexHints index_hints)
      : symbol_table_(symbol_table), ast_storage_(ast_storage), db_(db), index_hints_(std::move(index_hints)) {}

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
        std::unordered_set<Symbol> modified_symbols;
        // Number of symbols is small
        for (const auto &filter : op.all_filters_) {
          modified_symbols.insert(filter.used_symbols.begin(), filter.used_symbols.end());
        }
        auto does_modify = [&]() {
          const auto &symbols = input->ModifiedSymbols(*symbol_table_);
          return std::any_of(symbols.begin(), symbols.end(), [&modified_symbols](const auto &sym_in) {
            return modified_symbols.find(sym_in) != modified_symbols.end();
          });
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

  bool PreVisit(ScanAllByEdge &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(ScanAllByEdge &op) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgeId &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(ScanAllByEdgeId &op) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgeType &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(ScanAllByEdgeType &op) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypeProperty &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(ScanAllByEdgeTypeProperty &op) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypePropertyValue &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(ScanAllByEdgeTypePropertyValue &op) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypePropertyRange &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(ScanAllByEdgeTypePropertyRange &op) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgeProperty &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(ScanAllByEdgeProperty &op) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgePropertyValue &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(ScanAllByEdgePropertyValue &op) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgePropertyRange &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(ScanAllByEdgePropertyRange &op) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByPointDistance &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(ScanAllByPointDistance &op) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAll &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  // Replace ScanAll with ScanAllBy<Index> in PostVisit, because removal of
  // ScanAll may remove the last reference and thus free the memory. PostVisit
  // should be the last thing ScanAll::Accept does, so it should be safe.
  bool PostVisit(ScanAll &scan) override {
    prev_ops_.pop_back();
    auto indexed_scan = GenScanByIndex(scan);
    if (indexed_scan) {
      SetOnParent(std::move(indexed_scan));
    }
    return true;
  }

  bool PreVisit(Expand &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(Expand & /*expand*/) override {
    prev_ops_.pop_back();
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
    if (expand.common_.existing_node) {
      return true;
    }
    if (expand.type_ == EdgeAtom::Type::BREADTH_FIRST && expand.filter_lambda_.accumulated_path_symbol) {
      // When accumulated path is used, we cannot use ST shortest path algorithm.
      return false;
    }

    std::unique_ptr<LogicalOperator> indexed_scan;
    ScanAll dst_scan(expand.input(), expand.common_.node_symbol, storage::View::OLD);
    // With expand to existing we only get real gains with BFS, because we use a
    // different algorithm then, so prefer expand to existing.
    if (expand.type_ == EdgeAtom::Type::BREADTH_FIRST) {
      // TODO: Perhaps take average node degree into consideration, instead of
      // unconditionally creating an indexed scan.
      indexed_scan = GenScanByIndex(dst_scan);
    } else {
      indexed_scan = GenScanByIndex(dst_scan, FLAGS_query_vertex_count_to_expand_existing);
    }
    if (indexed_scan) {
      expand.set_input(std::move(indexed_scan));
      expand.common_.existing_node = true;
    }
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

  // Rewriting Cartesian assumes that the input plan will have Filter operations
  // as soon as they are possible. Therefore we do not track filters above
  // Cartesian because they should be irrelevant.
  //
  // For example, the following plan is not expected to be an input to
  // IndexLookupRewriter.
  //
  // Filter n.prop = 16
  // |
  // Cartesian
  // |
  // |\
  // | ScanAll (n)
  // |
  // ScanAll (m)
  //
  // Instead, the equivalent set of operations should be done this way:
  //
  // Cartesian
  // |
  // |\
  // | Filter n.prop = 16
  // | |
  // | ScanAll (n)
  // |
  // ScanAll (m)
  bool PreVisit(Cartesian &op) override {
    prev_ops_.push_back(&op);
    RewriteBranch(&op.left_op_);

    // we add the symbols that we encountered in the left part of the cartesian
    // the reason for that is that in right part of the cartesian, we could be
    // possibly using an indexed operation instead of a scan all
    additional_bound_symbols_.insert(op.left_symbols_.begin(), op.left_symbols_.end());
    op.right_op_->Accept(*this);

    return false;
  }

  bool PostVisit(Cartesian &) override {
    prev_ops_.pop_back();

    // clear cartesian symbols as we exited the cartesian operator
    additional_bound_symbols_.clear();

    return true;
  }

  bool PreVisit(IndexedJoin &op) override {
    prev_ops_.push_back(&op);
    RewriteBranch(&op.main_branch_);
    RewriteBranch(&op.sub_branch_);
    return false;
  }

  bool PostVisit(IndexedJoin & /*unused*/) override {
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

  // The remaining operators should work by just traversing into their input.

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

  bool PreVisit(ScanAllByLabelProperties &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByLabelProperties &) override {
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
    return false;
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

  bool PreVisit(SetNestedProperty &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(SetNestedProperty & /*op*/) override {
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
  IndexHints index_hints_;

  // additional symbols that are present from other non-main branches but have influence on indexing
  std::unordered_set<Symbol> additional_bound_symbols_;

  struct LabelPropertyIndex {
    LabelIx label;
    std::vector<storage::PropertyPath> properties;  // need props ids to associate
                                                    // this with the actual index
    // FilterInfos, each with a PropertyFilter.
    std::vector<FilterInfo> filters;
    int64_t vertex_count;
    std::optional<storage::LabelPropertyIndexStats> index_stats;
  };

  struct PointLabelPropertyIndex {
    LabelIx label;
    // FilterInfo with PropertyFilter.
    FilterInfo filter;
    int64_t vertex_count;
  };

  struct IndexGroup {
    std::vector<std::variant<LabelIx, LabelPropertyIndex>> indices;
    int64_t vertex_count;
    int64_t num_of_index_hints;
  };

  bool DefaultPreVisit() override { throw utils::NotYetImplemented("optimizing index lookup"); }

  void SetOnParent(const std::shared_ptr<LogicalOperator> &input) {
    MG_ASSERT(input);
    if (prev_ops_.empty()) {
      MG_ASSERT(!new_root_);
      new_root_ = input;
      return;
    }

    auto *parent = prev_ops_.back();
    if (parent->HasSingleInput()) {
      parent->set_input(input);
      return;
    }

    if (parent->GetTypeInfo() == Cartesian::kType) {
      auto *parent_cartesian = dynamic_cast<Cartesian *>(parent);
      parent_cartesian->right_op_ = input;
      parent_cartesian->right_symbols_ = input->ModifiedSymbols(*symbol_table_);
      return;
    }

    if (parent->GetTypeInfo() == plan::RollUpApply::kType) {
      auto *parent_rollup = dynamic_cast<plan::RollUpApply *>(parent);
      parent_rollup->input_ = input;
      return;
    }

    // if we're sure that we want to set on parent, this should never happen
    LOG_FATAL("Error during index rewriting of the query!");
  }

  void RewriteBranch(std::shared_ptr<LogicalOperator> *branch) {
    IndexLookupRewriter<TDbAccessor> rewriter(symbol_table_, ast_storage_, db_, index_hints_);
    (*branch)->Accept(rewriter);
    if (rewriter.new_root_) {
      *branch = rewriter.new_root_;
    }
  }

  storage::LabelId GetLabel(const LabelIx &label) { return db_->NameToLabel(label.name); }

  storage::PropertyId GetProperty(const PropertyIx &prop) { return db_->NameToProperty(prop.name); }

  std::optional<LabelIx> FindBestLabelIndex(const std::unordered_set<LabelIx> &labels) {
    MG_ASSERT(!labels.empty(), "Trying to find the best label without any labels.");

    for (const auto &[index_type, label, _] : index_hints_.label_index_hints_) {
      if (labels.contains(label)) {
        return label;
      }
    }

    std::optional<LabelIx> best_label;
    for (const auto &label : labels) {
      if (!db_->LabelIndexReady(GetLabel(label))) continue;
      if (!best_label) {
        best_label = label;
        continue;
      }
      if (db_->VerticesCount(GetLabel(label)) < db_->VerticesCount(GetLabel(*best_label))) best_label = label;
    }
    return best_label;
  }

  struct PointIndexInfo {
    storage::LabelId label_{};
    storage::PropertyId property_{};
  };

  struct PointIndexCandidate {
    FilterInfo filter_;    // filter that can be replaced/augmented by a scan over a given index
    PointIndexInfo info_;  // exact label+property used to lookup relevant index
  };

  using CandidatePointIndices = std::unordered_map<std::pair<LabelIx, PropertyIx>, PointIndexCandidate, HashPair>;

  auto GetCandidatePointIndices(const Symbol &symbol,
                                const std::unordered_set<Symbol> &bound_symbols) -> CandidatePointIndices {
    auto are_bound = [&bound_symbols](const auto &used_symbols) {
      for (const auto &used_symbol : used_symbols) {
        if (!utils::Contains(bound_symbols, used_symbol)) {
          return false;
        }
      }
      return true;
    };

    auto candidate_point_indices = CandidatePointIndices{};
    for (const auto &label : filters_.FilteredLabels(symbol)) {
      for (const auto &filter : filters_.PointFilters(symbol)) {
        if (!are_bound(filter.used_symbols)) {
          // TODO: better more accurate comment
          // Skip filter expressions which use the symbol whose property we are
          // looking up or aren't bound. We cannot scan by such expressions. For
          // example, in `n.a = 2 + n.b` both sides of `=` refer to `n`, so we
          // cannot scan `n` by property index.
          continue;
        }

        const auto &property = filter.point_filter->property_;
        auto storage_label = GetLabel(label);
        auto storage_property = GetProperty(property);
        if (!db_->PointIndexExists(storage_label, storage_property)) {
          continue;
        }
        candidate_point_indices.insert(
            {std::pair{label, property}, {filter, {.label_ = storage_label, .property_ = storage_property}}});
      }
    }

    return candidate_point_indices;
  }

  auto FindBestPointLabelPropertyIndex(const Symbol &symbol, const std::unordered_set<Symbol> &bound_symbols)
      -> std::optional<PointLabelPropertyIndex> {
    auto candidate_point_indices = GetCandidatePointIndices(symbol, bound_symbols);

    // TODO: Can point_index_hints_ be populated?
    //  indexHints: INDEX indexHint ( ',' indexHint )* ;
    //  indexHint: ':' labelName ( '(' propertyKeyName ')' )? ;

    // First match with the provided hints
    for (const auto &[index_type, label, properties] : index_hints_.point_index_hints_) {
      auto property_ix = properties[0].path[0];
      auto filter_it = candidate_point_indices.find(std::make_pair(label, property_ix));
      if (filter_it != candidate_point_indices.cend()) {
        // TODO: isn't .vertex_count as max value wrong?
        return PointLabelPropertyIndex{.label = label,
                                       .filter = filter_it->second.filter_,
                                       .vertex_count = std::numeric_limits<std::int64_t>::max()};
      }
    }

    // Second find a good candidate
    std::optional<PointLabelPropertyIndex> found;
    for (const auto &[key, info] : candidate_point_indices) {
      const auto &[label, _] = key;

      // TODO: ATM we are looking at index size, are there other situations to select a candidate index over another?
      auto vertex_count = db_->VerticesPointCount(info.info_.label_, info.info_.property_);
      if (!vertex_count) continue;

      if (!found || vertex_count < found->vertex_count) {
        // TODO: can we introduce stats for point indices?
        found.emplace(label, info.filter_, *vertex_count);
        continue;
      }
    }
    return found;
  }

  struct LabelPropertiesIndexInfo {
    storage::LabelId label_{};
    std::vector<storage::PropertyPath> properties_{};
  };

  struct LabelPropertiesIndexCandidate {
    std::vector<FilterInfo> filters_;  // filters that can be replaced/augmented by a scan over a given index
    LabelPropertiesIndexInfo info_;    // exact label+properties used to lookup relevant index
  };

  using CandidateLabelPropertiesIndices =
      std::multimap<std::pair<LabelIx, std::vector<query::PropertyIxPath>>, LabelPropertiesIndexCandidate, std::less<>>;

  auto GetCandidateLabelPropertiesIndices(const Symbol &symbol, const std::unordered_set<Symbol> &bound_symbols)
      -> CandidateLabelPropertiesIndices {
    auto are_bound = [&bound_symbols](const auto &used_symbols) {
      for (const auto &used_symbol : used_symbols) {
        if (!utils::Contains(bound_symbols, used_symbol)) {
          return false;
        }
      }
      return true;
    };

    auto candidate_label_properties_indices = CandidateLabelPropertiesIndices{};

    namespace r = ranges;
    namespace rv = r::views;

    auto as_storage_label = [&](auto const &label) { return GetLabel(label); };
    auto valid_filter = [&](auto const &filter) {
      // Skip filter expressions which use the symbol whose property we are
      // looking up or aren't bound. We cannot scan by such expressions. For
      // example, in `n.a = 2 + n.b` both sides of `=` refer to `n`, so we
      // cannot scan `n` by property index.

      // TODO: technically we could filter for existance of n.a or n.b, BUT ATM when we replace
      //       scan+filter with index based scanby we remove the associated filter
      //       `n.a = 2 + n.b` would an example of a filter that could be enhanced by an index but does not
      //       remove the need for the filter
      return !filter.property_filter->is_symbol_in_value_ && are_bound(filter.used_symbols);
    };
    auto as_propertyIX = [&](auto const &filter) -> auto const & { return filter.property_filter->property_ids_; };
    auto as_property_path = [&](auto const &filter) -> storage::PropertyPath {
      std::vector<storage::PropertyId> storage_property_ids;
      for (auto const &property : filter.property_filter->property_ids_.path) {
        storage_property_ids.emplace_back(GetProperty(property));
      }
      return {std::move(storage_property_ids)};
    };

    auto labelIXs = filters_.FilteredLabels(symbol) | r::to_vector;
    auto or_labels = filters_.FilteredOrLabels(symbol);
    for (auto const &label_vec : or_labels) {
      labelIXs.insert(labelIXs.end(), std::make_move_iterator(label_vec.begin()),
                      std::make_move_iterator(label_vec.end()));
    }
    auto property_filters1 = filters_.PropertyFilters(symbol);
    auto property_filters = property_filters1 | rv::filter(valid_filter) | r::to_vector;
    auto labels = labelIXs | rv::transform(as_storage_label) | r::to_vector;
    ranges::stable_sort(property_filters, {}, as_property_path);
    auto properties = property_filters | rv::transform(as_property_path) | r::to_vector;

    // TODO: extract as a common util if this is ever needed elsewhere.
    auto filters_grouped_by_property = std::map<storage::PropertyPath, std::vector<FilterInfo>>{};
    auto grouped = ranges::views::zip(properties, property_filters) |
                   ranges::views::chunk_by([&](auto &&a, auto &&b) { return a.first == b.first; });
    for (auto &&group : grouped) {
      auto prop = group.front().first;
      auto &group_for_prop = filters_grouped_by_property[prop];
      for (auto const &[_, filter] : group) {
        group_for_prop.emplace_back(filter);
      }
    }

    properties = filters_grouped_by_property | rv::keys | r::to_vector;

    for (auto const &[label_pos, properties_poses, index_label, index_properties] :
         db_->RelevantLabelPropertiesIndicesInfo(labels, properties)) {
      // properties_poses: [5,3,-1,4]
      constexpr long MISSING = -1;
      auto first_missing = std::ranges::find(properties_poses, MISSING);
      auto prop_positions_prefix = std::ranges::subrange{properties_poses.begin(), first_missing};
      // properties_prefix: [5,3]
      if (prop_positions_prefix.empty()) continue;

      auto to_filters = [&](auto &&property_position) {
        return filters_grouped_by_property[properties[property_position]];
      };
      auto filters = prop_positions_prefix | rv::transform(to_filters) | r::to_vector;

      auto const &label = labelIXs[label_pos];
      utils::cartesian_product(filters, [&](std::vector<FilterInfo> const &filters) {
        // filters [n.E < 42, 10 < n.C]

        auto prop_ixs = filters | rv::transform(as_propertyIX) | r::to_vector;
        // prop_ixs [E, C]

        candidate_label_properties_indices.insert(
            {std::make_pair(label, prop_ixs),
             LabelPropertiesIndexCandidate{.filters_ = filters,
                                           .info_ = {.label_ = index_label, .properties_ = index_properties}}});
      });
    }

    return candidate_label_properties_indices;
  }

  // Finds the label-property combination. The first criteria based on number of vertices indexed -> if one index has
  // 10x less than the other one, always choose the smaller one. Otherwise, choose the index with smallest average group
  // size based on key distribution. If average group size is equal, choose the index that has distribution closer to
  // uniform distribution. Conditions based on average group size and key distribution can be only taken into account if
  // the user has run `ANALYZE GRAPH` query before If the index cannot be found, nullopt is returned.
  auto FindBestLabelPropertiesIndex(const Symbol &symbol, const std::unordered_set<Symbol> &bound_symbols)
      -> std::optional<LabelPropertyIndex> {
    /*
     * Comparator function between two indices. If new index has >= 10x vertices than the existing, it cannot be
     * better. If it is <= 10x in number of vertices, check average group size of property values. The index with
     * smaller average group size is better. If the average group size is the same, choose the one closer to the
     * uniform distribution
     * @param found: Current best label-property index.
     * @param new_stats: Label-property index candidate.
     * @param vertex_count: New index's number of vertices.
     * @return -1 if the new index is better, 0 if they are equal and 1 if the existing one is better.
     */
    auto compare_indices = [](std::optional<LabelPropertyIndex> &found,
                              std::optional<storage::LabelPropertyIndexStats> &new_stats, int vertex_count) {
      if (!new_stats.has_value()) {
        return 0;
      }

      if (found->vertex_count < vertex_count / 10.0) {
        return 1;
      }
      int cmp_avg_group = utils::CompareDecimal(new_stats->avg_group_size, found->index_stats->avg_group_size);
      if (cmp_avg_group != 0) return cmp_avg_group;
      return utils::CompareDecimal(new_stats->statistic, found->index_stats->statistic);
    };

    auto const candidate_label_properties_indices = GetCandidateLabelPropertiesIndices(symbol, bound_symbols);

    // try and match requested hint with candidate index
    for (const auto &[index_type, label, properties] : index_hints_.label_property_index_hints_) {
      auto it = candidate_label_properties_indices.find(std::make_pair(label, properties));
      if (it == candidate_label_properties_indices.end()) continue;
      // Hints may only ask for exact matches on the candidate index
      if (it->second.info_.properties_.size() != properties.size()) continue;

      return LabelPropertyIndex{.label = label,
                                .properties = it->second.info_.properties_,
                                .filters = it->second.filters_,
                                .vertex_count = std::numeric_limits<std::int64_t>::max()};
    }

    std::optional<LabelPropertyIndex> found;

    namespace r = ranges;
    namespace rv = r::views;

    auto is_better_type = [](std::span<FilterInfo const> candidate_filters, LabelPropertyIndex const &found) {
      auto to_score = [](auto const &filters) {
        auto filter_type_score = [](FilterInfo const &fi) -> double {
          // Given cardinality is the same which would be prefered as the cost of a per element cost
          switch (fi.property_filter->type_) {
            using enum PropertyFilter::Type;
            case IS_NOT_NULL:
              return 20.0;  // cheapest, just a raw scan of the index skip list
            case EQUAL:
              return 10.0;
            case RANGE: {
              if (fi.property_filter->lower_bound_ && fi.property_filter->upper_bound_) {
                return 7.0;
              } else {
                return 6.0;
              }
            }
            case REGEX_MATCH:
              return 5.0;  // REGEX compare is more expensive
            case IN:
              return 1.0;  // ATM multiple scans...not a good prederence
          }
        };

        return r::fold_left(filters | rv::transform(filter_type_score), 1.0, std::multiplies<>{});
      };

      return to_score(found.filters) < to_score(candidate_filters);
    };

    for (const auto &[key, candidate] : candidate_label_properties_indices) {
      const auto &[label_ix, prop_ixs] = key;

      auto const &storage_label = candidate.info_.label_;
      auto const &storage_properties = candidate.info_.properties_;

      // Conditions, from more to less important:
      // the index with 10x less vertices is better.
      // the index with smaller average group size is better.
      // the index with equal avg group size and distribution closer to the uniform is better.
      // the index with less vertices is better.
      // the index with same number of vertices but more optimized filter is better.

      int64_t vertex_count = db_->VerticesCount(storage_label, storage_properties);
      std::optional<storage::LabelPropertyIndexStats> new_stats = db_->GetIndexStats(storage_label, storage_properties);
      auto const make_label_property_index = [&]() -> LabelPropertyIndex {
        return {label_ix, candidate.info_.properties_, candidate.filters_, vertex_count, new_stats};
      };

      if (!found) {
        // this sets LabelPropertyIndex which communitcates which fiters are to be replaced by a LabelPropertyIndex
        found = make_label_property_index();
        continue;
      }

      // Obvious order of magnitude better?
      if (vertex_count * 10 < found->vertex_count) {
        found = make_label_property_index();
        continue;
      }

      // If the index is less composite then it is prefered
      if (candidate.info_.properties_.size() < found->properties.size()) {
        found = make_label_property_index();
        continue;
      }

      if (int cmp_res = compare_indices(found, new_stats, vertex_count);
          cmp_res == -1 ||
          (cmp_res == 0 && (vertex_count < found->vertex_count ||
                            (vertex_count == found->vertex_count && is_better_type(candidate.filters_, *found))))) {
        found = make_label_property_index();
      }
    }
    return found;
  }

  // Find the best index group for the given symbol. Firstly, we prioritize the group with most index hints.
  // After that the best index group is determined by the number of vertices in
  // the whole group combined. The group is constructed by trying to find the best LabelPropertyIndex and if not
  // possible then LabelIndex. If there is no LabelIndex, the group is empty.
  // TODO: Find a better way to determine best index than just number of vertices
  IndexGroup FindBestIndexGroup(const Symbol &symbol, const std::unordered_set<Symbol> &bound_symbols,
                                const std::vector<std::vector<LabelIx>> &or_labels) {
    IndexGroup best_group = {
        .indices = {}, .vertex_count = std::numeric_limits<std::int64_t>::max(), .num_of_index_hints = 0};
    auto candidate_label_properties_indices = GetCandidateLabelPropertiesIndices(symbol, bound_symbols);

    auto indices_with_label = [&](LabelIx label) {
      std::vector<LabelPropertiesIndexCandidate> indices;
      for (const auto &[key, candidate] : candidate_label_properties_indices) {
        if (key.first == label) {
          indices.push_back(candidate);
        }
      }
      return indices;
    };

    // Iterate through label groups and attempt to construct index groups
    for (const auto &group : or_labels) {
      IndexGroup current_group = {.indices = {}, .vertex_count = 0, .num_of_index_hints = 0};

      for (const auto &label : group) {
        auto label_id = GetLabel(label);
        int64_t best_vertex_count = std::numeric_limits<std::int64_t>::max();
        std::optional<LabelPropertiesIndexCandidate> best_label_property_index;
        bool best_has_hint = false;
        auto indices = indices_with_label(label);

        // Try to find the best LabelPropertyIndex
        for (const auto &label_prop : indices) {
          auto storage_properties = label_prop.info_.properties_;
          auto vertex_count = db_->VerticesCount(label_id, storage_properties);
          bool has_hint = index_hints_.HasLabelPropertiesIndex(db_, label_id, storage_properties);
          if (vertex_count < best_vertex_count || (has_hint && !best_has_hint)) {
            if (!best_has_hint && has_hint) {
              current_group.num_of_index_hints++;
            }
            best_has_hint = has_hint;
            best_vertex_count = vertex_count;
            best_label_property_index = label_prop;
          }
        }

        // Check if there is a LabelIndex available
        auto label_index_exists = db_->LabelIndexReady(label_id);
        if (label_index_exists && !best_has_hint && index_hints_.HasLabelIndex(db_, label_id)) {
          // LabelIndex is available and has hint
          best_vertex_count = db_->VerticesCount(label_id);
          current_group.num_of_index_hints++;
          current_group.indices.push_back(label);
        } else if (best_label_property_index) {
          // LabelPropertyIndex is available
          current_group.indices.emplace_back(
              LabelPropertyIndex{.label = label,
                                 .properties = std::move(best_label_property_index->info_.properties_),
                                 .filters = std::move(best_label_property_index->filters_),
                                 .vertex_count = best_vertex_count,
                                 .index_stats = {}});
        } else {  // Try LabelIndex as a fallback
          if (!label_index_exists) continue;
          best_vertex_count = db_->VerticesCount(label_id);
          if (best_vertex_count > 0) {
            current_group.indices.push_back(label);
          }
        }

        current_group.vertex_count += best_vertex_count;
      }

      if (current_group.indices.size() != group.size()) {
        continue;  // Skip if index isn't found for all labels in the group -> use ScanAll + Filter
      }

      // Prioritize groups with more index hints; if equal, use the lowest vertex count
      if (current_group.num_of_index_hints > best_group.num_of_index_hints ||
          (current_group.num_of_index_hints == best_group.num_of_index_hints &&
           current_group.vertex_count < best_group.vertex_count)) {
        best_group = std::move(current_group);
      }
    }

    return best_group;
  }

  // Creates a ScanAll by the best possible index for the `node_symbol`. If the node
  // does not have at least a label, no indexed lookup can be created and
  // `nullptr` is returned. The operator is chained after `input`. Optional
  // `max_vertex_count` controls, whether no operator should be created if the
  // vertex count in the best index exceeds this number. In such a case,
  // `nullptr` is returned and `input` is not chained.
  // In case of a "or" expression on labels the Distinct operator will be returned with the
  // Union operator as input. Union will have as input the ScanAll operator.
  // TODO: Add new operator instead of Distinct + Union
  std::unique_ptr<LogicalOperator> GenScanByIndex(const ScanAll &scan,
                                                  const std::optional<int64_t> &max_vertex_count = std::nullopt) {
    auto input = scan.input();
    const auto &node_symbol = scan.output_symbol_;
    const auto &view = scan.view_;

    const auto &modified_symbols = scan.ModifiedSymbols(*symbol_table_);

    std::unordered_set<Symbol> bound_symbols(modified_symbols.begin(), modified_symbols.end());
    bound_symbols.insert(additional_bound_symbols_.begin(), additional_bound_symbols_.end());

    auto are_bound = [&bound_symbols](const auto &used_symbols) {
      for (const auto &used_symbol : used_symbols) {
        if (!utils::Contains(bound_symbols, used_symbol)) {
          return false;
        }
      }
      return true;
    };

    auto const to_expression_range = [&](auto &&filter) -> ExpressionRange {
      DMG_ASSERT(filter.property_filter);
      switch (filter.property_filter->type_) {
        case PropertyFilter::Type::EQUAL:
        case PropertyFilter::Type::IN: {
          // Because of the unwind rewrite IN is the same as EQUAL
          return ExpressionRange::Equal(filter.property_filter->value_);
        }
        case PropertyFilter::Type::REGEX_MATCH: {
          return ExpressionRange::RegexMatch();
        }
        case PropertyFilter::Type::RANGE: {
          return ExpressionRange::Range(filter.property_filter->lower_bound_, filter.property_filter->upper_bound_);
        }
        case PropertyFilter::Type::IS_NOT_NULL: {
          return ExpressionRange::IsNotNull();
        }
      }
    };

    // For any IN filters, we need to unwind
    // TODO(buda): ScanAllByLabelProperty + Filter should be considered
    // here once the operator and the right cardinality estimation exist.
    // TODO: Currently IN uses unwind, this means multiple scans, this could be better
    //  performance if we use single scan
    // NOTE: make_unwinds has side-effectm changes input to include new unwind stage
    auto make_unwinds = [&](FilterInfo const &filter_info) -> FilterInfo {
      if (filter_info.property_filter->type_ == PropertyFilter::Type::IN) {
        auto const &symbol = symbol_table_->CreateAnonymousSymbol();
        auto *expression = ast_storage_->Create<Identifier>(symbol.name_);
        expression->MapTo(symbol);
        input = std::make_unique<Unwind>(input, filter_info.property_filter->value_, symbol);
        FilterInfo cpy = filter_info;
        cpy.property_filter->value_ = expression;
        return cpy;
      }
      return filter_info;
    };

    // First, try to see if we can find a vertex by ID.
    if (!max_vertex_count || *max_vertex_count >= 1) {
      for (const auto &filter : filters_.IdFilters(node_symbol)) {
        if (filter.id_filter->is_symbol_in_value_ || !are_bound(filter.used_symbols)) continue;
        auto *value = filter.id_filter->value_;
        filter_exprs_for_removal_.insert(filter.expression);
        filters_.EraseFilter(filter);
        return std::make_unique<ScanAllById>(input, node_symbol, value, view);
      }
    }
    // Now try to see if we can use label+property index. If not, try to use
    // just the label index.
    auto labels = filters_.FilteredLabels(node_symbol);
    auto or_labels = filters_.FilteredOrLabels(node_symbol);
    if (labels.empty() && or_labels.empty()) {
      // Without labels, we cannot generate any indexed ScanAll.
      return nullptr;
    }

    // Point index prefered over regular label+property index
    // TODO: figure out how to make NEW work
    if (view == storage::View::OLD) {
      auto found_index = FindBestPointLabelPropertyIndex(node_symbol, bound_symbols);

      if (found_index) {
        FilterInfo const &filter = found_index->filter;
        auto const &point_filter = filter.point_filter.value();

        filters_.EraseFilter(filter);
        std::vector<Expression *> removed_expressions;  // out parameter
        filters_.EraseLabelFilter(node_symbol, found_index->label, &removed_expressions);
        filter_exprs_for_removal_.insert(filter.expression);
        filter_exprs_for_removal_.insert(removed_expressions.begin(), removed_expressions.end());

        switch (point_filter.function_) {
          using enum PointFilter::Function;
          case DISTANCE: {
            return std::make_unique<ScanAllByPointDistance>(
                input, node_symbol, GetLabel(found_index->label), GetProperty(point_filter.property_),
                point_filter.distance_.cmp_value_,  // uses the CRS from here
                point_filter.distance_.boundary_value_, point_filter.distance_.boundary_condition_);
          }
          case WITHINBBOX: {
            auto *expr = std::invoke([&]() -> Expression * {
              // if condition known at plan time, use PrimitiveLiteral
              if (point_filter.withinbbox_.condition_) {
                auto is_inside = point_filter.withinbbox_.condition_ == WithinBBoxCondition::INSIDE;

                auto *new_expr = ast_storage_->Create<PrimitiveLiteral>();
                new_expr->value_ = storage::ExternalPropertyValue{is_inside};
                return new_expr;
              }
              // else use provided evaluation time expression
              return point_filter.withinbbox_.boundary_value_;
            });
            return std::make_unique<ScanAllByPointWithinbbox>(
                input, node_symbol, GetLabel(found_index->label), GetProperty(point_filter.property_),
                point_filter.withinbbox_.bottom_left_, point_filter.withinbbox_.top_right_, expr);
          }
        }
      }
    }
    std::optional<LabelPropertyIndex> found_index = FindBestLabelPropertiesIndex(node_symbol, bound_symbols);
    if (found_index &&
        // Use label+property index if we satisfy max_vertex_count.
        (!max_vertex_count || *max_vertex_count >= found_index->vertex_count) && or_labels.empty()) {
      // Filter cleanup, track which expressions to remove
      for (auto const &filter_info : found_index->filters) {
        const PropertyFilter prop_filter = *filter_info.property_filter;

        if (prop_filter.type_ != PropertyFilter::Type::REGEX_MATCH) {
          // Remove the original expression from Filter operation only if it's not
          // a regex match. In such a case we need to perform the matching even
          // after we've scanned the index.
          filter_exprs_for_removal_.insert(filter_info.expression);
        }

        filters_.EraseFilter(filter_info);
        std::vector<Expression *> removed_expressions;
        filters_.EraseLabelFilter(node_symbol, found_index->label, &removed_expressions);
        filter_exprs_for_removal_.insert(removed_expressions.begin(), removed_expressions.end());
      }

      auto value_expressions = found_index->filters | ranges::views::transform(make_unwinds) | ranges::to_vector;
      auto expr_ranges = value_expressions | ranges::views::transform(to_expression_range) | ranges::to_vector;

      return std::make_unique<ScanAllByLabelProperties>(input, node_symbol, GetLabel(found_index->label),
                                                        std::move(found_index->properties), std::move(expr_ranges),
                                                        view);
    }
    if (!labels.empty()) {
      auto maybe_label = FindBestLabelIndex(labels);
      if (maybe_label) {
        const auto &label = *maybe_label;
        if (!max_vertex_count || db_->VerticesCount(GetLabel(label)) <= *max_vertex_count) {
          std::vector<Expression *> removed_expressions;
          filters_.EraseLabelFilter(node_symbol, label, &removed_expressions);
          filter_exprs_for_removal_.insert(removed_expressions.begin(), removed_expressions.end());
          return std::make_unique<ScanAllByLabel>(input, node_symbol, GetLabel(label), view);
        }
      }
    }
    if (!or_labels.empty()) {
      auto best_group = FindBestIndexGroup(node_symbol, bound_symbols, or_labels);
      // If we satisfy max_vertex_count and if there is a group for which we can find an index let's use it and chain it
      // in unions
      if ((!max_vertex_count || best_group.vertex_count <= *max_vertex_count) && !best_group.indices.empty()) {
        std::unique_ptr<LogicalOperator> prev;
        std::vector<LabelIx> labels_to_erase;
        labels_to_erase.reserve(best_group.indices.size());
        std::vector<Expression *> removed_expressions;
        std::optional<std::vector<storage::PropertyPath>>
            filtered_property_ids;  // Used to check if all indices uses the same filter
        bool all_property_filters_same =
            true;  // Used to check if all indices uses the same filter -> if yes we can remove the filter
        for (const auto &index : best_group.indices) {
          if (std::holds_alternative<LabelIx>(index)) {
            all_property_filters_same = false;
            labels_to_erase.push_back(std::get<LabelIx>(index));
            auto scan = std::make_unique<ScanAllByLabel>(input, node_symbol, GetLabel(std::get<LabelIx>(index)), view);
            if (prev) {
              auto union_op =
                  std::make_unique<Union>(std::move(prev), std::move(scan), std::vector<Symbol>{node_symbol},
                                          std::vector<Symbol>{node_symbol}, std::vector<Symbol>{node_symbol});
              prev = std::make_unique<Distinct>(std::move(union_op), std::vector<Symbol>{node_symbol});
            } else {
              prev = std::move(scan);
            }
          } else {
            auto &label_property_index = std::get<LabelPropertyIndex>(index);
            labels_to_erase.push_back(label_property_index.label);
            if (filtered_property_ids && *filtered_property_ids != label_property_index.properties) {
              all_property_filters_same = false;
            }
            filtered_property_ids = label_property_index.properties;
            // Filter cleanup, track which expressions to remove
            for (auto const &filter_info : label_property_index.filters) {
              const PropertyFilter prop_filter = *filter_info.property_filter;
              if (prop_filter.type_ != PropertyFilter::Type::REGEX_MATCH) {
                // Remove the original expression from Filter operation only if it's not
                // a regex match. In such a case we need to perform the matching even
                // after we've scanned the index.
                removed_expressions.push_back(filter_info.expression);
              }
            }
            auto value_expressions =
                label_property_index.filters | ranges::views::transform(make_unwinds) | ranges::to_vector;
            auto expr_ranges = value_expressions | ranges::views::transform(to_expression_range) | ranges::to_vector;
            auto label_property_index_scan = std::make_unique<ScanAllByLabelProperties>(
                input, node_symbol, GetLabel(label_property_index.label), std::move(label_property_index.properties),
                std::move(expr_ranges), view);
            if (prev) {
              auto union_op = std::make_unique<Union>(
                  std::move(prev), std::move(label_property_index_scan), std::vector<Symbol>{node_symbol},
                  std::vector<Symbol>{node_symbol}, std::vector<Symbol>{node_symbol});
              prev = std::make_unique<Distinct>(std::move(union_op), std::vector<Symbol>{node_symbol});
            } else {
              prev = std::move(label_property_index_scan);
            }
          }
        }
        if (!all_property_filters_same) removed_expressions.clear();
        filters_.EraseOrLabelFilter(node_symbol, labels_to_erase, &removed_expressions);
        filter_exprs_for_removal_.insert(removed_expressions.begin(), removed_expressions.end());
        return prev;
      }
    }
    return nullptr;
  }
};

}  // namespace impl

template <class TDbAccessor>
std::unique_ptr<LogicalOperator> RewriteWithIndexLookup(std::unique_ptr<LogicalOperator> root_op,
                                                        SymbolTable *symbol_table, AstStorage *ast_storage,
                                                        TDbAccessor *db, IndexHints index_hints) {
  impl::IndexLookupRewriter<TDbAccessor> rewriter(symbol_table, ast_storage, db, index_hints);
  root_op->Accept(rewriter);
  if (rewriter.new_root_) {
    // This shouldn't happen in real use case, because IndexLookupRewriter
    // removes Filter operations and they cannot be the root op. In case we
    // somehow missed this, raise NotYetImplemented instead of MG_ASSERT
    // crashing the application.
    throw utils::NotYetImplemented("optimizing index lookup");
  }
  return root_op;
}

}  // namespace memgraph::query::plan
