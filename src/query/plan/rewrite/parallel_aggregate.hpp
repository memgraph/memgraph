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

#pragma once

#include <memory>
#include <set>
#include <vector>
#include "query/dependant_symbol_visitor.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/read_write_type_checker.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query::plan {

namespace impl {

template <class TDbAccessor>
class ParallelAggregateRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  ParallelAggregateRewriter(SymbolTable *symbolTable, AstStorage *astStorage, TDbAccessor *db, size_t num_threads)
      : symbol_table(symbolTable), ast_storage(astStorage), db(db), num_threads_(num_threads) {}

  ~ParallelAggregateRewriter() override = default;

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &) override { return true; }

  // Track parent operators - need to handle operators that could be parents of Aggregate
  // Using DefaultPreVisit to track all operators we don't explicitly handle
  bool DefaultPreVisit() override {
    // For operators we don't explicitly handle, we still need to track them as potential parents
    // But we can't do this generically, so we'll implement specific PreVisit methods
    return true;
  }

  // Operators that might be parents of Aggregate - need to track all single-input operators
  // and operators with branches that could contain Aggregate

  bool PreVisit(Accumulate &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(Accumulate &) override {
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

  bool PreVisit(Skip &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(Skip &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Filter &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(Filter &) override {
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

  bool PreVisit(Unwind &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(Unwind &) override {
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

  bool PreVisit(ConstructNamedPath &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ConstructNamedPath &) override {
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

  bool PreVisit(EvaluatePatternFilter &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(EvaluatePatternFilter &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(LoadCsv &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(LoadCsv &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(LoadParquet &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(LoadParquet &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(PeriodicCommit &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(PeriodicCommit &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(SetNestedProperty &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(SetNestedProperty &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(RemoveNestedProperty &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(RemoveNestedProperty &) override {
    prev_ops_.pop_back();
    return true;
  }

  // Operators with branches that could contain Aggregate
  bool PreVisit(Merge &op) override {
    prev_ops_.push_back(&op);
    return op.input()->Accept(*this) && op.merge_match_->Accept(*this) && op.merge_create_->Accept(*this);
  }
  bool PostVisit(Merge &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Optional &op) override {
    prev_ops_.push_back(&op);
    return op.input()->Accept(*this) && op.optional_->Accept(*this);
  }
  bool PostVisit(Optional &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Foreach &op) override {
    prev_ops_.push_back(&op);
    return op.input()->Accept(*this) && op.update_clauses_->Accept(*this);
  }
  bool PostVisit(Foreach &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(RollUpApply &op) override {
    prev_ops_.push_back(&op);
    return op.input()->Accept(*this) && op.list_collection_branch_->Accept(*this);
  }
  bool PostVisit(RollUpApply &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(PeriodicSubquery &op) override {
    prev_ops_.push_back(&op);
    return op.input()->Accept(*this) && op.subquery_->Accept(*this);
  }
  bool PostVisit(PeriodicSubquery &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Apply &op) override {
    prev_ops_.push_back(&op);
    return op.input()->Accept(*this) && op.subquery_->Accept(*this);
  }
  bool PostVisit(Apply &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Aggregate &op) override {
    // Collect symbols needed by this Aggregate (from group_by and aggregations)
    std::set<Symbol::Position_t> required_symbols;
    DependantSymbolVisitor symbol_visitor(required_symbols);

    // Collect symbols from group_by expressions
    for (auto *group_by_expr : op.group_by_) {
      if (group_by_expr != nullptr) {
        group_by_expr->Accept(symbol_visitor);
      }
    }

    // Collect symbols from aggregation expressions
    for (const auto &agg_elem : op.aggregations_) {
      if (agg_elem.arg1 != nullptr) {
        agg_elem.arg1->Accept(symbol_visitor);
      }
      if (agg_elem.arg2 != nullptr) {
        agg_elem.arg2->Accept(symbol_visitor);
      }
    }

    // Find the Scan operator that produces the required symbols
    std::shared_ptr<LogicalOperator> target_scan;
    std::shared_ptr<LogicalOperator> scan_parent;
    if (!FindScanForSymbols(op.input(), required_symbols, target_scan, scan_parent)) {
      // No suitable Scan found, skip rewriting
      prev_ops_.push_back(&op);
      return true;
    }

    // Verify the path from Aggregate to Scan is read-only
    if (!IsPathReadOnly(&op, target_scan.get())) {
      // Path contains write operators, cannot parallelize
      prev_ops_.push_back(&op);
      return true;
    }

    // Create AggregateParallel operator
    // post_scan_input is everything after the target Scan
    // agg_inputs is the entire input branch of Aggregate

    auto post_scan_input = target_scan->input();
    auto state_symbol = symbol_table->CreateAnonymousSymbol();
    auto scan_input = CreateScanParallel(target_scan, post_scan_input, state_symbol);
    if (!scan_input) {
      // Unsupported scan type; try to find another operator to parallelize
      prev_ops_.push_back(&op);
      return true;
    }
    auto parallel_merge = std::make_shared<ParallelMerge>(scan_input);
    // scan_parent may be an operator without a single input. Check and handle appropriately.
    const auto &scan_type = target_scan->GetTypeInfo();
    std::shared_ptr<LogicalOperator> scan_chunk;

    // Check if this is an edge scan type
    if (utils::IsSubtype(scan_type, ScanAllByEdge::kType)) {
      auto *scan_edge = dynamic_cast<ScanAllByEdge *>(target_scan.get());
      MG_ASSERT(scan_edge, "Expected ScanAllByEdge or subtype");
      scan_chunk = std::make_shared<ScanChunkByEdge>(parallel_merge, scan_edge->common_.edge_symbol,
                                                     scan_edge->common_.node1_symbol, scan_edge->common_.node2_symbol,
                                                     scan_edge->common_.direction, scan_edge->common_.edge_types,
                                                     scan_edge->view_, state_symbol);
    } else {
      // Vertex scan - use ScanChunk
      auto *scan = dynamic_cast<ScanAll *>(target_scan.get());
      MG_ASSERT(scan, "Expected ScanAll or subtype");
      scan_chunk = std::make_shared<ScanChunk>(parallel_merge, scan->output_symbol_, scan->view_, state_symbol);
    }

    // Replace the scan with scan_chunk in the operator tree
    if (scan_parent) {
      if (scan_parent->HasSingleInput()) {
        scan_parent->set_input(scan_chunk);
      } else {
        // Handle special case for multi-input operators
        // Try to set the correct child in multi-input operator
        if (auto *union_op = dynamic_cast<Union *>(scan_parent.get())) {
          // Check which child is target_scan
          if (union_op->left_op_ && union_op->left_op_.get() == target_scan.get()) {
            union_op->left_op_ = scan_chunk;
          } else if (union_op->right_op_ && union_op->right_op_.get() == target_scan.get()) {
            union_op->right_op_ = scan_chunk;
          } else {
            spdlog::error("Unexpected operator in chain: {}", scan_parent->ToString());
            return false;
          }
        } else if (auto *cartesian_op = dynamic_cast<Cartesian *>(scan_parent.get())) {
          if (cartesian_op->left_op_ && cartesian_op->left_op_.get() == target_scan.get()) {
            cartesian_op->left_op_ = scan_chunk;
          } else if (cartesian_op->right_op_ && cartesian_op->right_op_.get() == target_scan.get()) {
            cartesian_op->right_op_ = scan_chunk;
          } else {
            spdlog::error("Unexpected operator in chain: {}", scan_parent->ToString());
            return false;
          }
        } else if (auto *indexed_join_op = dynamic_cast<IndexedJoin *>(scan_parent.get())) {
          if (indexed_join_op->main_branch_ && indexed_join_op->main_branch_.get() == target_scan.get()) {
            indexed_join_op->main_branch_ = scan_chunk;
          } else if (indexed_join_op->sub_branch_ && indexed_join_op->sub_branch_.get() == target_scan.get()) {
            indexed_join_op->sub_branch_ = scan_chunk;
          } else {
            spdlog::error("Unexpected operator in chain: {}", scan_parent->ToString());
            return false;
          }
        } else if (auto *hash_join_op = dynamic_cast<HashJoin *>(scan_parent.get())) {
          if (hash_join_op->left_op_ && hash_join_op->left_op_.get() == target_scan.get()) {
            hash_join_op->left_op_ = scan_chunk;
          } else if (hash_join_op->right_op_ && hash_join_op->right_op_.get() == target_scan.get()) {
            hash_join_op->right_op_ = scan_chunk;
          } else {
            spdlog::error("Unexpected operator in chain: {}", scan_parent->ToString());
            return false;
          }
        } else if (auto *rollup_apply_op = dynamic_cast<RollUpApply *>(scan_parent.get())) {
          if (rollup_apply_op->input_ && rollup_apply_op->input_.get() == target_scan.get()) {
            rollup_apply_op->input_ = scan_chunk;
          } else {
            spdlog::error("Unexpected operator in chain: {}", scan_parent->ToString());
            return false;
          }
        } else if (dynamic_cast<Once *>(scan_parent.get()) != nullptr) {
          // Once has no inputs, so it cannot be a parent of a scan
          spdlog::error("Once operator cannot be a parent of a scan: {}", scan_parent->ToString());
          return false;
        } else if (dynamic_cast<OutputTable *>(scan_parent.get()) != nullptr) {
          // OutputTable has no inputs, so it cannot be a parent of a scan
          spdlog::error("OutputTable operator cannot be a parent of a scan: {}", scan_parent->ToString());
          return false;
        } else if (dynamic_cast<OutputTableStream *>(scan_parent.get()) != nullptr) {
          // OutputTableStream has no inputs, so it cannot be a parent of a scan
          spdlog::error("OutputTableStream operator cannot be a parent of a scan: {}", scan_parent->ToString());
          return false;
        } else {
          spdlog::error(
              "Unsupported operator in parallel chain: {}. Please contact Memgraph support as this scenario should not "
              "happen!",
              scan_parent->ToString());
          return false;
        }
      }
    } else {
      // scan_parent is null, meaning the scan is directly the input of Aggregate
      // (or the first operator in the chain we're checking)
      // In this case, we need to replace it in the Aggregate's input
      if (op.input().get() == target_scan.get()) {
        // The scan is directly the input of Aggregate
        // Replace it by setting the Aggregate's input to scan_chunk
        op.set_input(scan_chunk);
      } else {
        // The scan is somewhere in the input chain, find and replace it
        if (!ReplaceScanInChain(prev_ops_.back()->input() /* op */, target_scan, scan_chunk)) {
          // If we couldn't find it, something is wrong
          prev_ops_.push_back(&op);
          return true;
        }
      }
    }

    // Create AggregateParallel with default num_threads
    auto parallel_agg = std::make_shared<AggregateParallel>(prev_ops_.back()->input() /*op */, num_threads_);

    // Switch the parent operator (if any) to use AggregateParallel instead of Aggregate
    // This makes: Parent -> AggregateParallel instead of Parent -> Aggregate
    // Note: Don't push Aggregate to prev_ops_ before calling SetOnParent, because
    // SetOnParent needs to access the parent (prev_ops_.back()), not Aggregate itself
    SetOnParent(parallel_agg);

    // Now push Aggregate to track it for any children it might have
    prev_ops_.push_back(&op);

    return true;  // Continue visiting to handle nested aggregations
  }

  bool PostVisit(Aggregate &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Expand &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool PostVisit(Expand &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ExpandVariable &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ExpandVariable &) override {
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

  bool PreVisit(ScanAll &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAll &) override {
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

  bool PreVisit(ScanAllByEdge &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByEdge &) override {
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

  bool PreVisit(ScanAllByEdgeTypeProperty &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByEdgeTypeProperty &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypePropertyValue &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByEdgeTypePropertyValue &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgeTypePropertyRange &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByEdgeTypePropertyRange &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgeProperty &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByEdgeProperty &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgePropertyValue &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByEdgePropertyValue &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByEdgePropertyRange &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByEdgePropertyRange &) override {
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

  bool PreVisit(ScanAllByPointDistance &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByPointDistance &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanAllByPointWithinbbox &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanAllByPointWithinbbox &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Union &op) override {
    prev_ops_.push_back(&op);
    op.left_op_->Accept(*this);
    op.right_op_->Accept(*this);
    return false;
  }
  bool PostVisit(Union &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Cartesian &op) override {
    prev_ops_.push_back(&op);
    return op.left_op_->Accept(*this) && op.right_op_->Accept(*this);
  }
  bool PostVisit(Cartesian &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(HashJoin &op) override {
    prev_ops_.push_back(&op);
    return op.left_op_->Accept(*this) && op.right_op_->Accept(*this);
  }
  bool PostVisit(HashJoin &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(IndexedJoin &op) override {
    prev_ops_.push_back(&op);
    return op.main_branch_->Accept(*this) && op.sub_branch_->Accept(*this);
  }
  bool PostVisit(IndexedJoin &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(AggregateParallel &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(AggregateParallel &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ParallelMerge &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ParallelMerge &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanParallelByPointDistance &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanParallelByPointDistance &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanParallelByWithinbbox &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanParallelByWithinbbox &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanParallelByEdge &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanParallelByEdge &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(ScanParallelByEdgeTypePropertyValue &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(ScanParallelByEdgeTypePropertyValue &) override {
    prev_ops_.pop_back();
    return true;
  }

  std::shared_ptr<LogicalOperator> new_root_;

 private:
  SymbolTable *symbol_table;
  AstStorage *ast_storage;
  TDbAccessor *db;
  std::vector<LogicalOperator *> prev_ops_;
  size_t num_threads_;

  void SetOnParent(const std::shared_ptr<LogicalOperator> &input) {
    MG_ASSERT(input);
    if (prev_ops_.empty()) {
      // Aggregate is the root, so AggregateParallel becomes the new root
      MG_ASSERT(!new_root_);
      new_root_ = input;
      return;
    }
    // Set the parent's input to AggregateParallel
    prev_ops_.back()->set_input(input);
  }
  // Helper function to create the appropriate ScanParallel variant based on the scan type
  std::shared_ptr<ScanParallel> CreateScanParallel(std::shared_ptr<LogicalOperator> scan_op,
                                                   std::shared_ptr<LogicalOperator> input, Symbol state_symbol) {
    const auto &scan_type = scan_op->GetTypeInfo();
    auto *scan_all = dynamic_cast<ScanAll *>(scan_op.get());
    MG_ASSERT(scan_all, "Expected ScanAll or subtype");

    // Handle vertex scan variants
    if (scan_type == ScanAll::kType) {
      return std::make_shared<ScanParallel>(input, scan_all->view_, num_threads_, state_symbol);
    }
    if (scan_type == ScanAllById::kType) {
      // ScanAllById returns a single vertex, so we can't parallelize it
      return nullptr;
    }
    if (scan_type == ScanAllByLabel::kType) {
      auto *scan = dynamic_cast<ScanAllByLabel *>(scan_op.get());
      return std::make_shared<ScanParallelByLabel>(input, scan->view_, num_threads_, state_symbol, scan->label_);
    }
    if (scan_type == ScanAllByLabelProperties::kType) {
      auto *scan = dynamic_cast<ScanAllByLabelProperties *>(scan_op.get());
      return std::make_shared<ScanParallelByLabelProperties>(input, scan->view_, num_threads_, state_symbol,
                                                             scan->label_, scan->properties_, scan->expression_ranges_);
    }
    if (scan_type == ScanAllByPointDistance::kType) {
      auto *scan = dynamic_cast<ScanAllByPointDistance *>(scan_op.get());
      return std::make_shared<ScanParallelByPointDistance>(input, scan->view_, num_threads_, state_symbol, scan->label_,
                                                           scan->property_, scan->cmp_value_, scan->boundary_value_,
                                                           scan->boundary_condition_);
    }
    if (scan_type == ScanAllByPointWithinbbox::kType) {
      auto *scan = dynamic_cast<ScanAllByPointWithinbbox *>(scan_op.get());
      return std::make_shared<ScanParallelByWithinbbox>(input, scan->view_, num_threads_, state_symbol, scan->label_,
                                                        scan->property_, scan->bottom_left_, scan->top_right_,
                                                        scan->boundary_value_);
    }

    // Handle edge scan variants
    if (scan_type == ScanAllByEdgeId::kType) {
      // ScanAllByEdgeId returns a single edge, so we can't parallelize it
      return nullptr;
    }
    if (scan_type == ScanAllByEdge::kType) {
      auto *scan = dynamic_cast<ScanAllByEdge *>(scan_op.get());
      return std::make_shared<ScanParallelByEdge>(input, scan->view_, num_threads_, state_symbol,
                                                  scan->common_.edge_symbol, scan->common_.node1_symbol,
                                                  scan->common_.node2_symbol, scan->common_.direction);
    }
    if (scan_type == ScanAllByEdgeType::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeType *>(scan_op.get());
      return std::make_shared<ScanParallelByEdgeType>(input, scan->view_, num_threads_, state_symbol,
                                                      scan->common_.edge_types[0]);
    }
    if (scan_type == ScanAllByEdgeTypeProperty::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeTypeProperty *>(scan_op.get());
      return std::make_shared<ScanParallelByEdgeTypeProperty>(input, scan->view_, num_threads_, state_symbol,
                                                              scan->common_.edge_types[0], scan->property_);
    }
    if (scan_type == ScanAllByEdgeTypePropertyValue::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeTypePropertyValue *>(scan_op.get());
      return std::make_shared<ScanParallelByEdgeTypePropertyValue>(input, scan->view_, num_threads_, state_symbol,
                                                                   scan->common_.edge_types[0], scan->property_,
                                                                   scan->expression_->Clone(ast_storage));
    }
    if (scan_type == ScanAllByEdgeTypePropertyRange::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeTypePropertyRange *>(scan_op.get());
      return std::make_shared<ScanParallelByEdgeTypePropertyRange>(input, scan->view_, num_threads_, state_symbol,
                                                                   scan->common_.edge_types[0], scan->property_,
                                                                   scan->lower_bound_, scan->upper_bound_);
    }
    if (scan_type == ScanAllByEdgeProperty::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeProperty *>(scan_op.get());
      return std::make_shared<ScanParallelByEdgeProperty>(input, scan->view_, num_threads_, state_symbol,
                                                          scan->property_);
    }
    if (scan_type == ScanAllByEdgePropertyValue::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgePropertyValue *>(scan_op.get());
      return std::make_shared<ScanParallelByEdgePropertyValue>(input, scan->view_, num_threads_, state_symbol,
                                                               scan->property_, scan->expression_);
    }
    if (scan_type == ScanAllByEdgePropertyRange::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgePropertyRange *>(scan_op.get());
      return std::make_shared<ScanParallelByEdgePropertyRange>(input, scan->view_, num_threads_, state_symbol,
                                                               scan->property_, scan->lower_bound_, scan->upper_bound_);
    }

    // Unsupported scan type
    spdlog::error(
        "Unsupported scan type in parallel chain: {}. Please contact Memgraph support as this scenario should not "
        "happen!",
        scan_type.name);
    return nullptr;
  }

  // Helper function to check if a path from start to target contains only read operators
  bool IsPathReadOnly(LogicalOperator *start, LogicalOperator *target) {
    if (!start || !target) {
      return false;
    }
    // This checks the whole path (till the end)
    // Temporary cut off the plan after target
    const auto input = target->input();
    target->set_input(std::make_shared<Once>());
    ReadWriteTypeChecker checker;
    start->Accept(checker);
    target->set_input(input);
    return checker.type == ReadWriteTypeChecker::RWType::R || checker.type == ReadWriteTypeChecker::RWType::NONE;
  }

  // Helper function to get symbols produced by a Scan operator
  std::vector<Symbol> GetScanOutputSymbols(const std::shared_ptr<LogicalOperator> &scan) {
    std::vector<Symbol> symbols;

    if (utils::IsSubtype(*scan, ScanAllByEdge::kType)) {
      auto *scan_edge = dynamic_cast<ScanAllByEdge *>(scan.get());
      if (scan_edge != nullptr) {
        symbols.push_back(scan_edge->common_.edge_symbol);
        symbols.push_back(scan_edge->common_.node1_symbol);
        symbols.push_back(scan_edge->common_.node2_symbol);
        // Also include the output_symbol_ from the base ScanAll
        symbols.push_back(scan_edge->output_symbol_);
      }
    } else if (utils::IsSubtype(*scan, ScanAll::kType)) {
      auto *scan_all = dynamic_cast<ScanAll *>(scan.get());
      if (scan_all != nullptr) {
        symbols.push_back(scan_all->output_symbol_);
      }
    }

    return symbols;
  }

  // Helper function to check if a Scan produces any of the required symbols
  size_t ScanProducesSymbols(const std::shared_ptr<LogicalOperator> &scan,
                             const std::set<Symbol::Position_t> &required_symbols) {
    auto scan_symbols = GetScanOutputSymbols(scan);
    size_t found = 0;
    for (const auto &sym : scan_symbols) {
      if (required_symbols.find(sym.position()) != required_symbols.end()) {
        found++;
      }
    }
    return found;
  }

  // Helper function to extract input symbols that an operator depends on
  // Uses DependantSymbolVisitor to analyze expressions generically
  // Returns the set of input symbols the operator depends on
  std::set<Symbol::Position_t> ExtractInputSymbolsFromOperator(const std::shared_ptr<LogicalOperator> &op) {
    std::set<Symbol::Position_t> input_symbols;
    DependantSymbolVisitor symbol_visitor(input_symbols);

    // Try to extract symbols from operator's expressions
    // Different operators have expressions in different places

    // Produce: named_expressions
    if (auto *produce_op = dynamic_cast<Produce *>(op.get())) {
      for (auto *named_expr : produce_op->named_expressions_) {
        if (named_expr != nullptr && named_expr->expression_ != nullptr) {
          named_expr->expression_->Accept(symbol_visitor);
        }
      }
      return input_symbols;
    }

    // Filter: expression and all_filters
    if (auto *filter_op = dynamic_cast<Filter *>(op.get())) {
      if (filter_op->expression_ != nullptr) {
        filter_op->expression_->Accept(symbol_visitor);
      }
      // FilterInfo has an 'expression' field (not 'expression_')
      for (const auto &filter : filter_op->all_filters_) {
        if (filter.expression != nullptr) {
          filter.expression->Accept(symbol_visitor);
        }
      }
      return input_symbols;
    }

    // For other operators without expressions we can analyze,
    // return empty set - caller will fall back to using ModifiedSymbols from input
    return input_symbols;
  }

  // Helper function to check if an operator produces any of the required symbols
  bool OperatorProducesSymbols(const std::shared_ptr<LogicalOperator> &op,
                               const std::set<Symbol::Position_t> &required_symbols) {
    // Get symbols produced by this operator
    auto produced_symbols = op->ModifiedSymbols(*symbol_table);
    return std::ranges::any_of(produced_symbols, [&required_symbols](const Symbol &sym) {
      return required_symbols.find(sym.position()) != required_symbols.end();
    });
  }

  // Helper function to find the Scan operator that produces the required symbols
  // This traces symbol dependencies backwards through operators
  // Returns true if a suitable Scan is found, false otherwise
  bool FindScanForSymbols(std::shared_ptr<LogicalOperator> start, const std::set<Symbol::Position_t> &required_symbols,
                          std::shared_ptr<LogicalOperator> &target_scan,
                          std::shared_ptr<LogicalOperator> &scan_parent) {
    if (!start) {
      return false;
    }

    target_scan = nullptr;
    scan_parent = nullptr;

    std::shared_ptr<LogicalOperator> parent = nullptr;
    std::shared_ptr<LogicalOperator> current = start;
    std::set<Symbol::Position_t> symbols_to_find = required_symbols;

    // Traverse down the input chain, tracing symbol dependencies backwards
    while (current && current->GetTypeInfo() != Once::kType) {
      // If we encounter another Aggregate or AggregateParallel, abandon the current one
      // The nested Aggregate will be processed when we visit it
      // This prevents trying to parallelize outer Aggregates that depend on inner ones
      if (current->GetTypeInfo() == Aggregate::kType || current->GetTypeInfo() == AggregateParallel::kType) {
        return target_scan != nullptr;
      }

      // Check if this is a Scan that produces any of the symbols we're looking for
      if (utils::IsSubtype(*current, ScanAll::kType)) {
        size_t found = ScanProducesSymbols(current, symbols_to_find);
        // Exact match
        if (found == symbols_to_find.size()) {
          target_scan = current;
          scan_parent = parent;
          return true;
        }
        // Continue to find the deepest scan
        if (found > 0) {
          target_scan = current;
          scan_parent = parent;
        }
      }

      // Check if this operator's ModifiedSymbols contains any symbols we're looking for
      // This is the most generic check - works for all operators
      auto operator_symbols = current->ModifiedSymbols(*symbol_table);
      std::set<Symbol::Position_t> operator_symbol_positions;
      for (const auto &sym : operator_symbols) {
        operator_symbol_positions.insert(sym.position());
      }

      // Check if operator has any of the symbols we're looking for
      bool operator_has_symbols = false;
      for (const auto &sym_pos : symbols_to_find) {
        if (operator_symbol_positions.find(sym_pos) != operator_symbol_positions.end()) {
          operator_has_symbols = true;
          break;
        }
      }

      if (operator_has_symbols) {
        // Try to extract input symbols from operator's expressions
        // This gives us precise information about what the operator depends on
        auto extracted_symbols = ExtractInputSymbolsFromOperator(current);

        // Get input symbols for pass-through detection
        std::set<Symbol::Position_t> input_symbol_positions;

        // For join operators, use the left branch to find symbols
        if (current->HasSingleInput()) {
          if (current->input()) {
            auto input_symbols = current->input()->ModifiedSymbols(*symbol_table);
            for (const auto &sym : input_symbols) {
              input_symbol_positions.insert(sym.position());
            }
          }
        } else if (auto *cartesian_op = dynamic_cast<Cartesian *>(current.get())) {
          if (cartesian_op->left_op_) {
            auto left_symbols = cartesian_op->left_op_->ModifiedSymbols(*symbol_table);
            for (const auto &sym : left_symbols) {
              input_symbol_positions.insert(sym.position());
            }
          }
        } else if (auto *hash_join_op = dynamic_cast<HashJoin *>(current.get())) {
          if (hash_join_op->left_op_) {
            auto left_symbols = hash_join_op->left_op_->ModifiedSymbols(*symbol_table);
            for (const auto &sym : left_symbols) {
              input_symbol_positions.insert(sym.position());
            }
          }
        } else if (auto *indexed_join_op = dynamic_cast<IndexedJoin *>(current.get())) {
          if (indexed_join_op->main_branch_) {
            auto main_symbols = indexed_join_op->main_branch_->ModifiedSymbols(*symbol_table);
            for (const auto &sym : main_symbols) {
              input_symbol_positions.insert(sym.position());
            }
          }
        } else if (auto *rollup_apply_op = dynamic_cast<RollUpApply *>(current.get())) {
          if (rollup_apply_op->input_) {
            auto input_symbols = rollup_apply_op->input_->ModifiedSymbols(*symbol_table);
            for (const auto &sym : input_symbols) {
              input_symbol_positions.insert(sym.position());
            }
          }
        } else if (dynamic_cast<Once *>(current.get()) != nullptr ||
                   dynamic_cast<OutputTable *>(current.get()) != nullptr ||
                   dynamic_cast<OutputTableStream *>(current.get()) != nullptr) {
          // Terminal operators with no inputs - cannot have input symbols
          // Continue with empty input_symbol_positions
        } else {
          spdlog::error(
              "Unsupported operator in operator chain: {}. Please contact Memgraph support as this scenario should not "
              "happen!",
              current->ToString());
          return false;
        }

        // Update symbols_to_find to include input symbols this operator depends on
        std::set<Symbol::Position_t> new_symbols_to_find = symbols_to_find;
        if (!extracted_symbols.empty()) {
          // We found expressions to analyze - use the extracted symbols
          // These are the input symbols the operator depends on
          for (const auto &sym_pos : extracted_symbols) {
            new_symbols_to_find.insert(sym_pos);
          }
        } else if (!input_symbol_positions.empty()) {
          // No expressions found - add all input symbols to continue tracing
          // For joins, this will be symbols from the left branch only
          for (const auto &sym_pos : input_symbol_positions) {
            new_symbols_to_find.insert(sym_pos);
          }
        }

        // Remove symbols that this operator produces (not passed through)
        // A symbol is produced if it's in operator's ModifiedSymbols but not in input's
        for (const auto &sym : operator_symbols) {
          // If symbol is in our search set and NOT in input, operator produces it
          if (symbols_to_find.find(sym.position()) != symbols_to_find.end() &&
              input_symbol_positions.find(sym.position()) == input_symbol_positions.end()) {
            // Operator produces this symbol - remove it from search
            new_symbols_to_find.erase(sym.position());
          }
        }

        symbols_to_find = new_symbols_to_find;
      }

      parent = current;

      if (current->HasSingleInput()) {
        current = current->input();
      } else {
        // For multi-input operators, check all branches
        std::shared_ptr<LogicalOperator> found_scan;
        std::shared_ptr<LogicalOperator> found_parent;

        if (auto *union_op = dynamic_cast<Union *>(current.get())) {
          if (union_op->left_op_ && FindScanForSymbols(union_op->left_op_, symbols_to_find, found_scan, found_parent)) {
            if (!target_scan || IsDeeperThan(found_scan, target_scan, start)) {
              target_scan = found_scan;
              scan_parent = found_parent ? found_parent : current;
            }
          }
          if (union_op->right_op_ &&
              FindScanForSymbols(union_op->right_op_, symbols_to_find, found_scan, found_parent)) {
            if (!target_scan || IsDeeperThan(found_scan, target_scan, start)) {
              target_scan = found_scan;
              scan_parent = found_parent ? found_parent : current;
            }
          }
        } else if (auto *cartesian_op = dynamic_cast<Cartesian *>(current.get())) {
          // For Cartesian, use the left branch to find the scan to parallelize
          if (cartesian_op->left_op_ &&
              FindScanForSymbols(cartesian_op->left_op_, symbols_to_find, found_scan, found_parent)) {
            if (!target_scan || IsDeeperThan(found_scan, target_scan, start)) {
              target_scan = found_scan;
              scan_parent = found_parent ? found_parent : current;
            }
          }
        } else if (auto *indexed_join_op = dynamic_cast<IndexedJoin *>(current.get())) {
          // For IndexedJoin, use the main_branch (left branch) to find the scan to parallelize
          if (indexed_join_op->main_branch_ &&
              FindScanForSymbols(indexed_join_op->main_branch_, symbols_to_find, found_scan, found_parent)) {
            if (!target_scan || IsDeeperThan(found_scan, target_scan, start)) {
              target_scan = found_scan;
              scan_parent = found_parent ? found_parent : current;
            }
          }
        } else if (auto *hash_join_op = dynamic_cast<HashJoin *>(current.get())) {
          // For HashJoin, use the left branch to find the scan to parallelize
          if (hash_join_op->left_op_ &&
              FindScanForSymbols(hash_join_op->left_op_, symbols_to_find, found_scan, found_parent)) {
            if (!target_scan || IsDeeperThan(found_scan, target_scan, start)) {
              target_scan = found_scan;
              scan_parent = found_parent ? found_parent : current;
            }
          }
        } else if (auto *rollup_apply_op = dynamic_cast<RollUpApply *>(current.get())) {
          if (rollup_apply_op->input_ &&
              FindScanForSymbols(rollup_apply_op->input_, symbols_to_find, found_scan, found_parent)) {
            if (!target_scan || IsDeeperThan(found_scan, target_scan, start)) {
              target_scan = found_scan;
              scan_parent = found_parent ? found_parent : current;
            }
          }
        } else if (dynamic_cast<Once *>(current.get()) != nullptr ||
                   dynamic_cast<OutputTable *>(current.get()) != nullptr ||
                   dynamic_cast<OutputTableStream *>(current.get()) != nullptr) {
          // Terminal operators with no inputs - cannot contain a scan
          // Stop traversal
        }

        // Stop traversal for multi-input operators (we've checked all branches)
        break;
      }
    }

    return target_scan != nullptr;
  }

  // Helper function to check if scan1 is deeper (closer to Once) than scan2
  bool IsDeeperThan(const std::shared_ptr<LogicalOperator> &scan1, const std::shared_ptr<LogicalOperator> &scan2,
                    const std::shared_ptr<LogicalOperator> &root) {
    // Count depth from root to each scan
    int depth1 = GetDepth(root, scan1);
    int depth2 = GetDepth(root, scan2);
    return depth1 > depth2;
  }

  // Helper function to get the depth of target from root
  int GetDepth(const std::shared_ptr<LogicalOperator> &root, const std::shared_ptr<LogicalOperator> &target) {
    if (!root || !target) {
      return -1;
    }
    if (root == target) {
      return 0;
    }

    if (root->HasSingleInput()) {
      int depth = GetDepth(root->input(), target);
      return depth >= 0 ? depth + 1 : -1;
    }

    // For multi-input operators, check all branches
    int max_depth = -1;
    if (auto *union_op = dynamic_cast<Union *>(root.get())) {
      if (union_op->left_op_) {
        int depth = GetDepth(union_op->left_op_, target);
        if (depth >= 0) {
          max_depth = std::max(max_depth, depth + 1);
        }
      }
      if (union_op->right_op_) {
        int depth = GetDepth(union_op->right_op_, target);
        if (depth >= 0) {
          max_depth = std::max(max_depth, depth + 1);
        }
      }
    } else if (auto *cartesian_op = dynamic_cast<Cartesian *>(root.get())) {
      if (cartesian_op->left_op_) {
        int depth = GetDepth(cartesian_op->left_op_, target);
        if (depth >= 0) {
          max_depth = std::max(max_depth, depth + 1);
        }
      }
      if (cartesian_op->right_op_) {
        int depth = GetDepth(cartesian_op->right_op_, target);
        if (depth >= 0) {
          max_depth = std::max(max_depth, depth + 1);
        }
      }
    } else if (auto *indexed_join_op = dynamic_cast<IndexedJoin *>(root.get())) {
      if (indexed_join_op->main_branch_) {
        int depth = GetDepth(indexed_join_op->main_branch_, target);
        if (depth >= 0) {
          max_depth = std::max(max_depth, depth + 1);
        }
      }
      if (indexed_join_op->sub_branch_) {
        int depth = GetDepth(indexed_join_op->sub_branch_, target);
        if (depth >= 0) {
          max_depth = std::max(max_depth, depth + 1);
        }
      }
    } else if (auto *hash_join_op = dynamic_cast<HashJoin *>(root.get())) {
      if (hash_join_op->left_op_) {
        int depth = GetDepth(hash_join_op->left_op_, target);
        if (depth >= 0) {
          max_depth = std::max(max_depth, depth + 1);
        }
      }
      if (hash_join_op->right_op_) {
        int depth = GetDepth(hash_join_op->right_op_, target);
        if (depth >= 0) {
          max_depth = std::max(max_depth, depth + 1);
        }
      }
    } else if (auto *rollup_apply_op = dynamic_cast<RollUpApply *>(root.get())) {
      if (rollup_apply_op->input_) {
        int depth = GetDepth(rollup_apply_op->input_, target);
        if (depth >= 0) {
          max_depth = std::max(max_depth, depth + 1);
        }
      }
    } else if (dynamic_cast<Once *>(root.get()) != nullptr || dynamic_cast<OutputTable *>(root.get()) != nullptr ||
               dynamic_cast<OutputTableStream *>(root.get()) != nullptr) {
      // Terminal operators with no inputs - cannot contain target
      return -1;
    }

    return max_depth;
  }

  // Helper function to replace a scan in the input chain when scan_parent is null
  // This happens when the scan is directly the input of Aggregate or the first operator
  bool ReplaceScanInChain(const std::shared_ptr<LogicalOperator> &current,
                          const std::shared_ptr<LogicalOperator> &target_scan,
                          const std::shared_ptr<LogicalOperator> &scan_chunk) {
    if (!current) {
      return false;
    }

    // Check if current is the target scan
    if (current.get() == target_scan.get()) {
      // This shouldn't happen if scan_parent is null, but handle it anyway
      return false;
    }

    // Check if current's input is the target scan
    if (current->HasSingleInput() && current->input().get() == target_scan.get()) {
      current->set_input(scan_chunk);
      return true;
    }

    // Recursively check the input chain
    if (current->HasSingleInput()) [[likely]] {
      // Recursively check the input chain
      return ReplaceScanInChain(current->input(), target_scan, scan_chunk);
    }
    // For multi-input operators, check only main branch
    if (auto *union_op = dynamic_cast<Union *>(current.get())) {
      if (union_op->left_op_ && union_op->left_op_.get() == target_scan.get()) {
        union_op->left_op_ = scan_chunk;
        return true;
      }
      if (ReplaceScanInChain(union_op->left_op_, target_scan, scan_chunk)) {
        return true;
      }
    } else if (auto *cartesian_op = dynamic_cast<Cartesian *>(current.get())) {
      if (cartesian_op->left_op_ && cartesian_op->left_op_.get() == target_scan.get()) {
        cartesian_op->left_op_ = scan_chunk;
        return true;
      }
      if (ReplaceScanInChain(cartesian_op->left_op_, target_scan, scan_chunk)) {
        return true;
      }
    } else if (auto *indexed_join_op = dynamic_cast<IndexedJoin *>(current.get())) {
      if (indexed_join_op->main_branch_ && indexed_join_op->main_branch_.get() == target_scan.get()) {
        indexed_join_op->main_branch_ = scan_chunk;
        return true;
      }
      if (ReplaceScanInChain(indexed_join_op->main_branch_, target_scan, scan_chunk)) {
        return true;
      }
    } else if (auto *hash_join_op = dynamic_cast<HashJoin *>(current.get())) {
      if (hash_join_op->left_op_ && hash_join_op->left_op_.get() == target_scan.get()) {
        hash_join_op->left_op_ = scan_chunk;
        return true;
      }
      if (ReplaceScanInChain(hash_join_op->left_op_, target_scan, scan_chunk)) {
        return true;
      }
    } else if (auto *rollup_apply_op = dynamic_cast<RollUpApply *>(current.get())) {
      if (rollup_apply_op->input_ && rollup_apply_op->input_.get() == target_scan.get()) {
        rollup_apply_op->input_ = scan_chunk;
        return true;
      }
      if (ReplaceScanInChain(rollup_apply_op->input_, target_scan, scan_chunk)) {
        return true;
      }
    } else if (dynamic_cast<Once *>(current.get()) != nullptr ||
               dynamic_cast<OutputTable *>(current.get()) != nullptr ||
               dynamic_cast<OutputTableStream *>(current.get()) != nullptr) {
      // Terminal operators with no inputs - cannot contain target_scan
      return false;
    }

    return false;
  }
};

}  // namespace impl

template <class TDbAccessor>
std::unique_ptr<LogicalOperator> RewriteParallelAggregate(std::unique_ptr<LogicalOperator> root_op,
                                                          SymbolTable *symbol_table, AstStorage *ast_storage,
                                                          TDbAccessor *db, size_t num_threads) {
  auto rewriter = impl::ParallelAggregateRewriter<TDbAccessor>{symbol_table, ast_storage, db, num_threads};
  root_op->Accept(rewriter);
  // If Aggregate was the root and we created a new root, clone it to get a unique_ptr
  if (rewriter.new_root_) {
    // Clone the root operator tree to convert from shared_ptr to unique_ptr
    return rewriter.new_root_->Clone(ast_storage);
  }
  return root_op;
}

}  // namespace memgraph::query::plan
