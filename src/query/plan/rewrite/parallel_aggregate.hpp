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
#include <vector>
#include "query/plan/operator.hpp"
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
    op.input()->Accept(*this);
    // Note: merge_match_ branch could also contain Aggregate, but we handle the input branch here
    return false;
  }
  bool PostVisit(Merge &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Optional &op) override {
    prev_ops_.push_back(&op);
    op.input()->Accept(*this);
    // Note: optional_ branch could also contain Aggregate, but we handle the input branch here
    return false;
  }
  bool PostVisit(Optional &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Foreach &op) override {
    prev_ops_.push_back(&op);
    op.input()->Accept(*this);
    // Note: update_clauses_ branch could also contain Aggregate
    return false;
  }
  bool PostVisit(Foreach &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Apply &op) override {
    prev_ops_.push_back(&op);
    op.input()->Accept(*this);
    // Note: subquery_ branch could also contain Aggregate
    return false;
  }
  bool PostVisit(Apply &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(RollUpApply &op) override {
    prev_ops_.push_back(&op);
    op.input()->Accept(*this);
    // Note: list_collection_branch_ could also contain Aggregate
    return false;
  }
  bool PostVisit(RollUpApply &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(PeriodicSubquery &op) override {
    prev_ops_.push_back(&op);
    op.input()->Accept(*this);
    // Note: subquery_ branch could also contain Aggregate
    return false;
  }
  bool PostVisit(PeriodicSubquery &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(Aggregate &op) override {
    // Only process the first Aggregate we encounter
    if (aggregate_processed_) {
      return false;
    }
    aggregate_processed_ = true;

    // TODO This does not work
    // We need to find the last Scan operator that produces the symbols we need to aggregate on

    // Find the last Scan operator in the input branch
    std::shared_ptr<LogicalOperator> last_scan;
    std::shared_ptr<LogicalOperator> scan_parent;
    FindLastScan(prev_ops_.back()->input() /* op */, last_scan, scan_parent);
    if (!last_scan || last_scan->GetTypeInfo() != ScanAll::kType) {
      // No Scan operator found, skip rewriting
      aggregate_processed_ = false;  // TODO Check if this is correct
      prev_ops_.push_back(&op);
      return true;
    }

    // Create AggregateParallel operator
    // post_scan_input is everything after the last Scan
    // agg_inputs is the entire input branch of Aggregate
    // TODO: last_scan->set_input(nullptr); // Set Once at the end of the parallel scan branch

    // TODO Scan type based on last scan operator
    auto post_scan_input = last_scan->input();
    // TODO generic
    auto *scan = dynamic_cast<ScanAll *>(last_scan.get());
    auto state_symbol = symbol_table->CreateAnonymousSymbol();
    auto scan_input = std::make_shared<ScanParallel>(post_scan_input, scan->view_, num_threads_, state_symbol);
    auto parallel_merge = std::make_shared<ParallelMerge>(scan_input);
    // scan_parent may be an operator without a single input. Check and handle appropriately.
    auto scan_chunk = std::make_shared<ScanChunk>(parallel_merge, scan->output_symbol_, scan->view_, state_symbol);
    if (scan_parent) {
      if (scan_parent->HasSingleInput()) {
        scan_parent->set_input(scan_chunk);
      } else {
        // NOTE We just go down the left branch.
        // TODO The whole branch need to be checked, not just the firs op
        // We would also need to always for parallelization on the left branch.
        // Handle special case for Union and Cartesian
        // Try to set the correct child in multi-input operator
        // Currently handle Union and Cartesian
        if (auto *union_op = dynamic_cast<Union *>(scan_parent.get())) {
          // Check which child is last_scan
          if (union_op->left_op_ && union_op->left_op_.get() == last_scan.get()) {
            union_op->left_op_ = scan_chunk;
          } else {
            throw 1;  // TODO
          }
        } else if (auto *cartesian_op = dynamic_cast<Cartesian *>(scan_parent.get())) {
          if (cartesian_op->left_op_ && cartesian_op->left_op_.get() == last_scan.get()) {
            cartesian_op->left_op_ = scan_chunk;
          } else {
            throw 1;  // TODO
          }
        } else if (auto *indexed_join_op = dynamic_cast<IndexedJoin *>(scan_parent.get())) {
          if (indexed_join_op->main_branch_ && indexed_join_op->main_branch_.get() == last_scan.get()) {
            indexed_join_op->main_branch_ = scan_chunk;
          } else {
            throw 1;  // TODO
          }
        } else if (auto *hash_join_op = dynamic_cast<HashJoin *>(scan_parent.get())) {
          if (hash_join_op->left_op_ && hash_join_op->left_op_.get() == last_scan.get()) {
            hash_join_op->left_op_ = scan_chunk;
          } else {
            throw 1;  // TODO
          }
        } else if (auto *rollup_apply_op = dynamic_cast<RollUpApply *>(scan_parent.get())) {
          if (rollup_apply_op->input_ && rollup_apply_op->input_.get() == last_scan.get()) {
            rollup_apply_op->input_ = scan_chunk;
          } else {
            throw 1;  // TODO
          }
        } else {
          throw 1;  // TODO
        }
        // TODO Handle other multi-input operators
      }
    }

    // Create AggregateParallel with default num_threads
    auto parallel_agg = std::make_shared<AggregateParallel>(prev_ops_.back()->input(),  // op
                                                            num_threads_);

    // Switch the parent operator (if any) to use AggregateParallel instead of Aggregate
    // This makes: Parent -> AggregateParallel instead of Parent -> Aggregate
    // Note: Don't push Aggregate to prev_ops_ before calling SetOnParent, because
    // SetOnParent needs to access the parent (prev_ops_.back()), not Aggregate itself
    SetOnParent(parallel_agg);

    // Now push Aggregate to track it for any children it might have
    prev_ops_.push_back(&op);

    return false;  // Only a single rewrite is supported
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
    op.left_op_->Accept(*this);
    op.right_op_->Accept(*this);
    return false;
  }
  bool PostVisit(Cartesian &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(HashJoin &op) override {
    prev_ops_.push_back(&op);
    op.left_op_->Accept(*this);
    op.right_op_->Accept(*this);
    return false;
  }
  bool PostVisit(HashJoin &) override {
    prev_ops_.pop_back();
    return true;
  }

  bool PreVisit(IndexedJoin &op) override {
    prev_ops_.push_back(&op);
    op.main_branch_->Accept(*this);
    op.sub_branch_->Accept(*this);
    return false;
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

  std::shared_ptr<LogicalOperator> new_root_;

 private:
  SymbolTable *symbol_table;
  AstStorage *ast_storage;
  TDbAccessor *db;
  bool aggregate_processed_{false};
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
  // Helper function to find the last Scan operator in a branch
  // This traverses down the input chain until it finds the last Scan operator. This way (the idea is) we parallelize
  // most elements.
  // TODO Using the Last or First scan has similar results. Ideally you would parallelize the scan with the most
  // elements.
  void FindLastScan(std::shared_ptr<LogicalOperator> op, std::shared_ptr<LogicalOperator> &last_scan,
                    std::shared_ptr<LogicalOperator> &scan_parent) {
    // TODO Rewrite to a loop
    auto parent = op;
    // std::cout << "FindLastScan" << std::endl;
    while (op && op->GetTypeInfo() != Once::kType) {
      // std::cout << "op: " << op->ToString() << std::endl;
      if (utils::IsSubtype(*op, ScanAll::kType)) {
        last_scan = op;
        scan_parent = parent;
      }
      parent = op;
      // TODO Handle joins (parallelize a singel scan branch)

      if (op->HasSingleInput()) {
        op = op->input();
        continue;
      }

      // Cartesian is a special case, we need to parallelize the right branch
      if (op->GetTypeInfo() == Cartesian::kType) {
        auto cartesian = std::dynamic_pointer_cast<Cartesian>(op);
        op = cartesian->left_op_;
      } else if (op->GetTypeInfo() == plan::IndexedJoin::kType) {
        auto indexed_join = std::dynamic_pointer_cast<plan::IndexedJoin>(op);
        op = indexed_join->main_branch_;
      } else if (op->GetTypeInfo() == plan::HashJoin::kType) {
        auto hash_join = std::dynamic_pointer_cast<plan::HashJoin>(op);
        op = hash_join->left_op_;
      } else if (op->GetTypeInfo() == plan::Union::kType) {
        auto union_ = std::dynamic_pointer_cast<plan::Union>(op);
        op = union_->left_op_;
      } else if (op->GetTypeInfo() == plan::RollUpApply::kType) {
        auto rollup = std::dynamic_pointer_cast<plan::RollUpApply>(op);
        op = rollup->input_;
      } else {
        throw 1;  // TODO
      }
    }
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
