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

  // Operators that might be parents of Aggregate - need to track all single-input operators
  // and operators with branches that could contain Aggregate

#define DEFAULT_VISITS(op)         \
  bool PreVisit(op &op) override { \
    prev_ops_.push_back(&op);      \
    return true;                   \
  }                                \
  bool PostVisit(op &) override {  \
    prev_ops_.pop_back();          \
    return true;                   \
  }

  DEFAULT_VISITS(Accumulate)
  DEFAULT_VISITS(Produce)
  DEFAULT_VISITS(Limit)
  DEFAULT_VISITS(OrderBy)
  DEFAULT_VISITS(Skip)
  DEFAULT_VISITS(Filter)
  DEFAULT_VISITS(Distinct)
  DEFAULT_VISITS(Unwind)
  DEFAULT_VISITS(EmptyResult)
  DEFAULT_VISITS(Delete)
  DEFAULT_VISITS(SetProperty)
  DEFAULT_VISITS(SetProperties)
  DEFAULT_VISITS(SetLabels)
  DEFAULT_VISITS(RemoveProperty)
  DEFAULT_VISITS(RemoveLabels)
  DEFAULT_VISITS(EdgeUniquenessFilter)
  DEFAULT_VISITS(ConstructNamedPath)
  DEFAULT_VISITS(CallProcedure)
  DEFAULT_VISITS(EvaluatePatternFilter)
  DEFAULT_VISITS(LoadCsv)
  DEFAULT_VISITS(LoadParquet)
  DEFAULT_VISITS(PeriodicCommit)
  DEFAULT_VISITS(SetNestedProperty)
  DEFAULT_VISITS(RemoveNestedProperty)
  DEFAULT_VISITS(Expand)
  DEFAULT_VISITS(ExpandVariable)
  DEFAULT_VISITS(CreateNode)
  DEFAULT_VISITS(CreateExpand)
  DEFAULT_VISITS(ScanAll)
  DEFAULT_VISITS(ScanAllByLabel)
  DEFAULT_VISITS(ScanAllByLabelProperties)
  DEFAULT_VISITS(ScanAllById)
  DEFAULT_VISITS(ScanAllByEdge)
  DEFAULT_VISITS(ScanAllByEdgeType)
  DEFAULT_VISITS(ScanAllByEdgeTypeProperty)
  DEFAULT_VISITS(ScanAllByEdgeTypePropertyValue)
  DEFAULT_VISITS(ScanAllByEdgeTypePropertyRange)
  DEFAULT_VISITS(ScanAllByEdgeProperty)
  DEFAULT_VISITS(ScanAllByEdgePropertyValue)
  DEFAULT_VISITS(ScanAllByEdgePropertyRange)
  DEFAULT_VISITS(ScanAllByEdgeId)
  DEFAULT_VISITS(ScanAllByPointDistance)
  DEFAULT_VISITS(ScanAllByPointWithinbbox)
  DEFAULT_VISITS(ScanChunk)
  DEFAULT_VISITS(ScanChunkByEdge)
  DEFAULT_VISITS(ScanParallel)
  DEFAULT_VISITS(ScanParallelByLabel)
  DEFAULT_VISITS(ScanParallelByLabelProperties)
  DEFAULT_VISITS(ScanParallelByPointDistance)
  DEFAULT_VISITS(ScanParallelByWithinbbox)
  DEFAULT_VISITS(ScanParallelByEdge)
  DEFAULT_VISITS(ScanParallelByEdgeType)
  DEFAULT_VISITS(ScanParallelByEdgeTypeProperty)
  DEFAULT_VISITS(ScanParallelByEdgeTypePropertyValue)
  DEFAULT_VISITS(ScanParallelByEdgeTypePropertyRange)
  DEFAULT_VISITS(ScanParallelByEdgeProperty)
  DEFAULT_VISITS(ScanParallelByEdgePropertyValue)
  DEFAULT_VISITS(ScanParallelByEdgePropertyRange)
  DEFAULT_VISITS(ParallelMerge)
  DEFAULT_VISITS(AggregateParallel)

#undef DEFAULT_VISITS

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

  // Multi-input operators
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

  // Single threaded Aggregate (potentially parallelizable)
  bool PreVisit(Aggregate &op) override {
    auto failure = [&](std::string_view error_message = "") {
      spdlog::trace("Parallel aggregate rewrite failed: {}", error_message);
      // Failed to rewrite, continue searching for other opportunities to parallelize
      prev_ops_.push_back(&op);
      return true;
    };

    // Special case for DISTINCT operator - we don't support parallelizing DISTINCT operators
    // We will try to move the DISTINCT under the aggregation function
    // Rewrite MATCH(n) WITH DISTINCT n RETURN count(*) to MATCH(n) RETURN count(DISTINCT n)
    auto rewrite_distinct = [&](Aggregate &op) {
      // Find pair of Distinct and Produce operators
      if (auto *distinct_op = dynamic_cast<Distinct *>(op.input().get())) {
        auto *maybe_produce = dynamic_cast<Produce *>(distinct_op->input().get());
        if (!maybe_produce || maybe_produce->named_expressions_.size() != 1) return;
        // Support only if aggregation is the top operator
        if (prev_ops_.size() != 1) return;
        auto *parent = dynamic_cast<Produce *>(prev_ops_.front());
        if (!parent || parent->named_expressions_.size() != 1) return;
        // COUNT(*) is the only case where input expression is optional
        auto agg = op.aggregations_.begin();
        if (op.aggregations_.size() != 1 || agg->op != Aggregation::Op::COUNT) return;
        auto *named_expr = *maybe_produce->named_expressions_.begin();
        if (agg->arg1 != nullptr && agg->arg1 != named_expr->expression_) return;
        // TODO Make sure op is the top operator
        // Rewrite the query to MATCH(n) RETURN count(DISTINCT n)
        agg->distinct = true;
        agg->arg1 = named_expr->expression_;
        agg->arg2 = nullptr;
        // Remove the Produce and Distinct operators
        op.set_input(maybe_produce->input());
      }
    };
    rewrite_distinct(op);

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
    LogicalOperator *target_scan = nullptr;
    LogicalOperator *scan_parent = &op;
    if (!FindScanForSymbols(op.input().get(), required_symbols, target_scan, scan_parent)) {
      // No suitable Scan found, skip rewriting
      return failure("No suitable Scan found");
    }
    if (!target_scan || !scan_parent) {
      return failure("No target scan or scan parent found");
    }

    // Verify the path from Aggregate to Scan is read-only
    if (!IsPathReadOnly(&op, target_scan)) {
      return failure("Path from Aggregate to Scan is not read-only");
    }

    // Aggregate -> Scan -> etc is rewritten to AggregateParallel -> ScanChunk -> ParallelMerge -> ScanParallel -> etc
    auto scan_input = target_scan->input();
    auto state_symbol = symbol_table->CreateAnonymousSymbol();
    auto scan_parallel = CreateScanParallel(target_scan, scan_input, state_symbol);
    if (!scan_parallel) {
      return failure("Failed to create ScanParallel");
    }
    auto parallel_merge = std::make_shared<ParallelMerge>(scan_parallel);

    // scan_parent may be an operator without a single input. Check and handle appropriately.
    const auto &scan_type = target_scan->GetTypeInfo();
    std::shared_ptr<LogicalOperator> scan_chunk;
    // Check if this is an edge scan type
    if (utils::IsSubtype(scan_type, ScanAllByEdge::kType)) {
      auto *scan_edge = dynamic_cast<ScanAllByEdge *>(target_scan);
      MG_ASSERT(scan_edge, "Expected ScanAllByEdge or subtype");
      scan_chunk = std::make_shared<ScanChunkByEdge>(parallel_merge, scan_edge->common_.edge_symbol,
                                                     scan_edge->common_.node1_symbol, scan_edge->common_.node2_symbol,
                                                     scan_edge->common_.direction, scan_edge->common_.edge_types,
                                                     scan_edge->view_, state_symbol);
    } else {
      // Vertex scan - use ScanChunk
      auto *scan = dynamic_cast<ScanAll *>(target_scan);
      MG_ASSERT(scan, "Expected ScanAll or subtype");
      scan_chunk = std::make_shared<ScanChunk>(parallel_merge, scan->output_symbol_, scan->view_, state_symbol);
    }

    // Replace the scan with scan_chunk in the operator tree
    if (scan_parent->HasSingleInput()) {
      scan_parent->set_input(scan_chunk);
    } else {
      // Handle special case for multi-input operators
      // Try to set the correct child in multi-input operator (use only main/left branch)
      if (auto *union_op = dynamic_cast<Union *>(scan_parent)) {
        if (union_op->left_op_.get() == target_scan) {
          union_op->left_op_ = scan_chunk;
        } else {
          return failure("Target scan not found in left branch of Union");
        }
      } else if (auto *cartesian_op = dynamic_cast<Cartesian *>(scan_parent)) {
        if (cartesian_op->left_op_.get() == target_scan) {
          cartesian_op->left_op_ = scan_chunk;
        } else {
          return failure("Target scan not found in left branch of Cartesian");
        }
      } else if (auto *indexed_join_op = dynamic_cast<IndexedJoin *>(scan_parent)) {
        if (indexed_join_op->main_branch_.get() == target_scan) {
          indexed_join_op->main_branch_ = scan_chunk;
        } else {
          return failure("Target scan not found in main branch of IndexedJoin");
        }
      } else if (auto *hash_join_op = dynamic_cast<HashJoin *>(scan_parent)) {
        if (hash_join_op->left_op_.get() == target_scan) {
          hash_join_op->left_op_ = scan_chunk;
        } else {
          return failure("Target scan not found in left branch of HashJoin");
        }
      } else if (auto *rollup_apply_op = dynamic_cast<RollUpApply *>(scan_parent)) {
        if (rollup_apply_op->input_.get() == target_scan) {
          rollup_apply_op->input_ = scan_chunk;
        } else {
          return failure("Target scan not found in input branch of RollUpApply");
        }
        // Special case for operators that don't have any inputs
      } else if (dynamic_cast<Once *>(scan_parent) != nullptr) {
        return failure("Once operator cannot be a parent of a scan");
      } else if (dynamic_cast<OutputTable *>(scan_parent) != nullptr) {
        return failure("OutputTable operator cannot be a parent of a scan");
      } else if (dynamic_cast<OutputTableStream *>(scan_parent) != nullptr) {
        return failure("OutputTableStream operator cannot be a parent of a scan");
      } else {
        return failure("Unsupported operator in parallel chain" + scan_parent->ToString());
      }
    }

    // Create AggregateParallel with default num_threads
    if (!prev_ops_.back()->HasSingleInput()) {
      return failure("Expected single input operator");
    }
    auto parallel_agg = std::make_shared<AggregateParallel>(prev_ops_.back()->input() /*op */, num_threads_);

    // Switch the parent operator (if any) to use AggregateParallel instead of Aggregate
    // This makes: Parent -> AggregateParallel instead of Parent -> Aggregate
    // Note: Don't push Aggregate to prev_ops_ before calling SetOnParent, because
    // SetOnParent needs to access the parent (prev_ops_.back()), not Aggregate itself
    SetOnParent(parallel_agg);

    // Now push Aggregate to track it for any children it might have
    // TODO should this be the parallel version?
    prev_ops_.push_back(&op);
    return true;  // Continue visiting to handle nested aggregations
  }

  bool PostVisit(Aggregate &) override {
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
    if (!prev_ops_.back()->HasSingleInput()) {
      throw std::runtime_error("Expected single input operator");
    }
    prev_ops_.back()->set_input(input);
  }

  // Helper function to create the appropriate ScanParallel variant based on the scan type
  std::shared_ptr<ScanParallel> CreateScanParallel(LogicalOperator *scan_op, std::shared_ptr<LogicalOperator> input,
                                                   Symbol state_symbol) {
    const auto &scan_type = scan_op->GetTypeInfo();
    auto *scan_all = dynamic_cast<ScanAll *>(scan_op);
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
      auto *scan = dynamic_cast<ScanAllByLabel *>(scan_op);
      return std::make_shared<ScanParallelByLabel>(input, scan->view_, num_threads_, state_symbol, scan->label_);
    }
    if (scan_type == ScanAllByLabelProperties::kType) {
      auto *scan = dynamic_cast<ScanAllByLabelProperties *>(scan_op);
      return std::make_shared<ScanParallelByLabelProperties>(input, scan->view_, num_threads_, state_symbol,
                                                             scan->label_, scan->properties_, scan->expression_ranges_);
    }
    if (scan_type == ScanAllByPointDistance::kType) {
      auto *scan = dynamic_cast<ScanAllByPointDistance *>(scan_op);
      return std::make_shared<ScanParallelByPointDistance>(input, scan->view_, num_threads_, state_symbol, scan->label_,
                                                           scan->property_, scan->cmp_value_, scan->boundary_value_,
                                                           scan->boundary_condition_);
    }
    if (scan_type == ScanAllByPointWithinbbox::kType) {
      auto *scan = dynamic_cast<ScanAllByPointWithinbbox *>(scan_op);
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
      auto *scan = dynamic_cast<ScanAllByEdge *>(scan_op);
      return std::make_shared<ScanParallelByEdge>(input, scan->view_, num_threads_, state_symbol,
                                                  scan->common_.edge_symbol, scan->common_.node1_symbol,
                                                  scan->common_.node2_symbol, scan->common_.direction);
    }
    if (scan_type == ScanAllByEdgeType::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeType *>(scan_op);
      return std::make_shared<ScanParallelByEdgeType>(input, scan->view_, num_threads_, state_symbol,
                                                      scan->common_.edge_types[0]);
    }
    if (scan_type == ScanAllByEdgeTypeProperty::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeTypeProperty *>(scan_op);
      return std::make_shared<ScanParallelByEdgeTypeProperty>(input, scan->view_, num_threads_, state_symbol,
                                                              scan->common_.edge_types[0], scan->property_);
    }
    if (scan_type == ScanAllByEdgeTypePropertyValue::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeTypePropertyValue *>(scan_op);
      return std::make_shared<ScanParallelByEdgeTypePropertyValue>(input, scan->view_, num_threads_, state_symbol,
                                                                   scan->common_.edge_types[0], scan->property_,
                                                                   scan->expression_->Clone(ast_storage));
    }
    if (scan_type == ScanAllByEdgeTypePropertyRange::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeTypePropertyRange *>(scan_op);
      return std::make_shared<ScanParallelByEdgeTypePropertyRange>(input, scan->view_, num_threads_, state_symbol,
                                                                   scan->common_.edge_types[0], scan->property_,
                                                                   scan->lower_bound_, scan->upper_bound_);
    }
    if (scan_type == ScanAllByEdgeProperty::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeProperty *>(scan_op);
      return std::make_shared<ScanParallelByEdgeProperty>(input, scan->view_, num_threads_, state_symbol,
                                                          scan->property_);
    }
    if (scan_type == ScanAllByEdgePropertyValue::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgePropertyValue *>(scan_op);
      return std::make_shared<ScanParallelByEdgePropertyValue>(input, scan->view_, num_threads_, state_symbol,
                                                               scan->property_, scan->expression_);
    }
    if (scan_type == ScanAllByEdgePropertyRange::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgePropertyRange *>(scan_op);
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
  std::vector<Symbol> GetScanOutputSymbols(LogicalOperator *scan) {
    std::vector<Symbol> symbols;

    if (utils::IsSubtype(*scan, ScanAllByEdge::kType)) {
      auto *scan_edge = dynamic_cast<ScanAllByEdge *>(scan);
      if (scan_edge != nullptr) {
        symbols.push_back(scan_edge->common_.edge_symbol);
        symbols.push_back(scan_edge->common_.node1_symbol);
        symbols.push_back(scan_edge->common_.node2_symbol);
        // Also include the output_symbol_ from the base ScanAll
        symbols.push_back(scan_edge->output_symbol_);
      }
    } else if (utils::IsSubtype(*scan, ScanAll::kType)) {
      auto *scan_all = dynamic_cast<ScanAll *>(scan);
      if (scan_all != nullptr) {
        symbols.push_back(scan_all->output_symbol_);
      }
    }

    return symbols;
  }

  // Helper function to check if a Scan produces any of the required symbols
  size_t ScanProducesSymbols(LogicalOperator *scan, const std::set<Symbol::Position_t> &required_symbols) {
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
  std::set<Symbol::Position_t> ExtractInputSymbolsFromOperator(LogicalOperator *op) {
    std::set<Symbol::Position_t> input_symbols;
    DependantSymbolVisitor symbol_visitor(input_symbols);

    // Try to extract symbols from operator's expressions
    // Different operators have expressions in different places

    // Produce: named_expressions
    if (auto *produce_op = dynamic_cast<Produce *>(op)) {
      for (auto *named_expr : produce_op->named_expressions_) {
        if (named_expr != nullptr && named_expr->expression_ != nullptr) {
          named_expr->expression_->Accept(symbol_visitor);
        }
      }
      return input_symbols;
    }

    // Filter: expression and all_filters
    if (auto *filter_op = dynamic_cast<Filter *>(op)) {
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

  // Helper function to find the Scan operator that produces the required symbols
  // This traces symbol dependencies backwards through operators
  // Returns true if a suitable Scan is found, false otherwise
  bool FindScanForSymbols(LogicalOperator *start, const std::set<Symbol::Position_t> &required_symbols,
                          LogicalOperator *&target_scan, LogicalOperator *&scan_parent) {
    if (!start || !scan_parent) {
      return false;
    }

    target_scan = nullptr;

    LogicalOperator *parent = scan_parent;
    LogicalOperator *current = start;
    std::set<Symbol::Position_t> symbols_to_find = required_symbols;

    // Traverse down the input chain, tracing symbol dependencies backwards
    while (current && current->GetTypeInfo() != Once::kType) {
      // If we encounter another Aggregate or AggregateParallel, abandon the current one
      // The nested Aggregate will be processed when we visit it
      // This prevents trying to parallelize outer Aggregates that depend on inner ones
      const auto current_type = current->GetTypeInfo();
      if (current_type == Aggregate::kType || current_type == AggregateParallel::kType) {
        return target_scan != nullptr;
      }
      // Unsupported operators
      if (current_type == Distinct::kType) {
        spdlog::info(
            "Query has unsupported parallel DISTINCT operator. Try rewriting the query to move DISTINCT under the "
            "aggregation function.");
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
        if (operator_symbol_positions.contains(sym_pos)) {
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
        } else if (auto *cartesian_op = dynamic_cast<Cartesian *>(current)) {
          if (cartesian_op->left_op_) {
            auto left_symbols = cartesian_op->left_op_->ModifiedSymbols(*symbol_table);
            for (const auto &sym : left_symbols) {
              input_symbol_positions.insert(sym.position());
            }
          }
        } else if (auto *hash_join_op = dynamic_cast<HashJoin *>(current)) {
          if (hash_join_op->left_op_) {
            auto left_symbols = hash_join_op->left_op_->ModifiedSymbols(*symbol_table);
            for (const auto &sym : left_symbols) {
              input_symbol_positions.insert(sym.position());
            }
          }
        } else if (auto *indexed_join_op = dynamic_cast<IndexedJoin *>(current)) {
          if (indexed_join_op->main_branch_) {
            auto main_symbols = indexed_join_op->main_branch_->ModifiedSymbols(*symbol_table);
            for (const auto &sym : main_symbols) {
              input_symbol_positions.insert(sym.position());
            }
          }
        } else if (auto *rollup_apply_op = dynamic_cast<RollUpApply *>(current)) {
          if (rollup_apply_op->input_) {
            auto input_symbols = rollup_apply_op->input_->ModifiedSymbols(*symbol_table);
            for (const auto &sym : input_symbols) {
              input_symbol_positions.insert(sym.position());
            }
          }
        } else if (dynamic_cast<Once *>(current) != nullptr || dynamic_cast<OutputTable *>(current) != nullptr ||
                   dynamic_cast<OutputTableStream *>(current) != nullptr) {
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
          if (symbols_to_find.contains(sym.position()) && !input_symbol_positions.contains(sym.position())) {
            // Operator produces this symbol - remove it from search
            new_symbols_to_find.erase(sym.position());
          }
        }

        symbols_to_find = new_symbols_to_find;
      }

      parent = current;

      if (current->HasSingleInput()) {
        current = current->input().get();
      } else {
        // For multi-input operators, check only main/left branches
        LogicalOperator *found_scan = nullptr;
        LogicalOperator *found_parent = current;
        if (auto *union_op = dynamic_cast<Union *>(current)) {
          if (FindScanForSymbols(union_op->left_op_.get(), symbols_to_find, found_scan, found_parent)) {
            target_scan = found_scan;
            scan_parent = found_parent;
          }
        } else if (auto *cartesian_op = dynamic_cast<Cartesian *>(current)) {
          if (FindScanForSymbols(cartesian_op->left_op_.get(), symbols_to_find, found_scan, found_parent)) {
            target_scan = found_scan;
            scan_parent = found_parent;
          }
        } else if (auto *indexed_join_op = dynamic_cast<IndexedJoin *>(current)) {
          if (FindScanForSymbols(indexed_join_op->main_branch_.get(), symbols_to_find, found_scan, found_parent)) {
            target_scan = found_scan;
            scan_parent = found_parent;
          }
        } else if (auto *hash_join_op = dynamic_cast<HashJoin *>(current)) {
          if (FindScanForSymbols(hash_join_op->left_op_.get(), symbols_to_find, found_scan, found_parent)) {
            target_scan = found_scan;
            scan_parent = found_parent;
          }
        } else if (auto *rollup_apply_op = dynamic_cast<RollUpApply *>(current)) {
          if (FindScanForSymbols(rollup_apply_op->input_.get(), symbols_to_find, found_scan, found_parent)) {
            target_scan = found_scan;
            scan_parent = found_parent;
          }
        } else if (dynamic_cast<Once *>(current) != nullptr || dynamic_cast<OutputTable *>(current) != nullptr ||
                   dynamic_cast<OutputTableStream *>(current) != nullptr) {
          // Terminal operators with no inputs - cannot contain a scan
          // Stop traversal
          break;
        }
        // Multi-input operators have already been checked
        // Stop traversal
        break;
      }
    }

    return target_scan != nullptr;
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
