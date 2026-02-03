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

#pragma once

#include "license/license.hpp"
#ifdef MG_ENTERPRISE

#include <algorithm>
#include <memory>
#include <set>
#include <vector>
#include "context.hpp"
#include "flags/bolt.hpp"
#include "interpret/eval.hpp"
#include "query/dependant_symbol_visitor.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/read_write_type_checker.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query::plan {

namespace impl {

template <class TDbAccessor>
class ParallelRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  ParallelRewriter(SymbolTable *symbolTable, AstStorage *astStorage, TDbAccessor *db, size_t num_threads)
      : symbol_table(symbolTable), ast_storage(astStorage), db(db), num_threads_(num_threads) {}

  std::unique_ptr<LogicalOperator> Rewrite(std::unique_ptr<LogicalOperator> root) {
    root_ = std::move(root);
    root_->Accept(*this);
    return std::move(root_);
  }

  ~ParallelRewriter() override = default;

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
  DEFAULT_VISITS(Apply)
  DEFAULT_VISITS(Produce)
  DEFAULT_VISITS(Merge)
  DEFAULT_VISITS(Optional)
  DEFAULT_VISITS(Foreach)
  DEFAULT_VISITS(RollUpApply)
  DEFAULT_VISITS(PeriodicSubquery)
  DEFAULT_VISITS(Union)
  DEFAULT_VISITS(Cartesian)
  DEFAULT_VISITS(IndexedJoin)
  DEFAULT_VISITS(HashJoin)
  DEFAULT_VISITS(Limit)
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

  // Single threaded Aggregate (potentially parallelizable)
  bool PreVisit(Aggregate &op) override {
    // Special case for DISTINCT operator - we don't support parallelizing DISTINCT operators
    // We will try to move the DISTINCT under the aggregation function
    // Rewrite MATCH(n) WITH DISTINCT n RETURN count(*) to MATCH(n) RETURN count(DISTINCT n)
    auto rewrite_distinct = [&](Aggregate &op) {
      auto expressions_same_symbol = [](Expression *e1, const NamedExpression *e2) -> bool {
        auto *id1 = dynamic_cast<Identifier *>(e1);
        return id1 && id1->symbol_pos_ == e2->symbol_pos_;
      };
      if (op.aggregations_.size() != 1) return;  // Support only single aggregation
      auto &agg = op.aggregations_.front();
      // Find pair of Distinct and Produce operators
      if (auto *distinct_op = dynamic_cast<Distinct *>(op.input().get())) {
        auto *maybe_produce = dynamic_cast<Produce *>(distinct_op->input().get());
        if (!maybe_produce || maybe_produce->named_expressions_.size() != 1) return;
        // Support only if aggregation is the top operator
        if (prev_ops_.size() != 1) return;
        auto *parent = dynamic_cast<Produce *>(prev_ops_.front());
        if (!parent || parent->named_expressions_.size() != 1) return;
        auto *named_expr = maybe_produce->named_expressions_.front();
        if (agg.arg1 != nullptr && !expressions_same_symbol(agg.arg1, named_expr)) return;
        // Rewrite the query to MATCH(n) RETURN count(DISTINCT n)
        agg.distinct = true;
        agg.arg1 = named_expr->expression_;
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

    // Use the generic parallel rewrite logic
    auto create_parallel = [this](auto op) {
      using OpType = std::decay_t<decltype(op)>;
      if constexpr (std::is_same_v<OpType, std::unique_ptr<LogicalOperator>>) {
        return std::make_unique<AggregateParallel>(std::shared_ptr<LogicalOperator>(std::move(op)), num_threads_);
      } else {
        return std::make_shared<AggregateParallel>(std::move(op), num_threads_);
      }
    };
    return TryParallelizeOperator(op, required_symbols, "aggregate", create_parallel);
  }

  bool PostVisit(Aggregate &) override {
    prev_ops_.pop_back();
    return true;
  }

  // OrderBy (potentially parallelizable)
  bool PreVisit(OrderBy &op) override {
    // Collect symbols needed by this OrderBy (from order_by expressions and output_symbols)
    std::set<Symbol::Position_t> required_symbols;
    DependantSymbolVisitor symbol_visitor(required_symbols);

    // Collect symbols from order_by expressions
    for (auto *order_by_expr : op.order_by_) {
      if (order_by_expr != nullptr) {
        order_by_expr->Accept(symbol_visitor);
      }
    }

    // Collect symbols from output_symbols
    for (const auto &output_sym : op.output_symbols_) {
      required_symbols.insert(output_sym.position());
    }

    // Use the generic parallel rewrite logic
    auto create_parallel = [this](auto op) {
      if (auto *orderby = dynamic_cast<OrderBy *>(op.get())) {
        orderby->set_parallel_execution(true);
      }
      using OpType = std::decay_t<decltype(op)>;
      if constexpr (std::is_same_v<OpType, std::unique_ptr<LogicalOperator>>) {
        return std::make_unique<OrderByParallel>(std::shared_ptr<LogicalOperator>(std::move(op)), num_threads_);
      } else {
        return std::make_shared<OrderByParallel>(std::move(op), num_threads_);
      }
    };
    return TryParallelizeOperator(op, required_symbols, "orderby", create_parallel);
  }

  bool PostVisit(OrderBy &) override {
    prev_ops_.pop_back();
    return true;
  }

 private:
  std::unique_ptr<LogicalOperator> root_;
  SymbolTable *symbol_table;
  AstStorage *ast_storage;
  TDbAccessor *db;
  std::vector<LogicalOperator *> prev_ops_;
  size_t num_threads_;

  /**
   * Generic function to parallelize an operator.
   * @param op The operator to parallelize
   * @param required_symbols The symbols required by the operator
   * @param op_name The name of the operator (for logging)
   * @param create_parallel_op Factory function to create the parallel operator
   * @return true to continue visiting, false to stop
   */
  template <typename TOperator, typename TParallelFactory>
  bool TryParallelizeOperator(TOperator &op, const std::set<Symbol::Position_t> &required_symbols,
                              std::string_view op_name, TParallelFactory &&create_parallel_op) {
    auto failure = [&](std::string_view error_message = "") {
      spdlog::trace("Parallel {} rewrite failed: {}", op_name, error_message);
      // Failed to rewrite, continue searching for other opportunities to parallelize
      prev_ops_.push_back(&op);
      return true;
    };

    // Find the Scan operator that produces the required symbols
    LogicalOperator *target_scan = nullptr;
    LogicalOperator *scan_parent = &op;
    std::vector<LogicalOperator *> update_ops;
    if (!FindScanForSymbols(op.input().get(), required_symbols, target_scan, scan_parent, update_ops)) {
      // No suitable Scan found, skip rewriting
      return failure("No suitable Scan found");
    }
    if (!target_scan || !scan_parent) {
      return failure("No target scan or scan parent found");
    }

    // Operator -> Scan -> etc is rewritten to
    // OperatorParallel -> Operator -> ScanChunk -> ParallelMerge -> ScanParallel -> etc
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
      scan_chunk = std::make_shared<ScanChunkByEdge>(parallel_merge,
                                                     scan_edge->common_.edge_symbol,
                                                     scan_edge->common_.node1_symbol,
                                                     scan_edge->common_.node2_symbol,
                                                     scan_edge->common_.direction,
                                                     scan_edge->common_.edge_types,
                                                     scan_edge->view_,
                                                     state_symbol);
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

    // Create and insert the parallel operator
    InsertParallelOperator(&op, std::forward<decltype(create_parallel_op)>(create_parallel_op));

    // Success, update operators
    for (auto *update_op : update_ops) {
      if (auto *skip_op = dynamic_cast<Skip *>(update_op)) {
        skip_op->parallel_execution_.emplace(num_threads_);
      } else if (auto *limit_op = dynamic_cast<Limit *>(update_op)) {
        limit_op->parallel_execution_.emplace(num_threads_);
      } else if (auto *distinct_op = dynamic_cast<Distinct *>(update_op)) {
        distinct_op->parallel_execution_.emplace(num_threads_);
      } else {
        return failure("Unsupported operator in parallel chain " + update_op->ToString());
      }
    }

    // Now push operator to track it for any children it might have
    prev_ops_.push_back(&op);
    return true;  // Continue visiting to handle nested operations
  }

  void InsertParallelOperator(LogicalOperator *original_op, auto &&create_parallel_op) {
    if (prev_ops_.empty()) {
      // No prev ops, means this is the first operator in the chain
      // Set root to the new operator and pass in the input
      if (!root_) {
        throw std::runtime_error("Single input operator expected");
      }
      root_ = create_parallel_op(std::move(root_));
      return;
    }
    // Not the first operator, means we need to find the parent, pass in its current input and set the input to the
    // parallel operator
    auto *current = prev_ops_.back();
    if (current->HasSingleInput()) {
      // Special case for single input operators that have embedded queries
      if (auto *periodic_subquery_op = dynamic_cast<PeriodicSubquery *>(current)) {
        if (periodic_subquery_op->subquery_.get() == original_op) {
          periodic_subquery_op->subquery_ = create_parallel_op(periodic_subquery_op->subquery_);
          return;
        }
      } else if (auto *apply_op = dynamic_cast<Apply *>(current)) {
        if (apply_op->subquery_.get() == original_op) {
          apply_op->subquery_ = create_parallel_op(apply_op->subquery_);
          return;
        }
      } else if (auto *optional_op = dynamic_cast<Optional *>(current)) {
        if (optional_op->optional_.get() == original_op) {
          optional_op->optional_ = create_parallel_op(optional_op->optional_);
          return;
        }
      } else if (auto *unwind_op = dynamic_cast<Unwind *>(current)) {
        if (unwind_op->input_.get() == original_op) {
          unwind_op->input_ = create_parallel_op(unwind_op->input_);
          return;
        }
      }
      // Main input
      current->set_input(create_parallel_op(current->input()));
    } else {
      if (!original_op) {
        throw std::runtime_error("Single input operator expected");
      }
      if (auto *cartesian_op = dynamic_cast<Cartesian *>(current)) {
        if (cartesian_op->left_op_.get() == original_op) {
          cartesian_op->left_op_ = create_parallel_op(cartesian_op->left_op_);
        } else if (cartesian_op->right_op_.get() == original_op) {
          cartesian_op->right_op_ = create_parallel_op(cartesian_op->right_op_);
        } else {
          throw std::runtime_error("Unsupported operator in operator chain: " + current->ToString());
        }
      } else if (auto *hash_join_op = dynamic_cast<HashJoin *>(current)) {
        if (hash_join_op->left_op_.get() == original_op) {
          hash_join_op->left_op_ = create_parallel_op(hash_join_op->left_op_);
        } else if (hash_join_op->right_op_.get() == original_op) {
          hash_join_op->right_op_ = create_parallel_op(hash_join_op->right_op_);
        } else {
          throw std::runtime_error("Unsupported operator in operator chain: " + current->ToString());
        }
      } else if (auto *indexed_join_op = dynamic_cast<IndexedJoin *>(current)) {
        if (indexed_join_op->main_branch_.get() == original_op) {
          indexed_join_op->main_branch_ = create_parallel_op(indexed_join_op->main_branch_);
        } else if (indexed_join_op->sub_branch_.get() == original_op) {
          indexed_join_op->sub_branch_ = create_parallel_op(indexed_join_op->sub_branch_);
        } else {
          throw std::runtime_error("Unsupported operator in operator chain: " + current->ToString());
        }
      } else if (auto *rollup_apply_op = dynamic_cast<RollUpApply *>(current)) {
        if (rollup_apply_op->input_.get() == original_op) {
          rollup_apply_op->input_ = create_parallel_op(rollup_apply_op->input_);
        } else if (rollup_apply_op->list_collection_branch_.get() == original_op) {
          rollup_apply_op->list_collection_branch_ = create_parallel_op(rollup_apply_op->list_collection_branch_);
        } else {
          throw std::runtime_error("Unsupported operator in operator chain: " + current->ToString());
        }
      } else {
        throw std::runtime_error("Unsupported operator in operator chain: " + current->ToString());
      }
    }
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
      return std::make_shared<ScanParallelByLabelProperties>(
          input, scan->view_, num_threads_, state_symbol, scan->label_, scan->properties_, scan->expression_ranges_);
    }
    if (scan_type == ScanAllByPointDistance::kType) {
      // Not supported at the moment
      return nullptr;
    }
    if (scan_type == ScanAllByPointWithinbbox::kType) {
      // Not supported at the moment
      return nullptr;
    }

    // Handle edge scan variants
    if (scan_type == ScanAllByEdgeId::kType) {
      // ScanAllByEdgeId returns a single edge, so we can't parallelize it
      return nullptr;
    }
    if (scan_type == ScanAllByEdge::kType) {
      auto *scan = dynamic_cast<ScanAllByEdge *>(scan_op);
      return std::make_shared<ScanParallelByEdge>(input,
                                                  scan->view_,
                                                  num_threads_,
                                                  state_symbol,
                                                  scan->common_.edge_symbol,
                                                  scan->common_.node1_symbol,
                                                  scan->common_.node2_symbol,
                                                  scan->common_.direction);
    }
    if (scan_type == ScanAllByEdgeType::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeType *>(scan_op);
      return std::make_shared<ScanParallelByEdgeType>(
          input, scan->view_, num_threads_, state_symbol, scan->common_.edge_types[0]);
    }
    if (scan_type == ScanAllByEdgeTypeProperty::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeTypeProperty *>(scan_op);
      return std::make_shared<ScanParallelByEdgeTypeProperty>(
          input, scan->view_, num_threads_, state_symbol, scan->common_.edge_types[0], scan->property_);
    }
    if (scan_type == ScanAllByEdgeTypePropertyValue::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeTypePropertyValue *>(scan_op);
      return std::make_shared<ScanParallelByEdgeTypePropertyValue>(input,
                                                                   scan->view_,
                                                                   num_threads_,
                                                                   state_symbol,
                                                                   scan->common_.edge_types[0],
                                                                   scan->property_,
                                                                   scan->expression_->Clone(ast_storage));
    }
    if (scan_type == ScanAllByEdgeTypePropertyRange::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeTypePropertyRange *>(scan_op);
      return std::make_shared<ScanParallelByEdgeTypePropertyRange>(input,
                                                                   scan->view_,
                                                                   num_threads_,
                                                                   state_symbol,
                                                                   scan->common_.edge_types[0],
                                                                   scan->property_,
                                                                   scan->lower_bound_,
                                                                   scan->upper_bound_);
    }
    if (scan_type == ScanAllByEdgeProperty::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgeProperty *>(scan_op);
      return std::make_shared<ScanParallelByEdgeProperty>(
          input, scan->view_, num_threads_, state_symbol, scan->property_);
    }
    if (scan_type == ScanAllByEdgePropertyValue::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgePropertyValue *>(scan_op);
      return std::make_shared<ScanParallelByEdgePropertyValue>(
          input, scan->view_, num_threads_, state_symbol, scan->property_, scan->expression_);
    }
    if (scan_type == ScanAllByEdgePropertyRange::kType) {
      auto *scan = dynamic_cast<ScanAllByEdgePropertyRange *>(scan_op);
      return std::make_shared<ScanParallelByEdgePropertyRange>(
          input, scan->view_, num_threads_, state_symbol, scan->property_, scan->lower_bound_, scan->upper_bound_);
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

  static constexpr auto update_types = std::array{Skip::kType, Limit::kType, Distinct::kType};
  static constexpr auto conflicting_types =
      std::array{Aggregate::kType, AggregateParallel::kType, OrderBy::kType, OrderByParallel::kType};

  bool ConflictingOperators(LogicalOperator *start) {
    if (!start) {
      return false;
    }
    auto *current = start;
    // Traverse down the input chain, tracing symbol dependencies backwards
    while (current) {
      if (std::ranges::find(conflicting_types, current->GetTypeInfo()) != conflicting_types.end()) {
        return true;
      }
      if (current->HasSingleInput()) {
        // Single input operators - but some have additional branches that need checking
        if (auto *periodic_subquery_op = dynamic_cast<PeriodicSubquery *>(current)) {
          if (ConflictingOperators(periodic_subquery_op->subquery_.get())) {
            return true;
          }
        } else if (auto *apply_op = dynamic_cast<Apply *>(current)) {
          if (ConflictingOperators(apply_op->subquery_.get())) {
            return true;
          }
        } else if (auto *optional_op = dynamic_cast<Optional *>(current)) {
          if (ConflictingOperators(optional_op->optional_.get())) {
            return true;
          }
        } else if (auto *filter_op = dynamic_cast<Filter *>(current)) {
          for (const auto &pf : filter_op->pattern_filters_) {
            if (ConflictingOperators(pf.get())) {
              return true;
            }
          }
        }
        // Continue down the main input branch
        current = current->input().get();
      } else if (auto *cartesian_op = dynamic_cast<Cartesian *>(current)) {
        if (ConflictingOperators(cartesian_op->left_op_.get()) || ConflictingOperators(cartesian_op->right_op_.get())) {
          return true;
        }
        break;
      } else if (auto *hash_join_op = dynamic_cast<HashJoin *>(current)) {
        if (ConflictingOperators(hash_join_op->left_op_.get()) || ConflictingOperators(hash_join_op->right_op_.get())) {
          return true;
        }
        break;
      } else if (auto *indexed_join_op = dynamic_cast<IndexedJoin *>(current)) {
        if (ConflictingOperators(indexed_join_op->main_branch_.get()) ||
            ConflictingOperators(indexed_join_op->sub_branch_.get())) {
          return true;
        }
        break;
      } else if (auto *rollup_apply_op = dynamic_cast<RollUpApply *>(current)) {
        if (ConflictingOperators(rollup_apply_op->input_.get()) ||
            ConflictingOperators(rollup_apply_op->list_collection_branch_.get())) {
          return true;
        }
        break;
      } else if (dynamic_cast<Once *>(current) != nullptr || dynamic_cast<OutputTable *>(current) != nullptr ||
                 dynamic_cast<OutputTableStream *>(current) != nullptr) {
        // Terminal operators with no inputs - cannot have input symbols
        return false;
      } else {
        spdlog::error(
            "Unsupported operator in operator chain: {}. Please contact Memgraph support as this scenario should not "
            "happen!",
            current->ToString());
        return false;
      }
    }
    return false;
  }

  // Helper function to find the Scan operator that produces the required symbols
  // This traces symbol dependencies backwards through operators
  // Returns true if a suitable Scan is found, false otherwise
  // original_start: the original start of the search (used for IsPathReadOnly check to include
  //                 operators before multi-input branches like Cartesian)
  bool FindScanForSymbols(LogicalOperator *start, const std::set<Symbol::Position_t> &required_symbols,
                          LogicalOperator *&target_scan, LogicalOperator *&scan_parent,
                          std::vector<LogicalOperator *> &update_ops, LogicalOperator *original_start = nullptr) {
    if (!start || !scan_parent) {
      return false;
    }

    // If original_start is not set, use start (first call)
    if (!original_start) {
      original_start = start;
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
      if (std::ranges::find(conflicting_types, current_type) != conflicting_types.end()) {
        return target_scan != nullptr;
      }
      // Note: Distinct is now supported in parallel chains via SharedDistinctState
      // Each DistinctCursor in the parallel context shares a thread-safe seen_rows set
      // Operators that need to be updated if running in parallel
      if (std::ranges::find(update_types, current_type) != update_types.end()) {
        update_ops.push_back(current);
      }

      // Check if this is a Scan that produces any of the symbols we're looking for
      if (utils::IsSubtype(*current, ScanAll::kType)) {
        // Make sure the path from the original start to this scan is read-only
        // This includes operators before multi-input branches (like Cartesian)
        if (!IsPathReadOnly(original_start, current)) {
          return target_scan != nullptr;
        }
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
        } else if (auto *union_op = dynamic_cast<Union *>(current)) {
          if (union_op->left_op_) {
            auto left_symbols = union_op->left_op_->ModifiedSymbols(*symbol_table);
            for (const auto &sym : left_symbols) {
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
        // Special case for single input operators that have embedded queries
        if (auto *periodic_subquery_op = dynamic_cast<PeriodicSubquery *>(current)) {
          if (ConflictingOperators(periodic_subquery_op->subquery_.get())) break;
        } else if (auto *apply_op = dynamic_cast<Apply *>(current)) {
          if (ConflictingOperators(apply_op->subquery_.get())) break;
        } else if (auto *optional_op = dynamic_cast<Optional *>(current)) {
          if (ConflictingOperators(optional_op->optional_.get())) break;
        } else if (auto *filter_op = dynamic_cast<Filter *>(current)) {
          bool has_conflict = false;
          for (const auto &pf : filter_op->pattern_filters_) {
            if (ConflictingOperators(pf.get())) {
              has_conflict = true;
              break;
            }
          }
          if (has_conflict) break;
        }
        // We still go down the main branch
        current = current->input().get();
      } else {
        // For multi-input operators, check only main/left branches
        // The right branch is going to be fully executed by each parallel branch, so no need to update operators to
        // their parallel versions
        // However, the right branch needs to be checked for conflicting operators
        LogicalOperator *found_scan = nullptr;
        LogicalOperator *found_parent = current;
        std::vector<LogicalOperator *> update_ops;
        if (auto *union_op = dynamic_cast<Union *>(current)) {
          if (ConflictingOperators(union_op->right_op_.get())) break;
          if (FindScanForSymbols(
                  union_op->left_op_.get(), symbols_to_find, found_scan, found_parent, update_ops, original_start)) {
            target_scan = found_scan;
            scan_parent = found_parent;
          }
        } else if (auto *cartesian_op = dynamic_cast<Cartesian *>(current)) {
          if (ConflictingOperators(cartesian_op->right_op_.get())) break;
          if (FindScanForSymbols(cartesian_op->left_op_.get(),
                                 symbols_to_find,
                                 found_scan,
                                 found_parent,
                                 update_ops,
                                 original_start)) {
            target_scan = found_scan;
            scan_parent = found_parent;
          }
        } else if (auto *indexed_join_op = dynamic_cast<IndexedJoin *>(current)) {
          if (ConflictingOperators(indexed_join_op->sub_branch_.get())) break;
          if (FindScanForSymbols(indexed_join_op->main_branch_.get(),
                                 symbols_to_find,
                                 found_scan,
                                 found_parent,
                                 update_ops,
                                 original_start)) {
            target_scan = found_scan;
            scan_parent = found_parent;
          }
        } else if (auto *hash_join_op = dynamic_cast<HashJoin *>(current)) {
          if (ConflictingOperators(hash_join_op->right_op_.get())) break;
          if (FindScanForSymbols(hash_join_op->left_op_.get(),
                                 symbols_to_find,
                                 found_scan,
                                 found_parent,
                                 update_ops,
                                 original_start)) {
            target_scan = found_scan;
            scan_parent = found_parent;
          }
        } else if (auto *rollup_apply_op = dynamic_cast<RollUpApply *>(current)) {
          if (ConflictingOperators(rollup_apply_op->list_collection_branch_.get())) break;
          if (FindScanForSymbols(rollup_apply_op->input_.get(),
                                 symbols_to_find,
                                 found_scan,
                                 found_parent,
                                 update_ops,
                                 original_start)) {
            target_scan = found_scan;
            scan_parent = found_parent;
          }
        } else if (auto *union_op = dynamic_cast<Union *>(current)) {
          if (ConflictingOperators(union_op->right_op_.get())) break;
          if (FindScanForSymbols(
                  union_op->left_op_.get(), symbols_to_find, found_scan, found_parent, update_ops, original_start)) {
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
std::unique_ptr<LogicalOperator> RewriteParallelExecution(
    std::unique_ptr<LogicalOperator> root_op, SymbolTable *symbol_table, AstStorage *ast_storage, TDbAccessor *db,
    const memgraph::query::PreQueryDirectives &pre_query_directives, const Parameters &parameters) {
  if (pre_query_directives.parallel_execution_) {
    if (!license::global_license_checker.IsEnterpriseValidFast()) {
      throw QueryException("Parallel execution is not supported in the community edition");
    }
    auto get_num_threads = [&]() -> size_t {
      if (auto *num_threads = pre_query_directives.num_threads_) {
        // Create a minimal evaluation context to evaluate the expression
        // This handles both PrimitiveLiteral (when query is not cached) and
        // ParameterLookup (when query is cached and stripped)
        EvaluationContext eval_ctx{.parameters = parameters};
        PrimitiveLiteralExpressionEvaluator evaluator{eval_ctx};

        TypedValue value = num_threads->Accept(evaluator);
        if (!value.IsInt() || value.ValueInt() < 0) {
          throw QueryException("Number of threads must be a non-negative integer");
        }
        const auto threads = static_cast<size_t>(value.ValueInt());
        if (threads > FLAGS_bolt_num_workers) {
          spdlog::trace(
              "Requesting {} threads, more than available. Forcing {} threads.", threads, FLAGS_bolt_num_workers);
          return FLAGS_bolt_num_workers;
        }
        return threads;
      }
      // Default value
      return FLAGS_bolt_num_workers;
    };
    impl::ParallelRewriter<TDbAccessor> rewriter{symbol_table, ast_storage, db, get_num_threads()};
    return rewriter.Rewrite(std::move(root_op));
  }
  return root_op;
}

}  // namespace memgraph::query::plan

#endif
