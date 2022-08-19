// Copyright 2022 Memgraph Ltd.
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

#include <iterator>
#include <memory>
#include <vector>

#include "query/v2/common.hpp"
#include "query/v2/context.hpp"
#include "query/v2/db_accessor.hpp"
#include "query/v2/frontend/semantic/symbol_table.hpp"
#include "query/v2/interpret/frame.hpp"
#include "query/v2/plan/operator.hpp"
#include "query/v2/plan/operator_distributed.hpp"
#include "storage/v3/storage.hpp"
#include "utils/logging.hpp"

#include "query_v2_query_common.hpp"

using namespace memgraph::query::v2;
using namespace memgraph::query::v2::plan;

using Bound = ScanAllByLabelPropertyRange::Bound;

ExecutionContext MakeContext(const AstStorage &storage, const SymbolTable &symbol_table,
                             memgraph::query::v2::DbAccessor *dba) {
  ExecutionContext context{dba};
  context.symbol_table = symbol_table;
  context.evaluation_context.properties = NamesToProperties(storage.properties_, dba);
  context.evaluation_context.labels = NamesToLabels(storage.labels_, dba);
  return context;
}

ExecutionContext MakeContext_Distributed(const AstStorage &storage, const SymbolTable &symbol_table,
                                         memgraph::query::v2::DbAccessor *dba) {
  // #NoCommit not needed?
  return MakeContext(storage, symbol_table, dba);
}

/** Helper function that collects all the results from the given Produce. */
std::vector<std::vector<TypedValue>> CollectProduce(const Produce &produce, ExecutionContext *context) {
  Frame frame(context->symbol_table.max_position());

  // top level node in the operator tree is a produce (return)
  // so stream out results

  // collect the symbols from the return clause
  std::vector<Symbol> symbols;
  for (auto named_expression : produce.named_expressions_)
    symbols.emplace_back(context->symbol_table.at(*named_expression));

  // stream out results
  auto cursor = produce.MakeCursor(memgraph::utils::NewDeleteResource());
  std::vector<std::vector<TypedValue>> results;
  while (cursor->Pull(frame, *context)) {
    std::vector<TypedValue> values;
    for (auto &symbol : symbols) values.emplace_back(frame[symbol]);
    results.emplace_back(values);
  }

  return results;
}

std::vector<std::vector<TypedValue>> CollectProduce_Distributed(const distributed::Produce &produce,
                                                                ExecutionContext *context, size_t size_of_batch) {
  auto frames_memory_owner =
      std::vector<std::unique_ptr<Frame>>{};  // #NoCommit We could also pass it directly to
                                              // operators, to be decided later. If so, remember we do a resize() in the
                                              // cursors when processing the last batch.
  frames_memory_owner.reserve(size_of_batch);
  std::generate_n(std::back_inserter(frames_memory_owner), size_of_batch,
                  [&context] { return std::make_unique<Frame>(context->symbol_table.max_position()); });

  auto frames = std::vector<Frame *>();
  frames.reserve(size_of_batch);
  std::transform(frames_memory_owner.begin(), frames_memory_owner.end(), std::back_inserter(frames),
                 [](std::unique_ptr<Frame> &frame_uptr) { return frame_uptr.get(); });

  // top level node in the operator tree is a produce (return)
  // so stream out results

  // collect the symbols from the return clause
  std::vector<Symbol> symbols;
  for (auto named_expression : produce.named_expressions_)
    symbols.emplace_back(context->symbol_table.at(*named_expression));

  // stream out results
  auto cursor = produce.MakeCursor(memgraph::utils::NewDeleteResource());
  std::vector<std::vector<TypedValue>> results;
  while (cursor->Pull(frames, *context)) {
    for (auto *frame : frames) {
      std::vector<TypedValue> values;
      for (auto &symbol : symbols) {
        values.emplace_back((*frame)[symbol]);
      }
      results.emplace_back(values);
    }
  }

  return results;
}

int PullAll(const LogicalOperator &logical_op, ExecutionContext *context) {
  Frame frame(context->symbol_table.max_position());
  auto cursor = logical_op.MakeCursor(memgraph::utils::NewDeleteResource());
  int count = 0;
  while (cursor->Pull(frame, *context)) count++;
  return count;
}

template <typename... TNamedExpressions>
auto MakeProduce(std::shared_ptr<LogicalOperator> input, TNamedExpressions... named_expressions) {
  return std::make_shared<Produce>(input, std::vector<NamedExpression *>{named_expressions...});
}

template <typename... TNamedExpressions>
auto MakeProduce_Distributed(std::shared_ptr<distributed::LogicalOperator> input,
                             TNamedExpressions... named_expressions) {
  // #NoCommit not needed?
  return std::make_shared<distributed::Produce>(input, std::vector<NamedExpression *>{named_expressions...});
}

struct ScanAllTuple {
  NodeAtom *node_;
  std::shared_ptr<LogicalOperator> op_;
  Symbol sym_;
};

struct ScanAllTuple_distributed {
  NodeAtom *node_;
  std::shared_ptr<distributed::LogicalOperator> op_;
  Symbol sym_;
};

/**
 * Creates and returns a tuple of stuff for a scan-all starting
 * from the node with the given name.
 *
 * Returns ScanAllTuple(node_atom, scan_all_logical_op, symbol).
 */
ScanAllTuple MakeScanAll(AstStorage &storage, SymbolTable &symbol_table, const std::string &identifier,
                         std::shared_ptr<LogicalOperator> input = {nullptr},
                         memgraph::storage::v3::View view = memgraph::storage::v3::View::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op = std::make_shared<ScanAll>(input, symbol, view);
  return ScanAllTuple{node, logical_op, symbol};
}

ScanAllTuple_distributed MakeScanAll_Distributed(AstStorage &storage, SymbolTable &symbol_table,
                                                 const std::string &identifier,
                                                 std::shared_ptr<distributed::LogicalOperator> input = {nullptr},
                                                 memgraph::storage::v3::View view = memgraph::storage::v3::View::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op = std::make_shared<distributed::ScanAll>(input, symbol, view);
  return ScanAllTuple_distributed{node, logical_op, symbol};
}

ScanAllTuple_distributed MakeScanAllById_Distributed(
    AstStorage &storage, SymbolTable &symbol_table, const std::string &identifier, Expression *value,
    std::shared_ptr<distributed::LogicalOperator> input = {nullptr},
    memgraph::storage::v3::View view = memgraph::storage::v3::View::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op = std::make_shared<distributed::ScanAllById>(input, symbol, value, view);
  return ScanAllTuple_distributed{node, logical_op, symbol};
}

ScanAllTuple MakeScanAllNew(AstStorage &storage, SymbolTable &symbol_table, const std::string &identifier,
                            std::shared_ptr<LogicalOperator> input = {nullptr},
                            memgraph::storage::v3::View view = memgraph::storage::v3::View::OLD) {
  auto *node = NODE(identifier, "label");
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op = std::make_shared<ScanAll>(input, symbol, view);
  return ScanAllTuple{node, logical_op, symbol};
}

/**
 * Creates and returns a tuple of stuff for a scan-all starting
 * from the node with the given name and label.
 *
 * Returns ScanAllTuple(node_atom, scan_all_logical_op, symbol).
 */
ScanAllTuple MakeScanAllByLabel(AstStorage &storage, SymbolTable &symbol_table, const std::string &identifier,
                                memgraph::storage::v3::LabelId label,
                                std::shared_ptr<LogicalOperator> input = {nullptr},
                                memgraph::storage::v3::View view = memgraph::storage::v3::View::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op = std::make_shared<ScanAllByLabel>(input, symbol, label, view);
  return ScanAllTuple{node, logical_op, symbol};
}

ScanAllTuple_distributed MakeScanAllByLabel_Distributed(
    AstStorage &storage, SymbolTable &symbol_table, const std::string &identifier, memgraph::storage::v3::LabelId label,
    std::shared_ptr<distributed::LogicalOperator> input = {nullptr},
    memgraph::storage::v3::View view = memgraph::storage::v3::View::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op = std::make_shared<distributed::ScanAllByLabel>(input, symbol, label, view);
  return ScanAllTuple_distributed{node, logical_op, symbol};
}

/**
 * Creates and returns a tuple of stuff for a scan-all starting from the node
 * with the given name and label whose property values are in range.
 *
 * Returns ScanAllTuple(node_atom, scan_all_logical_op, symbol).
 */
ScanAllTuple MakeScanAllByLabelPropertyRange(AstStorage &storage, SymbolTable &symbol_table, std::string identifier,
                                             memgraph::storage::v3::LabelId label,
                                             memgraph::storage::v3::PropertyId property,
                                             const std::string &property_name, std::optional<Bound> lower_bound,
                                             std::optional<Bound> upper_bound,
                                             std::shared_ptr<LogicalOperator> input = {nullptr},
                                             memgraph::storage::v3::View view = memgraph::storage::v3::View::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op = std::make_shared<ScanAllByLabelPropertyRange>(input, symbol, label, property, property_name,
                                                                  lower_bound, upper_bound, view);
  return ScanAllTuple{node, logical_op, symbol};
}

/**
 * Creates and returns a tuple of stuff for a scan-all starting from the node
 * with the given name and label whose property value is equal to given value.
 *
 * Returns ScanAllTuple(node_atom, scan_all_logical_op, symbol).
 */
ScanAllTuple MakeScanAllByLabelPropertyValue(AstStorage &storage, SymbolTable &symbol_table, std::string identifier,
                                             memgraph::storage::v3::LabelId label,
                                             memgraph::storage::v3::PropertyId property,
                                             const std::string &property_name, Expression *value,
                                             std::shared_ptr<LogicalOperator> input = {nullptr},
                                             memgraph::storage::v3::View view = memgraph::storage::v3::View::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op =
      std::make_shared<ScanAllByLabelPropertyValue>(input, symbol, label, property, property_name, value, view);
  return ScanAllTuple{node, logical_op, symbol};
}

ScanAllTuple_distributed MakeScanAllByLabelPropertyValue_Distributed(
    AstStorage &storage, SymbolTable &symbol_table, std::string identifier, memgraph::storage::v3::LabelId label,
    memgraph::storage::v3::PropertyId property, const std::string &property_name, Expression *value,
    std::shared_ptr<distributed::LogicalOperator> input = {nullptr},
    memgraph::storage::v3::View view = memgraph::storage::v3::View::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op = std::make_shared<distributed::ScanAllByLabelPropertyValue>(input, symbol, label, property,
                                                                               property_name, value, view);
  return ScanAllTuple_distributed{node, logical_op, symbol};
}

struct ExpandTuple {
  EdgeAtom *edge_;
  Symbol edge_sym_;
  NodeAtom *node_;
  Symbol node_sym_;
  std::shared_ptr<LogicalOperator> op_;
};

struct ExpandTuple_distributed {
  EdgeAtom *edge_;
  Symbol edge_sym_;
  NodeAtom *node_;
  Symbol node_sym_;
  std::shared_ptr<distributed::LogicalOperator> op_;
};

ExpandTuple MakeExpand(AstStorage &storage, SymbolTable &symbol_table, std::shared_ptr<LogicalOperator> input,
                       Symbol input_symbol, const std::string &edge_identifier, EdgeAtom::Direction direction,
                       const std::vector<memgraph::storage::v3::EdgeTypeId> &edge_types,
                       const std::string &node_identifier, bool existing_node, memgraph::storage::v3::View view) {
  auto edge = EDGE(edge_identifier, direction);
  auto edge_sym = symbol_table.CreateSymbol(edge_identifier, true);
  edge->identifier_->MapTo(edge_sym);

  auto node = NODE(node_identifier);
  auto node_sym = symbol_table.CreateSymbol(node_identifier, true);
  node->identifier_->MapTo(node_sym);

  auto op =
      std::make_shared<Expand>(input, input_symbol, node_sym, edge_sym, direction, edge_types, existing_node, view);

  return ExpandTuple{edge, edge_sym, node, node_sym, op};
}

// ExpandTuple_distributed MakeExpand_Distributed(AstStorage &storage, SymbolTable &symbol_table,
//                                                std::shared_ptr<distributed::LogicalOperator> input, Symbol
//                                                input_symbol, const std::string &edge_identifier, EdgeAtom::Direction
//                                                direction, const std::vector<memgraph::storage::v3::EdgeTypeId>
//                                                &edge_types, const std::string &node_identifier, bool existing_node,
//                                                memgraph::storage::v3::View view) {
//   auto edge = EDGE(edge_identifier, direction);
//   auto edge_sym = symbol_table.CreateSymbol(edge_identifier, true);
//   edge->identifier_->MapTo(edge_sym);

//   auto node = NODE(node_identifier);
//   auto node_sym = symbol_table.CreateSymbol(node_identifier, true);
//   node->identifier_->MapTo(node_sym);

//   auto op = std::make_shared<distributed::Expand>(input, input_symbol, node_sym, edge_sym, direction, edge_types,
//                                                   existing_node, view);

//   return ExpandTuple_distributed{edge, edge_sym, node, node_sym, op};
// }
struct UnwindTuple {
  Symbol sym_;
  std::shared_ptr<LogicalOperator> op_;
};

UnwindTuple MakeUnwind(SymbolTable &symbol_table, const std::string &symbol_name,
                       std::shared_ptr<LogicalOperator> input, Expression *input_expression) {
  auto sym = symbol_table.CreateSymbol(symbol_name, true);
  auto op = std::make_shared<memgraph::query::v2::plan::Unwind>(input, input_expression, sym);
  return UnwindTuple{sym, op};
}

template <typename TIterable>
auto CountIterable(TIterable &&iterable) {
  uint64_t count = 0;
  for (auto it = iterable.begin(); it != iterable.end(); ++it) {
    ++count;
  }
  return count;
}

inline uint64_t CountEdges(memgraph::query::v2::DbAccessor *dba, memgraph::storage::v3::View view) {
  uint64_t count = 0;
  for (auto vertex : dba->Vertices(view)) {
    auto maybe_edges = vertex.OutEdges(view);
    MG_ASSERT(maybe_edges.HasValue());
    count += CountIterable(*maybe_edges);
  }
  return count;
}
