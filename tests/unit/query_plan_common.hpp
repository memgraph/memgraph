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

#pragma once

#include <iterator>
#include <memory>
#include <vector>

#include "auth/models.hpp"
#include "glue/auth_checker.hpp"
#include "query/common.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "storage/v2/storage.hpp"
#include "utils/logging.hpp"

#include "query_common.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;

using Bound = ScanAllByLabelPropertyRange::Bound;

ExecutionContext MakeContext(const AstStorage &storage, const SymbolTable &symbol_table,
                             memgraph::query::DbAccessor *dba) {
  ExecutionContext context{.db_accessor = dba};
  context.symbol_table = symbol_table;
  context.evaluation_context.properties = NamesToProperties(storage.properties_, dba);
  context.evaluation_context.labels = NamesToLabels(storage.labels_, dba);
  return context;
}
#ifdef MG_ENTERPRISE
ExecutionContext MakeContextWithFineGrainedChecker(const AstStorage &storage, const SymbolTable &symbol_table,
                                                   memgraph::query::DbAccessor *dba,
                                                   memgraph::glue::FineGrainedAuthChecker *auth_checker) {
  ExecutionContext context{.db_accessor = dba};
  context.symbol_table = symbol_table;
  context.evaluation_context.properties = NamesToProperties(storage.properties_, dba);
  context.evaluation_context.labels = NamesToLabels(storage.labels_, dba);
  context.auth_checker = std::make_unique<memgraph::glue::FineGrainedAuthChecker>(std::move(*auth_checker));

  return context;
}
#endif

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

int PullAll(const LogicalOperator &logical_op, ExecutionContext *context) {
  Frame frame(context->symbol_table.max_position());
  auto cursor = logical_op.MakeCursor(memgraph::utils::NewDeleteResource());
  int count = 0;
  while (cursor->Pull(frame, *context)) {
    count++;
  }
  return count;
}

template <typename... TNamedExpressions>
auto MakeProduce(std::shared_ptr<LogicalOperator> input, TNamedExpressions... named_expressions) {
  return std::make_shared<Produce>(input, std::vector<NamedExpression *>{named_expressions...});
}

struct ScanAllTuple {
  NodeAtom *node_;
  std::shared_ptr<LogicalOperator> op_;
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
                         memgraph::storage::View view = memgraph::storage::View::OLD) {
  auto node = memgraph::query::test_common::GetNode(storage, identifier);
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
                                memgraph::storage::LabelId label, std::shared_ptr<LogicalOperator> input = {nullptr},
                                memgraph::storage::View view = memgraph::storage::View::OLD) {
  auto node = memgraph::query::test_common::GetNode(storage, identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op = std::make_shared<ScanAllByLabel>(input, symbol, label, view);
  return ScanAllTuple{node, logical_op, symbol};
}

/**
 * Creates and returns a tuple of stuff for a scan-all starting from the node
 * with the given name and label whose property values are in range.
 *
 * Returns ScanAllTuple(node_atom, scan_all_logical_op, symbol).
 */
ScanAllTuple MakeScanAllByLabelPropertyRange(AstStorage &storage, SymbolTable &symbol_table, std::string identifier,
                                             memgraph::storage::LabelId label, memgraph::storage::PropertyId property,
                                             const std::string &property_name, std::optional<Bound> lower_bound,
                                             std::optional<Bound> upper_bound,
                                             std::shared_ptr<LogicalOperator> input = {nullptr},
                                             memgraph::storage::View view = memgraph::storage::View::OLD) {
  auto node = memgraph::query::test_common::GetNode(storage, identifier);
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
                                             memgraph::storage::LabelId label, memgraph::storage::PropertyId property,
                                             const std::string &property_name, Expression *value,
                                             std::shared_ptr<LogicalOperator> input = {nullptr},
                                             memgraph::storage::View view = memgraph::storage::View::OLD) {
  auto node = memgraph::query::test_common::GetNode(storage, identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op =
      std::make_shared<ScanAllByLabelPropertyValue>(input, symbol, label, property, property_name, value, view);
  return ScanAllTuple{node, logical_op, symbol};
}

struct ExpandTuple {
  EdgeAtom *edge_;
  Symbol edge_sym_;
  NodeAtom *node_;
  Symbol node_sym_;
  std::shared_ptr<LogicalOperator> op_;
};

ExpandTuple MakeExpand(AstStorage &storage, SymbolTable &symbol_table, std::shared_ptr<LogicalOperator> input,
                       Symbol input_symbol, const std::string &edge_identifier, EdgeAtom::Direction direction,
                       const std::vector<memgraph::storage::EdgeTypeId> &edge_types, const std::string &node_identifier,
                       bool existing_node, memgraph::storage::View view) {
  auto edge = memgraph::query::test_common::GetEdge(storage, edge_identifier, direction);
  auto edge_sym = symbol_table.CreateSymbol(edge_identifier, true);
  edge->identifier_->MapTo(edge_sym);

  auto node = memgraph::query::test_common::GetNode(storage, node_identifier);
  auto node_sym = symbol_table.CreateSymbol(node_identifier, true);
  node->identifier_->MapTo(node_sym);

  auto op =
      std::make_shared<Expand>(input, input_symbol, node_sym, edge_sym, direction, edge_types, existing_node, view);

  return ExpandTuple{edge, edge_sym, node, node_sym, op};
}

struct UnwindTuple {
  Symbol sym_;
  std::shared_ptr<LogicalOperator> op_;
};

UnwindTuple MakeUnwind(SymbolTable &symbol_table, const std::string &symbol_name,
                       std::shared_ptr<LogicalOperator> input, Expression *input_expression) {
  auto sym = symbol_table.CreateSymbol(symbol_name, true);
  auto op = std::make_shared<memgraph::query::plan::Unwind>(input, input_expression, sym);
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

inline uint64_t CountEdges(memgraph::query::DbAccessor *dba, memgraph::storage::View view) {
  uint64_t count = 0;
  for (auto vertex : dba->Vertices(view)) {
    dba->PrefetchOutEdges(vertex);
    auto maybe_edges = vertex.OutEdges(view);
    MG_ASSERT(maybe_edges.HasValue());
    count += CountIterable(maybe_edges->edges);
  }
  return count;
}
