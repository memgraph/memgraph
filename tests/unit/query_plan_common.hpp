//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 14.03.17.
//

#pragma once

#include <iterator>
#include <memory>
#include <vector>

#include "communication/result_stream_faker.hpp"
#include "query/common.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"

#include "query_common.hpp"

using namespace query;
using namespace query::plan;

/**
 * Helper function that collects all the results from the given
 * Produce into a ResultStreamFaker and returns the results from it.
 *
 * @param produce
 * @param symbol_table
 * @param db_accessor
 * @return
 */
std::vector<std::vector<TypedValue>> CollectProduce(
    Produce *produce, SymbolTable &symbol_table, GraphDbAccessor &db_accessor) {
  ResultStreamFaker stream;
  Frame frame(symbol_table.max_position());

  // top level node in the operator tree is a produce (return)
  // so stream out results

  // generate header
  std::vector<std::string> header;
  for (auto named_expression : produce->named_expressions())
    header.push_back(named_expression->name_);
  stream.Header(header);

  // collect the symbols from the return clause
  std::vector<Symbol> symbols;
  for (auto named_expression : produce->named_expressions())
    symbols.emplace_back(symbol_table[*named_expression]);

  // stream out results
  auto cursor = produce->MakeCursor(db_accessor);
  while (cursor->Pull(frame, symbol_table)) {
    std::vector<TypedValue> values;
    for (auto &symbol : symbols) values.emplace_back(frame[symbol]);
    stream.Result(values);
  }

  stream.Summary({{std::string("type"), TypedValue("r")}});

  return stream.GetResults();
}

int PullAll(std::shared_ptr<LogicalOperator> logical_op, GraphDbAccessor &db,
            SymbolTable &symbol_table) {
  Frame frame(symbol_table.max_position());
  auto cursor = logical_op->MakeCursor(db);
  int count = 0;
  while (cursor->Pull(frame, symbol_table)) count++;
  return count;
}

template <typename... TNamedExpressions>
auto MakeProduce(std::shared_ptr<LogicalOperator> input,
                 TNamedExpressions... named_expressions) {
  return std::make_shared<Produce>(
      input, std::vector<NamedExpression *>{named_expressions...});
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
ScanAllTuple MakeScanAll(AstTreeStorage &storage, SymbolTable &symbol_table,
                         const std::string &identifier,
                         std::shared_ptr<LogicalOperator> input = {nullptr},
                         GraphView graph_view = GraphView::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  symbol_table[*node->identifier_] = symbol;
  auto logical_op = std::make_shared<ScanAll>(input, symbol, graph_view);
  return ScanAllTuple{node, logical_op, symbol};
}

/**
 * Creates and returns a tuple of stuff for a scan-all starting
 * from the node with the given name and label.
 *
 * Returns ScanAllTuple(node_atom, scan_all_logical_op, symbol).
 */
ScanAllTuple MakeScanAllByLabel(
    AstTreeStorage &storage, SymbolTable &symbol_table,
    const std::string &identifier, const GraphDbTypes::Label &label,
    std::shared_ptr<LogicalOperator> input = {nullptr},
    GraphView graph_view = GraphView::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  symbol_table[*node->identifier_] = symbol;
  auto logical_op =
      std::make_shared<ScanAllByLabel>(input, symbol, label, graph_view);
  return ScanAllTuple{node, logical_op, symbol};
}

struct ExpandTuple {
  EdgeAtom *edge_;
  Symbol edge_sym_;
  NodeAtom *node_;
  Symbol node_sym_;
  std::shared_ptr<LogicalOperator> op_;
};

ExpandTuple MakeExpand(AstTreeStorage &storage, SymbolTable &symbol_table,
                       std::shared_ptr<LogicalOperator> input,
                       Symbol input_symbol, const std::string &edge_identifier,
                       EdgeAtom::Direction direction, bool existing_edge,
                       const std::string &node_identifier, bool existing_node,
                       GraphView graph_view = GraphView::AS_IS) {
  auto edge = EDGE(edge_identifier, direction);
  auto edge_sym = symbol_table.CreateSymbol(edge_identifier, true);
  symbol_table[*edge->identifier_] = edge_sym;

  auto node = NODE(node_identifier);
  auto node_sym = symbol_table.CreateSymbol(node_identifier, true);
  symbol_table[*node->identifier_] = node_sym;

  auto op = std::make_shared<Expand>(node_sym, edge_sym, direction, input,
                                     input_symbol, existing_node, existing_edge,
                                     graph_view);

  return ExpandTuple{edge, edge_sym, node, node_sym, op};
}

template <typename TIterable>
auto CountIterable(TIterable iterable) {
  return std::distance(iterable.begin(), iterable.end());
}
