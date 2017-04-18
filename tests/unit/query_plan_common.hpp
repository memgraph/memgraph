//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 14.03.17.
//

#include <iterator>
#include <memory>
#include <vector>

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/frame.hpp"

#include "query_common.hpp"

using namespace query;
using namespace query::plan;

/**
 * Helper function that collects all the results from the given
 * Produce into a ResultStreamFaker and returns that object.
 *
 * @param produce
 * @param symbol_table
 * @param db_accessor
 * @return
 */
auto CollectProduce(std::shared_ptr<Produce> produce, SymbolTable &symbol_table,
                    GraphDbAccessor &db_accessor) {
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

  return stream;
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
 * Returns (node_atom, scan_all_logical_op, symbol).
 */
ScanAllTuple MakeScanAll(AstTreeStorage &storage, SymbolTable &symbol_table,
                         const std::string &identifier,
                         std::shared_ptr<LogicalOperator> input = {nullptr}) {
  auto node = NODE(identifier);
  auto logical_op = std::make_shared<ScanAll>(node, input);
  auto symbol = symbol_table.CreateSymbol(identifier);
  symbol_table[*node->identifier_] = symbol;
  //  return std::make_tuple(node, logical_op, symbol);
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
                       EdgeAtom::Direction direction, bool edge_cycle,
                       const std::string &node_identifier, bool node_cycle) {
  auto edge = EDGE(edge_identifier, direction);
  auto edge_sym = symbol_table.CreateSymbol(edge_identifier);
  symbol_table[*edge->identifier_] = edge_sym;

  auto node = NODE(node_identifier);
  auto node_sym = symbol_table.CreateSymbol(node_identifier);
  symbol_table[*node->identifier_] = node_sym;

  auto op = std::make_shared<Expand>(node, edge, input, input_symbol,
                                     node_cycle, edge_cycle);

  return ExpandTuple{edge, edge_sym, node, node_sym, op};
}

template <typename TIterable>
auto CountIterable(TIterable iterable) {
  return std::distance(iterable.begin(), iterable.end());
}
