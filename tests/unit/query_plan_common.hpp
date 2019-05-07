#pragma once

#include <iterator>
#include <memory>
#include <vector>

#include "query/common.hpp"
#include "query/context.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"

#include "query_common.hpp"

using namespace query;
using namespace query::plan;

using Bound = ScanAllByLabelPropertyRange::Bound;

ExecutionContext MakeContext(const AstStorage &storage,
                             const SymbolTable &symbol_table,
                             database::GraphDbAccessor *dba) {
  ExecutionContext context{dba};
  context.symbol_table = symbol_table;
  context.evaluation_context.properties =
      NamesToProperties(storage.properties_, dba);
  context.evaluation_context.labels = NamesToLabels(storage.labels_, dba);
  return context;
}

/** Helper function that collects all the results from the given Produce. */
std::vector<std::vector<TypedValue>> CollectProduce(const Produce &produce,
                                                    ExecutionContext *context) {
  Frame frame(context->symbol_table.max_position());

  // top level node in the operator tree is a produce (return)
  // so stream out results

  // collect the symbols from the return clause
  std::vector<Symbol> symbols;
  for (auto named_expression : produce.named_expressions_)
    symbols.emplace_back(context->symbol_table.at(*named_expression));

  // stream out results
  auto cursor =
      produce.MakeCursor(context->db_accessor, utils::NewDeleteResource());
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
  auto cursor =
      logical_op.MakeCursor(context->db_accessor, utils::NewDeleteResource());
  int count = 0;
  while (cursor->Pull(frame, *context)) count++;
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
ScanAllTuple MakeScanAll(AstStorage &storage, SymbolTable &symbol_table,
                         const std::string &identifier,
                         std::shared_ptr<LogicalOperator> input = {nullptr},
                         GraphView graph_view = GraphView::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
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
    AstStorage &storage, SymbolTable &symbol_table,
    const std::string &identifier, storage::Label label,
    std::shared_ptr<LogicalOperator> input = {nullptr},
    GraphView graph_view = GraphView::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op =
      std::make_shared<ScanAllByLabel>(input, symbol, label, graph_view);
  return ScanAllTuple{node, logical_op, symbol};
}

/**
 * Creates and returns a tuple of stuff for a scan-all starting from the node
 * with the given name and label whose property values are in range.
 *
 * Returns ScanAllTuple(node_atom, scan_all_logical_op, symbol).
 */
ScanAllTuple MakeScanAllByLabelPropertyRange(
    AstStorage &storage, SymbolTable &symbol_table, std::string identifier,
    storage::Label label, storage::Property property,
    const std::string &property_name, std::optional<Bound> lower_bound,
    std::optional<Bound> upper_bound,
    std::shared_ptr<LogicalOperator> input = {nullptr},
    GraphView graph_view = GraphView::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op = std::make_shared<ScanAllByLabelPropertyRange>(
      input, symbol, label, property, property_name, lower_bound, upper_bound,
      graph_view);
  return ScanAllTuple{node, logical_op, symbol};
}

/**
 * Creates and returns a tuple of stuff for a scan-all starting from the node
 * with the given name and label whose property value is equal to given value.
 *
 * Returns ScanAllTuple(node_atom, scan_all_logical_op, symbol).
 */
ScanAllTuple MakeScanAllByLabelPropertyValue(
    AstStorage &storage, SymbolTable &symbol_table, std::string identifier,
    storage::Label label, storage::Property property,
    const std::string &property_name, Expression *value,
    std::shared_ptr<LogicalOperator> input = {nullptr},
    GraphView graph_view = GraphView::OLD) {
  auto node = NODE(identifier);
  auto symbol = symbol_table.CreateSymbol(identifier, true);
  node->identifier_->MapTo(symbol);
  auto logical_op = std::make_shared<ScanAllByLabelPropertyValue>(
      input, symbol, label, property, property_name, value, graph_view);
  return ScanAllTuple{node, logical_op, symbol};
}

struct ExpandTuple {
  EdgeAtom *edge_;
  Symbol edge_sym_;
  NodeAtom *node_;
  Symbol node_sym_;
  std::shared_ptr<LogicalOperator> op_;
};

ExpandTuple MakeExpand(AstStorage &storage, SymbolTable &symbol_table,
                       std::shared_ptr<LogicalOperator> input,
                       Symbol input_symbol, const std::string &edge_identifier,
                       EdgeAtom::Direction direction,
                       const std::vector<storage::EdgeType> &edge_types,
                       const std::string &node_identifier, bool existing_node,
                       GraphView graph_view) {
  auto edge = EDGE(edge_identifier, direction);
  auto edge_sym = symbol_table.CreateSymbol(edge_identifier, true);
  edge->identifier_->MapTo(edge_sym);

  auto node = NODE(node_identifier);
  auto node_sym = symbol_table.CreateSymbol(node_identifier, true);
  node->identifier_->MapTo(node_sym);

  auto op = std::make_shared<Expand>(input, input_symbol, node_sym, edge_sym,
                                     direction, edge_types, existing_node,
                                     graph_view);

  return ExpandTuple{edge, edge_sym, node, node_sym, op};
}

struct UnwindTuple {
  Symbol sym_;
  std::shared_ptr<LogicalOperator> op_;
};

UnwindTuple MakeUnwind(SymbolTable &symbol_table,
                       const std::string &symbol_name,
                       std::shared_ptr<LogicalOperator> input,
                       Expression *input_expression) {
  auto sym = symbol_table.CreateSymbol(symbol_name, true);
  auto op = std::make_shared<query::plan::Unwind>(input, input_expression, sym);
  return UnwindTuple{sym, op};
}

template <typename TIterable>
auto CountIterable(TIterable iterable) {
  return std::distance(iterable.begin(), iterable.end());
}
