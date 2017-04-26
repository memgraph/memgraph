#pragma once

#include <ctime>

#include "database/graph_db_accessor.hpp"
#include "query/context.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/planner.hpp"

namespace query {

template <typename Stream>
void Interpret(const std::string &query, GraphDbAccessor &db_accessor,
               Stream &stream) {
  clock_t start_time = clock();

  Config config;
  Context ctx(config, db_accessor);
  std::map<std::string, TypedValue> summary;

  // query -> AST
  frontend::opencypher::Parser parser(query);
  auto low_level_tree = parser.tree();

  // AST -> high level tree
  frontend::CypherMainVisitor visitor(ctx);
  visitor.visit(low_level_tree);
  auto high_level_tree = visitor.query();

  // symbol table fill
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  high_level_tree->Accept(symbol_generator);

  // high level tree -> logical plan
  auto logical_plan = plan::MakeLogicalPlan(*high_level_tree, symbol_table);

  // generate frame based on symbol table max_position
  Frame frame(symbol_table.max_position());

  std::vector<std::string> header;
  bool is_return = false;
  std::vector<Symbol> output_symbols;
  if (auto produce = dynamic_cast<plan::Produce *>(logical_plan.get())) {
    is_return = true;
    // collect the symbols from the return clause
    for (auto named_expression : produce->named_expressions())
      output_symbols.emplace_back(symbol_table[*named_expression]);
  } else if (auto order_by =
                 dynamic_cast<plan::OrderBy *>(logical_plan.get())) {
    is_return = true;
    output_symbols = order_by->output_symbols();
  }
  if (is_return) {
    // top level node in the operator tree is a produce/order_by (return)
    // so stream out results

    // generate header
    for (const auto &symbol : output_symbols) header.push_back(symbol.name_);
    stream.Header(header);

    // stream out results
    auto cursor = logical_plan->MakeCursor(db_accessor);
    while (cursor->Pull(frame, symbol_table)) {
      std::vector<TypedValue> values;
      for (const auto &symbol : output_symbols)
        values.emplace_back(frame[symbol]);
      stream.Result(values);
    }
  } else if (dynamic_cast<plan::CreateNode *>(logical_plan.get()) ||
             dynamic_cast<plan::CreateExpand *>(logical_plan.get()) ||
             dynamic_cast<plan::SetProperty *>(logical_plan.get()) ||
             dynamic_cast<plan::SetProperties *>(logical_plan.get()) ||
             dynamic_cast<plan::SetLabels *>(logical_plan.get()) ||
             dynamic_cast<plan::RemoveProperty *>(logical_plan.get()) ||
             dynamic_cast<plan::RemoveLabels *>(logical_plan.get()) ||
             dynamic_cast<plan::Delete *>(logical_plan.get()) ||
             dynamic_cast<plan::Merge *>(logical_plan.get())) {
    stream.Header(header);
    auto cursor = logical_plan->MakeCursor(db_accessor);
    while (cursor->Pull(frame, symbol_table)) continue;
  } else {
    throw QueryRuntimeException("Unknown top level LogicalOp");
  }

  clock_t end_time = clock();
  double time_second = double(end_time - start_time) / CLOCKS_PER_SEC;
  summary["query_time_sec"] = TypedValue(time_second);
  // TODO set summary['type'] based on transaction metadata
  // the type can't be determined based only on top level LogicalOp
  // (for example MATCH DELETE RETURN will have Produce as it's top)
  // for now always se "rw" because something must be set, but it doesn't
  // have to be correct (for Bolt clients)
  summary["type"] = "rw";
  stream.Summary(summary);
}
}
