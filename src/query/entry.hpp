#pragma once

#include "database/graph_db_accessor.hpp"
#include "query/context.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/typecheck/typecheck.hpp"

namespace query {

template <typename Stream>
class Engine {
 public:
  Engine() {}
  auto Execute(const std::string &query, GraphDbAccessor &db_accessor,
               Stream &stream) {
    Config config;
    Context ctx(config, db_accessor);

    // query -> AST
    ::frontend::opencypher::Parser parser(query);
    auto low_level_tree = parser.tree();

    // AST -> high level tree
    auto high_level_tree = low2high_tree.Apply(ctx, low_level_tree);

    // symbol table fill
    SymbolTable symbol_table;
    TypeCheckVisitor typecheck_visitor;
    high_level_tree.Accept(typecheck_visitor);

    // high level tree -> logical plan
    auto logical_plan = query::Apply(high_level_tree);

    // generate frame based on symbol table max_position
    Frame frame(symbol_table.max_position());

    // interpret
    auto cursor = logical_plan.MakeCursor(frame);
    logical_plan.WriteHeader(stream);
    auto symbols = logical_plan.OutputSymbols(symbol_table);
    while (cursor.pull(frame, context)) {
      std::vector<TypedValue> values(symbols.size());
      for (auto symbol : symbols) {
        values.emplace_back(frame[symbol]);
      }
      stream.Result(values);
    }
    stream.Summary({"type" : "r"});
  }

 private:
  Context ctx;
  HighLevelAstConversion low2high_tree;
};
}
