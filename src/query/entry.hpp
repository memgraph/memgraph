#pragma once

#include "database/graph_db_accessor.hpp"
#include "query/context.hpp"
#include "query/context.hpp"
#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/logical/planner.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"

namespace query {

/**
 * A Stream implementation that writes out to the
 * console (for testing and debugging only).
 */
// TODO move somewhere to /test/manual or so
class ConsoleResultStream : public Loggable {
 public:
  ConsoleResultStream() : Loggable("ConsoleResultStream") {}

  void Header(const std::vector<std::string> &) { logger.info("header"); }

  void Result(std::vector<TypedValue> &values) {
    for (auto value : values) {
      auto va = value.Value<VertexAccessor>();
      logger.info("    {}", va.labels().size());
    }
  }

  void Summary(const std::map<std::string, TypedValue> &) {
    logger.info("summary");
  }
};

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
    HighLevelAstConversion low2high_tree;
    auto high_level_tree = low2high_tree.Apply(ctx, low_level_tree);
    Execute(*high_level_tree, db_accessor, stream);
  }

  auto Execute(Query &query, GraphDbAccessor &db_accessor, Stream stream) {
    // symbol table fill
    SymbolTable symbol_table;
    SymbolGenerator symbol_generator(symbol_table);
    query.Accept(symbol_generator);

    // high level tree -> logical plan
    auto logical_plan = MakeLogicalPlan(query);

    // generate frame based on symbol table max_position
    Frame frame(symbol_table.max_position());

    auto *produce = dynamic_cast<Produce *>(logical_plan.get());

    if (produce) {
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
        for (auto &symbol : symbols)
          values.emplace_back(frame[symbol]);
        stream.Result(values);
      }

      stream.Summary({{std::string("type"), TypedValue("r")}});
    }
  }
};
}
