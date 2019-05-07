#include <string>

#include <benchmark/benchmark.h>

#include "communication/result_stream_faker.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/required_privileges.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpreter.hpp"
#include "query/plan/planner.hpp"

static void AddVertices(database::GraphDb *db, int vertex_count) {
  auto dba = db->Access();
  for (int i = 0; i < vertex_count; i++) dba.InsertVertex();
  dba.Commit();
}

static query::CypherQuery *ParseCypherQuery(const std::string &query_string,
                                            query::AstStorage *ast) {
  query::frontend::ParsingContext parsing_context;
  parsing_context.is_query_cached = false;
  query::frontend::opencypher::Parser parser(query_string);
  // Convert antlr4 AST into Memgraph AST.
  query::frontend::CypherMainVisitor cypher_visitor(parsing_context, ast);
  cypher_visitor.visit(parser.tree());
  query::Interpreter::ParsedQuery parsed_query{
      cypher_visitor.query(),
      query::GetRequiredPrivileges(cypher_visitor.query())};
  return utils::Downcast<query::CypherQuery>(parsed_query.query);
};

// NOLINTNEXTLINE(google-runtime-references)
static void DistinctDefaultAllocator(benchmark::State &state) {
  query::AstStorage ast;
  query::Parameters parameters;
  database::GraphDb db;
  AddVertices(&db, state.range(0));
  auto dba = db.Access();
  auto query_string = "MATCH (s) RETURN DISTINCT s";
  auto *cypher_query = ParseCypherQuery(query_string, &ast);
  auto symbol_table = query::MakeSymbolTable(cypher_query);
  auto context =
      query::plan::MakePlanningContext(&ast, &symbol_table, cypher_query, &dba);
  auto plan_and_cost =
      query::plan::MakeLogicalPlan(&context, parameters, false);
  ResultStreamFaker<query::TypedValue> results;
  query::Frame frame(symbol_table.max_position());
  // Nothing should be used from the EvaluationContext, so leave it empty.
  query::EvaluationContext evaluation_context;
  while (state.KeepRunning()) {
    query::ExecutionContext execution_context{&dba, symbol_table,
                                              evaluation_context};
    auto cursor =
        plan_and_cost.first->MakeCursor(&dba, utils::NewDeleteResource());
    while (cursor->Pull(frame, execution_context))
      ;
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(DistinctDefaultAllocator)
    ->Range(1024, 1U << 21U)
    ->Unit(benchmark::kMicrosecond);

// NOLINTNEXTLINE(google-runtime-references)
static void DistinctLinearAllocator(benchmark::State &state) {
  query::AstStorage ast;
  query::Parameters parameters;
  database::GraphDb db;
  auto dba = db.Access();
  AddVertices(&db, state.range(0));
  auto query_string = "MATCH (s) RETURN DISTINCT s";
  auto *cypher_query = ParseCypherQuery(query_string, &ast);
  auto symbol_table = query::MakeSymbolTable(cypher_query);
  auto context =
      query::plan::MakePlanningContext(&ast, &symbol_table, cypher_query, &dba);
  auto plan_and_cost =
      query::plan::MakeLogicalPlan(&context, parameters, false);
  ResultStreamFaker<query::TypedValue> results;
  query::Frame frame(symbol_table.max_position());
  // Nothing should be used from the EvaluationContext, so leave it empty.
  query::EvaluationContext evaluation_context;
  while (state.KeepRunning()) {
    query::ExecutionContext execution_context{&dba, symbol_table,
                                              evaluation_context};
    utils::MonotonicBufferResource memory(1 * 1024 * 1024);
    auto cursor = plan_and_cost.first->MakeCursor(&dba, &memory);
    while (cursor->Pull(frame, execution_context))
      ;
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(DistinctLinearAllocator)
    ->Range(1024, 1U << 21U)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
