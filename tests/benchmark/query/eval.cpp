#include <benchmark/benchmark.h>

#include "query/frontend/ast/ast.hpp"
#include "query/interpret/eval.hpp"

static void BenchmarkCoalesceCallWithNulls(benchmark::State &state) {
  int64_t num_args = state.range(0);
  query::AstStorage ast_storage;
  std::vector<query::Expression *> arguments;
  arguments.reserve(num_args);
  for (int64_t i = 0; i < num_args; ++i) {
    arguments.emplace_back(
        ast_storage.Create<query::PrimitiveLiteral>(query::TypedValue::Null));
  }
  auto *function = ast_storage.Create<query::Function>("COALESCE", arguments);
  query::Frame frame(0);
  database::GraphDbAccessor *dba = nullptr;
  query::Context context(*dba);
  query::ExpressionEvaluator evaluator(
      &frame, context.symbol_table_, context.parameters_,
      context.evaluation_context_, &context.db_accessor_,
      query::GraphView::OLD);
  while (state.KeepRunning()) {
    function->Accept(evaluator);
  }
}

static void BenchmarkCoalesceCallWithStrings(benchmark::State &state) {
  int64_t num_args = state.range(0);
  query::AstStorage ast_storage;
  std::vector<query::Expression *> arguments;
  arguments.reserve(num_args);
  for (int64_t i = 0; i < num_args; ++i) {
    std::string val = "some_string " + std::to_string(i);
    arguments.emplace_back(ast_storage.Create<query::PrimitiveLiteral>(val));
  }
  auto *function = ast_storage.Create<query::Function>("COALESCE", arguments);
  query::Frame frame(0);
  database::GraphDbAccessor *dba = nullptr;
  query::Context context(*dba);
  query::ExpressionEvaluator evaluator(
      &frame, context.symbol_table_, context.parameters_,
      context.evaluation_context_, &context.db_accessor_,
      query::GraphView::OLD);
  while (state.KeepRunning()) {
    function->Accept(evaluator);
  }
}

// We are interested in benchmarking the usual amount of arguments
BENCHMARK(BenchmarkCoalesceCallWithNulls)
    ->RangeMultiplier(2)
    ->Range(1, 256)
    ->ThreadRange(1, 16);

BENCHMARK(BenchmarkCoalesceCallWithStrings)
    ->RangeMultiplier(2)
    ->Range(1, 256)
    ->ThreadRange(1, 16);

BENCHMARK_MAIN();
