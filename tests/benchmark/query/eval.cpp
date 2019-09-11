#include <benchmark/benchmark.h>

#include "query/db_accessor.hpp"
#include "query/interpret/eval.hpp"
#include "query/transaction_engine.hpp"

// The following classes are wrappers for utils::MemoryResource, so that we can
// use BENCHMARK_TEMPLATE

class MonotonicBufferResource final {
  utils::MonotonicBufferResource memory_{query::kExecutionMemoryBlockSize};

 public:
  utils::MemoryResource *get() { return &memory_; }
};

class NewDeleteResource final {
 public:
  utils::MemoryResource *get() { return utils::NewDeleteResource(); }
};

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void MapLiteral(benchmark::State &state) {
  query::AstStorage ast;
  query::SymbolTable symbol_table;
  TMemory memory;
  query::Frame frame(symbol_table.max_position(), memory.get());
  database::GraphDb db;
  auto dba = db.Access();
  query::DbAccessor execution_dba(&dba);
  std::unordered_map<query::PropertyIx, query::Expression *> elements;
  for (int64_t i = 0; i < state.range(0); ++i) {
    elements.emplace(ast.GetPropertyIx("prop" + std::to_string(i)),
                     ast.Create<query::PrimitiveLiteral>(i));
  }
  auto *expr = ast.Create<query::MapLiteral>(elements);
  query::EvaluationContext evaluation_context{memory.get()};
  evaluation_context.properties =
      query::NamesToProperties(ast.properties_, &execution_dba);
  query::ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context,
                                       &execution_dba, storage::View::NEW);
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(expr->Accept(evaluator));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(MapLiteral, NewDeleteResource)
    ->Range(512, 1U << 15U)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(MapLiteral, MonotonicBufferResource)
    ->Range(512, 1U << 15U)
    ->Unit(benchmark::kMicrosecond);

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void AdditionOperator(benchmark::State &state) {
  query::AstStorage ast;
  query::SymbolTable symbol_table;
  TMemory memory;
  query::Frame frame(symbol_table.max_position(), memory.get());
  database::GraphDb db;
  auto dba = db.Access();
  query::Expression *expr = ast.Create<query::PrimitiveLiteral>(0);
  for (int64_t i = 0; i < state.range(0); ++i) {
    expr = ast.Create<query::AdditionOperator>(
        expr, ast.Create<query::PrimitiveLiteral>(i));
  }
  query::EvaluationContext evaluation_context{memory.get()};
  query::DbAccessor execution_dba(&dba);
  query::ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context,
                                       &execution_dba, storage::View::NEW);
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(expr->Accept(evaluator));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(AdditionOperator, NewDeleteResource)
    ->Range(1024, 1U << 15U)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(AdditionOperator, MonotonicBufferResource)
    ->Range(1024, 1U << 15U)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
