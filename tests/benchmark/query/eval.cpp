// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <benchmark/benchmark.h>

#include "query/db_accessor.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/storage.hpp"

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
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  std::unordered_map<query::PropertyIx, query::Expression *> elements;
  for (int64_t i = 0; i < state.range(0); ++i) {
    elements.emplace(ast.GetPropertyIx("prop" + std::to_string(i)), ast.Create<query::PrimitiveLiteral>(i));
  }
  auto *expr = ast.Create<query::MapLiteral>(elements);
  query::EvaluationContext evaluation_context{memory.get()};
  evaluation_context.properties = query::NamesToProperties(ast.properties_, &dba);
  query::ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, &dba, storage::View::NEW);
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(expr->Accept(evaluator));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(MapLiteral, NewDeleteResource)->Range(512, 1U << 15U)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(MapLiteral, MonotonicBufferResource)->Range(512, 1U << 15U)->Unit(benchmark::kMicrosecond);

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void AdditionOperator(benchmark::State &state) {
  query::AstStorage ast;
  query::SymbolTable symbol_table;
  TMemory memory;
  query::Frame frame(symbol_table.max_position(), memory.get());
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  query::Expression *expr = ast.Create<query::PrimitiveLiteral>(0);
  for (int64_t i = 0; i < state.range(0); ++i) {
    expr = ast.Create<query::AdditionOperator>(expr, ast.Create<query::PrimitiveLiteral>(i));
  }
  query::EvaluationContext evaluation_context{memory.get()};
  query::ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, &dba, storage::View::NEW);
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(expr->Accept(evaluator));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(AdditionOperator, NewDeleteResource)->Range(1024, 1U << 15U)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(AdditionOperator, MonotonicBufferResource)->Range(1024, 1U << 15U)->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
