// Copyright 2024 Memgraph Ltd.
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
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

using memgraph::replication::ReplicationRole;
// The following classes are wrappers for memgraph::utils::MemoryResource, so that we can
// use BENCHMARK_TEMPLATE

class MonotonicBufferResource final {
  memgraph::utils::MonotonicBufferResource memory_{memgraph::query::kExecutionMemoryBlockSize};

 public:
  memgraph::utils::MemoryResource *get() { return &memory_; }
};

class NewDeleteResource final {
 public:
  memgraph::utils::MemoryResource *get() { return memgraph::utils::NewDeleteResource(); }
};

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void MapLiteral(benchmark::State &state) {
  memgraph::query::AstStorage ast;
  memgraph::query::SymbolTable symbol_table;
  TMemory memory;
  memgraph::query::Frame frame(symbol_table.max_position(), memory.get());
  std::unique_ptr<memgraph::storage::Storage> db(new memgraph::storage::InMemoryStorage());
  auto storage_dba = db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  std::unordered_map<memgraph::query::PropertyIx, memgraph::query::Expression *> elements;
  for (int64_t i = 0; i < state.range(0); ++i) {
    elements.emplace(ast.GetPropertyIx("prop" + std::to_string(i)), ast.Create<memgraph::query::PrimitiveLiteral>(i));
  }
  auto *expr = ast.Create<memgraph::query::MapLiteral>(elements);
  memgraph::query::EvaluationContext evaluation_context{memory.get()};
  evaluation_context.properties = memgraph::query::NamesToProperties(ast.properties_, &dba);
  memgraph::query::ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, &dba,
                                                 memgraph::storage::View::NEW);
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(expr->Accept(evaluator));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(MapLiteral, NewDeleteResource)->Range(512, 1U << 15U)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(MapLiteral, MonotonicBufferResource)->Range(512, 1U << 15U)->Unit(benchmark::kMicrosecond);

// TODO ante benchmark template for MapProjectionLiteral

template <class TMemory>
// NOLINTNEXTLINE(google-runtime-references)
static void AdditionOperator(benchmark::State &state) {
  memgraph::query::AstStorage ast;
  memgraph::query::SymbolTable symbol_table;
  TMemory memory;
  memgraph::query::Frame frame(symbol_table.max_position(), memory.get());
  std::unique_ptr<memgraph::storage::Storage> db(new memgraph::storage::InMemoryStorage());
  auto storage_dba = db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  memgraph::query::Expression *expr = ast.Create<memgraph::query::PrimitiveLiteral>(0);
  for (int64_t i = 0; i < state.range(0); ++i) {
    expr = ast.Create<memgraph::query::AdditionOperator>(expr, ast.Create<memgraph::query::PrimitiveLiteral>(i));
  }
  memgraph::query::EvaluationContext evaluation_context{memory.get()};
  memgraph::query::ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, &dba,
                                                 memgraph::storage::View::NEW);
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(expr->Accept(evaluator));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(AdditionOperator, NewDeleteResource)->Range(1024, 1U << 15U)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(AdditionOperator, MonotonicBufferResource)->Range(1024, 1U << 15U)->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
