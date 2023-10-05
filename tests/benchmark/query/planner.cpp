// Copyright 2023 Memgraph Ltd.
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
#include <string>
#include <variant>

#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/cost_estimator.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/vertex_count_cache.hpp"
#include "storage/v2/inmemory/storage.hpp"

// Add chained MATCH (node1) -- (node2), MATCH (node2) -- (node3) ... clauses.
static memgraph::query::CypherQuery *AddChainedMatches(int num_matches, memgraph::query::AstStorage &storage) {
  auto *query = storage.Create<memgraph::query::CypherQuery>();
  for (int i = 0; i < num_matches; ++i) {
    auto *match = storage.Create<memgraph::query::Match>();
    auto *pattern = storage.Create<memgraph::query::Pattern>();
    auto *single_query = storage.Create<memgraph::query::SingleQuery>();
    pattern->identifier_ = storage.Create<memgraph::query::Identifier>("path");
    match->patterns_.emplace_back(pattern);
    std::string node1_name = "node" + std::to_string(i - 1);
    pattern->atoms_.emplace_back(
        storage.Create<memgraph::query::NodeAtom>(storage.Create<memgraph::query::Identifier>(node1_name)));
    pattern->atoms_.emplace_back(storage.Create<memgraph::query::EdgeAtom>(
        storage.Create<memgraph::query::Identifier>("edge" + std::to_string(i)),
        memgraph::query::EdgeAtom::Type::SINGLE, memgraph::query::EdgeAtom::Direction::BOTH));
    pattern->atoms_.emplace_back(storage.Create<memgraph::query::NodeAtom>(
        storage.Create<memgraph::query::Identifier>("node" + std::to_string(i))));
    single_query->clauses_.emplace_back(match);
    query->single_query_ = single_query;
  }
  return query;
}

static void BM_PlanChainedMatches(benchmark::State &state) {
  std::unique_ptr<memgraph::storage::Storage> db(new memgraph::storage::InMemoryStorage());
  auto storage_dba = db->Access();
  memgraph::query::DbAccessor dba(storage_dba.get());
  while (state.KeepRunning()) {
    state.PauseTiming();
    memgraph::query::AstStorage storage;
    int num_matches = state.range(0);
    auto *query = AddChainedMatches(num_matches, storage);
    auto symbol_table = memgraph::query::MakeSymbolTable(query);
    auto ctx = memgraph::query::plan::MakePlanningContext(&storage, &symbol_table, query, &dba);
    state.ResumeTiming();
    auto query_parts = memgraph::query::plan::CollectQueryParts(symbol_table, storage, query);
    if (query_parts.query_parts.size() == 0) {
      std::exit(EXIT_FAILURE);
    }
    auto plans = memgraph::query::plan::MakeLogicalPlanForSingleQuery<memgraph::query::plan::VariableStartPlanner>(
        query_parts, &ctx);
    for (const auto &plan : plans) {
      // Exhaust through all generated plans, since they are lazily generated.
      benchmark::DoNotOptimize(plan.get());
    }
  }
}

BENCHMARK(BM_PlanChainedMatches)->RangeMultiplier(2)->Range(50, 400)->Unit(benchmark::kMillisecond);

static memgraph::query::CypherQuery *AddIndexedMatches(int num_matches, const std::string &label,
                                                       const std::string &property,
                                                       memgraph::query::AstStorage &storage) {
  auto *query = storage.Create<memgraph::query::CypherQuery>();
  for (int i = 0; i < num_matches; ++i) {
    auto *match = storage.Create<memgraph::query::Match>();
    auto *pattern = storage.Create<memgraph::query::Pattern>();
    auto *single_query = storage.Create<memgraph::query::SingleQuery>();
    pattern->identifier_ = storage.Create<memgraph::query::Identifier>("path");
    match->patterns_.emplace_back(pattern);
    std::string node1_name = "node" + std::to_string(i - 1);
    auto *node = storage.Create<memgraph::query::NodeAtom>(storage.Create<memgraph::query::Identifier>(node1_name));
    node->labels_.emplace_back(storage.GetLabelIx(label));
    std::get<0>(node->properties_)[storage.GetPropertyIx(property)] =
        storage.Create<memgraph::query::PrimitiveLiteral>(i);
    pattern->atoms_.emplace_back(node);
    single_query->clauses_.emplace_back(match);
    query->single_query_ = single_query;
  }
  return query;
}

static auto CreateIndexedVertices(int index_count, int vertex_count, memgraph::storage::Storage *db) {
  auto label = db->NameToLabel("label");
  auto prop = db->NameToProperty("prop");
  {
    auto unique_acc = db->UniqueAccess();
    [[maybe_unused]] auto _ = unique_acc->CreateIndex(label, prop);
  }
  auto dba = db->Access();
  for (int vi = 0; vi < vertex_count; ++vi) {
    for (int index = 0; index < index_count; ++index) {
      auto vertex = dba->CreateVertex();
      MG_ASSERT(vertex.AddLabel(label).HasValue());
      MG_ASSERT(vertex.SetProperty(prop, memgraph::storage::PropertyValue(index)).HasValue());
    }
  }
  MG_ASSERT(!dba->Commit().HasError());
  return std::make_pair("label", "prop");
}

static void BM_PlanAndEstimateIndexedMatching(benchmark::State &state) {
  std::unique_ptr<memgraph::storage::Storage> db(new memgraph::storage::InMemoryStorage());
  std::string label;
  std::string prop;
  int index_count = state.range(0);
  int vertex_count = state.range(1);
  std::tie(label, prop) = CreateIndexedVertices(index_count, vertex_count, db.get());
  auto storage_dba = db->Access();
  memgraph::query::DbAccessor dba(storage_dba.get());
  memgraph::query::Parameters parameters;
  while (state.KeepRunning()) {
    state.PauseTiming();
    memgraph::query::AstStorage storage;
    auto *query = AddIndexedMatches(index_count, label, prop, storage);
    auto symbol_table = memgraph::query::MakeSymbolTable(query);
    state.ResumeTiming();
    auto ctx = memgraph::query::plan::MakePlanningContext(&storage, &symbol_table, query, &dba);
    auto query_parts = memgraph::query::plan::CollectQueryParts(symbol_table, storage, query);
    if (query_parts.query_parts.size() == 0) {
      std::exit(EXIT_FAILURE);
    }
    auto plans = memgraph::query::plan::MakeLogicalPlanForSingleQuery<memgraph::query::plan::VariableStartPlanner>(
        query_parts, &ctx);
    for (auto plan : plans) {
      memgraph::query::plan::EstimatePlanCost(&dba, symbol_table, parameters, *plan);
    }
  }
}

static void BM_PlanAndEstimateIndexedMatchingWithCachedCounts(benchmark::State &state) {
  std::unique_ptr<memgraph::storage::Storage> db(new memgraph::storage::InMemoryStorage());
  std::string label;
  std::string prop;
  int index_count = state.range(0);
  int vertex_count = state.range(1);
  std::tie(label, prop) = CreateIndexedVertices(index_count, vertex_count, db.get());
  auto storage_dba = db->Access();
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto vertex_counts = memgraph::query::plan::MakeVertexCountCache(&dba);
  memgraph::query::Parameters parameters;
  while (state.KeepRunning()) {
    state.PauseTiming();
    memgraph::query::AstStorage storage;
    auto *query = AddIndexedMatches(index_count, label, prop, storage);
    auto symbol_table = memgraph::query::MakeSymbolTable(query);
    state.ResumeTiming();
    auto ctx = memgraph::query::plan::MakePlanningContext(&storage, &symbol_table, query, &vertex_counts);
    auto query_parts = memgraph::query::plan::CollectQueryParts(symbol_table, storage, query);
    if (query_parts.query_parts.size() == 0) {
      std::exit(EXIT_FAILURE);
    }
    auto plans = memgraph::query::plan::MakeLogicalPlanForSingleQuery<memgraph::query::plan::VariableStartPlanner>(
        query_parts, &ctx);
    for (auto plan : plans) {
      memgraph::query::plan::EstimatePlanCost(&vertex_counts, symbol_table, parameters, *plan);
    }
  }
}

BENCHMARK(BM_PlanAndEstimateIndexedMatching)
    ->RangeMultiplier(4)
    ->Ranges({{1, 100}, {100, 1000}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_PlanAndEstimateIndexedMatchingWithCachedCounts)
    ->RangeMultiplier(4)
    ->Ranges({{1, 100}, {100, 1000}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
