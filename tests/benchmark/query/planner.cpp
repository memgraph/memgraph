#include <string>

#include <benchmark/benchmark_api.h>

#include "database/dbms.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/cost_estimator.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/vertex_count_cache.hpp"

// Add chained MATCH (node1) -- (node2), MATCH (node2) -- (node3) ... clauses.
static void AddChainedMatches(int num_matches, query::AstTreeStorage &storage) {
  for (int i = 0; i < num_matches; ++i) {
    auto *match = storage.Create<query::Match>();
    auto *pattern = storage.Create<query::Pattern>();
    pattern->identifier_ = storage.Create<query::Identifier>("path");
    match->patterns_.emplace_back(pattern);
    std::string node1_name = "node" + std::to_string(i - 1);
    pattern->atoms_.emplace_back(storage.Create<query::NodeAtom>(
        storage.Create<query::Identifier>(node1_name)));
    pattern->atoms_.emplace_back(storage.Create<query::EdgeAtom>(
        storage.Create<query::Identifier>("edge" + std::to_string(i)),
        query::EdgeAtom::Direction::BOTH));
    pattern->atoms_.emplace_back(storage.Create<query::NodeAtom>(
        storage.Create<query::Identifier>("node" + std::to_string(i))));
    storage.query()->clauses_.emplace_back(match);
  }
}

static void BM_PlanChainedMatches(benchmark::State &state) {
  Dbms dbms;
  auto dba = dbms.active();
  while (state.KeepRunning()) {
    state.PauseTiming();
    query::AstTreeStorage storage;
    int num_matches = state.range(0);
    AddChainedMatches(num_matches, storage);
    query::SymbolTable symbol_table;
    query::SymbolGenerator symbol_generator(symbol_table);
    storage.query()->Accept(symbol_generator);
    state.ResumeTiming();
    query::plan::MakeLogicalPlan<query::plan::VariableStartPlanner>(
        storage, symbol_table, *dba);
  }
}

BENCHMARK(BM_PlanChainedMatches)
    ->RangeMultiplier(2)
    ->Range(50, 400)
    ->Unit(benchmark::kMillisecond);

static void AddIndexedMatches(
    int num_matches, const GraphDbTypes::Label &label,
    const std::pair<std::string, GraphDbTypes::Property> &property,
    query::AstTreeStorage &storage) {
  for (int i = 0; i < num_matches; ++i) {
    auto *match = storage.Create<query::Match>();
    auto *pattern = storage.Create<query::Pattern>();
    pattern->identifier_ = storage.Create<query::Identifier>("path");
    match->patterns_.emplace_back(pattern);
    std::string node1_name = "node" + std::to_string(i - 1);
    auto *node = storage.Create<query::NodeAtom>(
        storage.Create<query::Identifier>(node1_name));
    node->labels_.emplace_back(label);
    node->properties_[property] = storage.Create<query::PrimitiveLiteral>(i);
    pattern->atoms_.emplace_back(node);
    storage.query()->clauses_.emplace_back(match);
  }
}

static auto CreateIndexedVertices(int index_count, int vertex_count,
                                  Dbms &dbms) {
  auto dba = dbms.active();
  auto label = dba->Label("label");
  auto prop = dba->Property("prop");
  dba->BuildIndex(label, prop);
  dba = dbms.active();
  for (int vi = 0; vi < vertex_count; ++vi) {
    for (int index = 0; index < index_count; ++index) {
      auto vertex = dba->InsertVertex();
      vertex.add_label(label);
      vertex.PropsSet(prop, index);
    }
  }
  dba->Commit();
  return std::make_pair(label, prop);
}

static void BM_PlanAndEstimateIndexedMatching(benchmark::State &state) {
  Dbms dbms;
  GraphDbTypes::Label label;
  GraphDbTypes::Property prop;
  int index_count = state.range(0);
  int vertex_count = state.range(1);
  std::tie(label, prop) =
      CreateIndexedVertices(index_count, vertex_count, dbms);
  auto dba = dbms.active();
  while (state.KeepRunning()) {
    state.PauseTiming();
    query::AstTreeStorage storage;
    AddIndexedMatches(index_count, label, std::make_pair("prop", prop),
                      storage);
    query::SymbolTable symbol_table;
    query::SymbolGenerator symbol_generator(symbol_table);
    storage.query()->Accept(symbol_generator);
    state.ResumeTiming();
    auto plans =
        query::plan::MakeLogicalPlan<query::plan::VariableStartPlanner>(
            storage, symbol_table, *dba);
    for (auto &plan : plans) {
      query::plan::EstimatePlanCost(*dba, *plan);
    }
  }
}

static void BM_PlanAndEstimateIndexedMatchingWithCachedCounts(
    benchmark::State &state) {
  Dbms dbms;
  GraphDbTypes::Label label;
  GraphDbTypes::Property prop;
  int index_count = state.range(0);
  int vertex_count = state.range(1);
  std::tie(label, prop) =
      CreateIndexedVertices(index_count, vertex_count, dbms);
  auto dba = dbms.active();
  auto vertex_counts = query::plan::MakeVertexCountCache(*dba);
  while (state.KeepRunning()) {
    state.PauseTiming();
    query::AstTreeStorage storage;
    AddIndexedMatches(index_count, label, std::make_pair("prop", prop),
                      storage);
    query::SymbolTable symbol_table;
    query::SymbolGenerator symbol_generator(symbol_table);
    storage.query()->Accept(symbol_generator);
    state.ResumeTiming();
    auto plans =
        query::plan::MakeLogicalPlan<query::plan::VariableStartPlanner>(
            storage, symbol_table, vertex_counts);
    for (auto &plan : plans) {
      query::plan::EstimatePlanCost(vertex_counts, *plan);
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
