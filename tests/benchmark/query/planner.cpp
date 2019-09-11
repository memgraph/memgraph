#include <string>

#include <benchmark/benchmark_api.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/cost_estimator.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/vertex_count_cache.hpp"

// Add chained MATCH (node1) -- (node2), MATCH (node2) -- (node3) ... clauses.
static query::CypherQuery *AddChainedMatches(int num_matches,
                                             query::AstStorage &storage) {
  auto *query = storage.Create<query::CypherQuery>();
  for (int i = 0; i < num_matches; ++i) {
    auto *match = storage.Create<query::Match>();
    auto *pattern = storage.Create<query::Pattern>();
    auto *single_query = storage.Create<query::SingleQuery>();
    pattern->identifier_ = storage.Create<query::Identifier>("path");
    match->patterns_.emplace_back(pattern);
    std::string node1_name = "node" + std::to_string(i - 1);
    pattern->atoms_.emplace_back(storage.Create<query::NodeAtom>(
        storage.Create<query::Identifier>(node1_name)));
    pattern->atoms_.emplace_back(storage.Create<query::EdgeAtom>(
        storage.Create<query::Identifier>("edge" + std::to_string(i)),
        query::EdgeAtom::Type::SINGLE, query::EdgeAtom::Direction::BOTH));
    pattern->atoms_.emplace_back(storage.Create<query::NodeAtom>(
        storage.Create<query::Identifier>("node" + std::to_string(i))));
    single_query->clauses_.emplace_back(match);
    query->single_query_ = single_query;
  }
  return query;
}

static void BM_PlanChainedMatches(benchmark::State &state) {
  database::GraphDb db;
  auto dba = db.Access();
  while (state.KeepRunning()) {
    state.PauseTiming();
    query::AstStorage storage;
    int num_matches = state.range(0);
    auto *query = AddChainedMatches(num_matches, storage);
    auto symbol_table = query::MakeSymbolTable(query);
    query::DbAccessor exec_dba(&dba);
    auto ctx = query::plan::MakePlanningContext(&storage, &symbol_table, query,
                                                &exec_dba);
    state.ResumeTiming();
    auto query_parts =
        query::plan::CollectQueryParts(symbol_table, storage, query);
    if (query_parts.query_parts.size() == 0) {
      std::exit(EXIT_FAILURE);
    }
    auto single_query_parts = query_parts.query_parts.at(0).single_query_parts;
    auto plans = query::plan::MakeLogicalPlanForSingleQuery<
        query::plan::VariableStartPlanner>(single_query_parts, &ctx);
    for (const auto &plan : plans) {
      // Exhaust through all generated plans, since they are lazily generated.
      benchmark::DoNotOptimize(plan.get());
    }
  }
}

BENCHMARK(BM_PlanChainedMatches)
    ->RangeMultiplier(2)
    ->Range(50, 400)
    ->Unit(benchmark::kMillisecond);

static query::CypherQuery *AddIndexedMatches(int num_matches,
                                             const std::string &label,
                                             const std::string &property,
                                             query::AstStorage &storage) {
  auto *query = storage.Create<query::CypherQuery>();
  for (int i = 0; i < num_matches; ++i) {
    auto *match = storage.Create<query::Match>();
    auto *pattern = storage.Create<query::Pattern>();
    auto *single_query = storage.Create<query::SingleQuery>();
    pattern->identifier_ = storage.Create<query::Identifier>("path");
    match->patterns_.emplace_back(pattern);
    std::string node1_name = "node" + std::to_string(i - 1);
    auto *node = storage.Create<query::NodeAtom>(
        storage.Create<query::Identifier>(node1_name));
    node->labels_.emplace_back(storage.GetLabelIx(label));
    node->properties_[storage.GetPropertyIx(property)] =
        storage.Create<query::PrimitiveLiteral>(i);
    pattern->atoms_.emplace_back(node);
    single_query->clauses_.emplace_back(match);
    query->single_query_ = single_query;
  }
  return query;
}

static auto CreateIndexedVertices(int index_count, int vertex_count,
                                  database::GraphDb &db) {
  auto label = db.Access().Label("label");
  auto prop = db.Access().Property("prop");
  db.Access().BuildIndex(label, prop);
  auto dba = db.Access();
  for (int vi = 0; vi < vertex_count; ++vi) {
    for (int index = 0; index < index_count; ++index) {
      auto vertex = dba.InsertVertex();
      vertex.add_label(label);
      vertex.PropsSet(prop, PropertyValue(index));
    }
  }
  dba.Commit();
  return std::make_pair("label", "prop");
}

static void BM_PlanAndEstimateIndexedMatching(benchmark::State &state) {
  database::GraphDb db;
  std::string label;
  std::string prop;
  int index_count = state.range(0);
  int vertex_count = state.range(1);
  std::tie(label, prop) = CreateIndexedVertices(index_count, vertex_count, db);
  auto dba = db.Access();
  query::Parameters parameters;
  while (state.KeepRunning()) {
    state.PauseTiming();
    query::AstStorage storage;
    auto *query = AddIndexedMatches(index_count, label, prop, storage);
    auto symbol_table = query::MakeSymbolTable(query);
    state.ResumeTiming();
    query::DbAccessor exec_dba(&dba);
    auto ctx = query::plan::MakePlanningContext(&storage, &symbol_table, query,
                                                &exec_dba);
    auto query_parts =
        query::plan::CollectQueryParts(symbol_table, storage, query);
    if (query_parts.query_parts.size() == 0) {
      std::exit(EXIT_FAILURE);
    }
    auto single_query_parts = query_parts.query_parts.at(0).single_query_parts;
    auto plans = query::plan::MakeLogicalPlanForSingleQuery<
        query::plan::VariableStartPlanner>(single_query_parts, &ctx);
    for (auto plan : plans) {
      query::plan::EstimatePlanCost(&dba, parameters, *plan);
    }
  }
}

static void BM_PlanAndEstimateIndexedMatchingWithCachedCounts(
    benchmark::State &state) {
  database::GraphDb db;
  std::string label;
  std::string prop;
  int index_count = state.range(0);
  int vertex_count = state.range(1);
  std::tie(label, prop) = CreateIndexedVertices(index_count, vertex_count, db);
  auto dba = db.Access();
  query::DbAccessor exec_dba(&dba);
  auto vertex_counts = query::plan::MakeVertexCountCache(&exec_dba);
  query::Parameters parameters;
  while (state.KeepRunning()) {
    state.PauseTiming();
    query::AstStorage storage;
    auto *query = AddIndexedMatches(index_count, label, prop, storage);
    auto symbol_table = query::MakeSymbolTable(query);
    state.ResumeTiming();
    auto ctx = query::plan::MakePlanningContext(&storage, &symbol_table, query,
                                                &vertex_counts);
    auto query_parts =
        query::plan::CollectQueryParts(symbol_table, storage, query);
    if (query_parts.query_parts.size() == 0) {
      std::exit(EXIT_FAILURE);
    }
    auto single_query_parts = query_parts.query_parts.at(0).single_query_parts;
    auto plans = query::plan::MakeLogicalPlanForSingleQuery<
        query::plan::VariableStartPlanner>(single_query_parts, &ctx);
    for (auto plan : plans) {
      query::plan::EstimatePlanCost(&vertex_counts, parameters, *plan);
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
