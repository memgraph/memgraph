#include <string>

#include <benchmark/benchmark_api.h>

#include "database/dbms.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/planner.hpp"

// Add chained MATCH (node1) -- (node2), MATCH (node2) -- (node3) ... clauses.
static void AddMatches(int num_matches, query::AstTreeStorage &storage) {
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

static void BM_MakeLogicalPlan(benchmark::State &state) {
  while (state.KeepRunning()) {
    state.PauseTiming();
    Dbms dbms;
    auto dba = dbms.active();
    query::AstTreeStorage storage;
    int num_matches = state.range(0);
    AddMatches(num_matches, storage);
    query::SymbolTable symbol_table;
    query::SymbolGenerator symbol_generator(symbol_table);
    storage.query()->Accept(symbol_generator);
    state.ResumeTiming();
    query::plan::MakeLogicalPlan<query::plan::VariableStartPlanner>(
        storage, symbol_table, *dba);
  }
};

BENCHMARK(BM_MakeLogicalPlan)
    ->RangeMultiplier(2)
    ->Range(50, 400)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
