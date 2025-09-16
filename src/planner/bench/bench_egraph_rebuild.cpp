// Copyright 2025 Memgraph Ltd.
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
#include <random>
#include <vector>

#include "range/v3/all.hpp"

#include "planner/core/egraph.hpp"
#include "planner/core/processing_context.hpp"

using namespace memgraph::planner::core;

enum class TestSymbol : uint32_t { A, F };

struct NoAnalysis {};

using TestEGraph = EGraph<TestSymbol, NoAnalysis>;
using TestContext = ProcessingContext<TestSymbol>;

// Benchmark leaf node creation (expected to be cheap)
static void BM_EGraph_CongruenceChain(benchmark::State &state) {
  TestEGraph egraph;
  TestContext ctx;

  auto chain_length = state.range(0);

  for (auto _ : state) {
    state.PauseTiming();
    egraph.clear();
    // build the chains

    std::vector<EClassId> chain_head;
    for (auto chain_num = 0; chain_num != 5; ++chain_num) {
      auto previous = egraph.emplace(TestSymbol::A, chain_num);
      chain_head.emplace_back(previous);
      for (auto i = 0; i != chain_length; ++i) {
        previous = egraph.emplace(TestSymbol::F, {previous});
      }
    }
    // merge the chains
    auto rng = ranges::views::zip(chain_head | ranges::views::drop(1), chain_head);
    for (auto [a, b] : rng) {
      egraph.merge(a, b);
    }
    state.ResumeTiming();
    egraph.rebuild(ctx);
  }

  state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_EGraph_CongruenceChain)->Range(100, 10000)->Unit(benchmark::kMicrosecond);
