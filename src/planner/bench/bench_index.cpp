// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "bench_common.hpp"

#include <array>
#include <vector>

using namespace memgraph::planner::bench;

// Index rebuild happens after every e-graph modification.

class IndexBuildFixture : public MatcherFixtureBase {
 protected:
  using benchmark::Fixture::SetUp;
  int64_t graph_size_ = 0;

  void SetUp(const benchmark::State &state) override {
    graph_size_ = state.range(0);
    SetupGraphAndMatcher([this](TestEGraph &g) { BuildIndependentAdds(g, graph_size_); });
  }
};

BENCHMARK_DEFINE_F(IndexBuildFixture, FullRebuild)(benchmark::State &state) {
  for (auto _ : state) {
    matcher_->rebuild_index();
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * graph_size_);
}

BENCHMARK_REGISTER_F(IndexBuildFixture, FullRebuild)
    ->Name("Index/FullRebuild")
    ->Args({kSmall})
    ->Args({kMedium})
    ->Args({kLarge})
    ->Args({kMassive})
    ->ArgNames({"size"})
    ->Unit(benchmark::kMicrosecond);

// Incremental updates should be faster than full rebuilds.

class IncrementalUpdateFixture : public MatcherFixtureBase {
 protected:
  using benchmark::Fixture::SetUp;
  int64_t initial_size_ = 0;
  int64_t increment_size_ = 0;

  void SetUp(const benchmark::State &state) override {
    initial_size_ = state.range(0);
    increment_size_ = state.range(1);
    SetupGraphAndMatcher([this](TestEGraph &g) { BuildIndependentAdds(g, initial_size_); });
  }
};

BENCHMARK_DEFINE_F(IncrementalUpdateFixture, Update)(benchmark::State &state) {
  int64_t var_counter = initial_size_ * 2;
  for (auto _ : state) {
    state.PauseTiming();
    std::vector<EClassId> added_eclasses;
    for (int64_t i = 0; i < increment_size_; ++i) {
      auto var1 = egraph_.emplace(Op::Var, static_cast<uint64_t>(var_counter++)).eclass_id;
      auto var2 = egraph_.emplace(Op::Var, static_cast<uint64_t>(var_counter++)).eclass_id;
      auto add = egraph_.emplace(Op::Add, {var1, var2}).eclass_id;
      added_eclasses.append_range(std::array{var1, var2, add});
    }
    state.ResumeTiming();
    matcher_->rebuild_index(added_eclasses);
  }
  state.SetItemsProcessed(state.iterations() * increment_size_);
}

BENCHMARK_REGISTER_F(IncrementalUpdateFixture, Update)
    ->Name("Index/IncrementalUpdate")
    ->ArgsProduct({{kMedium, kLarge, kMassive}, {10, 50, 100}})
    ->ArgNames({"init", "incr"})
    ->Unit(benchmark::kMicrosecond);
