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

// ============================================================================
// Egglog-style Benchmarks
// ============================================================================
//
// These benchmarks use a complex e-graph structure matching the egglog
// comparison tests. The graph has:
//   - 180 Const leaves, 60 Var leaves
//   - 320 layers of Add/Neg/Mul/F/F2 nodes
//   - Periodic unions creating non-trivial e-classes
//
// Benchmarks measure:
//   1. Pattern compilation (VM compiler)
//   2. Pattern matching (EMatcher vs VM executor)
//
// This replaces the old benchmark__egglog.cpp GTest-based benchmark.
// ============================================================================

#include "bench_common.hpp"

#include <array>
#include <optional>

using namespace memgraph::planner::bench;
using namespace memgraph::planner::bench::ranges;
using namespace memgraph::planner::core::vm;

// ============================================================================
// Fixture: Complex E-Graph (Egglog-style)
// ============================================================================

class EgglogFixtureBase : public VMFixtureBase {
 protected:
  void SetUp(const benchmark::State &) override {
    SetupGraph([](TestEGraph &g) { BuildComplexGraph(g); });
  }
};

// ============================================================================
// Pattern Compilation Benchmarks
// ============================================================================
//
// Measures: Cost of compiling patterns to VM bytecode.
// Why it matters: Compilation happens once per pattern, amortized over matches.

class PatternCompileFixture : public EgglogFixtureBase {};

BENCHMARK_DEFINE_F(PatternCompileFixture, CompileNeg)(benchmark::State &state) {
  auto pattern = PatternEgglogNeg();
  for (auto _ : state) {
    auto compiled = compiler_.compile(pattern);
    benchmark::DoNotOptimize(compiled);
  }
}

BENCHMARK_REGISTER_F(PatternCompileFixture, CompileNeg)->Unit(benchmark::kNanosecond);

BENCHMARK_DEFINE_F(PatternCompileFixture, CompileAddSame)(benchmark::State &state) {
  auto pattern = PatternEgglogAddSame();
  for (auto _ : state) {
    auto compiled = compiler_.compile(pattern);
    benchmark::DoNotOptimize(compiled);
  }
}

BENCHMARK_REGISTER_F(PatternCompileFixture, CompileAddSame)->Unit(benchmark::kNanosecond);

BENCHMARK_DEFINE_F(PatternCompileFixture, CompileFAddNeg)(benchmark::State &state) {
  auto pattern = PatternEgglogFAddNeg();
  for (auto _ : state) {
    auto compiled = compiler_.compile(pattern);
    benchmark::DoNotOptimize(compiled);
  }
}

BENCHMARK_REGISTER_F(PatternCompileFixture, CompileFAddNeg)->Unit(benchmark::kNanosecond);

BENCHMARK_DEFINE_F(PatternCompileFixture, CompileF2FMul)(benchmark::State &state) {
  auto pattern = PatternEgglogF2FMul();
  for (auto _ : state) {
    auto compiled = compiler_.compile(pattern);
    benchmark::DoNotOptimize(compiled);
  }
}

BENCHMARK_REGISTER_F(PatternCompileFixture, CompileF2FMul)->Unit(benchmark::kNanosecond);

BENCHMARK_DEFINE_F(PatternCompileFixture, CompileTest)(benchmark::State &state) {
  auto pattern = PatternEgglogTest();
  for (auto _ : state) {
    auto compiled = compiler_.compile(pattern);
    benchmark::DoNotOptimize(compiled);
  }
}

BENCHMARK_REGISTER_F(PatternCompileFixture, CompileTest)->Unit(benchmark::kNanosecond);

// Compile all 5 patterns together (amortized)
BENCHMARK_DEFINE_F(PatternCompileFixture, CompileAllPatterns)(benchmark::State &state) {
  std::array patterns = {
      PatternEgglogNeg(), PatternEgglogAddSame(), PatternEgglogFAddNeg(), PatternEgglogF2FMul(), PatternEgglogTest()};
  for (auto _ : state) {
    for (auto const &p : patterns) {
      auto compiled = compiler_.compile(p);
      benchmark::DoNotOptimize(compiled);
    }
  }
  state.SetItemsProcessed(state.iterations() * patterns.size());
}

BENCHMARK_REGISTER_F(PatternCompileFixture, CompileAllPatterns)->Unit(benchmark::kNanosecond);

// ============================================================================
// EMatcher Pattern Matching Benchmarks
// ============================================================================
//
// Measures: Cost of pattern matching using EMatcher (tree-walking approach).

class EMatcherMatchFixture : public EgglogFixtureBase {};

BENCHMARK_DEFINE_F(EMatcherMatchFixture, MatchNeg)(benchmark::State &state) {
  auto pattern = PatternEgglogNeg();
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    matcher_->match_into(pattern, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.counters["matches"] = static_cast<double>(matches_.size());
}

BENCHMARK_REGISTER_F(EMatcherMatchFixture, MatchNeg)->Unit(benchmark::kMicrosecond);

BENCHMARK_DEFINE_F(EMatcherMatchFixture, MatchAddSame)(benchmark::State &state) {
  auto pattern = PatternEgglogAddSame();
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    matcher_->match_into(pattern, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.counters["matches"] = static_cast<double>(matches_.size());
}

BENCHMARK_REGISTER_F(EMatcherMatchFixture, MatchAddSame)->Unit(benchmark::kMicrosecond);

BENCHMARK_DEFINE_F(EMatcherMatchFixture, MatchFAddNeg)(benchmark::State &state) {
  auto pattern = PatternEgglogFAddNeg();
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    matcher_->match_into(pattern, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.counters["matches"] = static_cast<double>(matches_.size());
}

BENCHMARK_REGISTER_F(EMatcherMatchFixture, MatchFAddNeg)->Unit(benchmark::kMicrosecond);

BENCHMARK_DEFINE_F(EMatcherMatchFixture, MatchF2FMul)(benchmark::State &state) {
  auto pattern = PatternEgglogF2FMul();
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    matcher_->match_into(pattern, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.counters["matches"] = static_cast<double>(matches_.size());
}

BENCHMARK_REGISTER_F(EMatcherMatchFixture, MatchF2FMul)->Unit(benchmark::kMicrosecond);

BENCHMARK_DEFINE_F(EMatcherMatchFixture, MatchTest)(benchmark::State &state) {
  auto pattern = PatternEgglogTest();
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    matcher_->match_into(pattern, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.counters["matches"] = static_cast<double>(matches_.size());
}

BENCHMARK_REGISTER_F(EMatcherMatchFixture, MatchTest)->Unit(benchmark::kMicrosecond);

// Match all 5 patterns consecutively
BENCHMARK_DEFINE_F(EMatcherMatchFixture, MatchAllPatterns)(benchmark::State &state) {
  std::array patterns = {
      PatternEgglogNeg(), PatternEgglogAddSame(), PatternEgglogFAddNeg(), PatternEgglogF2FMul(), PatternEgglogTest()};
  std::size_t total_matches = 0;
  for (auto _ : state) {
    for (auto const &p : patterns) {
      match_context_.clear();
      matches_.clear();
      matcher_->match_into(p, match_context_, matches_);
      benchmark::DoNotOptimize(matches_);
      total_matches = matches_.size();  // Last iteration's count
    }
  }
  state.SetItemsProcessed(state.iterations() * patterns.size());
}

BENCHMARK_REGISTER_F(EMatcherMatchFixture, MatchAllPatterns)->Unit(benchmark::kMicrosecond);

// ============================================================================
// VM Executor Pattern Matching Benchmarks
// ============================================================================
//
// Measures: Cost of pattern matching using VM executor (bytecode approach).
// Patterns are pre-compiled; this measures execution only.

class VMMatchFixture : public EgglogFixtureBase {
 protected:
  std::optional<CompiledPattern<Op>> compiled_neg_;
  std::optional<CompiledPattern<Op>> compiled_add_same_;
  std::optional<CompiledPattern<Op>> compiled_f_add_neg_;
  std::optional<CompiledPattern<Op>> compiled_f2_f_mul_;
  std::optional<CompiledPattern<Op>> compiled_test_;

  void SetUp(const benchmark::State &state) override {
    EgglogFixtureBase::SetUp(state);
    compiled_neg_ = compiler_.compile(PatternEgglogNeg());
    compiled_add_same_ = compiler_.compile(PatternEgglogAddSame());
    compiled_f_add_neg_ = compiler_.compile(PatternEgglogFAddNeg());
    compiled_f2_f_mul_ = compiler_.compile(PatternEgglogF2FMul());
    compiled_test_ = compiler_.compile(PatternEgglogTest());
  }
};

BENCHMARK_DEFINE_F(VMMatchFixture, MatchNeg)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    executor.execute(*compiled_neg_, *matcher_, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.counters["matches"] = static_cast<double>(matches_.size());
}

BENCHMARK_REGISTER_F(VMMatchFixture, MatchNeg)->Unit(benchmark::kMicrosecond);

BENCHMARK_DEFINE_F(VMMatchFixture, MatchAddSame)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    executor.execute(*compiled_add_same_, *matcher_, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.counters["matches"] = static_cast<double>(matches_.size());
}

BENCHMARK_REGISTER_F(VMMatchFixture, MatchAddSame)->Unit(benchmark::kMicrosecond);

BENCHMARK_DEFINE_F(VMMatchFixture, MatchFAddNeg)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    executor.execute(*compiled_f_add_neg_, *matcher_, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.counters["matches"] = static_cast<double>(matches_.size());
}

BENCHMARK_REGISTER_F(VMMatchFixture, MatchFAddNeg)->Unit(benchmark::kMicrosecond);

BENCHMARK_DEFINE_F(VMMatchFixture, MatchF2FMul)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    executor.execute(*compiled_f2_f_mul_, *matcher_, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.counters["matches"] = static_cast<double>(matches_.size());
}

BENCHMARK_REGISTER_F(VMMatchFixture, MatchF2FMul)->Unit(benchmark::kMicrosecond);

BENCHMARK_DEFINE_F(VMMatchFixture, MatchTest)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    executor.execute(*compiled_test_, *matcher_, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.counters["matches"] = static_cast<double>(matches_.size());
}

BENCHMARK_REGISTER_F(VMMatchFixture, MatchTest)->Unit(benchmark::kMicrosecond);

// Match all 5 patterns consecutively (pre-compiled)
BENCHMARK_DEFINE_F(VMMatchFixture, MatchAllPatterns)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  std::array compiled = {
      &compiled_neg_, &compiled_add_same_, &compiled_f_add_neg_, &compiled_f2_f_mul_, &compiled_test_};
  for (auto _ : state) {
    for (auto const *c : compiled) {
      match_context_.clear();
      matches_.clear();
      executor.execute(**c, *matcher_, match_context_, matches_);
      benchmark::DoNotOptimize(matches_);
    }
  }
  state.SetItemsProcessed(state.iterations() * compiled.size());
}

BENCHMARK_REGISTER_F(VMMatchFixture, MatchAllPatterns)->Unit(benchmark::kMicrosecond);

// ============================================================================
// VM Compile + Execute (Full Pipeline)
// ============================================================================
//
// Measures: Cost of compile + execute together (no pre-compilation).
// This is the fair comparison against EMatcher which has no separate compile step.

class VMFullPipelineFixture : public EgglogFixtureBase {};

BENCHMARK_DEFINE_F(VMFullPipelineFixture, CompileAndMatchAllPatterns)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  std::array patterns = {
      PatternEgglogNeg(), PatternEgglogAddSame(), PatternEgglogFAddNeg(), PatternEgglogF2FMul(), PatternEgglogTest()};
  for (auto _ : state) {
    for (auto const &p : patterns) {
      auto compiled = compiler_.compile(p);
      match_context_.clear();
      matches_.clear();
      executor.execute(*compiled, *matcher_, match_context_, matches_);
      benchmark::DoNotOptimize(matches_);
    }
  }
  state.SetItemsProcessed(state.iterations() * patterns.size());
}

BENCHMARK_REGISTER_F(VMFullPipelineFixture, CompileAndMatchAllPatterns)->Unit(benchmark::kMicrosecond);
