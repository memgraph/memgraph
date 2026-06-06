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

// Steady-state "no-op saturation" benchmark for the plan_v2 rewrite engine.
//
// An e-graph keeps the pre-fold e-node alive in its class, so once a graph is
// saturated the matcher still re-finds every match each pass and each rule body
// re-runs (re-reading analysis, re-interning the folded Literal, re-attempting
// an already-done merge) only for the merge to be a no-op. This benchmark
// isolates that wasted work: it builds a graph that fires every DefaultRules
// rule, saturates once (outside the timed region), then times repeated
// iterate_once() passes on the now-unchanged graph.
//
// The hot loop is one no-op pass, so its cost is purely redundant. Scaling by
// range(0) (the number of independent coverage units) grows the e-graph the
// matcher must re-scan, exposing whether the waste is O(e-graph size) - the
// signal that the fix is incremental / dirty-tracked matching rather than a
// rule-body micro-optimisation.
//
// Profiling (run a SINGLE size so allocations/samples aren't mixed):
//   perf record -g -- ./plan_v2_rewrite_saturation \
//       --benchmark_filter='BM_PlanV2_NoopSaturation/128'
//   heaptrack ./plan_v2_rewrite_saturation \
//       --benchmark_filter='BM_PlanV2_NoopSaturation/128' --benchmark_min_time=2s

#include <cstdint>
#include <string>

#include <benchmark/benchmark.h>

#include "planner/rewrite/rewriter.hpp"
#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"
#include "query/plan_v2/rewrite/rewrites_internal.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"

namespace {

using memgraph::planner::core::rewrite::ArmingMode;
using memgraph::planner::core::rewrite::RewriteConfig;
using memgraph::planner::core::rewrite::Rewriter;
using memgraph::query::plan::v2::DefaultRules;
using memgraph::query::plan::v2::eclass;
using memgraph::query::plan::v2::egraph;
using memgraph::query::plan::v2::impl_of;
using memgraph::storage::ExternalPropertyValue;

// One coverage unit: an independent cluster that triggers every DefaultRules
// rule at least once. `base` offsets the integer literals so units do not
// hash-cons into one another (otherwise replicas would collapse and the graph
// would not actually grow with the scale factor). A few fragments are nested
// (mul feeds on add; the boolean ops and Not feed on comparison results) so the
// initial saturation is genuinely multi-iteration.
void BuildUnit(egraph &eg, int64_t base) {
  auto lit = [&](int64_t v) { return eg.MakeLiteral(ExternalPropertyValue{v}); };
  auto const i1 = lit(base + 1);
  auto const i2 = lit(base + 2);
  auto const i3 = lit(base + 3);
  auto const two = lit(2);  // shared exponent / divisor; non-zero, bounds the result

  // Arithmetic (6): distinct per base, non-zero divisors, bounded exponent.
  auto const add = eg.MakeAdd(i1, i2);
  benchmark::DoNotOptimize(eg.MakeSub(i3, i1));
  benchmark::DoNotOptimize(eg.MakeMul(add, i2));  // composes once `add` folds
  benchmark::DoNotOptimize(eg.MakeDiv(i3, two));
  benchmark::DoNotOptimize(eg.MakeMod(i3, two));
  benchmark::DoNotOptimize(eg.MakeExp(i1, two));

  // Comparison (6): distinct per base, fold to a boolean.
  auto const eq = eg.MakeEq(i1, i1);
  auto const neq = eg.MakeNeq(i1, i2);
  auto const lt = eg.MakeLt(i1, i2);
  auto const lte = eg.MakeLte(i1, i2);
  auto const gt = eg.MakeGt(i2, i1);
  auto const gte = eg.MakeGte(i2, i1);

  // Boolean (3): fed comparison results, so they fold a pass later and stay
  // distinct per base.
  benchmark::DoNotOptimize(eg.MakeAnd(eq, lt));
  benchmark::DoNotOptimize(eg.MakeOr(neq, gt));
  benchmark::DoNotOptimize(eg.MakeXor(lte, gte));

  // Unary (3).
  benchmark::DoNotOptimize(eg.MakeNot(eq));  // composes once `eq` folds
  benchmark::DoNotOptimize(eg.MakeUnaryMinus(i1));
  benchmark::DoNotOptimize(eg.MakeUnaryPlus(i2));

  // Inline rule: Bind a symbol referenced by an Identifier.
  auto const sym = eg.MakeSymbol(static_cast<int32_t>(base), "s_" + std::to_string(base));
  benchmark::DoNotOptimize(eg.MakeBind(eg.MakeOnce(), sym, i1));
  benchmark::DoNotOptimize(eg.MakeIdentifier(sym));
}

auto BuildGraph(int64_t units) -> egraph {
  egraph eg;
  for (int64_t u = 0; u < units; ++u) BuildUnit(eg, u * 1000);
  return eg;
}

void BM_PlanV2_NoopSaturation(benchmark::State &state) {
  auto eg = BuildGraph(state.range(0));
  Rewriter rewriter{impl_of(eg).graph, DefaultRules()};

  // Guard 1: reach a true fixpoint before timing (default limits could stop a
  // large graph short of saturation).
  auto const result = rewriter.saturate(RewriteConfig::Unlimited());
  MG_ASSERT(result.saturated(),
            "setup saturation did not reach a fixpoint (stop_reason={})",
            static_cast<int>(result.stop_reason));

  // Guard 2: every rule must have fired, or the graph no longer covers the rule
  // set and the benchmark would measure the wrong thing.
  for (std::size_t i = 0; i < result.rewrites_per_rule.size(); ++i) {
    MG_ASSERT(result.rewrites_per_rule[i] > 0,
              "coverage gap: rule at index {} never fired - the graph no longer triggers every rule",
              i);
  }

  // Guard 3: a further pass really is a no-op, so the loop measures only waste.
  MG_ASSERT(rewriter.iterate_once() == 0, "graph is not at a fixpoint after saturation");

  for (auto _ : state) {
    benchmark::DoNotOptimize(rewriter.iterate_once());
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_PlanV2_NoopSaturation)->RangeMultiplier(2)->Range(1, 128)->Unit(benchmark::kMicrosecond);

// Full saturate-to-fixpoint, comparing the arming modes. range(0) = units;
// range(1) = mode (0 = ArmAll reference, 1 = Latched). The build cost is
// identical across modes, so the delta between modes at a size is the latch's
// saving on the later passes (settled and irrelevant rules no longer re-run).
void BM_PlanV2_Saturate(benchmark::State &state) {
  auto const units = state.range(0);
  auto const mode = state.range(1) == 0 ? ArmingMode::ArmAll : ArmingMode::Latched;
  for (auto _ : state) {
    auto eg = BuildGraph(units);
    Rewriter rewriter{impl_of(eg).graph, DefaultRules()};
    benchmark::DoNotOptimize(rewriter.saturate(RewriteConfig::Unlimited(), mode));
  }
  state.SetItemsProcessed(state.iterations() * units);
}

BENCHMARK(BM_PlanV2_Saturate)->ArgsProduct({{1, 8, 64}, {0, 1}})->Unit(benchmark::kMicrosecond);

// Re-saturate an already-settled graph - the "saturate then re-apply" scenario
// and the incremental-saturation case. range(1): 0 = ArmAll, 1 = Latched.
// ArmAll re-matches every rule over the whole settled graph each call; Latched
// sees an empty touched-set and returns at once.
void BM_PlanV2_ReSaturate(benchmark::State &state) {
  auto const units = state.range(0);
  auto const mode = state.range(1) == 0 ? ArmingMode::ArmAll : ArmingMode::Latched;
  auto eg = BuildGraph(units);
  Rewriter rewriter{impl_of(eg).graph, DefaultRules()};
  rewriter.saturate(RewriteConfig::Unlimited(), mode);  // settle once, outside timing
  for (auto _ : state) {
    benchmark::DoNotOptimize(rewriter.saturate(RewriteConfig::Unlimited(), mode));
  }
  state.SetItemsProcessed(state.iterations() * units);
}

BENCHMARK(BM_PlanV2_ReSaturate)->ArgsProduct({{1, 8, 64}, {0, 1}})->Unit(benchmark::kMicrosecond);

}  // namespace

BENCHMARK_MAIN();
