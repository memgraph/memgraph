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

// Focused benchmarks for the extraction + ResolvePlanNode pipeline.
//
// Bypasses the Cypher parser by building the egraph directly through the
// public Make* API and calling ConvertToLogicalOperator(egraph, root), which
// invokes the extraction pipeline in egraph_converter.cpp:
//   1. ComputeFrontiers       (bottom-up Pareto frontier propagation)
//   2. ResolvePlanNode           (top-down Bind-aware DFS post-order selection, the focus)
//   3. Builder                (AST emission per selected enode)

#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include "query/plan/operator.hpp"
#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/frontend/egraph_converter.hpp"
#include "storage/v2/property_value.hpp"

namespace {

using memgraph::query::plan::v2::ConvertToLogicalOperator;
using memgraph::query::plan::v2::eclass;
using memgraph::query::plan::v2::egraph;
using memgraph::query::plan::v2::QueryPlannerContext;
using memgraph::storage::ExternalPropertyValue;

// Build a chain of N nested Bind layers, each binding a fresh symbol whose
// expression is a literal-only arithmetic chain.
//
// Per layer i (0-indexed):
//   sym_i  = Symbol(pos=i, "s_i")
//   expr_i = Add(Add(Add(Lit(i), Lit(i+1)), Lit(i+2)), Lit(i+3))   // 4 ops
//   layer_i = Bind(prev_layer, sym_i, expr_i)
// Final root = Output(layer_{N-1}, [NamedOutput(o_i, sym_i', Lit(i)) for i in 0..N])
//
// All Binds are "dead" (no demand propagation), so this exercises the full
// pipeline cost without engaging the alive/dead branch decision.
static auto BuildBindChain(int64_t depth) -> std::pair<egraph, eclass> {
  egraph eg;
  eclass current = eg.MakeOnce();

  for (int64_t i = 0; i < depth; ++i) {
    auto name = "s_" + std::to_string(i);
    auto sym = eg.MakeSymbol(static_cast<int32_t>(i), name);

    auto lit = [&](int64_t v) { return eg.MakeLiteral(ExternalPropertyValue{v}); };
    auto expr = eg.MakeAdd(eg.MakeAdd(eg.MakeAdd(lit(i), lit(i + 1)), lit(i + 2)), lit(i + 3));

    current = eg.MakeBind(current, sym, expr);
  }

  std::vector<eclass> named_outputs;
  named_outputs.reserve(depth);
  for (int64_t i = 0; i < depth; ++i) {
    auto out_name = "o_" + std::to_string(i);
    auto out_sym = eg.MakeSymbol(static_cast<int32_t>(1'000'000 + i), out_name);
    auto out_expr = eg.MakeLiteral(ExternalPropertyValue{static_cast<int64_t>(i)});
    named_outputs.push_back(eg.MakeNamedOutput(out_name, out_sym, out_expr));
  }
  auto root = eg.MakeOutput(current, std::move(named_outputs));
  return {std::move(eg), root};
}

static void BM_PlanV2_BindChain(benchmark::State &state) {
  auto [eg, root] = BuildBindChain(state.range(0));
  // Hold the planner context outside the loop so iterations amortise the
  // per-call buffer allocations the way a long-lived Interpreter would.
  QueryPlannerContext planner_context;
  for (auto _ : state) {
    benchmark::DoNotOptimize(ConvertToLogicalOperator(eg, root, planner_context));
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_PlanV2_BindChain)->RangeMultiplier(2)->Range(4, 256)->Unit(benchmark::kMicrosecond);

// Multi-arg function: surface the sequential-pairwise CombineAlts chain
// over function arguments (egraph_converter.cpp Function case).  Each arg
// is a Range function whose own arg shape adds intermediate frontier
// width.  Use this shape to detect any future regression in n-arg
// composition cost without speculating about its asymptotic shape.
static void BM_PlanV2_MultiArgFunction(benchmark::State &state) {
  egraph eg;
  auto once = eg.MakeOnce();
  auto const num_args = state.range(0);
  std::vector<eclass> args;
  args.reserve(num_args);
  for (int64_t i = 0; i < num_args; ++i) {
    auto a = eg.MakeLiteral(ExternalPropertyValue{i});
    auto b = eg.MakeLiteral(ExternalPropertyValue{i + 5});
    args.push_back(eg.MakeFunction("range", {a, b}));
  }
  auto fn = eg.MakeFunction("coalesce", std::move(args));
  auto sym = eg.MakeSymbol(0, "r");
  auto named = eg.MakeNamedOutput("r", sym, fn);
  auto root = eg.MakeOutput(once, {named});

  QueryPlannerContext planner_context;
  for (auto _ : state) {
    benchmark::DoNotOptimize(ConvertToLogicalOperator(eg, root, planner_context));
  }
  state.SetItemsProcessed(state.iterations() * num_args);
}

BENCHMARK(BM_PlanV2_MultiArgFunction)->RangeMultiplier(2)->Range(2, 16)->Unit(benchmark::kMicrosecond);

}  // namespace

BENCHMARK_MAIN();
