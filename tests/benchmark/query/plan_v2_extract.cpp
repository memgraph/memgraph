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

// Focused benchmarks for the planner-v2 extraction + PlanResolver pipeline.
//
// Bypasses the Cypher parser by building the egraph directly through the
// public Make* API and calling ConvertToLogicalOperator(egraph, root).  That
// invokes all five extraction stages in egraph_converter.cpp:
//   1. ComputeFrontiers        — bottom-up Pareto frontier propagation
//   2. PlanResolver            — top-down Bind-aware selection (the focus)
//   3. CollectDependencies     — in-degree counting over selected enodes
//   4. TopologicalSort         — Kahn-order materialisation
//   5. Builder                 — AST emission per selected enode
//
// The pipeline integration test (tests/unit/query_plan_v2_pipeline.cpp) is
// dominated by ANTLR parsing for the small Cypher queries it exercises, so
// extraction-side perf deltas are below the noise floor there.  This bench
// puts the whole iteration budget into the planner.

#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include "query/plan/operator.hpp"
#include "query/plan_v2/egraph.hpp"
#include "query/plan_v2/egraph_converter.hpp"
#include "storage/v2/property_value.hpp"

namespace {

using memgraph::query::plan::v2::ConvertToLogicalOperator;
using memgraph::query::plan::v2::eclass;
using memgraph::query::plan::v2::egraph;
using memgraph::storage::ExternalPropertyValue;

// Build a chain of N nested Bind layers, each binding a fresh symbol whose
// expression is an arithmetic chain of literals (no unresolved Identifiers —
// production rewrites inline those before extraction; we skip the rewrite
// pipeline here and build the post-rewrite shape directly).
//
// Resulting shape per layer i (0-indexed):
//   sym_i  = Symbol(pos=i, "s_i")
//   expr_i = Add(Add(Add(Lit(i), Lit(i+1)), Lit(i+2)), Lit(i+3))   // 4 ops
//   layer_i = Bind(prev_layer, sym_i, expr_i)
// Final root = Output(layer_{N-1}, [NamedOutput(o_i, sym_i', Lit(i)) for i in 0..N])
//
// Every Bind has expr.required == {} (literals only), and Output's
// named_outputs are also required-free.  All paths therefore have a
// self-contained alternative at the root.  Binds are "dead" w.r.t. the input
// (no demand-set propagation upward), so this exercises:
//   - ComputeFrontiers across N+1 eclasses with cartesian products in expr
//   - PlanResolver's recursive descent + per-eclass cache
//   - CollectDependencies / TopologicalSort over the selected enodes
//   - Builder constructing one AST per selected enode
//
// What it doesn't exercise: the alive-Bind alive/dead branch decision.  To
// hit that we'd need the rewrite layer to inline Identifiers; that lives
// outside the public Make* API and is harder to spool up here.
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
  auto root = eg.MakeOutputs(current, std::move(named_outputs));
  return {std::move(eg), root};
}

static void BM_PlanV2_BindChain(benchmark::State &state) {
  auto [eg, root] = BuildBindChain(state.range(0));
  for (auto _ : state) {
    benchmark::DoNotOptimize(ConvertToLogicalOperator(eg, root));
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_PlanV2_BindChain)->RangeMultiplier(2)->Range(4, 256)->Unit(benchmark::kMicrosecond);

}  // namespace

BENCHMARK_MAIN();
