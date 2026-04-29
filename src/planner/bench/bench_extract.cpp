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

// Microbenchmarks for the extraction pipeline (planner::core::extract).
//
// Targets `detail::ComputeFrontiers` under three shapes:
//   * deep linear chain          — recursion depth, single enode per eclass.
//   * wide multi-enode eclasses  — exercises ParetoFrontier merge across enodes.
//   * shared-DAG re-visit        — exercises the frontier_map cache.
//
// Cost model carries a small required-set so we land on the Pareto-frontier
// CostResultBase path (the production cost model shape), not the scalar
// DefaultCostResult shortcut.

#include <span>
#include <vector>

#include <benchmark/benchmark.h>

#include <boost/container/flat_set.hpp>
#include <boost/container/small_vector.hpp>

#include "bench_common.hpp"
#include "planner/extract/extractor.hpp"
#include "planner/extract/pareto_frontier.hpp"

import memgraph.planner.core.egraph;

namespace {

using memgraph::planner::bench::NoAnalysis;
using memgraph::planner::bench::Op;
using memgraph::planner::bench::TestEGraph;
using memgraph::planner::core::EClassId;
using memgraph::planner::core::ENodeId;
using memgraph::planner::core::ProcessingContext;
using memgraph::planner::core::extract::CostResultBase;
using memgraph::planner::core::extract::FrontierMap;

using DemandSet = boost::container::flat_set<EClassId, std::less<>, boost::container::small_vector<EClassId, 8>>;

struct DemandAlt {
  double cost;
  DemandSet required;
  ENodeId enode_id;
};

struct DemandDominance {
  static auto operator()(DemandAlt const &a, DemandAlt const &b) -> bool {
    return b.cost <= a.cost && std::ranges::includes(a.required, b.required);
  }
};

struct DemandFrontier : CostResultBase<DemandFrontier, DemandAlt, DemandDominance> {
  using CostResultBase::CostResultBase;
};

struct CostModel {
  using CostResult = DemandFrontier;

  // Per-symbol cost; trivial since we only care about the pipeline shape.
  auto operator()(memgraph::planner::core::ENode<Op> const &node, ENodeId enode_id,
                  std::span<CostResult const> children) const -> CostResult {
    if (children.empty()) {
      return CostResult{{{.cost = 1.0, .required = {}, .enode_id = enode_id}}};
    }
    // Single child: pass-through with +1.
    if (children.size() == 1) {
      auto const &child = children[0];
      std::vector<DemandAlt> out;
      out.reserve(child.alts().size());
      for (auto const &a : child.alts()) {
        out.push_back({.cost = a.cost + 1.0, .required = a.required, .enode_id = enode_id});
      }
      return CostResult::from_unpruned(std::move(out));
    }
    // Two-child: cartesian product, +1 per pair.
    std::vector<DemandAlt> out;
    out.reserve(children[0].alts().size() * children[1].alts().size());
    for (auto const &l : children[0].alts()) {
      for (auto const &r : children[1].alts()) {
        DemandSet req;
        req.insert(l.required.begin(), l.required.end());
        req.insert(r.required.begin(), r.required.end());
        out.push_back({.cost = 1.0 + l.cost + r.cost, .required = std::move(req), .enode_id = enode_id});
      }
    }
    return CostResult::from_unpruned(std::move(out));
  }
};

// Build helpers — invoked once per benchmark size; the for-loop body just
// calls ComputeFrontiers on a fresh frontier_map.  Avoids per-iter rebuild
// dominating wall time.

// Deep linear chain F(F(...const)).
static auto BuildDeepChain(int64_t depth) -> std::pair<TestEGraph, EClassId> {
  TestEGraph egraph;
  ProcessingContext<Op> ctx;
  EClassId current = egraph.emplace(Op::Const, uint64_t{0}).eclass_id;
  for (int64_t i = 0; i < depth; ++i) {
    current = egraph.emplace(Op::F, {current}).eclass_id;
  }
  egraph.rebuild(ctx);
  return {std::move(egraph), current};
}

static void BM_Extract_DeepChain(benchmark::State &state) {
  auto [egraph, root] = BuildDeepChain(state.range(0));
  // Out-of-loop map mirrors production (ExtractionContext owns frontier_map across
  // calls); clear() each iter retains bucket capacity.
  FrontierMap<DemandFrontier> frontier_map;
  frontier_map.reserve(egraph.num_classes());
  for (auto _ : state) {
    frontier_map.clear();
    benchmark::DoNotOptimize(
        memgraph::planner::core::extract::detail::ComputeFrontiers(egraph, CostModel{}, root, frontier_map));
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_Extract_DeepChain)->RangeMultiplier(8)->Range(64, 4096)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// Wide multi-enode eclass — N enodes (each F(const_i) with a distinct leaf)
// share a single canonical eclass via merge().  ComputeFrontiers must merge()
// the per-enode frontiers N-1 times to fold them.  Stresses the rvalue-merge
// path inside the eclass-level accumulation loop.
//
// merge() canonicalises internally (Find on both args), so passing the
// non-canonical `root` repeatedly is safe; we just need to use the latest
// canonical id to ensure the final eclass ID is stable for ComputeFrontiers.
// ---------------------------------------------------------------------------
static auto BuildWideMerge(int64_t fanout) -> std::pair<TestEGraph, EClassId> {
  TestEGraph egraph;
  ProcessingContext<Op> ctx;
  EClassId root = egraph.emplace(Op::F, {egraph.emplace(Op::Const, uint64_t{0}).eclass_id}).eclass_id;
  for (int64_t i = 1; i < fanout; ++i) {
    EClassId leaf = egraph.emplace(Op::Const, static_cast<uint64_t>(i)).eclass_id;
    EClassId sibling = egraph.emplace(Op::F, {leaf}).eclass_id;
    auto result = egraph.merge(root, sibling);
    root = result.eclass_id;  // canonical id after merge
  }
  egraph.rebuild(ctx);
  root = egraph.find(root);  // canonical after rebuild
  return {std::move(egraph), root};
}

static void BM_Extract_WideMerge(benchmark::State &state) {
  auto [egraph, root] = BuildWideMerge(state.range(0));
  FrontierMap<DemandFrontier> frontier_map;
  frontier_map.reserve(egraph.num_classes());
  for (auto _ : state) {
    frontier_map.clear();
    benchmark::DoNotOptimize(
        memgraph::planner::core::extract::detail::ComputeFrontiers(egraph, CostModel{}, root, frontier_map));
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_Extract_WideMerge)->RangeMultiplier(4)->Range(8, 128)->Unit(benchmark::kMicrosecond);

}  // namespace
