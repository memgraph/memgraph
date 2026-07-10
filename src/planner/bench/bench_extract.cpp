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

#include <span>
#include <vector>

#include <benchmark/benchmark.h>

#include <boost/container/flat_set.hpp>
#include <boost/container/small_vector.hpp>

#include "bench_common.hpp"
#include "planner/extract/extractor.hpp"
#include "planner/extract/pareto_frontier.hpp"
#include "test_support/extract.hpp"

import memgraph.planner.core.egraph;

namespace {

using memgraph::planner::bench::NoAnalysis;
using memgraph::planner::bench::Op;
using memgraph::planner::bench::TestEGraph;
using memgraph::planner::core::EClassId;
using memgraph::planner::core::ENodeId;
using memgraph::planner::core::ProcessingContext;
using memgraph::planner::core::extract::CostResultBase;
using memgraph::planner::core::extract::FrontierContext;

using DemandSet = boost::container::flat_set<EClassId, std::less<>, boost::container::small_vector<EClassId, 8>>;

struct DemandAlt {
  double cost;
  DemandSet required;
  ENodeId enode_id;
};

namespace x = memgraph::planner::core::extract;

/// DemandAlt's Pareto dims: lower cost, smaller required-set.
using DemandDim_Cost = x::Dim<&DemandAlt::cost, x::LowerIsBetter>;
using DemandDim_Required = x::Dim<&DemandAlt::required, x::SmallerSubsetIsBetter>;

struct DemandFrontier : CostResultBase<DemandAlt, DemandDim_Cost, DemandDim_Required> {
  using CostResultBase::CostResultBase;
};

struct CostModel {
  using CostResult = DemandFrontier;

  // Per-symbol cost; trivial since we only care about the pipeline shape.
  auto operator()(memgraph::planner::core::ENode<Op> const &node, ENodeId enode_id,
                  std::span<CostResult const *const> children) const -> CostResult {
    if (children.empty()) {
      return CostResult{{{.cost = 1.0, .required = {}, .enode_id = enode_id}}};
    }
    // Single child: pass-through with +1.  Lazy view; no materialisation
    // until something iterates the result.
    if (children.size() == 1) {
      return CostResult::LazyMap(*children[0], 1.0, enode_id);
    }
    return CostResult::cartesian_product(
        *children[0], *children[1], [enode_id](DemandAlt const &l, DemandAlt const &r) {
          DemandSet req;
          req.insert(l.required.begin(), l.required.end());
          req.insert(r.required.begin(), r.required.end());
          return DemandAlt{.cost = 1.0 + l.cost + r.cost, .required = std::move(req), .enode_id = enode_id};
        });
  }
};

// Setup is out-of-loop; the for-loop body just calls ComputeFrontiers on a
// fresh frontier_map.  Avoids per-iter rebuild dominating wall time.

using memgraph::planner::core::test::BuildDeepChain;
using memgraph::planner::core::test::BuildWideMerge;

void BM_Extract_DeepChain(benchmark::State &state) {
  TestEGraph egraph;
  ProcessingContext<Op> ctx;
  EClassId root = BuildDeepChain(egraph, state.range(0));
  egraph.rebuild(ctx);
  // Out-of-loop map mirrors production (FrontierContext owns frontier_map across
  // calls); clear() each iter retains bucket capacity.
  FrontierContext<DemandFrontier> frontier_ctx;
  frontier_ctx.frontier_map.reserve(egraph.num_classes());
  for (auto _ : state) {
    frontier_ctx.clear();
    benchmark::DoNotOptimize(
        memgraph::planner::core::extract::ComputeFrontiers(egraph, CostModel{}, root, frontier_ctx));
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_Extract_DeepChain)
    ->Name("Extract/ComputeFrontiers/DeepChain")
    ->RangeMultiplier(8)
    ->Range(64, 4096)
    ->ArgName("size")
    ->Unit(benchmark::kMicrosecond);

// Wide multi-enode eclass - N enodes (each F(const_i) with a distinct leaf)
// share a single canonical eclass via merge().  ComputeFrontiers must merge()
// the per-enode frontiers N-1 times to fold them.  Stresses the rvalue-merge
// path inside the eclass-level accumulation loop.
void BM_Extract_WideMerge(benchmark::State &state) {
  TestEGraph egraph;
  ProcessingContext<Op> ctx;
  EClassId root = BuildWideMerge(egraph, state.range(0));
  egraph.rebuild(ctx);
  root = egraph.find(root);  // canonical after rebuild
  FrontierContext<DemandFrontier> frontier_ctx;
  frontier_ctx.frontier_map.reserve(egraph.num_classes());
  for (auto _ : state) {
    frontier_ctx.clear();
    benchmark::DoNotOptimize(
        memgraph::planner::core::extract::ComputeFrontiers(egraph, CostModel{}, root, frontier_ctx));
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_Extract_WideMerge)
    ->Name("Extract/ComputeFrontiers/WideMerge")
    ->RangeMultiplier(4)
    ->Range(8, 128)
    ->ArgName("width")
    ->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// Full-pipeline shapes - ComputeFrontiers + DefaultResolver
// ---------------------------------------------------------------------------
using memgraph::planner::test_support::DefaultResolver;

static void BM_Extract_FullPipeline_DeepChain(benchmark::State &state) {
  TestEGraph egraph;
  ProcessingContext<Op> ctx;
  EClassId root = BuildDeepChain(egraph, state.range(0));
  egraph.rebuild(ctx);
  FrontierContext<DemandFrontier> frontier_ctx;
  frontier_ctx.frontier_map.reserve(egraph.num_classes());
  std::vector<std::pair<EClassId, ENodeId>> out;
  out.reserve(egraph.num_classes());
  for (auto _ : state) {
    frontier_ctx.clear();
    out.clear();
    (void)memgraph::planner::core::extract::ComputeFrontiers(egraph, CostModel{}, root, frontier_ctx);
    DefaultResolver{}(egraph, frontier_ctx.frontier_map, root, out);
    benchmark::DoNotOptimize(out);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_Extract_FullPipeline_DeepChain)
    ->Name("Extract/FullPipeline/DeepChain")
    ->RangeMultiplier(8)
    ->Range(64, 4096)
    ->ArgName("size")
    ->Unit(benchmark::kMicrosecond);

void BM_Extract_FullPipeline_WideMerge(benchmark::State &state) {
  TestEGraph egraph;
  ProcessingContext<Op> ctx;
  EClassId root = BuildWideMerge(egraph, state.range(0));
  egraph.rebuild(ctx);
  root = egraph.find(root);  // canonical after rebuild
  FrontierContext<DemandFrontier> frontier_ctx;
  frontier_ctx.frontier_map.reserve(egraph.num_classes());
  std::vector<std::pair<EClassId, ENodeId>> out;
  out.reserve(egraph.num_classes());
  for (auto _ : state) {
    frontier_ctx.clear();
    out.clear();
    (void)memgraph::planner::core::extract::ComputeFrontiers(egraph, CostModel{}, root, frontier_ctx);
    DefaultResolver{}(egraph, frontier_ctx.frontier_map, root, out);
    benchmark::DoNotOptimize(out);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_Extract_FullPipeline_WideMerge)
    ->Name("Extract/FullPipeline/WideMerge")
    ->RangeMultiplier(4)
    ->Range(8, 128)
    ->ArgName("width")
    ->Unit(benchmark::kMicrosecond);

}  // namespace
