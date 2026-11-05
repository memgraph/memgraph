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

// What the analysis refresh costs once a symbol actually recomputes facts. The
// other benchmarks here use an analysis with no `update`, where the phase is a
// compile-time no-op, so none of them price it. The synthetic analysis below
// has the shape a real one does: a fact that only grows from the children.

#include <cstdint>

#include <benchmark/benchmark.h>

#include "bench_common.hpp"
#include "planner/rewrite/rewriter.hpp"
#include "test_support/op_make_traits.hpp"

namespace memgraph::planner::bench {

namespace {

using core::BaseProcessingContext;
using core::EClassId;
using core::MakeResult;
using core::SymbolSequence;
using core::TypedEGraph;
using core::test::Op;

/// One bit per seeded leaf, joined by union - associative, commutative and
/// idempotent, so it can only ever grow.
struct MaskAnalysis {
  std::uint64_t mask = 0;

  void merge(MaskAnalysis const &other) { mask |= other.mask; }
};

template <Op S>
struct mask_traits {
  struct storage_type {};

  static auto make(storage_type & /*s*/) -> MakeResult<MaskAnalysis> { return {}; }

  static auto make(storage_type & /*s*/, std::uint64_t bit) -> MakeResult<MaskAnalysis> {
    return {.lowered = {.children = {}, .disambiguator = bit}, .seed = MaskAnalysis{.mask = bit}};
  }

  static auto make(storage_type & /*s*/, EClassId lhs, EClassId rhs) -> MakeResult<MaskAnalysis> {
    return {.lowered = {.children = utils::small_vector<EClassId>{lhs, rhs}, .disambiguator = std::nullopt}};
  }
};

/// `Add` claims whatever its operands reach. The only symbol that recomputes,
/// so the graph pays for the phase while most of its nodes do not use it.
/// Spelled out rather than inheriting the primary, since each trait's storage
/// becomes a distinct base of the graph's combined storage.
template <>
struct mask_traits<Op::Add> {
  struct storage_type {};

  static auto make(storage_type & /*s*/, EClassId lhs, EClassId rhs) -> MakeResult<MaskAnalysis> {
    return {.lowered = {.children = utils::small_vector<EClassId>{lhs, rhs}, .disambiguator = std::nullopt}};
  }

  static auto update(core::EGraph<Op, MaskAnalysis> const &eg, storage_type const & /*s*/,
                     std::span<EClassId const> children, std::uint64_t /*disambiguator*/, MaskAnalysis &out) -> bool {
    auto mask = out.mask;
    for (auto const child : children) mask |= eg.analysis_of(child).mask;
    if (mask == out.mask) return false;
    out.mask = mask;
    return true;
  }
};

/// The same analysis with no `update` declared anywhere, so the graph carries
/// an identical fact and recomputes nothing. Comparing against it separates
/// what tracking changes costs from what carrying a wider fact costs.
template <Op S>
struct mask_seed_only_traits {
  struct storage_type {};

  static auto make(storage_type & /*s*/) -> MakeResult<MaskAnalysis> { return {}; }

  static auto make(storage_type & /*s*/, std::uint64_t bit) -> MakeResult<MaskAnalysis> {
    return {.lowered = {.children = {}, .disambiguator = bit}, .seed = MaskAnalysis{.mask = bit}};
  }

  static auto make(storage_type & /*s*/, EClassId lhs, EClassId rhs) -> MakeResult<MaskAnalysis> {
    return {.lowered = {.children = utils::small_vector<EClassId>{lhs, rhs}, .disambiguator = std::nullopt}};
  }
};

using MaskSeq = SymbolSequence<Op, Op::Add, Op::Mul, Op::Var, Op::Const>;
using MaskEGraph = TypedEGraph<Op, MaskAnalysis, MaskSeq, mask_traits>;
using MaskSeedOnlyEGraph = TypedEGraph<Op, MaskAnalysis, MaskSeq, mask_seed_only_traits>;

static_assert(MaskEGraph::updates_analysis, "the point of this file is a graph that recomputes facts");
static_assert(!MaskSeedOnlyEGraph::updates_analysis, "the control carries the same fact and recomputes nothing");
static_assert(!TypedTestEGraph::updates_analysis, "the other benchmarks price the no-op path");

/// `width` independent Add nodes over their own leaves, plus one Add whose
/// right operand is left unseeded so a later merge can widen it.
///
/// Every leaf takes a distinct disambiguator, without which the graph would not
/// grow with `width` at all: the e-graph hash-conses, so a repeated `Var(1)`
/// returns the class the previous iteration made and every width would build
/// the same six classes. The offsets keep these clear of the leaves the
/// benchmarks below mint for the probe and for the merge.
struct Fixture {
  MaskEGraph eg;
  EClassId opaque{};
  EClassId probe{};

  explicit Fixture(std::int64_t width) {
    for (std::int64_t i = 0; i < width; ++i) {
      auto const a = eg.Make<Op::Var>(static_cast<std::uint64_t>(i + 1)).eclass_id;
      auto const b = eg.Make<Op::Const>(static_cast<std::uint64_t>(i + 16)).eclass_id;
      eg.Make<Op::Add>(a, b);
    }
    opaque = eg.Make<Op::Var>(static_cast<std::uint64_t>(0)).eclass_id;
    auto const seeded = eg.Make<Op::Const>(static_cast<std::uint64_t>(4)).eclass_id;
    probe = eg.Make<Op::Add>(seeded, opaque).eclass_id;
  }
};

/// What the harness itself costs. Pausing and resuming the timer around the
/// fixture is not free, so a reading near this floor says the measured work is
/// below what this method can resolve, not that it took a microsecond. Two
/// pause/resume pairs per iteration, matching the benchmarks below, which also
/// pause to discard their fixture.
void BM_TimerFloor(benchmark::State &state) {
  for (auto _ : state) {
    state.PauseTiming();
    benchmark::DoNotOptimize(state.iterations());
    state.ResumeTiming();
    state.PauseTiming();
    benchmark::DoNotOptimize(state.iterations());
    state.ResumeTiming();
  }
}

BENCHMARK(BM_TimerFloor)->Iterations(30);

/// A single change against a graph of growing size. One fact moves at every
/// width, which the label reports, so the comparison to make is between the
/// time and the size beside it: a time that grows with the graph says the walk
/// visits what exists, and a time that grows far slower than the graph says the
/// walk is bounded by the change and the remainder is the cost of reaching
/// scattered memory in a bigger graph.
///
/// The fixture lives in a scope the timer is paused across, both to build it and
/// to discard it. Letting it fall out of scope with the timer running would
/// charge freeing the whole graph to the refresh, and the reading would grow
/// with `width` whatever the refresh did.
void BM_RefreshAfterOneChange(benchmark::State &state) {
  auto const width = state.range(0);
  std::size_t classes = 0;
  std::size_t moved = 0;
  for (auto _ : state) {
    state.PauseTiming();
    {
      Fixture f{width};
      auto ctx = BaseProcessingContext{};
      f.eg.refresh_analysis(ctx);  // settle what construction dirtied
      // Building the graph announced every class it created. A pass drains that
      // set before running, so leaving it full here would charge the refresh for
      // growing a set the size of the graph on its first announcement.
      f.eg.core().clear_touched();
      auto const widen = f.eg.Make<Op::Const>(static_cast<std::uint64_t>(8)).eclass_id;
      f.eg.core().merge(f.opaque, widen);
      classes = f.eg.core().num_classes();
      state.ResumeTiming();

      moved = f.eg.refresh_analysis(ctx);
      benchmark::DoNotOptimize(moved);
      state.PauseTiming();
    }
    state.ResumeTiming();
  }
  // The size reported is the one the fixture reached, not the one asked for, so
  // a flat reading cannot be read as the refresh scaling well when it is really
  // the graph that failed to grow. The facts that moved are reported for the
  // same reason: they are what the time is meant to be proportional to.
  state.SetLabel("one leaf minted and merged, graph of " + std::to_string(classes) + " e-classes, " +
                 std::to_string(moved) + " facts moved");
}

// Iterations fixed: the timed region is a few microseconds while the fixture
// it needs is not, so a time budget would buy thousands of graph rebuilds.
BENCHMARK(BM_RefreshAfterOneChange)->Arg(0)->Arg(256)->Arg(4096)->Arg(16384)->Iterations(30);

/// A change under one leaf that `fanout` Add nodes all read. Here the cost
/// should grow, because that many facts genuinely moved.
void BM_RefreshFanOut(benchmark::State &state) {
  auto const fanout = state.range(0);
  for (auto _ : state) {
    state.PauseTiming();
    {
      MaskEGraph eg;
      auto ctx = BaseProcessingContext{};
      auto const shared = eg.Make<Op::Var>(static_cast<std::uint64_t>(0)).eclass_id;
      for (std::int64_t i = 0; i < fanout; ++i) {
        // One distinct second operand per reader. Forcing the low bit would pair
        // each even `i` with its successor, hash-cons the two Adds together, and
        // leave half the readers the label claims.
        auto const other = eg.Make<Op::Const>(static_cast<std::uint64_t>(i)).eclass_id;
        eg.Make<Op::Add>(shared, other);
      }
      eg.refresh_analysis(ctx);
      auto const widen = eg.Make<Op::Const>(static_cast<std::uint64_t>(1) << 20U).eclass_id;
      eg.core().merge(shared, widen);
      state.ResumeTiming();

      benchmark::DoNotOptimize(eg.refresh_analysis(ctx));
      state.PauseTiming();
    }
    state.ResumeTiming();
  }
  state.SetLabel(std::to_string(fanout) + " readers of the changed leaf");
}

BENCHMARK(BM_RefreshFanOut)->Arg(1)->Arg(64)->Arg(1024)->Arg(8192)->Iterations(30);

/// Five towers of `depth` Add nodes over a shared leaf, their bases merged
/// pairwise. One rebuild collapses every level of every tower, so by the time
/// the refresh runs its change set holds an entry per merge the cascade made
/// while only a tower's worth of e-classes survived. The benchmarks above each
/// merge a single pair, where that distinction cannot appear.
struct CascadeFixture {
  MaskEGraph eg;
  std::vector<EClassId> bases;

  CascadeFixture(std::int64_t chains, std::int64_t depth) {
    auto const shared = eg.Make<Op::Const>(static_cast<std::uint64_t>(1)).eclass_id;
    for (std::int64_t c = 0; c < chains; ++c) {
      auto previous = eg.Make<Op::Var>(static_cast<std::uint64_t>(1) << static_cast<std::uint64_t>(c + 1)).eclass_id;
      bases.push_back(previous);
      for (std::int64_t d = 0; d < depth; ++d) {
        previous = eg.Make<Op::Add>(previous, shared).eclass_id;
      }
    }
  }
};

void BM_RefreshAfterCascade(benchmark::State &state) {
  auto const depth = state.range(0);
  for (auto _ : state) {
    state.PauseTiming();
    {
      CascadeFixture f{5, depth};
      auto ctx = core::ProcessingContext<Op>{};
      for (std::size_t i = 1; i < f.bases.size(); ++i) {
        f.eg.core().merge(f.bases[i], f.bases[i - 1]);
      }
      f.eg.core().rebuild(ctx);
      state.ResumeTiming();

      benchmark::DoNotOptimize(f.eg.refresh_analysis(ctx));
      state.PauseTiming();
    }
    state.ResumeTiming();
  }
  state.SetLabel("5 towers of depth " + std::to_string(depth) + ", collapsed by one rebuild");
}

BENCHMARK(BM_RefreshAfterCascade)->Arg(16)->Arg(256)->Arg(2048)->Iterations(30);

/// Building a graph with the phase on, against the same build with it off.
/// The gap is what a consumer pays for declaring an update at all, separate
/// from any refresh it then asks for.
template <typename Graph>
void BuildGraph(benchmark::State &state) {
  auto const width = state.range(0);
  for (auto _ : state) {
    Graph eg;
    // Distinct leaves per iteration, for the hash-consing reason in `Fixture`:
    // repeating one leaf would time `width` lookups against a three-class graph
    // instead of the build this is comparing.
    for (std::int64_t i = 0; i < width; ++i) {
      auto const a = eg.template Make<Op::Var>(static_cast<std::uint64_t>(i + 1)).eclass_id;
      auto const b = eg.template Make<Op::Const>(static_cast<std::uint64_t>(i + 16)).eclass_id;
      eg.template Make<Op::Add>(a, b);
    }
    benchmark::DoNotOptimize(eg.core().num_classes());
  }
}

void BM_BuildWithUpdatingAnalysis(benchmark::State &state) { BuildGraph<MaskEGraph>(state); }

BENCHMARK(BM_BuildWithUpdatingAnalysis)->Arg(256)->Arg(4096)->Iterations(50);

void BM_BuildSameFactNoUpdate(benchmark::State &state) { BuildGraph<MaskSeedOnlyEGraph>(state); }

BENCHMARK(BM_BuildSameFactNoUpdate)->Arg(256)->Arg(4096)->Iterations(50);

void BM_BuildWithoutUpdatingAnalysis(benchmark::State &state) { BuildGraph<TypedTestEGraph>(state); }

BENCHMARK(BM_BuildWithoutUpdatingAnalysis)->Arg(256)->Arg(4096)->Iterations(50);

}  // namespace

}  // namespace memgraph::planner::bench
