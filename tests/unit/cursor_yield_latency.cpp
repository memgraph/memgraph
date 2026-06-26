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

// COOPERATIVE-YIELD LATENCY MICROBENCH — INTER-YIELD INTERVAL
//
// METRIC: MAX UNINTERRUPTIBLE SPAN = the wall-time between two consecutive
// points where a yield can actually fire.  That is the worst-case latency a
// scheduler must wait before the worker parks.
//
// CHAIN TOPOLOGY: N=8 PullAwaitable coroutines.
//   depth 0 (root, MiddleNode) -- co_await depth1.Resume() -- ... -- depth N-1 (leaf, LeafNode)
//
// YIELD-POINT PLACEMENT:
//   • MiddleNode at depth d:   has YPA at the TOP of its per-output-row loop.
//                              (fires between output rows; independent of M)
//   • LeafNode at depth N-1:   has YPA INSIDE the inner m-consume loop when
//                              `leaf_has_yield_point` is true.
//                              (fires between each of the M sub-items; this is
//                              the KEY that gates uninterruptible-span length)
//
// yield_depth S = number of TOP frames (depths 0..S-1) that have a YPA.
// The leaf (depth N-1) has a YPA when N-1 < S, i.e. when S == N (full_coro).
// For S < N the leaf has NO mid-consume yield point — the M-item loop runs
// uninterrupted between two MiddleNode-level yields.
//
// THREE MODES:
//   root_only  : S = 1     only root MiddleNode has YPA; leaf has NONE
//   split      : S = N/2   top 4 MiddleNodes have YPA; leaf has NONE
//   full_coro  : S = N     all MiddleNodes have YPA AND leaf has YPA inside consume
//
// MEASUREMENT:
//   Drive one chain to completion with yr=true, period=1.  Record a wall-clock
//   timestamp at each Yielded return from ResumePullStep.  Compute per-cell:
//     inter-yield interval  = t[i] - t[i-1]  (first = t[0] - t_drive_start)
//     statistics: median_ns, p99_ns, max_ns of all intervals
//     total_yields, max_items_consumed_between_yields (= M for non-leaf modes,
//     1 for full_coro leaf mode)
//
// EXPECTED SHAPE (worst, M=100000):
//   full_coro  << split < root_only
//   (full_coro can yield after every DoWork call;
//    root_only must finish all 100k DoWork calls before the next yield fires)
//
// SCENARIOS:
//   best        : work_units=50,   M=1        (streaming, cheap)
//   real_world  : work_units=500,  M=4        (expand+filter)
//   worst       : work_units=500,  M=100000   (blocking aggregate)
//
// RUNTIME BOUNDS:
//   worst (M=100k, work_units=500): one output row = 100k*500 work iterations.
//   We use output_rows=20 for worst: root_only/split get up to 20 yield intervals
//   (one per output row); full_coro hits the max_yields=500 cap within the first
//   output row (it yields per inner item: 100k items > 500 cap).
//   best/real_world use more output rows for a richer distribution.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <cstdio>
#include <vector>

#include "query/context.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/cursor_awaitable.hpp"
#include "query/plan/cursor_awaitable_core.hpp"
#include "utils/counter.hpp"

namespace memgraph::query::plan {

// ─────────────────────────────────────────────────────────────────────────────
// Calibrated busy-loop. volatile prevents optimizer from eliding the work.
// ─────────────────────────────────────────────────────────────────────────────
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
static volatile uint64_t g_sink = 1;

static inline void DoWork(int work_units) noexcept {
  uint64_t s = g_sink;
  for (int k = 0; k < work_units; ++k) {
    s = s * static_cast<uint64_t>(k) + 1;
  }
  g_sink = s;
}

static constexpr int kChainDepth = 8;  // N

// ─────────────────────────────────────────────────────────────────────────────
// LeafNode — bottom-most coroutine (depth N-1).
//
// For each of total_output_rows output rows:
//   for m in 0..blocking_factor-1:
//     if has_yield_point:  co_await YPA   ← fires per inner item
//     DoWork(work_units)
//   co_yield true
//
// When has_yield_point==false the entire M-item consume runs uninterrupted
// between the MiddleNode-level yields above.
// When has_yield_point==true  the leaf can suspend between every DoWork call.
// ─────────────────────────────────────────────────────────────────────────────
static PullAwaitable LeafNode(Frame & /*f*/, ExecutionContext &ctx, int total_output_rows, int work_units,
                              int blocking_factor, bool has_yield_point) {
  utils::ResettableCounter counter{1};  // period=1: every call fires

  for (int out = 0; out < total_output_rows; ++out) {
    for (int m = 0; m < blocking_factor; ++m) {
      // YPA inside the inner consume loop — this is the KEY structural
      // difference: full_coro can suspend here; other modes cannot.
      if (has_yield_point) {
        co_await YieldPointAwaitable(ctx, counter);
      }
      DoWork(work_units);
    }
    co_yield true;
  }
  co_return false;
}

// ─────────────────────────────────────────────────────────────────────────────
// MiddleNode — pass-through at some depth 0..N-2.
//
// Per output-row loop:
//   if has_yield_point:  co_await YPA   ← fires between output rows
//   co_await child.Resume()
//   co_yield true
// ─────────────────────────────────────────────────────────────────────────────
static PullAwaitable MiddleNode(Frame & /*f*/, ExecutionContext &ctx, PullAwaitable &child, bool has_yield_point) {
  utils::ResettableCounter counter{1};

  while (true) {
    if (has_yield_point) {
      co_await YieldPointAwaitable(ctx, counter);
    }
    const bool has = co_await child.Resume();
    if (!has) co_return false;
    co_yield true;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Chain — owns all N PullAwaitables.
//   nodes[0]   = leaf (depth N-1)
//   nodes[k]   = middle at depth N-1-k
//   nodes[N-1] = root (depth 0)
// ra: non-owning view into the root.  All nodes must outlive ra.
// ─────────────────────────────────────────────────────────────────────────────
struct Chain {
  std::vector<PullAwaitable> nodes;
  PullAwaitable::ResumeAwaitable ra;
};

static Chain BuildChain(Frame &frame, ExecutionContext &ctx, int yield_depth, int total_output_rows, int work_units,
                        int blocking_factor) {
  Chain c;
  c.nodes.reserve(static_cast<std::size_t>(kChainDepth));

  // Leaf at depth kChainDepth-1 gets a YPA only in full_coro (S == N).
  const bool leaf_has_yield = (kChainDepth - 1) < yield_depth;
  c.nodes.push_back(LeafNode(frame, ctx, total_output_rows, work_units, blocking_factor, leaf_has_yield));

  // Middle nodes at depths kChainDepth-2 .. 0 (root).
  for (int depth = kChainDepth - 2; depth >= 0; --depth) {
    const bool mid_has_yield = depth < yield_depth;
    c.nodes.push_back(MiddleNode(frame, ctx, c.nodes.back(), mid_has_yield));
  }

  c.ra = c.nodes.back().Resume();
  return c;
}

// ─────────────────────────────────────────────────────────────────────────────
// CellResult — summary of inter-yield interval distribution.
// ─────────────────────────────────────────────────────────────────────────────
struct CellResult {
  int64_t median_ns{0};
  int64_t p99_ns{0};
  int64_t max_ns{0};
  int64_t total_yields{0};
  // Max number of leaf inner-items (DoWork calls) between two consecutive yields.
  // = blocking_factor for non-leaf-yield modes (root_only / split).
  // = 1 for full_coro (yield fires per item).
  int64_t max_items_between_yields{0};
};

// ─────────────────────────────────────────────────────────────────────────────
// MeasureCell — drive one chain to completion (or up to max_yields Yielded
// events), collecting inter-yield intervals.
//
// Protocol:
//   • yr = true throughout (yield is always requested).
//   • period = 1 (every YPA call fires).
//   • Record t_prev = drive start; at each Yielded record interval = now - t_prev,
//     update t_prev = now.  At HasRow or Done update t_prev (the chain ran
//     synchronously between yields; we still want intervals across rows).
//
// max_items_between_yields:
//   For non-leaf-yield modes, one inter-yield interval covers the entire
//   blocking_factor inner items (= blocking_factor items per interval).
//   For full_coro, it covers exactly 1 item per interval.
//   We compute this analytically from yield_depth and blocking_factor.
// ─────────────────────────────────────────────────────────────────────────────
static CellResult MeasureCell(int yield_depth, int work_units, int blocking_factor, int total_output_rows,
                              int max_yields = 500) {
  ExecutionContext ctx;
  Frame frame{0};

  std::atomic<bool> yr{true};
  ctx.stopping_context.yield_requested = &yr;

  Chain chain = BuildChain(frame, ctx, yield_depth, total_output_rows, work_units, blocking_factor);

  PullDriverScope scope{ctx, YieldMode::Enabled};

  std::vector<int64_t> intervals;
  intervals.reserve(static_cast<std::size_t>(max_yields + 8));

  int64_t total_yields = 0;
  auto t_prev = std::chrono::steady_clock::now();

  while (!chain.ra.Done()) {
    PullRunResult r = ResumePullStep(chain.ra, ctx);

    const auto t_now = std::chrono::steady_clock::now();
    const int64_t elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(t_now - t_prev).count();

    if (r.status == PullRunResult::Status::Yielded) {
      intervals.push_back(elapsed);
      ++total_yields;
      t_prev = t_now;  // next interval starts from this yield

      if (total_yields >= max_yields) break;  // enough samples
    } else if (r.status == PullRunResult::Status::HasRow) {
      // Row delivered synchronously — do NOT reset t_prev.
      // The inter-yield interval spans all work (including row delivery) between
      // two consecutive Yielded returns.  Resetting here would discard the
      // uninterruptible consume time (the whole point of the benchmark).
      (void)elapsed;
    } else {
      // Done
      break;
    }
  }

  if (intervals.empty()) {
    // Chain exhausted without a single yield (only possible when yield_depth==0,
    // which we don't test — but guard anyway).
    return {};
  }

  std::sort(intervals.begin(), intervals.end());
  const std::size_t n = intervals.size();

  // max_items_between_yields: analytical.
  //   full_coro (leaf has YPA): 1 inner item per interval.
  //   others: blocking_factor inner items per interval (the whole M-consume
  //   between two consecutive MiddleNode-level yields).
  const bool leaf_yields = (kChainDepth - 1) < yield_depth;
  const int64_t items_per_interval = leaf_yields ? 1LL : static_cast<int64_t>(blocking_factor);

  CellResult res;
  res.median_ns = intervals[n / 2];
  res.p99_ns = intervals[n * 99 / 100];
  res.max_ns = intervals[n - 1];
  res.total_yields = total_yields;
  res.max_items_between_yields = items_per_interval;
  return res;
}

}  // namespace memgraph::query::plan

// ─────────────────────────────────────────────────────────────────────────────
// Entry point.
// ─────────────────────────────────────────────────────────────────────────────
int main() {
  using namespace memgraph::query::plan;

  struct Scenario {
    const char *name;
    int work_units;
    int blocking_factor;
    int total_output_rows;
    int max_yields;  // cap on yield events collected per cell
  };

  // worst: M=100k items per output row.  At work_units=500 each DoWork call
  // takes ~hundreds of ns.  full_coro yields 100k times per output row —
  // capped at max_yields=500 (reached after first output row).
  // root_only: yields once per output row → need output_rows=50 to get 50
  // intervals; each row costs 100k*work_units work → ~seconds per cell.
  // We cap root_only at output_rows=20 for runtime bound; split same.
  // Full_coro hits cap in <1 row so output_rows=1 suffices but we use 20 too.
  static constexpr std::array<Scenario, 3> kScenarios{{
      {"best", 50, 1, 2000, 500},
      {"real_world", 500, 4, 500, 500},
      {"worst", 500, 100000, 20, 500},
  }};

  struct Mode {
    const char *name;
    int yield_depth;
  };

  static constexpr std::array<Mode, 3> kModes{{
      {"root_only", 1},
      {"split", kChainDepth / 2},
      {"full_coro", kChainDepth},
  }};

  // Header
  std::printf("%-12s  %-12s  %2s  %12s  %12s  %12s  %12s  %22s\n",
              "scenario",
              "mode",
              "S",
              "median_ns",
              "p99_ns",
              "max_ns",
              "total_yields",
              "max_items/interval");
  std::printf("%-12s  %-12s  %2s  %12s  %12s  %12s  %12s  %22s\n",
              "------------",
              "------------",
              "--",
              "------------",
              "------------",
              "------------",
              "------------",
              "----------------------");

  for (const auto &sc : kScenarios) {
    for (const auto &mode : kModes) {
      CellResult res =
          MeasureCell(mode.yield_depth, sc.work_units, sc.blocking_factor, sc.total_output_rows, sc.max_yields);

      std::printf("%-12s  %-12s  %2d  %12lld  %12lld  %12lld  %12lld  %22lld\n",
                  sc.name,
                  mode.name,
                  mode.yield_depth,
                  static_cast<long long>(res.median_ns),
                  static_cast<long long>(res.p99_ns),
                  static_cast<long long>(res.max_ns),
                  static_cast<long long>(res.total_yields),
                  static_cast<long long>(res.max_items_between_yields));
    }
  }

  std::printf("\nmax_items/interval: DoWork calls between consecutive yields (=1 for full_coro,\n");
  std::printf("=M (blocking_factor) for root_only/split — these modes cannot yield mid-consume).\n");
  std::printf("max_ns is the headline: the scheduler must wait at least this long before the worker parks.\n");

  return 0;
}
