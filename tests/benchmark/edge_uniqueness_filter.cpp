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

// Microbenchmark: EdgeUniquenessFilter data structure alternatives
//
// Context
// -------
// MATCH (a)-[e1]->()-[e2]->()-[e3]->()-[e4]->(b) produces three stacked
// EdgeUniquenessFilter (EUF) operators. The perf profile of a 4-hop query on a
// 150-vertex all-pairs graph shows:
//
//   EUF4 (e4 vs {e1,e2,e3})  492,818,097 hits   11,469 ms  24.5%  ← hot
//   EUF3 (e3 vs {e1,e2})       3,307,801 hits       72 ms   0.2%
//   EUF2 (e2 vs {e1})             22,202 hits        1 ms  <0.1%
//
// Per EUF4::Pull call: ~23 ns. The Gid comparison itself (~1 ns) is not the
// problem; the overhead is TypedValue dispatch (ValueEdge() called k times per
// inner iteration for stable "previous" edges that don't change during the loop)
// plus per-Pull boilerplate (OOMExceptionEnabler, AbortCheck).
//
// Two optimisations are compared:
//
//   CURRENT:  each EUF independently re-reads its k previous_symbols_ from the
//             frame and calls ValueEdge() on each, every inner iteration.
//             Cost at EUF4: k comparisons per candidate (linear scan).
//
//   PROPOSED: a single flat_hash_set<uint64_t> shared across all EUF cursors in
//             the same pattern. Each EUF manages only its own expand_symbol_:
//               - on accepting a candidate: insert its Gid into the shared set
//               - on re-entry (backtrack): erase the previously accepted Gid
//             Cost at EUF4: 1 hash lookup per candidate, amortised 1 insert +
//             1 erase per accepted result.
//
//             KEY CORRECTNESS POINT: in the proposed design the erase happens at
//             the START of the next Pull call (re-entry / backtrack), NOT
//             immediately after the insert. The old benchmark got this wrong by
//             doing insert+erase in the same inner-loop iteration, making the
//             rejected-candidate path look more expensive than it is.
//
// Benchmark parameters (state.range):
//   range(0) = k            number of previous edges already in the set
//   range(1) = rejection_%  percentage of candidates that are duplicates
//
// Two benchmark groups are provided:
//
//   *_Check      models the per-candidate cost only (no mutation).
//                Useful for understanding the raw lookup cost at different k.
//
//   *_PullCycle  models one full outer-Pull cycle: erase last accepted (if any),
//                then scan candidates until one is unique, insert it, and stop.
//                This matches the real cursor re-entry semantics.
//                Rejected candidates pay only a single hash-hit lookup.
//                Accepted candidates pay: 1 miss lookup + 1 insert (this cycle)
//                                       + 1 erase (charged to the next cycle).

#include <benchmark/benchmark.h>

#include <absl/container/flat_hash_set.h>
#include <algorithm>
#include <cstdint>
#include <numeric>
#include <random>
#include <roaring/roaring64map.hh>
#include <vector>

namespace {

// ---------------------------------------------------------------------------
// Scenario builder
// ---------------------------------------------------------------------------

struct Scenario {
  std::vector<uint64_t> prev_gids;   // the k "fixed" previous edges
  std::vector<uint64_t> candidates;  // stream of candidates EUF4 evaluates
};

// Candidates in [kFreshBase, kFreshBase + num_candidates) are guaranteed unique
// (never appear in prev_gids). Rejection duplicates come from prev_gids.
constexpr uint64_t kFreshBase = 1'000'000;
constexpr int kNumCandidates = 1024;

Scenario MakeScenario(int k, int rejection_pct) {
  std::mt19937 rng(42);
  Scenario s;

  // k distinct prev Gids in [0, kFreshBase)
  std::uniform_int_distribution<uint64_t> prev_dist(0, kFreshBase - 1);
  absl::flat_hash_set<uint64_t> tmp;
  while ((int)tmp.size() < k) tmp.insert(prev_dist(rng));
  s.prev_gids.assign(tmp.begin(), tmp.end());

  // candidate stream: rejection_pct% are duplicates from prev_gids
  std::uniform_int_distribution<int> pct(0, 99);
  std::uniform_int_distribution<int> dup_pick(0, k - 1);
  uint64_t fresh = kFreshBase;
  for (int i = 0; i < kNumCandidates; ++i) {
    if (pct(rng) < rejection_pct)
      s.candidates.push_back(s.prev_gids[dup_pick(rng)]);
    else
      s.candidates.push_back(fresh++);
  }
  return s;
}

struct BenchData {
  Scenario scenario;
  std::vector<uint64_t> sorted_prev;
  absl::flat_hash_set<uint64_t> hash_prev;
  roaring::Roaring64Map roaring_prev;
};

BenchData MakeBenchData(int k, int rejection_pct) {
  BenchData d;
  d.scenario = MakeScenario(k, rejection_pct);
  d.sorted_prev = d.scenario.prev_gids;
  std::sort(d.sorted_prev.begin(), d.sorted_prev.end());
  d.hash_prev.insert(d.scenario.prev_gids.begin(), d.scenario.prev_gids.end());
  for (uint64_t g : d.scenario.prev_gids) d.roaring_prev.add(g);
  return d;
}

// ---------------------------------------------------------------------------
// Helper: one PullCycle loop body
//
// Scans `candidates` from `start` until a candidate not in `seen` is found.
// Returns {index_of_accepted, accepted_gid} or {candidates.size(), 0} if
// the stream is exhausted. `start` should advance past the accepted index
// on the next cycle.
// ---------------------------------------------------------------------------

template <typename ContainsF, typename InsertF>
std::pair<size_t, uint64_t> FindNextAccepted(const std::vector<uint64_t> &candidates, size_t start, ContainsF contains,
                                             InsertF insert) {
  for (size_t i = start; i < candidates.size(); ++i) {
    if (!contains(candidates[i])) {
      insert(candidates[i]);
      return {i + 1, candidates[i]};
    }
  }
  return {candidates.size(), 0};
}

// ---------------------------------------------------------------------------
// Group 1: *_Check  — read-only membership test, no mutation
//
// Models the per-candidate comparison cost at the deepest EUF.
// Pre-conditions: previous Gids are already extracted (best-case for current).
// ---------------------------------------------------------------------------

static void BM_Current_LinearScan_Check(benchmark::State &state) {
  const int k = static_cast<int>(state.range(0));
  const int rej = static_cast<int>(state.range(1));
  auto d = MakeBenchData(k, rej);

  for (auto _ : state) {
    uint64_t accepted = 0;
    for (uint64_t c : d.scenario.candidates) {
      bool unique = true;
      for (uint64_t p : d.scenario.prev_gids)
        if (p == c) {
          unique = false;
          break;
        }
      if (unique) ++accepted;
    }
    benchmark::DoNotOptimize(accepted);
  }
  state.SetLabel("k=" + std::to_string(k) + " rej=" + std::to_string(rej) + "%");
}

static void BM_SortedVec_Check(benchmark::State &state) {
  const int k = static_cast<int>(state.range(0));
  const int rej = static_cast<int>(state.range(1));
  auto d = MakeBenchData(k, rej);

  for (auto _ : state) {
    uint64_t accepted = 0;
    for (uint64_t c : d.scenario.candidates) {
      auto it = std::lower_bound(d.sorted_prev.begin(), d.sorted_prev.end(), c);
      if (it == d.sorted_prev.end() || *it != c) ++accepted;
    }
    benchmark::DoNotOptimize(accepted);
  }
  state.SetLabel("k=" + std::to_string(k) + " rej=" + std::to_string(rej) + "%");
}

static void BM_FlatHashSet_Check(benchmark::State &state) {
  const int k = static_cast<int>(state.range(0));
  const int rej = static_cast<int>(state.range(1));
  auto d = MakeBenchData(k, rej);

  for (auto _ : state) {
    uint64_t accepted = 0;
    for (uint64_t c : d.scenario.candidates)
      if (!d.hash_prev.contains(c)) ++accepted;
    benchmark::DoNotOptimize(accepted);
  }
  state.SetLabel("k=" + std::to_string(k) + " rej=" + std::to_string(rej) + "%");
}

static void BM_Roaring64_Check(benchmark::State &state) {
  const int k = static_cast<int>(state.range(0));
  const int rej = static_cast<int>(state.range(1));
  auto d = MakeBenchData(k, rej);

  for (auto _ : state) {
    uint64_t accepted = 0;
    for (uint64_t c : d.scenario.candidates)
      if (!d.roaring_prev.contains(c)) ++accepted;
    benchmark::DoNotOptimize(accepted);
  }
  state.SetLabel("k=" + std::to_string(k) + " rej=" + std::to_string(rej) + "%");
}

// ---------------------------------------------------------------------------
// Group 2: *_PullCycle  — correct cursor re-entry semantics
//
// Models a sequence of EUF4::Pull calls as the outer cursor (Aggregate) keeps
// pulling results. Each "Pull cycle" is:
//   1. On re-entry: erase the Gid accepted in the PREVIOUS cycle (backtrack).
//   2. Scan candidates until one is not in the set.
//   3. Accept it (insert / record), return true.
//
// Rejected candidates pay only a lookup (hit). They never trigger insert/erase.
// The erase cost is charged to the NEXT cycle, not the current one.
//
// For the current design there is no shared set; re-entry just means continuing
// the while loop with a fresh linear scan against the stable k previous Gids.
// ---------------------------------------------------------------------------

// Current design: each inner-loop iteration re-reads k previous Gids.
// Here the previous Gids are pre-extracted into a vector (best case; the real
// code additionally calls TypedValue::ValueEdge() on each, adding ~2-5 ns/call).
static void BM_Current_LinearScan_PullCycle(benchmark::State &state) {
  const int k = static_cast<int>(state.range(0));
  const int rej = static_cast<int>(state.range(1));
  auto d = MakeBenchData(k, rej);

  for (auto _ : state) {
    uint64_t total_accepted = 0;
    size_t pos = 0;

    while (pos < d.scenario.candidates.size()) {
      // Scan until unique (re-entry: previous accepted no longer matters for
      // current design — just continue scanning from pos).
      bool found = false;
      while (pos < d.scenario.candidates.size()) {
        uint64_t c = d.scenario.candidates[pos++];
        bool unique = true;
        for (uint64_t p : d.scenario.prev_gids)
          if (p == c) {
            unique = false;
            break;
          }
        if (unique) {
          ++total_accepted;
          found = true;
          break;
        }
      }
      if (!found) break;
    }
    benchmark::DoNotOptimize(total_accepted);
  }
  state.SetLabel("k=" + std::to_string(k) + " rej=" + std::to_string(rej) + "%");
}

// Proposed design: shared flat_hash_set.
// Erase happens at the TOP of each cycle (re-entry backtrack), not after insert.
// Rejected candidates: 1 hash lookup (hit).
// Accepted candidates: 1 hash lookup (miss) + 1 insert this cycle + 1 erase next cycle.
static void BM_Proposed_FlatHashSet_PullCycle(benchmark::State &state) {
  const int k = static_cast<int>(state.range(0));
  const int rej = static_cast<int>(state.range(1));
  auto d = MakeBenchData(k, rej);

  for (auto _ : state) {
    // Shared set starts with the k fixed previous edges (seeded by inner EUFs).
    absl::flat_hash_set<uint64_t> shared(d.scenario.prev_gids.begin(), d.scenario.prev_gids.end());
    uint64_t last_accepted = 0;
    bool has_last = false;
    uint64_t total_accepted = 0;
    size_t pos = 0;

    while (pos < d.scenario.candidates.size()) {
      // Re-entry: remove what we accepted last time (backtrack).
      if (has_last) {
        shared.erase(last_accepted);
        has_last = false;
      }

      // Scan until unique.
      bool found = false;
      while (pos < d.scenario.candidates.size()) {
        uint64_t c = d.scenario.candidates[pos++];
        if (!shared.contains(c)) {
          shared.insert(c);
          last_accepted = c;
          has_last = true;
          ++total_accepted;
          found = true;
          break;
        }
      }
      if (!found) break;
    }
    benchmark::DoNotOptimize(total_accepted);
  }
  state.SetLabel("k=" + std::to_string(k) + " rej=" + std::to_string(rej) + "%");
}

// Proposed design: shared Roaring64Map.
static void BM_Proposed_Roaring64_PullCycle(benchmark::State &state) {
  const int k = static_cast<int>(state.range(0));
  const int rej = static_cast<int>(state.range(1));
  auto d = MakeBenchData(k, rej);

  for (auto _ : state) {
    roaring::Roaring64Map shared = d.roaring_prev;
    uint64_t last_accepted = 0;
    bool has_last = false;
    uint64_t total_accepted = 0;
    size_t pos = 0;

    while (pos < d.scenario.candidates.size()) {
      if (has_last) {
        shared.remove(last_accepted);
        has_last = false;
      }

      bool found = false;
      while (pos < d.scenario.candidates.size()) {
        uint64_t c = d.scenario.candidates[pos++];
        if (!shared.contains(c)) {
          shared.add(c);
          last_accepted = c;
          has_last = true;
          ++total_accepted;
          found = true;
          break;
        }
      }
      if (!found) break;
    }
    benchmark::DoNotOptimize(total_accepted);
  }
  state.SetLabel("k=" + std::to_string(k) + " rej=" + std::to_string(rej) + "%");
}

// ---------------------------------------------------------------------------
// Registration
//
// k in {1, 2, 3, 4, 7, 15}:
//   1-4: typical fixed-length Cypher MATCH patterns
//   7:   medium variable-length [*1..8]
//   15:  long variable-length [*1..16]
// ---------------------------------------------------------------------------

#define REGISTER_WITH_ARGS(BenchName) \
  BENCHMARK(BenchName)                \
      ->Args({1, 0})                  \
      ->Args({1, 50})                 \
      ->Args({1, 80})                 \
      ->Args({2, 0})                  \
      ->Args({2, 50})                 \
      ->Args({2, 80})                 \
      ->Args({3, 0})                  \
      ->Args({3, 50})                 \
      ->Args({3, 80})                 \
      ->Args({4, 0})                  \
      ->Args({4, 50})                 \
      ->Args({4, 80})                 \
      ->Args({7, 0})                  \
      ->Args({7, 50})                 \
      ->Args({7, 80})                 \
      ->Args({15, 0})                 \
      ->Args({15, 50})                \
      ->Args({15, 80})                \
      ->Unit(benchmark::kNanosecond)

// Check-only (no mutation): raw lookup cost per candidate
REGISTER_WITH_ARGS(BM_Current_LinearScan_Check);
REGISTER_WITH_ARGS(BM_SortedVec_Check);
REGISTER_WITH_ARGS(BM_FlatHashSet_Check);
REGISTER_WITH_ARGS(BM_Roaring64_Check);

// Full pull-cycle: correct cursor re-entry semantics
REGISTER_WITH_ARGS(BM_Current_LinearScan_PullCycle);
REGISTER_WITH_ARGS(BM_Proposed_FlatHashSet_PullCycle);
REGISTER_WITH_ARGS(BM_Proposed_Roaring64_PullCycle);

}  // namespace

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
