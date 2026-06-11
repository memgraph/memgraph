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

// Benchmark: deleted-edge GC bookkeeping container choice (list+splice vs vector).
//
// The GC tracks logically-deleted edges in `InMemoryStorage::deleted_edges_`
// (shared, under a SpinLock) and per-cycle locals `current_deleted_edges`.
// Master/PR1 used `std::list<Gid>` with an O(1) `splice` handover; the
// light-edges refactor carries `Edge*` directly and the open question is
// whether the container should stay a `std::list` (keeping the O(1) splice)
// or become a `std::vector`.
//
// The decision hinges on what runs UNDER the `deleted_edges_` SpinLock vs off
// it, because the handover blocks other committers:
//
//   HANDOVER  (under the lock):  append the per-cycle batch into the shared
//             member, OR swap it out in GC.
//               list  : splice  -> O(1) pointer relink, no alloc/copy
//               vector: insert  -> O(batch) alloc + memcpy. The GC drain swaps
//                       the member with a fresh empty vector, so it returns at
//                       capacity 0 -> the next insert REALLOCATES every time.
//               (the GC-side drain is swap() -> O(1) for both)
//
//   GC CONSUME (off the lock): build the batch (one push_back per deleted edge),
//             then three passes -- edges-metadata index (->gid), vector-edge
//             index (RemoveEdgesFromVectorEdgeIndices takes std::span<Edge* const>,
//             so a list must first copy itself into a contiguous temp vector;
//             a vector is passed directly), and skiplist remove (->gid).
//
// "HandoverXxx" is the metric that matters for lock contention; "GcConsumeXxx"
// is the off-lock cost ("we don't care how slow GC is, up to a point").
//
// Edge objects live in the `edges_` skip-list, not allocated by this flow, so
// they are pre-allocated once (scattered, for a realistic ->gid cache miss);
// the benchmark measures only the per-cycle container work.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <list>
#include <memory>
#include <numeric>
#include <span>
#include <vector>

#include "utils/db_aware_allocator.hpp"

namespace {

struct BenchEdge {
  uint64_t gid;
  char payload[120];  // ~128 B object, comparable to a real Edge
};

template <typename T>
using DbAlloc = memgraph::memory::DbAwareAllocator<T>;

using EdgeList = std::list<BenchEdge *, DbAlloc<BenchEdge *>>;
using EdgeVector = std::vector<BenchEdge *, DbAlloc<BenchEdge *>>;

uint64_t Sum(auto const &edges) {
  uint64_t acc = 0;
  for (auto *e : edges) acc += e->gid;
  return acc;
}

uint64_t SumSpan(std::span<BenchEdge *const> edges) {
  uint64_t acc = 0;
  for (auto *e : edges) acc += e->gid;
  return acc;
}

class DeletedEdgesFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &state) override {
    if (state.thread_index() != 0) return;
    const auto n = static_cast<std::size_t>(state.range(0));
    if (pool_.size() == n) return;
    pool_.clear();
    raw_.clear();
    pool_.reserve(n);
    raw_.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
      auto e = std::make_unique<BenchEdge>();
      e->gid = i;
      raw_.push_back(e.get());
      pool_.push_back(std::move(e));
    }
  }

  void TearDown(const benchmark::State &state) override {
    if (state.thread_index() != 0) return;
    pool_.clear();
    raw_.clear();
  }

  std::vector<std::unique_ptr<BenchEdge>> pool_;
  std::vector<BenchEdge *> raw_;
};

}  // namespace

// ============================ HANDOVER (under lock) ============================
// list: O(1) splice. The pre-built batch is spliced out then spliced back to
// restore it for the next iteration -- both O(1), so the timed work stays flat
// in the batch size B (that flatness is the whole point).
BENCHMARK_DEFINE_F(DeletedEdgesFixture, HandoverListSplice)(benchmark::State &state) {
  EdgeList batch(raw_.begin(), raw_.end());  // pre-built once (off-lock build is measured separately)
  for (auto _ : state) {
    EdgeList member;
    member.splice(member.end(), batch);  // batch -> member  (O(1), the critical-section op)
    batch.splice(batch.end(), member);   // restore for next iteration (O(1))
    benchmark::DoNotOptimize(batch.front());
  }
}

// vector: insert into a post-drain (capacity-0) member -> O(B) alloc + memcpy.
// `batch` persists (insert copies from it); `member` is fresh each iteration to
// model the cap-0 state the GC swap-drain leaves behind. NOTE: member's free at
// scope end is also timed and is itself O(B); in the real code that free happens
// off-lock in GC after the swap, so the true lock-hold is ~half the reported
// number -- still O(B), which is the point.
BENCHMARK_DEFINE_F(DeletedEdgesFixture, HandoverVectorInsert)(benchmark::State &state) {
  EdgeVector batch(raw_.begin(), raw_.end());  // pre-built once
  for (auto _ : state) {
    EdgeVector member;                                        // post-drain: empty, capacity 0 -> insert reallocates
    member.insert(member.end(), batch.begin(), batch.end());  // O(B) under the lock
    benchmark::DoNotOptimize(member.data());
  }
}

// ============================ GC CONSUME (off lock) ============================
// build + drain(swap) + 3 passes. `with_vector_index` (range(1)) toggles the
// vector-edge-index pass, where a list must materialize a contiguous temp vector.
BENCHMARK_DEFINE_F(DeletedEdgesFixture, GcConsumeList)(benchmark::State &state) {
  const bool with_vector_index = state.range(1) != 0;
  for (auto _ : state) {
    EdgeList current;
    for (auto *e : raw_) current.push_back(e);  // build (off-lock)
    uint64_t acc = Sum(current);                // metadata index
    if (with_vector_index) {
      EdgeVector tmp(current.begin(), current.end());  // list MUST copy to a span
      acc += SumSpan(tmp);
    }
    acc += Sum(current);  // skiplist remove
    benchmark::DoNotOptimize(acc);
  }
}

BENCHMARK_DEFINE_F(DeletedEdgesFixture, GcConsumeVector)(benchmark::State &state) {
  const bool with_vector_index = state.range(1) != 0;
  for (auto _ : state) {
    EdgeVector current;
    for (auto *e : raw_) current.push_back(e);  // build (off-lock)
    uint64_t acc = Sum(current);                // metadata index
    if (with_vector_index) {
      acc += SumSpan(std::span<BenchEdge *const>{current});  // zero-copy span
    }
    acc += Sum(current);  // skiplist remove
    benchmark::DoNotOptimize(acc);
  }
}

BENCHMARK_REGISTER_F(DeletedEdgesFixture, HandoverListSplice)
    ->Arg(1'000)
    ->Arg(100'000)
    ->Arg(1'000'000)
    ->Arg(5'000'000)
    ->Unit(benchmark::kNanosecond);

BENCHMARK_REGISTER_F(DeletedEdgesFixture, HandoverVectorInsert)
    ->Arg(1'000)
    ->Arg(100'000)
    ->Arg(1'000'000)
    ->Arg(5'000'000)
    ->Unit(benchmark::kNanosecond);

BENCHMARK_REGISTER_F(DeletedEdgesFixture, GcConsumeList)
    ->Args({1'000'000, 0})
    ->Args({1'000'000, 1})
    ->Args({5'000'000, 0})
    ->Args({5'000'000, 1})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(DeletedEdgesFixture, GcConsumeVector)
    ->Args({1'000'000, 0})
    ->Args({1'000'000, 1})
    ->Args({5'000'000, 0})
    ->Args({5'000'000, 1})
    ->Unit(benchmark::kMicrosecond);

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
