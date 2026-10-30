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

// Costs of the branch read path's two lock-free structures:
//  - BranchChangeFilters: a filter probe vs the diff-engine skip-list lookup it lets a read skip, at 0 / 1k / 8k / 64k
//    recorded keys, with the false-positive rate of the miss probes reported as a counter.
//  - StubIndex: a lookup with a per-reader Cursor vs without, over a dense and a sparse gid range.
//  - Both under 1 / 2 / 4 / 8 reader threads and one writer thread (thread 0).

#include <atomic>
#include <cstdint>
#include <memory>

#include <benchmark/benchmark.h>

#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/skip_list.hpp"
#include "versioning/branch_change_filter.hpp"
#include "versioning/stub_index.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::BranchChangeFilters;
using memgraph::versioning::StubIndex;

namespace {

ms::Gid G(uint64_t gid) { return ms::Gid::FromUint(gid); }

// Probed gids that were never recorded; far above every recorded range.
constexpr uint64_t kMissBase = uint64_t{1} << 40;

// Cheap stride so successive probes do not walk memory in order.
constexpr uint64_t kStride = 0x9E3779B1ULL;

ms::Vertex *FakeFork(uint64_t gid) { return reinterpret_cast<ms::Vertex *>((gid + 1) * 16 + 8); }

void FillFilter(BranchChangeFilters &filters, uint64_t keys) {
  for (uint64_t gid = 0; gid < keys; ++gid) filters.RecordPropertyChange(G(gid));
}

void FilterKeyArgs(benchmark::Benchmark *b) {
  for (int64_t keys : {0, 1'000, 8'000, 64'000}) b->Arg(keys);
}

void BM_FilterHit(benchmark::State &state) {
  const auto keys = static_cast<uint64_t>(state.range(0));
  if (keys == 0) {
    state.SkipWithMessage("no recorded key to hit");
    return;
  }
  BranchChangeFilters filters;
  FillFilter(filters, keys);
  uint64_t i = 0;
  for (auto _ : state) {
    i += kStride;
    benchmark::DoNotOptimize(filters.MayHavePropertyChange(G(i % keys)));
  }
}

void BM_FilterMiss(benchmark::State &state) {
  const auto keys = static_cast<uint64_t>(state.range(0));
  BranchChangeFilters filters;
  FillFilter(filters, keys);
  uint64_t i = 0;
  uint64_t false_positives = 0;
  for (auto _ : state) {
    i += kStride;
    bool may = filters.MayHavePropertyChange(G(kMissBase + (i & 0xFFFFFFFFULL)));
    false_positives += may ? 1 : 0;
    benchmark::DoNotOptimize(may);
  }
  state.counters["fp_rate"] = static_cast<double>(false_positives) / static_cast<double>(state.iterations());
}

// The work a filter hit-or-miss decides about: a find in the diff engine's vertex skip list.
void BM_SkipListFindHit(benchmark::State &state) {
  const auto keys = static_cast<uint64_t>(state.range(0));
  if (keys == 0) {
    state.SkipWithMessage("no recorded key to hit");
    return;
  }
  memgraph::utils::SkipListDb<ms::Vertex> vertices;
  {
    auto acc = vertices.access();
    for (uint64_t gid = 0; gid < keys; ++gid) acc.insert(ms::Vertex{G(gid), nullptr});
  }
  auto acc = vertices.access();
  uint64_t i = 0;
  for (auto _ : state) {
    i += kStride;
    benchmark::DoNotOptimize(acc.find(G(i % keys)));
  }
}

void BM_SkipListFindMiss(benchmark::State &state) {
  const auto keys = static_cast<uint64_t>(state.range(0));
  memgraph::utils::SkipListDb<ms::Vertex> vertices;
  {
    auto acc = vertices.access();
    for (uint64_t gid = 0; gid < keys; ++gid) acc.insert(ms::Vertex{G(gid), nullptr});
  }
  auto acc = vertices.access();
  uint64_t i = 0;
  for (auto _ : state) {
    i += kStride;
    benchmark::DoNotOptimize(acc.find(G(kMissBase + (i & 0xFFFFFFFFULL))));
  }
}

BENCHMARK(BM_FilterHit)->Apply(FilterKeyArgs);
BENCHMARK(BM_FilterMiss)->Apply(FilterKeyArgs);
BENCHMARK(BM_SkipListFindHit)->Apply(FilterKeyArgs);
BENCHMARK(BM_SkipListFindMiss)->Apply(FilterKeyArgs);

// Range 0 is the gid spacing: 1 is a dense range (full pages), 1024 a sparse one (one entry per page).
constexpr uint64_t kStubs = 64'000;

std::unique_ptr<StubIndex> MakeStubIndex(uint64_t spacing) {
  auto index = std::make_unique<StubIndex>();
  for (uint64_t n = 0; n < kStubs; ++n) index->GetOrCreateStub(n * spacing, FakeFork(n * spacing));
  return index;
}

// A scan visits gids in order, which is the access pattern a Cursor is built for.
void BM_StubIndexScanWithCursor(benchmark::State &state) {
  const auto spacing = static_cast<uint64_t>(state.range(0));
  const auto index = MakeStubIndex(spacing);
  StubIndex::Cursor cursor;
  uint64_t n = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(index->FindStub((n % kStubs) * spacing, cursor));
    ++n;
  }
}

void BM_StubIndexScanWithoutCursor(benchmark::State &state) {
  const auto spacing = static_cast<uint64_t>(state.range(0));
  const auto index = MakeStubIndex(spacing);
  uint64_t n = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(index->FindStub((n % kStubs) * spacing));
    ++n;
  }
}

void BM_StubIndexRandomWithCursor(benchmark::State &state) {
  const auto spacing = static_cast<uint64_t>(state.range(0));
  const auto index = MakeStubIndex(spacing);
  StubIndex::Cursor cursor;
  uint64_t i = 0;
  for (auto _ : state) {
    i += kStride;
    benchmark::DoNotOptimize(index->FindStub((i % kStubs) * spacing, cursor));
  }
}

BENCHMARK(BM_StubIndexScanWithCursor)->Arg(1)->Arg(1024);
BENCHMARK(BM_StubIndexScanWithoutCursor)->Arg(1)->Arg(1024);
BENCHMARK(BM_StubIndexRandomWithCursor)->Arg(1)->Arg(1024);

// Thread 0 writes while the others read. The writer cycles over a bounded key space so the filter does not saturate
// and the stub index stops allocating.
constexpr uint64_t kWriterKeys = 8'192;

std::unique_ptr<BranchChangeFilters> g_filters;
std::unique_ptr<StubIndex> g_stubs;

void BM_FilterReadersAndWriter(benchmark::State &state) {
  if (state.thread_index() == 0) {
    g_filters = std::make_unique<BranchChangeFilters>();
    FillFilter(*g_filters, kWriterKeys / 2);
  }
  uint64_t i = static_cast<uint64_t>(state.thread_index()) * 1'000'003;
  for (auto _ : state) {
    i += kStride;
    if (state.thread_index() == 0) {
      g_filters->RecordPropertyChange(G(i % kWriterKeys));
    } else {
      benchmark::DoNotOptimize(g_filters->MayHavePropertyChange(G(i % kWriterKeys)));
    }
  }
  if (state.thread_index() == 0) g_filters.reset();
}

void BM_StubIndexReadersAndWriter(benchmark::State &state) {
  if (state.thread_index() == 0) {
    g_stubs = std::make_unique<StubIndex>();
    for (uint64_t gid = 0; gid < kWriterKeys / 2; ++gid) g_stubs->GetOrCreateStub(gid, FakeFork(gid));
  }
  StubIndex::Cursor cursor;
  uint64_t i = static_cast<uint64_t>(state.thread_index()) * 1'000'003;
  for (auto _ : state) {
    i += kStride;
    const uint64_t gid = i % kWriterKeys;
    if (state.thread_index() == 0) {
      benchmark::DoNotOptimize(g_stubs->GetOrCreateStub(gid, FakeFork(gid)));
    } else {
      benchmark::DoNotOptimize(g_stubs->FindStub(gid, cursor));
    }
  }
  if (state.thread_index() == 0) g_stubs.reset();
}

// Threads are the writer plus 1 / 2 / 4 / 8 readers.
BENCHMARK(BM_FilterReadersAndWriter)->Threads(2)->Threads(3)->Threads(5)->Threads(9)->UseRealTime();
BENCHMARK(BM_StubIndexReadersAndWriter)->Threads(2)->Threads(3)->Threads(5)->Threads(9)->UseRealTime();

}  // namespace

BENCHMARK_MAIN();
