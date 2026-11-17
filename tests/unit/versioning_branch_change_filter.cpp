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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "versioning/branch_change_filter.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::BranchChangeFilters;

namespace {

ms::Gid G(uint64_t gid) { return ms::Gid::FromUint(gid); }

ms::PropertyId P(uint64_t pid) { return ms::PropertyId::FromUint(pid); }

// One bool per kind, so a record into one kind can be checked against every other.
std::vector<bool> Probe(const BranchChangeFilters &f, ms::Gid gid, ms::PropertyId pid) {
  return {f.MayHavePropertyChange(gid),
          f.MayHaveLabelChange(gid),
          f.MayHaveEdgeChange(gid, ms::EdgeDirection::OUT),
          f.MayHaveVertexDeleted(gid),
          f.MayHavePropertyFieldChange(gid, pid),
          f.MayHaveBaseEdgePropertyChange(gid),
          f.MayHaveBaseEdgePropertyFieldChange(gid, pid),
          f.MayHaveBaseEdgeDeleted(gid)};
}

}  // namespace

TEST(VersioningBranchChangeFilter, EachRecordHitsOnlyItsOwnKind) {
  using Record = void (*)(BranchChangeFilters &, ms::Gid, ms::PropertyId);
  const std::vector<std::pair<Record, std::vector<bool>>> cases = {
      {[](auto &f, auto g, auto) { f.RecordPropertyChange(g); }, {1, 0, 0, 0, 0, 0, 0, 0}},
      {[](auto &f, auto g, auto) { f.RecordLabelChange(g); }, {0, 1, 0, 0, 0, 0, 0, 0}},
      {[](auto &f, auto g, auto) { f.RecordEdgeChange(g, ms::EdgeDirection::OUT); }, {0, 0, 1, 0, 0, 0, 0, 0}},
      {[](auto &f, auto g, auto) { f.RecordVertexDeleted(g); }, {0, 0, 0, 1, 0, 0, 0, 0}},
      {[](auto &f, auto g, auto p) { f.RecordPropertyField(g, p); }, {0, 0, 0, 0, 1, 0, 0, 0}},
      {[](auto &f, auto g, auto p) { f.RecordBaseEdgeProperty(g, p); }, {0, 0, 0, 0, 0, 1, 1, 0}},
      {[](auto &f, auto g, auto) { f.RecordBaseEdgeDeleted(g); }, {0, 0, 0, 0, 0, 0, 0, 1}},
  };
  for (size_t i = 0; i < cases.size(); ++i) {
    BranchChangeFilters filters;
    EXPECT_EQ(Probe(filters, G(1), P(7)), std::vector<bool>(8, false)) << i;
    cases[i].first(filters, G(1), P(7));
    EXPECT_EQ(Probe(filters, G(1), P(7)), cases[i].second) << i;
    EXPECT_EQ(Probe(filters, G(1'000'000), P(8)), std::vector<bool>(8, false)) << "unrecorded gid and pid, case " << i;
  }
}

TEST(VersioningBranchChangeFilter, EdgeChangeIsPerDirection) {
  BranchChangeFilters filters;
  filters.RecordEdgeChange(G(1), ms::EdgeDirection::OUT);
  filters.RecordEdgeChange(G(2), ms::EdgeDirection::IN);
  EXPECT_TRUE(filters.MayHaveEdgeChange(G(1), ms::EdgeDirection::OUT));
  EXPECT_FALSE(filters.MayHaveEdgeChange(G(1), ms::EdgeDirection::IN));
  EXPECT_TRUE(filters.MayHaveEdgeChange(G(2), ms::EdgeDirection::IN));
  EXPECT_FALSE(filters.MayHaveEdgeChange(G(2), ms::EdgeDirection::OUT));
}

TEST(VersioningBranchChangeFilter, PidGateNeverHitsForAnUnwrittenPid) {
  BranchChangeFilters filters;
  for (uint64_t gid = 0; gid < 100'000; ++gid) {
    filters.RecordPropertyField(G(gid), P(7));
    filters.RecordBaseEdgeProperty(G(gid), P(7));
  }
  EXPECT_TRUE(filters.MayHavePropertyTouched(P(7)));
  EXPECT_FALSE(filters.MayHavePropertyTouched(P(8)));
  for (uint64_t gid = 0; gid < 10'000; ++gid) {
    ASSERT_TRUE(filters.MayHavePropertyFieldChange(G(gid), P(7)));
    ASSERT_TRUE(filters.MayHaveBaseEdgePropertyFieldChange(G(gid), P(7)));
    ASSERT_FALSE(filters.MayHavePropertyFieldChange(G(gid), P(8)));
    ASSERT_FALSE(filters.MayHaveBaseEdgePropertyFieldChange(G(gid), P(8)));
  }
}

// Gids that agree modulo the filter size must not all share one word and bit pair.
TEST(VersioningBranchChangeFilter, MultiplesOfTheFilterSizeDoNotAllCollide) {
  BranchChangeFilters filters;
  constexpr uint64_t kStride = BranchChangeFilters::kFilterWords;
  for (uint64_t i = 1; i <= 64; ++i) filters.RecordLabelChange(G(i * kStride));
  int hits = 0;
  for (uint64_t i = 65; i <= 128; ++i) hits += filters.MayHaveLabelChange(G(i * kStride)) ? 1 : 0;
  EXPECT_LT(hits, 8);
}

TEST(VersioningBranchChangeFilter, ConcurrentRecordsAreNeverMissedByReaders) {
  constexpr int kWriters = 4;
  constexpr uint64_t kPerWriter = 20'000;  // loads the filters heavily: false positives allowed, negatives not
  BranchChangeFilters filters;
  std::vector<std::atomic<uint64_t>> published(kWriters);
  std::atomic<bool> done{false};
  std::atomic<int> missed{0};

  auto gid_of = [](int w, uint64_t i) { return G(i * kWriters + w); };
  auto expect_all = [&](ms::Gid gid) {
    const auto pid = P(gid.AsUint() % 300);
    const auto hits = Probe(filters, gid, pid);
    return std::ranges::all_of(hits, std::identity{}) && filters.MayHaveAnyVertexDeleted();
  };

  std::vector<std::thread> readers;
  for (int r = 0; r < 2; ++r) {
    readers.emplace_back([&] {
      while (!done.load(std::memory_order_acquire)) {
        for (int w = 0; w < kWriters; ++w) {
          const uint64_t n = published[w].load(std::memory_order_acquire);
          for (uint64_t i = n > 64 ? n - 64 : 0; i < n; ++i) {
            if (!expect_all(gid_of(w, i))) missed.fetch_add(1);
          }
        }
      }
    });
  }
  std::vector<std::thread> writers;
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      for (uint64_t i = 0; i < kPerWriter; ++i) {
        const auto gid = gid_of(w, i);
        const auto pid = P(gid.AsUint() % 300);
        filters.RecordPropertyChange(gid);
        filters.RecordLabelChange(gid);
        filters.RecordPropertyField(gid, pid);
        filters.RecordEdgeChange(gid, ms::EdgeDirection::OUT);
        filters.RecordVertexDeleted(gid);
        filters.RecordBaseEdgeProperty(gid, pid);
        filters.RecordBaseEdgeDeleted(gid);
        published[w].store(i + 1, std::memory_order_release);
      }
    });
  }
  for (auto &t : writers) t.join();
  done.store(true, std::memory_order_release);
  for (auto &t : readers) t.join();
  EXPECT_EQ(missed.load(), 0);

  for (int w = 0; w < kWriters; ++w) {
    for (uint64_t i = 0; i < kPerWriter; ++i) ASSERT_TRUE(expect_all(gid_of(w, i))) << gid_of(w, i).AsUint();
  }
}
