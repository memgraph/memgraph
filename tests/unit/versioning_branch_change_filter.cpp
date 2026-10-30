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

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "versioning/branch_change_filter.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::BranchChangeFilters;

namespace {

ms::Gid G(uint64_t gid) { return ms::Gid::FromUint(gid); }

ms::PropertyId P(uint64_t pid) { return ms::PropertyId::FromUint(pid); }

void ExpectNoChange(const BranchChangeFilters &filters, ms::Gid gid, std::initializer_list<uint64_t> pids) {
  EXPECT_FALSE(filters.MayHavePropertyChange(gid));
  EXPECT_FALSE(filters.MayHaveLabelChange(gid));
  EXPECT_FALSE(filters.MayHaveEdgeChange(gid));
  EXPECT_FALSE(filters.MayHaveVertexDeleted(gid));
  EXPECT_FALSE(filters.MayHaveForkEdgePropertyChange(gid));
  EXPECT_FALSE(filters.MayHaveForkEdgeDeleted(gid));
  for (auto pid : pids) {
    EXPECT_FALSE(filters.MayHavePropertyFieldChange(gid, P(pid)));
    EXPECT_FALSE(filters.MayHaveForkEdgePropertyFieldChange(gid, P(pid)));
  }
}

}  // namespace

TEST(VersioningBranchChangeFilter, EachKindAnswersOnlyForItsOwnRecords) {
  BranchChangeFilters filters;
  ExpectNoChange(filters, G(1), {0, 7});
  EXPECT_FALSE(filters.MayHaveAnyVertexDeleted());

  filters.RecordPropertyChange(G(1));
  filters.RecordPropertyField(G(1), P(7));
  EXPECT_TRUE(filters.MayHavePropertyChange(G(1)));
  EXPECT_TRUE(filters.MayHavePropertyFieldChange(G(1), P(7)));
  EXPECT_FALSE(filters.MayHavePropertyFieldChange(G(1), P(8))) << "an unwritten pid must read the fork value";
  EXPECT_FALSE(filters.MayHaveLabelChange(G(1))) << "a property change must not disturb label reads";
  EXPECT_FALSE(filters.MayHaveEdgeChange(G(1)));
  EXPECT_FALSE(filters.MayHaveVertexDeleted(G(1)));
  EXPECT_FALSE(filters.MayHaveForkEdgePropertyFieldChange(G(1), P(7))) << "vertex pids must not open the edge gate";

  filters.RecordLabelChange(G(2));
  EXPECT_TRUE(filters.MayHaveLabelChange(G(2)));
  EXPECT_FALSE(filters.MayHavePropertyChange(G(2))) << "a label change must not disturb property reads";
  EXPECT_FALSE(filters.MayHavePropertyFieldChange(G(2), P(7)));
  EXPECT_TRUE(filters.MayHavePropertyChange(G(1))) << "filters only grow";

  filters.RecordEdgeChange(G(3));
  EXPECT_TRUE(filters.MayHaveEdgeChange(G(3)));
  EXPECT_FALSE(filters.MayHaveLabelChange(G(3)));
  EXPECT_FALSE(filters.MayHavePropertyChange(G(3)));

  EXPECT_FALSE(filters.MayHaveAnyVertexDeleted());
  filters.RecordVertexDeleted(G(4));
  EXPECT_TRUE(filters.MayHaveAnyVertexDeleted());
  EXPECT_TRUE(filters.MayHaveVertexDeleted(G(4)));
  EXPECT_FALSE(filters.MayHaveVertexDeleted(G(3)));

  filters.RecordForkEdgeProperty(G(5), P(9));
  EXPECT_TRUE(filters.MayHaveForkEdgePropertyChange(G(5)));
  EXPECT_TRUE(filters.MayHaveForkEdgePropertyFieldChange(G(5), P(9)));
  EXPECT_FALSE(filters.MayHaveForkEdgePropertyFieldChange(G(5), P(7))) << "an unwritten edge pid";
  EXPECT_FALSE(filters.MayHaveForkEdgeDeleted(G(5)));
  EXPECT_FALSE(filters.MayHavePropertyChange(G(5)));
  EXPECT_FALSE(filters.MayHavePropertyFieldChange(G(5), P(9))) << "edge pids must not open the vertex gate";
  filters.RecordForkEdgeDeleted(G(6));
  EXPECT_TRUE(filters.MayHaveForkEdgeDeleted(G(6)));
  EXPECT_FALSE(filters.MayHaveVertexDeleted(G(6)));

  ExpectNoChange(filters, G(1'000'000), {8});
}

TEST(VersioningBranchChangeFilter, InsertedKeyCountsGrowWithDistinctRecordsPerFilter) {
  BranchChangeFilters filters;
  const auto zero = filters.InsertedKeys();
  EXPECT_EQ(zero.label + zero.property + zero.edge + zero.property_field + zero.deleted + zero.fork_edge_property +
                zero.fork_edge_deleted,
            0U);

  for (int i = 0; i < 3; ++i) filters.RecordLabelChange(G(1));
  auto counts = filters.InsertedKeys();
  EXPECT_EQ(counts.label, 1U) << "re-recording the same gid sets no new bit";
  EXPECT_EQ(counts.property, 0U);
  EXPECT_EQ(counts.edge, 0U);

  constexpr uint64_t kKeys = 100;
  for (uint64_t gid = 10; gid < 10 + kKeys; ++gid) filters.RecordEdgeChange(G(gid));
  counts = filters.InsertedKeys();
  EXPECT_LE(counts.edge, kKeys);
  EXPECT_GE(counts.edge, kKeys - 5) << "far from saturation almost every distinct gid sets a new bit";
  EXPECT_EQ(counts.label, 1U) << "another filter's inserts must not count here";

  filters.RecordPropertyField(G(1), P(7));
  filters.RecordPropertyField(G(1), P(7));
  filters.RecordVertexDeleted(G(2));
  filters.RecordForkEdgeProperty(G(3), P(7));
  filters.RecordForkEdgeDeleted(G(4));
  counts = filters.InsertedKeys();
  EXPECT_EQ(counts.property_field, 1U);
  EXPECT_EQ(counts.deleted, 1U);
  EXPECT_EQ(counts.fork_edge_property, 1U);
  EXPECT_EQ(counts.fork_edge_deleted, 1U);
}

// Pid gate: a never-written pid stays off a saturated field filter; sparse filters rarely false-positive.
TEST(VersioningBranchChangeFilter, FiltersStaySelective) {
  BranchChangeFilters sparse;
  for (uint64_t gid = 0; gid < 2'000; ++gid) sparse.RecordPropertyChange(G(gid));
  for (uint64_t gid = 0; gid < 2'000; ++gid) ASSERT_TRUE(sparse.MayHavePropertyChange(G(gid)));
  int false_positives = 0;
  for (uint64_t i = 0; i < 200'000; ++i) false_positives += sparse.MayHavePropertyChange(G(1'000'000 + i)) ? 1 : 0;
  EXPECT_LT(false_positives, 200);

  BranchChangeFilters saturated;
  for (uint64_t gid = 0; gid < 100'000; ++gid) {
    saturated.RecordPropertyField(G(gid), P(7));
    saturated.RecordForkEdgeProperty(G(gid), P(7));
  }
  int vertex_hits = 0;
  int edge_hits = 0;
  for (uint64_t gid = 0; gid < 10'000; ++gid) {
    ASSERT_TRUE(saturated.MayHavePropertyFieldChange(G(gid), P(7)));
    ASSERT_TRUE(saturated.MayHaveForkEdgePropertyFieldChange(G(gid), P(7)));
    vertex_hits += saturated.MayHavePropertyFieldChange(G(gid), P(8)) ? 1 : 0;
    edge_hits += saturated.MayHaveForkEdgePropertyFieldChange(G(gid), P(8)) ? 1 : 0;
  }
  EXPECT_EQ(vertex_hits, 0);
  EXPECT_EQ(edge_hits, 0);
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
    return filters.MayHavePropertyChange(gid) && filters.MayHaveLabelChange(gid) && filters.MayHaveEdgeChange(gid) &&
           filters.MayHaveVertexDeleted(gid) && filters.MayHavePropertyFieldChange(gid, pid) &&
           filters.MayHaveForkEdgePropertyFieldChange(gid, pid) && filters.MayHaveForkEdgeDeleted(gid) &&
           filters.MayHaveAnyVertexDeleted();
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
        filters.RecordEdgeChange(gid);
        filters.RecordVertexDeleted(gid);
        filters.RecordForkEdgeProperty(gid, pid);
        filters.RecordForkEdgeDeleted(gid);
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
