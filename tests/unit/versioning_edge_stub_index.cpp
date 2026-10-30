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
#include <set>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "utils/memory_tracker.hpp"
#include "versioning/edge_stub_index.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::EdgeStubIndex;

TEST(VersioningEdgeStubIndex, InsertFindAndDeltaLessStub) {
  EdgeStubIndex index;
  EXPECT_EQ(index.Find(5), nullptr);

  auto *edge = index.GetOrCreate(5);
  ASSERT_NE(edge, nullptr);
  EXPECT_EQ(edge->gid.AsUint(), 5U);
  EXPECT_EQ(edge->delta(), nullptr);
  EXPECT_FALSE(edge->deleted());
  EXPECT_EQ(index.Find(5), edge);
  EXPECT_EQ(index.GetOrCreate(5), edge) << "a second call must return the same object";

  auto *other = index.GetOrCreate(5 + 64);  // same shard, different gid
  EXPECT_NE(other, edge);
  EXPECT_EQ(index.Find(5), edge);
  EXPECT_EQ(index.Find(5 + 64), other);
  EXPECT_EQ(index.Find(6), nullptr);
}

TEST(VersioningEdgeStubIndex, ForEachVisitsEveryStubOnceAndStopsEarly) {
  EdgeStubIndex index;
  constexpr uint64_t kCount = 1000;
  for (uint64_t gid = 0; gid < kCount; ++gid) index.GetOrCreate(gid * 7);

  std::set<uint64_t> seen;
  EXPECT_TRUE(index.ForEach([&](uint64_t gid, ms::Edge *edge) {
    EXPECT_EQ(edge->gid.AsUint(), gid);
    EXPECT_EQ(edge, index.Find(gid));
    EXPECT_TRUE(seen.insert(gid).second) << "gid " << gid << " visited twice";
    return true;
  }));
  EXPECT_EQ(seen.size(), kCount);

  size_t visited = 0;
  EXPECT_FALSE(index.ForEach([&](uint64_t, ms::Edge *) { return ++visited < 3; }));
  EXPECT_EQ(visited, 3U);

  EdgeStubIndex empty;
  EXPECT_TRUE(empty.ForEach([](uint64_t, ms::Edge *) { return false; }));
}

TEST(VersioningEdgeStubIndex, ConcurrentGetOrCreateSameGidReturnsOneStub) {
  constexpr unsigned kThreads = 16;
  constexpr uint64_t kGids = 2000;
  EdgeStubIndex index;

  std::vector<std::vector<ms::Edge *>> results(kThreads, std::vector<ms::Edge *>(kGids));
  std::atomic<unsigned> ready{0};
  {
    std::vector<std::jthread> threads;
    for (unsigned t = 0; t < kThreads; ++t) {
      threads.emplace_back([&, t] {
        ready.fetch_add(1);
        while (ready.load() < kThreads) std::this_thread::yield();
        for (uint64_t gid = 0; gid < kGids; ++gid) results[t][gid] = index.GetOrCreate(gid);
      });
    }
  }

  for (uint64_t gid = 0; gid < kGids; ++gid) {
    auto *expected = index.Find(gid);
    ASSERT_NE(expected, nullptr);
    EXPECT_EQ(expected->gid.AsUint(), gid);
    for (unsigned t = 0; t < kThreads; ++t) ASSERT_EQ(results[t][gid], expected) << "gid " << gid << " thread " << t;
  }
  size_t count = 0;
  index.ForEach([&](uint64_t, ms::Edge *) {
    ++count;
    return true;
  });
  EXPECT_EQ(count, kGids);
}

// Under an enabled OOM exception the insert runs blocked: a refused allocation must not corrupt the map. The tracker
// may report the overrun after the entry is published; the entry must then be found, and the index fully usable.
TEST(VersioningEdgeStubIndex, OomBlockedInsertLeavesIndexConsistent) {
  using memgraph::utils::MemoryTracker;
  using memgraph::utils::total_memory_tracker;
  constexpr uint64_t kOomGids = 4096;
  EdgeStubIndex index;
  size_t refusals = 0;

  // Unit tests do not run the jemalloc hooks, so put the tracker over a limit by hand: every block-exit check refuses.
  constexpr int64_t kDebt = 1 << 20;
  ASSERT_TRUE(total_memory_tracker.Alloc(kDebt));
  const auto previous_limit = total_memory_tracker.HardLimit();
  {
    const MemoryTracker::OutOfMemoryExceptionEnabler enabler;
    const MemoryTracker::RefusalHandledScope refusal_handled;
    total_memory_tracker.SetHardLimit(total_memory_tracker.Amount() - 1);
    for (uint64_t gid = 0; gid < kOomGids; ++gid) {
      try {
        EXPECT_NE(index.GetOrCreate(gid), nullptr);
      } catch (const memgraph::utils::OutOfMemoryException &) {
        // Reported after the entry was published; the verification below finds it.
        ++refusals;
      }
    }
    total_memory_tracker.SetHardLimit(previous_limit);
  }
  total_memory_tracker.Free(kDebt);
  EXPECT_EQ(refusals, kOomGids) << "every insert must be refused after it was published";

  for (uint64_t gid = 0; gid < kOomGids; ++gid) {
    auto *edge = index.GetOrCreate(gid);
    ASSERT_NE(edge, nullptr);
    EXPECT_EQ(edge->gid.AsUint(), gid);
    EXPECT_EQ(index.Find(gid), edge);
  }
  size_t count = 0;
  index.ForEach([&](uint64_t, ms::Edge *) {
    ++count;
    return true;
  });
  EXPECT_EQ(count, kOomGids);
}
