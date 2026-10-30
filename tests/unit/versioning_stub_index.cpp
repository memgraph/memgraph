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

#include "utils/memory_tracker.hpp"
#include "versioning/stub_index.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::StubIndex;

namespace {

// Fork pointers are never dereferenced; only compared.
ms::Vertex *FakeFork(uint64_t gid) { return reinterpret_cast<ms::Vertex *>((gid + 1) * 16 + 8); }

constexpr uint64_t kGids = 100'000;

bool Recorded(uint64_t gid) { return gid % 3 != 0; }

bool IsStubOf(const ms::Vertex *stub, uint64_t gid) { return stub != nullptr && stub->gid.AsUint() == gid; }

}  // namespace

TEST(VersioningStubIndex, SequentialAndSparseInsertsResolve) {
  StubIndex index;
  StubIndex::Cursor cursor;
  EXPECT_EQ(index.FindStub(7, cursor), nullptr);
  auto *stub7 = index.GetOrCreateStub(7, FakeFork(7));  // stale negative cursor must see this
  ASSERT_TRUE(IsStubOf(stub7, 7));
  EXPECT_EQ(index.FindStub(7, cursor), stub7);
  EXPECT_EQ(index.FindFork(7, cursor), FakeFork(7));
  EXPECT_EQ(stub7->delta(), nullptr);
  EXPECT_FALSE(stub7->deleted());

  // Grow one page through every capacity (1 -> 4 -> 16) and a far-away sparse gid.
  for (uint64_t gid = 0; gid < 16; ++gid) index.GetOrCreateStub(gid, FakeFork(gid));
  index.GetOrCreateStub(1ULL << 50, FakeFork(1ULL << 50));
  for (uint64_t gid = 0; gid < 16; ++gid) {
    // The cursor still holds the retired 1-entry page; the epoch bump on replacement must send it to the live one.
    EXPECT_EQ(index.FindStub(gid, cursor), index.FindStub(gid)) << gid;
    EXPECT_EQ(index.FindFork(gid, cursor), FakeFork(gid)) << gid;
    EXPECT_TRUE(IsStubOf(index.FindStub(gid), gid));
    EXPECT_EQ(index.FindFork(gid), FakeFork(gid));
  }
  EXPECT_EQ(index.FindStub(7), stub7) << "growing a page must keep the stub object";
  EXPECT_TRUE(IsStubOf(index.FindStub(1ULL << 50, cursor), 1ULL << 50));
  EXPECT_EQ(index.FindStub(16), nullptr);
  EXPECT_EQ(index.GetOrCreateStub(3, FakeFork(3)), index.FindStub(3));

  size_t visited = 0;
  index.ForEachStub([&](ms::Vertex *) {
    ++visited;
    return true;
  });
  EXPECT_EQ(visited, 17U);
}

TEST(VersioningStubIndex, ForEachStubStopsWhenTheCallbackReturnsFalse) {
  StubIndex index;
  for (uint64_t gid = 0; gid < 40; ++gid) index.GetOrCreateStub(gid, FakeFork(gid));

  size_t visited = 0;
  const bool completed = index.ForEachStub([&](ms::Vertex *) { return ++visited < 5; });
  EXPECT_FALSE(completed);
  EXPECT_EQ(visited, 5U) << "no stub may be visited after the callback asked to stop";

  visited = 0;
  EXPECT_TRUE(index.ForEachStub([&](ms::Vertex *) {
    ++visited;
    return true;
  }));
  EXPECT_EQ(visited, 40U);
}

TEST(VersioningStubIndex, ConcurrentCreatorsAgreeOnOneStubPerGid) {
  StubIndex index;
  constexpr int kWriters = 8;
  constexpr int kReaders = 2;
  std::atomic<bool> done{false};
  std::atomic<int> bad{0};

  StubIndex::Cursor stale;  // kept from early in the run
  auto *stub1 = index.GetOrCreateStub(1, FakeFork(1));
  EXPECT_EQ(index.FindStub(1, stale), stub1);
  EXPECT_EQ(index.FindStub(kGids - 1, stale), nullptr);

  std::vector<std::thread> readers;
  for (int r = 0; r < kReaders; ++r) {
    readers.emplace_back([&] {
      StubIndex::Cursor cursor;
      while (!done.load(std::memory_order_acquire)) {
        for (uint64_t gid = 0; gid < kGids; ++gid) {
          auto *stub = index.FindStub(gid, cursor);
          if (stub != nullptr && !IsStubOf(stub, gid)) bad.fetch_add(1);
          auto *fork = index.FindFork(gid, cursor);
          if (fork != nullptr && fork != FakeFork(gid)) bad.fetch_add(1);
          if (!Recorded(gid) && (stub != nullptr || fork != nullptr)) bad.fetch_add(1);
        }
      }
    });
  }

  // Overlapping ranges: each writer covers all gids, starting at a different offset.
  std::vector<std::vector<ms::Vertex *>> winners(kWriters, std::vector<ms::Vertex *>(kGids, nullptr));
  std::vector<std::thread> writers;
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      for (uint64_t i = 0; i < kGids; ++i) {
        const uint64_t gid = (i + w * (kGids / kWriters)) % kGids;
        if (Recorded(gid)) winners[w][gid] = index.GetOrCreateStub(gid, FakeFork(gid));
      }
    });
  }
  for (auto &t : writers) t.join();
  done.store(true, std::memory_order_release);
  for (auto &t : readers) t.join();
  EXPECT_EQ(bad.load(), 0);

  StubIndex::Cursor fresh;
  size_t recorded = 0;
  for (uint64_t gid = 0; gid < kGids; ++gid) {
    if (Recorded(gid)) {
      ++recorded;
      auto *stub = index.FindStub(gid, fresh);
      EXPECT_TRUE(IsStubOf(stub, gid)) << gid;
      for (int w = 0; w < kWriters; ++w) EXPECT_EQ(winners[w][gid], stub) << gid;
      EXPECT_EQ(index.FindFork(gid, fresh), FakeFork(gid)) << gid;
      EXPECT_EQ(index.FindStub(gid, stale), stub) << gid;
      EXPECT_EQ(index.FindFork(gid, stale), FakeFork(gid)) << gid;
    } else {
      EXPECT_EQ(index.FindStub(gid, fresh), nullptr) << gid;
      EXPECT_EQ(index.FindFork(gid, stale), nullptr) << gid;
    }
  }
  EXPECT_EQ(index.FindStub(kGids + 5, fresh), nullptr);
  EXPECT_EQ(index.FindFork(kGids + 5), nullptr);

  size_t visited = 0;
  index.ForEachStub([&](ms::Vertex *) {
    ++visited;
    return true;
  });
  EXPECT_EQ(visited, recorded) << "losing candidates must not be recorded";
}

// Even offsets are created by every thread (contended), a touched odd offset by exactly one, the rest never; all
// creators of a gid must get the same stub and untouched gids must stay absent.
TEST(VersioningStubIndex, RecordsExactlyTheTouchedGidsUnderConcurrentCreators) {
  constexpr uint64_t kBase = 1'000;  // fork gids are not zero-based
  constexpr size_t kVertices = 300;  // spans several 16-gid pages
  constexpr size_t kThreads = 4;

  std::vector<bool> is_touched(kVertices, false);
  for (size_t i = 0; i < kVertices; ++i) is_touched[i] = i % 2 == 0 || i % 3 != 0;
  auto creates = [&](size_t i, size_t t) { return is_touched[i] && (i % 2 == 0 || (i / 2) % kThreads == t); };

  StubIndex index;
  std::vector<std::vector<ms::Vertex *>> got(kThreads, std::vector<ms::Vertex *>(kVertices, nullptr));
  std::vector<std::vector<ms::Vertex *>> seen(kThreads, std::vector<ms::Vertex *>(kVertices, nullptr));
  std::vector<std::thread> threads;
  for (size_t t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      StubIndex::Cursor cursor;
      for (size_t i = 0; i < kVertices; ++i) {
        if (!creates(i, t)) continue;
        got[t][i] = index.GetOrCreateStub(kBase + i, FakeFork(kBase + i));
        seen[t][i] = index.FindStub(kBase + i, cursor);
      }
    });
  }
  for (auto &th : threads) th.join();

  size_t expected = 0;
  StubIndex::Cursor cursor;
  for (size_t i = 0; i < kVertices; ++i) {
    const uint64_t gid = kBase + i;
    auto *stub = index.FindStub(gid, cursor);
    if (!is_touched[i]) {
      EXPECT_EQ(stub, nullptr) << gid;
      EXPECT_EQ(index.FindFork(gid, cursor), nullptr) << gid;
      continue;
    }
    ++expected;
    ASSERT_TRUE(IsStubOf(stub, gid)) << gid;
    EXPECT_EQ(stub->delta(), nullptr) << gid;
    EXPECT_FALSE(stub->deleted()) << gid;
    EXPECT_EQ(index.FindFork(gid, cursor), FakeFork(gid)) << gid;
    for (size_t t = 0; t < kThreads; ++t) {
      if (!creates(i, t)) continue;
      EXPECT_EQ(got[t][i], stub) << "creator " << t << " got a losing candidate for " << gid;
      EXPECT_EQ(seen[t][i], stub) << "creator " << t << " could not find its own stub " << gid;
    }
  }
  EXPECT_EQ(index.FindStub(kBase - 1), nullptr);
  EXPECT_EQ(index.FindStub(kBase + kVertices), nullptr);

  size_t stubs = 0;
  index.ForEachStub([&](ms::Vertex *) {
    ++stubs;
    return true;
  });
  EXPECT_EQ(stubs, expected) << "exactly one stub per touched gid";
}

// Refusals fire after the entry is published, so a retry must find it and nothing may leak.
TEST(VersioningStubIndex, OomBlockedInsertLeavesIndexConsistent) {
  using memgraph::utils::MemoryTracker;
  using memgraph::utils::total_memory_tracker;
  constexpr uint64_t kOomGids = 4096;
  StubIndex index;
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
        EXPECT_TRUE(IsStubOf(index.GetOrCreateStub(gid, FakeFork(gid)), gid));
      } catch (const memgraph::utils::OutOfMemoryException &) {
        ++refusals;
      }
    }
    total_memory_tracker.SetHardLimit(previous_limit);
  }
  total_memory_tracker.Free(kDebt);
  EXPECT_EQ(refusals, kOomGids) << "every insert must be refused after it was published";

  StubIndex::Cursor cursor;
  for (uint64_t gid = 0; gid < kOomGids; ++gid) {
    auto *stub = index.GetOrCreateStub(gid, FakeFork(gid));
    ASSERT_TRUE(IsStubOf(stub, gid));
    EXPECT_EQ(index.FindStub(gid, cursor), stub);
    EXPECT_EQ(index.FindFork(gid, cursor), FakeFork(gid));
  }
  size_t count = 0;
  index.ForEachStub([&](ms::Vertex *) {
    ++count;
    return true;
  });
  EXPECT_EQ(count, kOomGids);
}
