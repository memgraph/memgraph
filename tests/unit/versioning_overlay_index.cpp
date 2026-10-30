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
#include "versioning/overlay_index.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::OverlayIndex;

namespace {

// Base pointers are never dereferenced; only compared.
ms::Vertex *FakeBase(uint64_t gid) { return reinterpret_cast<ms::Vertex *>((gid + 1) * 16 + 8); }

constexpr uint64_t kGids = 100'000;

bool Recorded(uint64_t gid) { return gid % 3 != 0; }

bool IsOverlayOf(const ms::Vertex *overlay, uint64_t gid) { return overlay != nullptr && overlay->gid.AsUint() == gid; }

}  // namespace

TEST(VersioningOverlayIndex, SequentialAndSparseInsertsResolve) {
  OverlayIndex index;
  OverlayIndex::Cursor cursor;
  EXPECT_EQ(index.FindOverlay(7, cursor), nullptr);
  auto *overlay7 = index.GetOrCreateOverlay(7, FakeBase(7));  // stale negative cursor must see this
  ASSERT_TRUE(IsOverlayOf(overlay7, 7));
  EXPECT_EQ(index.FindOverlay(7, cursor), overlay7);
  EXPECT_EQ(index.FindBase(7, cursor), FakeBase(7));
  EXPECT_EQ(overlay7->delta(), nullptr);
  EXPECT_FALSE(overlay7->deleted());

  // Grow one page through every capacity (1 -> 4 -> 16) and a far-away sparse gid.
  for (uint64_t gid = 0; gid < 16; ++gid) index.GetOrCreateOverlay(gid, FakeBase(gid));
  index.GetOrCreateOverlay(1ULL << 50, FakeBase(1ULL << 50));
  for (uint64_t gid = 0; gid < 16; ++gid) {
    // The cursor still holds the retired 1-entry page; the epoch bump on replacement must send it to the live one.
    EXPECT_EQ(index.FindOverlay(gid, cursor), index.FindOverlay(gid)) << gid;
    EXPECT_EQ(index.FindBase(gid, cursor), FakeBase(gid)) << gid;
    EXPECT_TRUE(IsOverlayOf(index.FindOverlay(gid), gid));
    EXPECT_EQ(index.FindBase(gid), FakeBase(gid));
  }
  EXPECT_EQ(index.FindOverlay(7), overlay7) << "growing a page must keep the overlay object";
  EXPECT_TRUE(IsOverlayOf(index.FindOverlay(1ULL << 50, cursor), 1ULL << 50));
  EXPECT_EQ(index.FindOverlay(16), nullptr);
  EXPECT_EQ(index.GetOrCreateOverlay(3, FakeBase(3)), index.FindOverlay(3));

  size_t visited = 0;
  index.ForEachOverlay([&](ms::Vertex *) {
    ++visited;
    return true;
  });
  EXPECT_EQ(visited, 17U);
}

TEST(VersioningOverlayIndex, ForEachOverlayStopsWhenTheCallbackReturnsFalse) {
  OverlayIndex index;
  for (uint64_t gid = 0; gid < 40; ++gid) index.GetOrCreateOverlay(gid, FakeBase(gid));

  size_t visited = 0;
  const bool completed = index.ForEachOverlay([&](ms::Vertex *) { return ++visited < 5; });
  EXPECT_FALSE(completed);
  EXPECT_EQ(visited, 5U) << "no overlay may be visited after the callback asked to stop";

  visited = 0;
  EXPECT_TRUE(index.ForEachOverlay([&](ms::Vertex *) {
    ++visited;
    return true;
  }));
  EXPECT_EQ(visited, 40U);
}

TEST(VersioningOverlayIndex, ConcurrentCreatorsAgreeOnOneOverlayPerGid) {
  OverlayIndex index;
  constexpr int kWriters = 8;
  constexpr int kReaders = 2;
  std::atomic<bool> done{false};
  std::atomic<int> bad{0};

  OverlayIndex::Cursor stale;  // kept from early in the run
  auto *overlay1 = index.GetOrCreateOverlay(1, FakeBase(1));
  EXPECT_EQ(index.FindOverlay(1, stale), overlay1);
  EXPECT_EQ(index.FindOverlay(kGids - 1, stale), nullptr);

  std::vector<std::thread> readers;
  for (int r = 0; r < kReaders; ++r) {
    readers.emplace_back([&] {
      OverlayIndex::Cursor cursor;
      while (!done.load(std::memory_order_acquire)) {
        for (uint64_t gid = 0; gid < kGids; ++gid) {
          auto *overlay = index.FindOverlay(gid, cursor);
          if (overlay != nullptr && !IsOverlayOf(overlay, gid)) bad.fetch_add(1);
          auto *base_vertex = index.FindBase(gid, cursor);
          if (base_vertex != nullptr && base_vertex != FakeBase(gid)) bad.fetch_add(1);
          if (!Recorded(gid) && (overlay != nullptr || base_vertex != nullptr)) bad.fetch_add(1);
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
        if (Recorded(gid)) winners[w][gid] = index.GetOrCreateOverlay(gid, FakeBase(gid));
      }
    });
  }
  for (auto &t : writers) t.join();
  done.store(true, std::memory_order_release);
  for (auto &t : readers) t.join();
  EXPECT_EQ(bad.load(), 0);

  OverlayIndex::Cursor fresh;
  size_t recorded = 0;
  for (uint64_t gid = 0; gid < kGids; ++gid) {
    if (Recorded(gid)) {
      ++recorded;
      auto *overlay = index.FindOverlay(gid, fresh);
      EXPECT_TRUE(IsOverlayOf(overlay, gid)) << gid;
      for (int w = 0; w < kWriters; ++w) EXPECT_EQ(winners[w][gid], overlay) << gid;
      EXPECT_EQ(index.FindBase(gid, fresh), FakeBase(gid)) << gid;
      EXPECT_EQ(index.FindOverlay(gid, stale), overlay) << gid;
      EXPECT_EQ(index.FindBase(gid, stale), FakeBase(gid)) << gid;
    } else {
      EXPECT_EQ(index.FindOverlay(gid, fresh), nullptr) << gid;
      EXPECT_EQ(index.FindBase(gid, stale), nullptr) << gid;
    }
  }
  EXPECT_EQ(index.FindOverlay(kGids + 5, fresh), nullptr);
  EXPECT_EQ(index.FindBase(kGids + 5), nullptr);

  size_t visited = 0;
  index.ForEachOverlay([&](ms::Vertex *) {
    ++visited;
    return true;
  });
  EXPECT_EQ(visited, recorded) << "losing candidates must not be recorded";
}

// Even offsets are created by every thread (contended), a touched odd offset by exactly one, the rest never; all
// creators of a gid must get the same overlay and untouched gids must stay absent.
TEST(VersioningOverlayIndex, RecordsExactlyTheTouchedGidsUnderConcurrentCreators) {
  constexpr uint64_t kBase = 1'000;  // base gids are not zero-based
  constexpr size_t kVertices = 300;  // spans several 16-gid pages
  constexpr size_t kThreads = 4;

  std::vector<bool> is_touched(kVertices, false);
  for (size_t i = 0; i < kVertices; ++i) is_touched[i] = i % 2 == 0 || i % 3 != 0;
  auto creates = [&](size_t i, size_t t) { return is_touched[i] && (i % 2 == 0 || (i / 2) % kThreads == t); };

  OverlayIndex index;
  std::vector<std::vector<ms::Vertex *>> got(kThreads, std::vector<ms::Vertex *>(kVertices, nullptr));
  std::vector<std::vector<ms::Vertex *>> seen(kThreads, std::vector<ms::Vertex *>(kVertices, nullptr));
  std::vector<std::thread> threads;
  for (size_t t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      OverlayIndex::Cursor cursor;
      for (size_t i = 0; i < kVertices; ++i) {
        if (!creates(i, t)) continue;
        got[t][i] = index.GetOrCreateOverlay(kBase + i, FakeBase(kBase + i));
        seen[t][i] = index.FindOverlay(kBase + i, cursor);
      }
    });
  }
  for (auto &th : threads) th.join();

  size_t expected = 0;
  OverlayIndex::Cursor cursor;
  for (size_t i = 0; i < kVertices; ++i) {
    const uint64_t gid = kBase + i;
    auto *overlay = index.FindOverlay(gid, cursor);
    if (!is_touched[i]) {
      EXPECT_EQ(overlay, nullptr) << gid;
      EXPECT_EQ(index.FindBase(gid, cursor), nullptr) << gid;
      continue;
    }
    ++expected;
    ASSERT_TRUE(IsOverlayOf(overlay, gid)) << gid;
    EXPECT_EQ(overlay->delta(), nullptr) << gid;
    EXPECT_FALSE(overlay->deleted()) << gid;
    EXPECT_EQ(index.FindBase(gid, cursor), FakeBase(gid)) << gid;
    for (size_t t = 0; t < kThreads; ++t) {
      if (!creates(i, t)) continue;
      EXPECT_EQ(got[t][i], overlay) << "creator " << t << " got a losing candidate for " << gid;
      EXPECT_EQ(seen[t][i], overlay) << "creator " << t << " could not find its own overlay " << gid;
    }
  }
  EXPECT_EQ(index.FindOverlay(kBase - 1), nullptr);
  EXPECT_EQ(index.FindOverlay(kBase + kVertices), nullptr);

  size_t overlays = 0;
  index.ForEachOverlay([&](ms::Vertex *) {
    ++overlays;
    return true;
  });
  EXPECT_EQ(overlays, expected) << "exactly one overlay per touched gid";
}

// Refusals fire after the entry is published, so a retry must find it and nothing may leak.
TEST(VersioningOverlayIndex, OomBlockedInsertLeavesIndexConsistent) {
  using memgraph::utils::MemoryTracker;
  using memgraph::utils::total_memory_tracker;
  constexpr uint64_t kOomGids = 4096;
  OverlayIndex index;
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
        EXPECT_TRUE(IsOverlayOf(index.GetOrCreateOverlay(gid, FakeBase(gid)), gid));
      } catch (const memgraph::utils::OutOfMemoryException &) {
        ++refusals;
      }
    }
    total_memory_tracker.SetHardLimit(previous_limit);
  }
  total_memory_tracker.Free(kDebt);
  EXPECT_EQ(refusals, kOomGids) << "every insert must be refused after it was published";

  OverlayIndex::Cursor cursor;
  for (uint64_t gid = 0; gid < kOomGids; ++gid) {
    auto *overlay = index.GetOrCreateOverlay(gid, FakeBase(gid));
    ASSERT_TRUE(IsOverlayOf(overlay, gid));
    EXPECT_EQ(index.FindOverlay(gid, cursor), overlay);
    EXPECT_EQ(index.FindBase(gid, cursor), FakeBase(gid));
  }
  size_t count = 0;
  index.ForEachOverlay([&](ms::Vertex *) {
    ++count;
    return true;
  });
  EXPECT_EQ(count, kOomGids);
}
