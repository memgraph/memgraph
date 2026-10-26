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

#include "versioning/stub_index.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::StubIndex;

namespace {

// Never dereferenced; only compared.
ms::Vertex *FakeStub(uint64_t gid) { return reinterpret_cast<ms::Vertex *>((gid + 1) * 16); }

ms::Vertex *FakeFork(uint64_t gid) { return reinterpret_cast<ms::Vertex *>((gid + 1) * 16 + 8); }

constexpr uint64_t kGids = 100'000;

// Every third gid of the range is left unrecorded.
bool Recorded(uint64_t gid) { return gid % 3 != 0; }

}  // namespace

TEST(VersioningStubIndex, SequentialAndSparseInsertsResolve) {
  StubIndex index;
  StubIndex::Cursor cursor;
  EXPECT_EQ(index.FindStub(7, cursor), nullptr);
  index.Insert(7, FakeStub(7), nullptr);  // stale negative cursor must see this
  EXPECT_EQ(index.FindStub(7, cursor), FakeStub(7));
  EXPECT_EQ(index.FindFork(7, cursor), nullptr);
  index.Insert(7, nullptr, FakeFork(7));
  EXPECT_EQ(index.FindFork(7, cursor), FakeFork(7));
  EXPECT_EQ(index.FindStub(7, cursor), FakeStub(7));

  // Grow one page through every capacity (1 -> 4 -> 16) and a far-away sparse gid.
  for (uint64_t gid = 0; gid < 16; ++gid) index.Insert(gid, FakeStub(gid), FakeFork(gid));
  index.Insert(1ULL << 50, FakeStub(1ULL << 50), nullptr);
  for (uint64_t gid = 0; gid < 16; ++gid) {
    EXPECT_EQ(index.FindStub(gid), FakeStub(gid));
    EXPECT_EQ(index.FindFork(gid), FakeFork(gid));
  }
  EXPECT_EQ(index.FindStub(1ULL << 50, cursor), FakeStub(1ULL << 50));
  EXPECT_EQ(index.FindStub(16), nullptr);
  EXPECT_EQ(index.FindFork(1ULL << 50), nullptr);

  // Idempotent re-insert changes nothing.
  index.Insert(3, FakeStub(3), FakeFork(3));
  EXPECT_EQ(index.FindStub(3), FakeStub(3));
}

TEST(VersioningStubIndex, ConcurrentWritersAndOrderedScanReaders) {
  StubIndex index;
  constexpr int kWriters = 8;
  constexpr int kReaders = 2;
  std::atomic<bool> done{false};
  std::atomic<int> bad{0};

  StubIndex::Cursor stale;  // kept from early in the run
  index.Insert(1, FakeStub(1), FakeFork(1));
  EXPECT_EQ(index.FindStub(1, stale), FakeStub(1));
  EXPECT_EQ(index.FindStub(kGids - 1, stale), nullptr);

  std::vector<std::thread> readers;
  for (int r = 0; r < kReaders; ++r) {
    readers.emplace_back([&] {
      StubIndex::Cursor cursor;
      while (!done.load(std::memory_order_acquire)) {
        for (uint64_t gid = 0; gid < kGids; ++gid) {
          auto *stub = index.FindStub(gid, cursor);
          if (stub != nullptr && stub != FakeStub(gid)) bad.fetch_add(1);
          auto *fork = index.FindFork(gid, cursor);
          if (fork != nullptr && fork != FakeFork(gid)) bad.fetch_add(1);
          if (!Recorded(gid) && (stub != nullptr || fork != nullptr)) bad.fetch_add(1);
        }
      }
    });
  }

  std::vector<std::thread> writers;
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      // Overlapping ranges: each writer covers all gids, starting at a different offset; stub-only pass then fork fill.
      for (uint64_t i = 0; i < kGids; ++i) {
        const uint64_t gid = (i + w * (kGids / kWriters)) % kGids;
        if (Recorded(gid)) index.Insert(gid, FakeStub(gid), nullptr);
      }
      for (uint64_t i = 0; i < kGids; ++i) {
        const uint64_t gid = (i + w * (kGids / kWriters)) % kGids;
        if (Recorded(gid)) index.Insert(gid, FakeStub(gid), FakeFork(gid));
      }
    });
  }
  for (auto &t : writers) t.join();
  done.store(true, std::memory_order_release);
  for (auto &t : readers) t.join();
  EXPECT_EQ(bad.load(), 0);

  StubIndex::Cursor fresh;
  for (uint64_t gid = 0; gid < kGids; ++gid) {
    if (Recorded(gid)) {
      EXPECT_EQ(index.FindStub(gid, fresh), FakeStub(gid)) << gid;
      EXPECT_EQ(index.FindFork(gid, fresh), FakeFork(gid)) << gid;
      EXPECT_EQ(index.FindStub(gid, stale), FakeStub(gid)) << gid;
      EXPECT_EQ(index.FindFork(gid, stale), FakeFork(gid)) << gid;
    } else {
      EXPECT_EQ(index.FindStub(gid, fresh), nullptr) << gid;
      EXPECT_EQ(index.FindFork(gid, stale), nullptr) << gid;
    }
  }
  EXPECT_EQ(index.FindStub(kGids + 5, fresh), nullptr);
  EXPECT_EQ(index.FindFork(kGids + 5), nullptr);
}
