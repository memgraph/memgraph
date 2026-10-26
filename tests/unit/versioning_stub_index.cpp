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

#include "storage/v2/vertex.hpp"
#include "versioning/stub_index.hpp"

using memgraph::storage::Vertex;
using memgraph::versioning::StubIndex;

namespace {
// Never dereferenced; distinct, non-null, derived from the gid.
Vertex *FakeStub(uint64_t gid) { return reinterpret_cast<Vertex *>((gid + 1) << 4); }

Vertex *FakeFork(uint64_t gid) { return reinterpret_cast<Vertex *>(((gid + 1) << 4) | 8); }
}  // namespace

TEST(VersioningStubIndex, EmptyAndGidZero) {
  StubIndex index;
  EXPECT_EQ(index.FindStub(0), nullptr);
  EXPECT_EQ(index.FindFork(123), nullptr);
  index.Insert(0, nullptr, nullptr);
  EXPECT_EQ(index.FindStub(0), nullptr);
  index.Insert(0, FakeStub(0), nullptr);
  EXPECT_EQ(index.FindStub(0), FakeStub(0));
  EXPECT_EQ(index.FindFork(0), nullptr);
  index.Insert(0, nullptr, FakeFork(0));
  EXPECT_EQ(index.FindStub(0), FakeStub(0));
  EXPECT_EQ(index.FindFork(0), FakeFork(0));
}

TEST(VersioningStubIndex, ConcurrentOverlappingInsertsAcrossSegmentGrowth) {
  constexpr uint64_t kGids = 200'000;
  constexpr unsigned kThreads = 8;
  constexpr uint64_t kSpan = 50'000;
  StubIndex index;
  std::atomic<bool> done{false};
  std::atomic<bool> bad_read{false};

  // Even threads record only the stub, odd threads stub+fork; neighbouring threads overlap, so each even gid is
  // inserted by one thread of each kind. Odd gids are never inserted.
  std::vector<std::thread> writers;
  for (unsigned t = 0; t < kThreads; ++t) {
    writers.emplace_back([&, t] {
      const uint64_t begin = t * (kGids / kThreads);
      for (uint64_t n = 0; n < kSpan; ++n) {
        const uint64_t gid = ((begin + n) % kGids) & ~uint64_t{1};
        index.Insert(gid, FakeStub(gid), t % 2 == 0 ? nullptr : FakeFork(gid));
      }
    });
  }
  std::thread reader{[&] {
    while (!done.load(std::memory_order_acquire)) {
      for (uint64_t gid = 0; gid < kGids; gid += 97) {
        auto *s = index.FindStub(gid);
        auto *f = index.FindFork(gid);
        if ((s != nullptr && s != FakeStub(gid)) || (f != nullptr && f != FakeFork(gid)) ||
            ((gid & 1) != 0 && (s != nullptr || f != nullptr))) {
          bad_read = true;
        }
      }
    }
  }};
  for (auto &w : writers) w.join();
  done = true;
  reader.join();
  EXPECT_FALSE(bad_read.load());

  for (uint64_t gid = 0; gid < kGids; ++gid) {
    if ((gid & 1) == 0) {
      ASSERT_EQ(index.FindStub(gid), FakeStub(gid)) << gid;
      ASSERT_EQ(index.FindFork(gid), FakeFork(gid)) << gid;
    } else {
      ASSERT_EQ(index.FindStub(gid), nullptr) << gid;
      ASSERT_EQ(index.FindFork(gid), nullptr) << gid;
    }
  }
  EXPECT_EQ(index.FindStub(kGids + 12345), nullptr);
}
