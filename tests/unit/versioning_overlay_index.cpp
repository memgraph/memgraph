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

#include <cstdint>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "versioning/overlay_index.hpp"
#include "versioning_oom_helper.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::OverlayIndex;

namespace {

// Base pointers are never dereferenced; only compared.
ms::Vertex *FakeBase(uint64_t gid) { return reinterpret_cast<ms::Vertex *>((gid + 1) * 16 + 8); }

}  // namespace

TEST(VersioningOverlayIndex, ConcurrentCreatorsGetOneOverlayPerGidAndUntouchedGidsStayAbsent) {
  constexpr size_t kThreads = 8;
  constexpr uint64_t kGids = 5000;  // even gids only
  OverlayIndex index;
  std::vector<std::vector<ms::Vertex *>> got(kThreads, std::vector<ms::Vertex *>(kGids, nullptr));
  {
    std::vector<std::jthread> threads;
    for (size_t t = 0; t < kThreads; ++t) {
      threads.emplace_back([&, t] {
        for (uint64_t i = 0; i < kGids; i += 2) got[t][i] = index.GetOrCreateOverlay(i, FakeBase(i));
      });
    }
  }

  size_t visited = 0;
  EXPECT_TRUE(index.ForEachOverlay([&](ms::Vertex *) {
    ++visited;
    return true;
  }));
  EXPECT_EQ(visited, kGids / 2);
  for (uint64_t gid = 0; gid < kGids; ++gid) {
    const auto found = index.Find(gid);
    if (gid % 2 == 1) {
      EXPECT_EQ(found.overlay, nullptr) << gid;
      EXPECT_EQ(index.FindBase(gid), nullptr) << gid;
      continue;
    }
    ASSERT_NE(found.overlay, nullptr) << gid;
    EXPECT_EQ(found.overlay->gid.AsUint(), gid);
    EXPECT_EQ(found.overlay->delta(), nullptr);
    EXPECT_EQ(found.base_vertex, FakeBase(gid));
    EXPECT_EQ(index.FindOverlay(gid), found.overlay);
    for (size_t t = 0; t < kThreads; ++t) ASSERT_EQ(got[t][gid], found.overlay) << gid;
  }
}

TEST(VersioningOverlayIndex, FirstCreatorsBaseStands) {
  OverlayIndex index;
  auto *overlay = index.GetOrCreateOverlay(3, FakeBase(3));
  EXPECT_EQ(index.GetOrCreateOverlay(3, FakeBase(4)), overlay);
  EXPECT_EQ(index.FindBase(3), FakeBase(3));
}

TEST(VersioningOverlayIndex, ForEachOverlayStopsWhenTheCallbackReturnsFalse) {
  OverlayIndex index;
  for (uint64_t gid = 0; gid < 40; ++gid) index.GetOrCreateOverlay(gid, FakeBase(gid));
  size_t visited = 0;
  EXPECT_FALSE(index.ForEachOverlay([&](ms::Vertex *) { return ++visited < 5; }));
  EXPECT_EQ(visited, 5U);
}

// A refusal is reported after the entry is published, so a retry must find it.
TEST(VersioningOverlayIndex, OomRefusalLeavesPublishedEntryAndRetryWorks) {
  constexpr uint64_t kGidCount = 256;
  OverlayIndex index;
  EXPECT_EQ(CountOomRefusals(kGidCount, [&](uint64_t gid) { index.GetOrCreateOverlay(gid, FakeBase(gid)); }),
            kGidCount);

  size_t count = 0;
  index.ForEachOverlay([&](ms::Vertex *) {
    ++count;
    return true;
  });
  EXPECT_EQ(count, kGidCount);
  for (uint64_t gid = 0; gid < kGidCount; ++gid) {
    auto *overlay = index.GetOrCreateOverlay(gid, FakeBase(gid));
    EXPECT_EQ(index.FindOverlay(gid), overlay);
    EXPECT_EQ(index.FindBase(gid), FakeBase(gid));
  }
}
