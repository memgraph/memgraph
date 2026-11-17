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

#include "storage/v2/id_types.hpp"
#include "versioning/edge_overlay_index.hpp"
#include "versioning_oom_helper.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::EdgeOverlayIndex;

TEST(VersioningEdgeOverlayIndex, FirstCreatorsBaseStandsAndOverlayIsDeltaLess) {
  EdgeOverlayIndex index;
  ms::Edge base_edge(ms::Gid::FromUint(9), nullptr);
  ms::Edge another(ms::Gid::FromUint(9), nullptr);
  EXPECT_EQ(index.Find(9), nullptr);
  EXPECT_FALSE(index.FindBase(9).has_value());

  auto *overlay = index.GetOrCreate(9, ms::EdgeRef{&base_edge});
  EXPECT_EQ(overlay->gid.AsUint(), 9U);
  EXPECT_EQ(overlay->delta(), nullptr);
  EXPECT_EQ(index.GetOrCreate(9, ms::EdgeRef{&another}), overlay);
  EXPECT_EQ(index.Find(9), overlay);
  EXPECT_EQ(index.FindBase(9)->ptr, &base_edge);
  EXPECT_EQ(EdgeOverlayIndex::BaseOf(overlay)->ptr, &base_edge);

  auto *bare = index.GetOrCreate(10);
  EXPECT_FALSE(index.FindBase(10).has_value());
  EXPECT_FALSE(EdgeOverlayIndex::BaseOf(bare).has_value());
}

TEST(VersioningEdgeOverlayIndex, ConcurrentGetOrCreateSameGidReturnsOneOverlay) {
  constexpr unsigned kThreads = 16;
  constexpr uint64_t kGids = 2000;
  EdgeOverlayIndex index;
  std::vector<std::vector<ms::Edge *>> results(kThreads, std::vector<ms::Edge *>(kGids));
  {
    std::vector<std::jthread> threads;
    for (unsigned t = 0; t < kThreads; ++t) {
      threads.emplace_back([&, t] {
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
}

// A refusal is reported after the entry is published, so a retry must find it.
TEST(VersioningEdgeOverlayIndex, OomRefusalLeavesPublishedEntryAndRetryWorks) {
  constexpr uint64_t kGidCount = 256;
  EdgeOverlayIndex index;
  EXPECT_EQ(CountOomRefusals(kGidCount, [&](uint64_t gid) { index.GetOrCreate(gid); }), kGidCount);
  for (uint64_t gid = 0; gid < kGidCount; ++gid) {
    auto *overlay = index.GetOrCreate(gid);
    EXPECT_EQ(index.Find(gid), overlay);
  }
}
