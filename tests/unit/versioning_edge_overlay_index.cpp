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
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "versioning/edge_overlay_index.hpp"
#include "versioning/overlay_index.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::BaseEdgeDeleteCandidates;
using memgraph::versioning::EdgeOverlayIndex;
using memgraph::versioning::ListBaseEdgeDelete;
using memgraph::versioning::OverlayIndex;

namespace {
ms::Gid G(uint64_t gid) { return ms::Gid::FromUint(gid); }
}  // namespace

TEST(VersioningEdgeOverlayIndex, FirstCreatorsBaseStandsAndOverlayIsDeltaLess) {
  EdgeOverlayIndex index;
  ms::Edge base_edge(ms::Gid::FromUint(9), nullptr);
  ms::Edge another(ms::Gid::FromUint(9), nullptr);
  EXPECT_EQ(index.Find(G(9)), nullptr);
  EXPECT_FALSE(index.FindBase(G(9)).has_value());

  auto *overlay = index.GetOrCreate(G(9), ms::EdgeRef{&base_edge});
  EXPECT_EQ(overlay->gid.AsUint(), 9U);
  EXPECT_EQ(overlay->delta(), nullptr);
  EXPECT_EQ(index.GetOrCreate(G(9), ms::EdgeRef{&another}), overlay);
  EXPECT_EQ(index.Find(G(9)), overlay);
  EXPECT_EQ(index.FindBase(G(9))->ptr, &base_edge);
  EXPECT_EQ(EdgeOverlayIndex::BaseOf(overlay)->ptr, &base_edge);

  auto *bare = index.GetOrCreate(G(10));
  EXPECT_FALSE(index.FindBase(G(10)).has_value());
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
        for (uint64_t gid = 0; gid < kGids; ++gid) results[t][gid] = index.GetOrCreate(G(gid));
      });
    }
  }
  for (uint64_t gid = 0; gid < kGids; ++gid) {
    auto *expected = index.Find(G(gid));
    ASSERT_NE(expected, nullptr);
    EXPECT_EQ(expected->gid.AsUint(), gid);
    for (unsigned t = 0; t < kThreads; ++t) ASSERT_EQ(results[t][gid], expected) << "gid " << gid << " thread " << t;
  }
}

namespace {

ms::Vertex *MakeOverlay(OverlayIndex &vertices, uint64_t gid) {
  return vertices.GetOrCreateOverlay(G(gid), reinterpret_cast<ms::Vertex *>((gid + 1) * 16 + 8), {});
}

}  // namespace

TEST(VersioningEdgeOverlayIndex, ListingFillsBothEndpointsOnceAndSelfLoopListsInBothDirections) {
  OverlayIndex vertices;
  EdgeOverlayIndex edges;
  auto *a = MakeOverlay(vertices, 1);
  auto *b = MakeOverlay(vertices, 2);
  auto *e1 = edges.GetOrCreate(G(1));
  auto *loop = edges.GetOrCreate(G(2));
  EXPECT_TRUE(BaseEdgeDeleteCandidates(a, ms::EdgeDirection::OUT).empty());

  ListBaseEdgeDelete(a, b, e1);
  EXPECT_EQ(BaseEdgeDeleteCandidates(a, ms::EdgeDirection::OUT), std::vector<ms::Edge *>{e1});
  EXPECT_TRUE(BaseEdgeDeleteCandidates(a, ms::EdgeDirection::IN).empty());
  EXPECT_EQ(BaseEdgeDeleteCandidates(b, ms::EdgeDirection::IN), std::vector<ms::Edge *>{e1});
  EXPECT_TRUE(BaseEdgeDeleteCandidates(b, ms::EdgeDirection::OUT).empty());

  // Second call is a no-op: `listed` short-circuits it.
  const auto before = BaseEdgeDeleteCandidates(a, ms::EdgeDirection::OUT);
  ListBaseEdgeDelete(a, b, e1);
  EXPECT_EQ(BaseEdgeDeleteCandidates(a, ms::EdgeDirection::OUT), before);
  EXPECT_EQ(BaseEdgeDeleteCandidates(b, ms::EdgeDirection::IN).size(), 1U);

  // A snapshot is unaffected by later appends.
  const auto snapshot = BaseEdgeDeleteCandidates(a, ms::EdgeDirection::OUT);
  ListBaseEdgeDelete(a, a, loop);
  EXPECT_EQ(snapshot, std::vector<ms::Edge *>{e1});
  EXPECT_EQ(BaseEdgeDeleteCandidates(a, ms::EdgeDirection::OUT), (std::vector<ms::Edge *>{e1, loop}));
  EXPECT_EQ(BaseEdgeDeleteCandidates(a, ms::EdgeDirection::IN), std::vector<ms::Edge *>{loop});
}

TEST(VersioningEdgeOverlayIndex, ConcurrentListingOnSharedEndpointLosesNoEdgeUnderConcurrentReads) {
  constexpr unsigned kWriters = 4;
  constexpr unsigned kReaders = 3;
  constexpr uint64_t kEdgesPerWriter = 500;
  OverlayIndex vertices;
  EdgeOverlayIndex edges;
  auto *hub = MakeOverlay(vertices, 0);
  std::vector<ms::Vertex *> leaves;
  for (uint64_t i = 1; i <= kWriters * kEdgesPerWriter; ++i) leaves.push_back(MakeOverlay(vertices, i));

  std::atomic<unsigned> writers_done{0};
  std::vector<std::thread> threads;
  for (unsigned w = 0; w < kWriters; ++w) {
    threads.emplace_back([&, w] {
      for (uint64_t i = 0; i < kEdgesPerWriter; ++i) {
        const uint64_t id = w * kEdgesPerWriter + i;
        ListBaseEdgeDelete(hub, leaves[id], edges.GetOrCreate(G(id + 1)));
      }
      writers_done.fetch_add(1, std::memory_order_release);
    });
  }
  for (unsigned r = 0; r < kReaders; ++r) {
    threads.emplace_back([&] {
      size_t last = 0;
      while (writers_done.load(std::memory_order_acquire) < kWriters) {
        const auto out = BaseEdgeDeleteCandidates(hub, ms::EdgeDirection::OUT);
        EXPECT_GE(out.size(), last);
        last = out.size();
      }
    });
  }
  for (auto &t : threads) t.join();

  auto out = BaseEdgeDeleteCandidates(hub, ms::EdgeDirection::OUT);
  EXPECT_EQ(out.size(), kWriters * kEdgesPerWriter);  // distinct edges: no duplicates possible
  for (uint64_t id = 0; id < kWriters * kEdgesPerWriter; ++id) {
    EXPECT_NE(std::ranges::find(out, edges.Find(G(id + 1))), out.end()) << id;
    EXPECT_EQ(BaseEdgeDeleteCandidates(leaves[id], ms::EdgeDirection::IN),
              std::vector<ms::Edge *>{edges.Find(G(id + 1))});
  }
}
