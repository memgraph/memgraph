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
#include <barrier>
#include <cstdint>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "versioning/edge_overlay_index.hpp"
#include "versioning/overlay_index.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::EdgeOverlayIndex;
using memgraph::versioning::kBothSides;
using memgraph::versioning::kFromSide;
using memgraph::versioning::kToSide;
using memgraph::versioning::ListBaseEdgeDelete;
using memgraph::versioning::OverlayIndex;

namespace {
constexpr uint64_t kGidFloor = uint64_t{1} << 20;

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
  EXPECT_EQ(EdgeOverlayIndex::BaseOf(overlay).ptr, &base_edge);
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
        for (uint64_t gid = 0; gid < kGids; ++gid) results[t][gid] = index.GetOrCreate(G(gid), ms::EdgeRef{G(gid)});
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

using Candidates = std::vector<ms::Edge *>;

Candidates CandidatesOf(const OverlayIndex &index, uint64_t gid, ms::EdgeDirection direction) {
  const auto copy = index.FindForExpansion(G(gid), direction, true).candidates;
  return {copy.begin(), copy.end()};
}

ms::Edge *MakeEdge(EdgeOverlayIndex &edges, uint64_t gid) { return edges.GetOrCreate(G(gid), ms::EdgeRef{G(gid)}); }

}  // namespace

TEST(VersioningEdgeOverlayIndex, ListingIsPerSideAndRepeatsAreNoOps) {
  constexpr auto kOut = ms::EdgeDirection::OUT;
  constexpr auto kIn = ms::EdgeDirection::IN;
  OverlayIndex vertices{kGidFloor};
  EdgeOverlayIndex edges;
  auto *e = MakeEdge(edges, 1);
  EXPECT_TRUE(CandidatesOf(vertices, 1, kOut).empty());

  ListBaseEdgeDelete(vertices, G(1), G(2), e, kToSide);
  EXPECT_TRUE(CandidatesOf(vertices, 1, kOut).empty());
  EXPECT_EQ(CandidatesOf(vertices, 2, kIn), Candidates{e});
  EXPECT_FALSE(vertices.HasEntry(G(1)));

  ListBaseEdgeDelete(vertices, G(1), G(2), e, kBothSides);
  EXPECT_EQ(CandidatesOf(vertices, 1, kOut), Candidates{e});
  EXPECT_EQ(CandidatesOf(vertices, 2, kIn), Candidates{e}) << "the to side is not listed again";
  EXPECT_TRUE(CandidatesOf(vertices, 1, kIn).empty());
  EXPECT_TRUE(CandidatesOf(vertices, 2, kOut).empty());

  ListBaseEdgeDelete(vertices, G(1), G(2), e);
  ListBaseEdgeDelete(vertices, G(1), G(2), e, kFromSide);
  EXPECT_EQ(CandidatesOf(vertices, 1, kOut).size(), 1U);
  EXPECT_EQ(CandidatesOf(vertices, 2, kIn).size(), 1U);
}

TEST(VersioningEdgeOverlayIndex, SelfLoopListsOnceInEachDirectionAndASnapshotIsUnaffectedByLaterAppends) {
  OverlayIndex vertices{kGidFloor};
  EdgeOverlayIndex edges;
  auto *e1 = MakeEdge(edges, 1);
  auto *loop = MakeEdge(edges, 2);
  ListBaseEdgeDelete(vertices, G(1), G(2), e1);

  const auto snapshot = CandidatesOf(vertices, 1, ms::EdgeDirection::OUT);
  ListBaseEdgeDelete(vertices, G(1), G(1), loop);
  EXPECT_EQ(snapshot, Candidates{e1});
  EXPECT_EQ(CandidatesOf(vertices, 1, ms::EdgeDirection::OUT), (Candidates{e1, loop}));
  EXPECT_EQ(CandidatesOf(vertices, 1, ms::EdgeDirection::IN), Candidates{loop});
}

TEST(VersioningEdgeOverlayIndex, ConcurrentListingOnSharedEndpointLosesNoEdgeUnderConcurrentReads) {
  constexpr unsigned kWriters = 4;
  constexpr unsigned kReaders = 3;
  constexpr uint64_t kEdgesPerWriter = 500;
  OverlayIndex vertices{kGidFloor};
  EdgeOverlayIndex edges;

  std::atomic<unsigned> writers_done{0};
  std::vector<std::thread> threads;
  for (unsigned w = 0; w < kWriters; ++w) {
    threads.emplace_back([&, w] {
      for (uint64_t i = 0; i < kEdgesPerWriter; ++i) {
        const uint64_t id = w * kEdgesPerWriter + i;
        ListBaseEdgeDelete(vertices, G(0), G(id + 1), MakeEdge(edges, id + 1));
      }
      writers_done.fetch_add(1, std::memory_order_release);
    });
  }
  for (unsigned r = 0; r < kReaders; ++r) {
    threads.emplace_back([&] {
      size_t last = 0;
      while (writers_done.load(std::memory_order_acquire) < kWriters) {
        const auto out = CandidatesOf(vertices, 0, ms::EdgeDirection::OUT);
        EXPECT_GE(out.size(), last);
        last = out.size();
      }
    });
  }
  for (auto &t : threads) t.join();

  auto out = CandidatesOf(vertices, 0, ms::EdgeDirection::OUT);
  EXPECT_EQ(out.size(), kWriters * kEdgesPerWriter);  // distinct edges: no duplicates possible
  for (uint64_t id = 0; id < kWriters * kEdgesPerWriter; ++id) {
    EXPECT_NE(std::ranges::find(out, edges.Find(G(id + 1))), out.end()) << id;
    EXPECT_EQ(CandidatesOf(vertices, id + 1, ms::EdgeDirection::IN), Candidates{edges.Find(G(id + 1))});
  }
}

TEST(VersioningEdgeOverlayIndex, RacingFromAndToListersOfOneEdgeListItOnBothEndpoints) {
  constexpr uint64_t kEdges = 300;
  OverlayIndex vertices{kGidFloor};
  EdgeOverlayIndex edges;
  std::barrier sync{2};
  const auto lister = [&](ms::Gid from, ms::Gid to, memgraph::versioning::ListSide side) {
    return [&, from, to, side] {
      for (uint64_t i = 0; i < kEdges; ++i) {
        sync.arrive_and_wait();
        ListBaseEdgeDelete(vertices, from, to, MakeEdge(edges, i + 1), side);
      }
    };
  };
  {
    std::jthread from_side(lister(G(1), G(2), kFromSide));
    std::jthread to_side(lister(G(1), G(2), kToSide));
  }

  const auto count = [](const Candidates &list, ms::Edge *e) { return std::ranges::count(list, e); };
  const auto out = CandidatesOf(vertices, 1, ms::EdgeDirection::OUT);
  const auto in = CandidatesOf(vertices, 2, ms::EdgeDirection::IN);
  for (uint64_t i = 0; i < kEdges; ++i) {
    auto *e = edges.Find(G(i + 1));
    EXPECT_EQ(static_cast<EdgeOverlayIndex::BaseEdgeOverlay *>(e)->listed_sides.load(), kBothSides) << i;
    EXPECT_GE(count(out, e), 1) << i;
    EXPECT_LE(count(out, e), 2) << i;
    EXPECT_GE(count(in, e), 1) << i;
    EXPECT_LE(count(in, e), 2) << i;
  }
}
