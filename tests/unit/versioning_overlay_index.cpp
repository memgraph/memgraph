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
#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <random>
#include <span>
#include <thread>
#include <vector>

#include <absl/container/inlined_vector.h>
#include <gtest/gtest.h>

#include "storage/v2/edge.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "versioning/overlay_index.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::OverlayIndex;

namespace {

ms::Gid G(uint64_t gid) { return ms::Gid::FromUint(gid); }

// Base pointers are never dereferenced; only compared.
ms::Vertex *FakeBase(uint64_t gid) { return reinterpret_cast<ms::Vertex *>((gid + 1) * 16 + 8); }

ms::Edge *FakeEdge(uint64_t id) { return reinterpret_cast<ms::Edge *>((id + 1) * 16); }

const auto kLabelA = ms::LabelId::FromUint(1);
const auto kLabelB = ms::LabelId::FromUint(2);
const auto kLabelC = ms::LabelId::FromUint(3);

std::vector<ms::LabelId> SortedLabels(const ms::Vertex *overlay) {
  auto labels = std::vector<ms::LabelId>{overlay->labels.begin(), overlay->labels.end()};
  std::ranges::sort(labels);
  return labels;
}

class VersioningOverlayIndexTest : public testing::Test {
 protected:
  ms::Vertex *Create(uint64_t gid, std::span<const ms::LabelId> labels = {}) {
    return index.GetOrCreateOverlay(G(gid), FakeBase(gid), labels);
  }

  OverlayIndex index;
};

}  // namespace

TEST_F(VersioningOverlayIndexTest, ConcurrentCreatorsGetOneOverlayPerGidAndUntouchedGidsStayAbsent) {
  constexpr size_t kThreads = 8;
  constexpr uint64_t kGids = 5000;  // even gids only
  std::vector<std::vector<ms::Vertex *>> got(kThreads, std::vector<ms::Vertex *>(kGids, nullptr));
  {
    std::vector<std::jthread> threads;
    for (size_t t = 0; t < kThreads; ++t) {
      threads.emplace_back([&, t] {
        for (uint64_t i = 0; i < kGids; i += 2) got[t][i] = Create(i);
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
    const auto found = index.Find(G(gid));
    if (gid % 2 == 1) {
      EXPECT_EQ(found.overlay, nullptr) << gid;
      EXPECT_EQ(index.FindBase(G(gid)), nullptr) << gid;
      continue;
    }
    ASSERT_NE(found.overlay, nullptr) << gid;
    EXPECT_EQ(found.overlay->gid.AsUint(), gid);
    EXPECT_EQ(found.overlay->delta(), nullptr);
    EXPECT_EQ(found.base_vertex, FakeBase(gid));
    EXPECT_EQ(index.FindOverlay(G(gid)), found.overlay);
    for (size_t t = 0; t < kThreads; ++t) ASSERT_EQ(got[t][gid], found.overlay) << gid;
  }
}

TEST_F(VersioningOverlayIndexTest, FirstCreatorsBaseStands) {
  auto *overlay = Create(3);
  EXPECT_EQ(index.GetOrCreateOverlay(G(3), FakeBase(4), {}), overlay);
  EXPECT_EQ(index.FindBase(G(3)), FakeBase(3));
}

TEST_F(VersioningOverlayIndexTest, ForEachOverlayStopsWhenTheCallbackReturnsFalse) {
  for (uint64_t gid = 0; gid < 40; ++gid) Create(gid);
  size_t visited = 0;
  EXPECT_FALSE(index.ForEachOverlay([&](ms::Vertex *) { return ++visited < 5; }));
  EXPECT_EQ(visited, 5U);
}

TEST_F(VersioningOverlayIndexTest, ANewOverlayHasExactlyTheBaseLabelsAndLaterCallsKeepThem) {
  auto *overlay = Create(5, std::array{kLabelC, kLabelA});
  EXPECT_EQ(SortedLabels(overlay), (std::vector{kLabelA, kLabelC}));
  EXPECT_EQ(Create(6), index.FindOverlay(G(6)));
  EXPECT_TRUE(index.FindOverlay(G(6))->labels.empty());

  EXPECT_EQ(Create(5, std::array{kLabelB}), overlay);
  EXPECT_EQ(SortedLabels(overlay), (std::vector{kLabelA, kLabelC})) << "only the creating call copies";
}

TEST_F(VersioningOverlayIndexTest, RacingCreatorsSeeOneOverlayWithTheWinnersLabels) {
  constexpr size_t kThreads = 8;
  constexpr uint64_t kGids = 500;
  std::vector<std::vector<ms::Vertex *>> got(kThreads, std::vector<ms::Vertex *>(kGids, nullptr));
  std::atomic<bool> labels_missing{false};
  {
    std::vector<std::jthread> threads;
    for (size_t t = 0; t < kThreads; ++t) {
      threads.emplace_back([&, t] {
        // Every thread proposes a different label set; whichever creates first wins.
        const std::array proposal{kLabelA, t % 2 == 0 ? kLabelB : kLabelC};
        for (uint64_t gid = 0; gid < kGids; ++gid) {
          auto *overlay = Create(gid, proposal);
          got[t][gid] = overlay;
          if (overlay->labels.size() != 2) labels_missing = true;  // an overlay must never be seen unlabelled
        }
      });
    }
  }
  EXPECT_FALSE(labels_missing.load());

  for (uint64_t gid = 0; gid < kGids; ++gid) {
    auto *overlay = index.FindOverlay(G(gid));
    ASSERT_NE(overlay, nullptr);
    for (size_t t = 0; t < kThreads; ++t) ASSERT_EQ(got[t][gid], overlay) << gid;
    const auto labels = SortedLabels(overlay);
    ASSERT_EQ(labels.size(), 2U) << gid;
    EXPECT_EQ(labels[0], kLabelA);
  }
}

TEST_F(VersioningOverlayIndexTest, DeleteCandidateOnAbsentGidMakesAnEntryButNoOverlayAndSurvivesOverlayCreation) {
  constexpr auto kOut = ms::EdgeDirection::OUT;
  constexpr auto kIn = ms::EdgeDirection::IN;
  index.AppendDeleteCandidate(G(7), kOut, FakeEdge(1));

  EXPECT_EQ(index.FindOverlay(G(7)), nullptr);
  EXPECT_EQ(index.FindBase(G(7)), nullptr);
  EXPECT_TRUE(index.HasEntry(G(7)));
  EXPECT_FALSE(index.HasEntry(G(8)));
  EXPECT_EQ(index.FindForExpansion(G(7), kOut, true).candidates, (absl::InlinedVector<ms::Edge *, 16>{FakeEdge(1)}));
  EXPECT_TRUE(index.FindForExpansion(G(7), kIn, true).candidates.empty());
  EXPECT_TRUE(index.FindForExpansion(G(7), kOut, false).candidates.empty());
  EXPECT_TRUE(index.ForEachOverlay([](ms::Vertex *) { return false; })) << "a deletes entry is not an overlay";

  auto *overlay = Create(7, std::array{kLabelA});
  const auto expansion = index.FindForExpansion(G(7), kOut, true);
  EXPECT_EQ(expansion.overlay, overlay);
  EXPECT_EQ(expansion.base_vertex, FakeBase(7));
  EXPECT_EQ(expansion.candidates, (absl::InlinedVector<ms::Edge *, 16>{FakeEdge(1)}));
  EXPECT_EQ(SortedLabels(overlay), std::vector{kLabelA});
}

TEST_F(VersioningOverlayIndexTest, ConcurrentAppendsOverlaysAndExpansionsLoseNothingAndCreateOneOverlay) {
  constexpr size_t kThreads = 8;
  constexpr uint64_t kGids = 64;
  // Four shards (gid % 64), sixteen gids each, so threads contend on the shard locks.
  constexpr auto kGidOf = [](uint64_t i) { return (i % 4) + (i / 4) * 64; };
  constexpr int kOpsPerThread = 4000;
  std::vector<std::vector<std::pair<uint64_t, ms::Edge *>>> appended(kThreads);
  std::vector<std::vector<ms::Vertex *>> overlays(kThreads, std::vector<ms::Vertex *>(kGids, nullptr));
  {
    std::vector<std::jthread> threads;
    for (size_t t = 0; t < kThreads; ++t) {
      threads.emplace_back([&, t] {
        std::mt19937_64 rng{t};
        uint64_t next_edge = t << 32;
        for (int i = 0; i < kOpsPerThread; ++i) {
          const uint64_t idx = rng() % kGids;
          const uint64_t gid = kGidOf(idx);
          switch (rng() % 3) {
            case 0: {
              auto *edge = FakeEdge(next_edge++);
              index.AppendDeleteCandidate(G(gid), ms::EdgeDirection::OUT, edge);
              appended[t].emplace_back(idx, edge);
              break;
            }
            case 1:
              overlays[t][idx] = Create(gid);
              break;
            default: {
              const auto found = index.FindForExpansion(G(gid), ms::EdgeDirection::OUT, true);
              if (found.overlay != nullptr) EXPECT_EQ(found.base_vertex, FakeBase(gid));
              break;
            }
          }
        }
      });
    }
  }

  std::vector<std::vector<ms::Edge *>> expected(kGids);
  for (const auto &per_thread : appended) {
    for (const auto &[idx, edge] : per_thread) expected[idx].push_back(edge);
  }
  for (uint64_t idx = 0; idx < kGids; ++idx) {
    const uint64_t gid = kGidOf(idx);
    auto candidates = index.FindForExpansion(G(gid), ms::EdgeDirection::OUT, true).candidates;
    EXPECT_TRUE(index.FindForExpansion(G(gid), ms::EdgeDirection::IN, true).candidates.empty());
    std::ranges::sort(candidates);
    std::ranges::sort(expected[idx]);
    EXPECT_TRUE(std::ranges::equal(candidates, expected[idx])) << gid;
    auto *overlay = index.FindOverlay(G(gid));
    for (size_t t = 0; t < kThreads; ++t) {
      if (overlays[t][idx] != nullptr) EXPECT_EQ(overlays[t][idx], overlay) << gid;
    }
  }
}
