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
#include <span>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/id_types.hpp"
#include "versioning/overlay_index.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::OverlayIndex;

namespace {

ms::Gid G(uint64_t gid) { return ms::Gid::FromUint(gid); }

// Base pointers are never dereferenced; only compared.
ms::Vertex *FakeBase(uint64_t gid) { return reinterpret_cast<ms::Vertex *>((gid + 1) * 16 + 8); }

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
