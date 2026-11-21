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

#include <gtest/gtest.h>
#include <boost/container/small_vector.hpp>

#include "storage/v2/edge.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "versioning/overlay_index.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::OverlayIndex;

namespace {

constexpr uint64_t kGidFloor = uint64_t{1} << 20;
constexpr uint64_t kSpan = OverlayIndex::kSpanGids;
constexpr uint64_t kDenseCap = OverlayIndex::kDenseCap;

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

  OverlayIndex index{kGidFloor};
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
      EXPECT_EQ(index.Find(G(gid)).base_vertex, nullptr) << gid;
      continue;
    }
    ASSERT_NE(found.overlay, nullptr) << gid;
    EXPECT_EQ(found.overlay->gid.AsUint(), gid);
    EXPECT_EQ(found.overlay->delta(), nullptr);
    EXPECT_EQ(found.base_vertex, FakeBase(gid));
    EXPECT_EQ(index.Find(G(gid)).overlay, found.overlay);
    for (size_t t = 0; t < kThreads; ++t) ASSERT_EQ(got[t][gid], found.overlay) << gid;
  }
}

TEST_F(VersioningOverlayIndexTest, FirstCreatorsBaseStands) {
  auto *overlay = Create(3);
  EXPECT_EQ(index.GetOrCreateOverlay(G(3), FakeBase(4), {}), overlay);
  EXPECT_EQ(index.Find(G(3)).base_vertex, FakeBase(3));
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
  EXPECT_EQ(Create(6), index.Find(G(6)).overlay);
  EXPECT_TRUE(index.Find(G(6)).overlay->labels.empty());

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
    auto *overlay = index.Find(G(gid)).overlay;
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

  EXPECT_EQ(index.Find(G(7)).overlay, nullptr);
  EXPECT_EQ(index.Find(G(7)).base_vertex, nullptr);
  EXPECT_TRUE(index.HasEntry(G(7)));
  EXPECT_FALSE(index.HasEntry(G(8)));
  EXPECT_EQ(index.FindForExpansion(G(7), kOut, true).candidates,
            (boost::container::small_vector<ms::Edge *, 16>{FakeEdge(1)}));
  EXPECT_TRUE(index.FindForExpansion(G(7), kIn, true).candidates.empty());
  EXPECT_TRUE(index.FindForExpansion(G(7), kOut, false).candidates.empty());
  EXPECT_TRUE(index.ForEachOverlay([](ms::Vertex *) { return false; })) << "a deletes entry is not an overlay";

  auto *overlay = Create(7, std::array{kLabelA});
  const auto expansion = index.FindForExpansion(G(7), kOut, true);
  EXPECT_EQ(expansion.overlay, overlay);
  EXPECT_EQ(expansion.base_vertex, FakeBase(7));
  EXPECT_EQ(expansion.candidates, (boost::container::small_vector<ms::Edge *, 16>{FakeEdge(1)}));
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
    auto *overlay = index.Find(G(gid)).overlay;
    for (size_t t = 0; t < kThreads; ++t) {
      if (overlays[t][idx] != nullptr) EXPECT_EQ(overlays[t][idx], overlay) << gid;
    }
  }
}

// Creators race readers and a walker over `gids` (strictly ascending, all below the index's floor). Every creator
// creates every gid in its own random order, so they meet on fresh chunks, on span locks, on growth, on middle inserts
// and on the sparse-to-dense switch.
static void RunStress(OverlayIndex &index, const std::vector<uint64_t> &gids, size_t creators, size_t readers) {
  const std::array labels{kLabelA, kLabelB};
  const auto in_set = [&](uint64_t gid) { return std::ranges::binary_search(gids, gid); };
  const auto complete = [&](const ms::Vertex *overlay, uint64_t gid) {
    return overlay->gid == G(gid) && overlay->labels.size() == 2;
  };
  std::atomic<size_t> creators_done{0};
  std::atomic<bool> bad{false};
  std::vector<std::vector<ms::Vertex *>> got(creators, std::vector<ms::Vertex *>(gids.size(), nullptr));
  {
    std::vector<std::jthread> threads;
    for (size_t c = 0; c < creators; ++c) {
      threads.emplace_back([&, c] {
        std::vector<size_t> order(gids.size());
        for (size_t i = 0; i < order.size(); ++i) order[i] = i;
        std::mt19937_64 rng{c};
        std::ranges::shuffle(order, rng);
        for (const size_t i : order) {
          auto *overlay = index.GetOrCreateOverlay(G(gids[i]), FakeBase(gids[i]), labels);
          got[c][i] = overlay;
          if (!complete(overlay, gids[i]) || index.Find(G(gids[i])).overlay != overlay) bad = true;
        }
        creators_done.fetch_add(1, std::memory_order_release);
      });
    }
    for (size_t r = 0; r < readers; ++r) {
      threads.emplace_back([&, r] {
        std::mt19937_64 rng{100 + r};
        while (creators_done.load(std::memory_order_acquire) < creators) {
          const uint64_t gid = gids[rng() % gids.size()];
          const auto found = index.Find(G(gid));  // may race its creation
          if (found.overlay != nullptr && (!complete(found.overlay, gid) || found.base_vertex != FakeBase(gid))) {
            bad = true;
          }
          if (!in_set(gid + 1) && index.Find(G(gid + 1)).overlay != nullptr) bad = true;  // never created
        }
      });
    }
    threads.emplace_back([&] {
      while (creators_done.load(std::memory_order_acquire) < creators) {
        uint64_t previous = 0;
        bool first = true;
        index.ForEachOverlay([&](ms::Vertex *overlay) {
          const uint64_t gid = overlay->gid.AsUint();
          if ((!first && gid <= previous) || !in_set(gid) || !complete(overlay, gid)) bad = true;
          first = false;
          previous = gid;
          return true;
        });
      }
    });
  }
  EXPECT_FALSE(bad.load());
  size_t next = 0;
  EXPECT_TRUE(index.ForEachOverlay([&](ms::Vertex *overlay) {
    EXPECT_LT(next, gids.size());
    if (next < gids.size()) EXPECT_EQ(overlay->gid, G(gids[next]));
    ++next;
    return true;
  }));
  EXPECT_EQ(next, gids.size());
  for (size_t i = 0; i < gids.size(); ++i) {
    auto *overlay = index.Find(G(gids[i])).overlay;
    ASSERT_NE(overlay, nullptr) << gids[i];
    for (size_t c = 0; c < creators; ++c) ASSERT_EQ(got[c][i], overlay) << gids[i];
  }
}

static std::vector<uint64_t> Range(uint64_t begin, uint64_t end, uint64_t stride = 1) {
  std::vector<uint64_t> gids;
  for (uint64_t gid = begin; gid < end; gid += stride) gids.push_back(gid);
  return gids;
}

TEST_F(VersioningOverlayIndexTest, StressSparseGidsSpreadOverManySpans) {
  RunStress(index, Range(0, kGidFloor, 37), 4, 4);
}

TEST_F(VersioningOverlayIndexTest, StressOneSpanFillsThroughGrowthMiddleInsertsAndTheDenseSwitch) {
  const auto gids = Range(0, kSpan);
  for (int round = 0; round < 50; ++round) {
    OverlayIndex span{kSpan};
    RunStress(span, gids, 6, 4);
  }
}

TEST_F(VersioningOverlayIndexTest, StressSpansBelowAndAboveTheSwitchWithAPartialLastSpanBelowTheFloor) {
  // Span 0 dense, span 1 stays sparse (stride 8: kSpan / 8 < kDenseCap), span 2 sparse, span 3 partial and dense.
  auto gids = Range(0, kSpan);
  for (const uint64_t gid : Range(kSpan, 2 * kSpan, 8)) gids.push_back(gid);
  for (const uint64_t gid : Range(2 * kSpan + 3, 2 * kSpan + 3 + kDenseCap / 2)) gids.push_back(gid);
  for (const uint64_t gid : Range(3 * kSpan, 3 * kSpan + kDenseCap + 100)) gids.push_back(gid);
  for (int round = 0; round < 20; ++round) {
    OverlayIndex partial{3 * kSpan + kDenseCap + 100};
    RunStress(partial, gids, 4, 4);
  }
}

TEST_F(VersioningOverlayIndexTest, FloorBoundariesIncludingAPartialLastSpanThatSwitchesToDense) {
  {
    OverlayIndex empty{0};
    EXPECT_EQ(empty.Find(G(0)).overlay, nullptr);
    EXPECT_FALSE(empty.HasEntry(G(0)));
    EXPECT_TRUE(empty.ForEachOverlay([](ms::Vertex *) { return false; }));
    EXPECT_DEATH((void)empty.GetOrCreateOverlay(G(0), FakeBase(0), {}), "an overlay is only for a base vertex");
  }
  const uint64_t floors[] = {1, kSpan - 1, kSpan, kSpan + 1, 2 * kSpan + kDenseCap + 100};
  for (const uint64_t floor : floors) {
    OverlayIndex bounded{floor};
    // Every gid of the last span (more than kDenseCap of them for the last floor), so it ends up dense and its array
    // extends past the floor.
    const uint64_t first = floor - 1 - (floor - 1) % kSpan;
    for (uint64_t gid = first; gid < floor; ++gid) {
      ASSERT_EQ(bounded.GetOrCreateOverlay(G(gid), FakeBase(gid), {})->gid, G(gid)) << floor;
    }
    for (uint64_t gid = first; gid < floor; ++gid)
      ASSERT_NE(bounded.Find(G(gid)).overlay, nullptr) << floor << " " << gid;
    for (uint64_t gid = floor; gid < floor + kSpan + 1; ++gid)
      ASSERT_EQ(bounded.Find(G(gid)).overlay, nullptr) << floor;
    EXPECT_FALSE(bounded.HasEntry(G(floor)));
    EXPECT_EQ(bounded.Find(G(floor + 123456789)).overlay, nullptr);
    if (first > 0) EXPECT_EQ(bounded.Find(G(first - 1)).overlay, nullptr) << "spans below were not touched";
    size_t visited = 0;
    EXPECT_TRUE(bounded.ForEachOverlay([&](ms::Vertex *) { return ++visited > 0; }));
    EXPECT_EQ(visited, floor - first) << floor;
  }
  OverlayIndex last{kSpan + 1};
  EXPECT_DEATH((void)last.GetOrCreateOverlay(G(kSpan + 1), FakeBase(kSpan + 1), {}),
               "an overlay is only for a base vertex");
}

TEST_F(VersioningOverlayIndexTest, EveryInsertOrderOfOneSpanIsFoundAndWalkedInOrderAcrossGrowthAndTheDenseSwitch) {
  std::mt19937_64 rng{7};
  std::vector<uint64_t> ascending = Range(0, kSpan);
  std::vector<uint64_t> descending{ascending.rbegin(), ascending.rend()};
  std::vector<uint64_t> shuffled = ascending;
  std::ranges::shuffle(shuffled, rng);
  for (const auto *order : {&ascending, &descending, &shuffled}) {
    OverlayIndex span{kSpan};
    std::vector<ms::Vertex *> made(kSpan, nullptr);
    for (size_t n = 0; n < order->size(); ++n) {
      made[(*order)[n]] = span.GetOrCreateOverlay(G((*order)[n]), FakeBase((*order)[n]), {});
      // Check densely around the switch, sparsely elsewhere.
      if (n > 8 && n % 211 != 0 && (n + 3 < kDenseCap || n > kDenseCap + 3) && n + 1 != order->size()) continue;
      std::vector<uint64_t> expected{order->begin(), order->begin() + static_cast<long>(n) + 1};
      std::ranges::sort(expected);
      std::vector<uint64_t> walked;
      span.ForEachOverlay([&](ms::Vertex *overlay) {
        walked.push_back(overlay->gid.AsUint());
        return true;
      });
      ASSERT_EQ(walked, expected) << n;
      for (uint64_t gid = 0; gid < kSpan; ++gid) {
        const bool present = std::ranges::binary_search(expected, gid);
        ASSERT_EQ(span.Find(G(gid)).overlay, present ? made[gid] : nullptr) << n << " " << gid;
      }
    }
    for (const uint64_t gid : *order) ASSERT_EQ(span.GetOrCreateOverlay(G(gid), FakeBase(gid), {}), made[gid]) << gid;
  }
}
