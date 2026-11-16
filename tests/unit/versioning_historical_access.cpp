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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "storage/v2/exceptions.hpp"
#include "storage/v2/indices/index_order.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/property_value.hpp"
#include "storage_test_utils.hpp"
#include "storage_v2_gc_metrics_fixture.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace ms = memgraph::storage;

namespace {

constexpr int kGcRounds = 6;

void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

void Gc(ms::Storage &storage) {
  for (int i = 0; i < kGcRounds; ++i) storage.FreeMemory();
}

ms::Gid CommitNewVertex(ms::Storage &storage, ms::PropertyId prop, int64_t value) {
  auto acc = storage.Access(ms::WRITE);
  auto v = acc->CreateVertex();
  const auto gid = v.Gid();
  EXPECT_TRUE(v.SetProperty(prop, ms::PropertyValue(value)).has_value());
  Commit(*acc);
  return gid;
}

void CommitSetProp(ms::Storage &storage, ms::Gid gid, ms::PropertyId prop, int64_t value) {
  auto acc = storage.Access(ms::WRITE);
  auto v = acc->FindVertex(gid, ms::View::OLD);
  ASSERT_TRUE(v.has_value());
  ASSERT_TRUE(v->SetProperty(prop, ms::PropertyValue(value)).has_value());
  Commit(*acc);
}

// One commit per label "L0".."L<n-1>", so every commit leaves a delta on the vertex.
void CommitLabels(ms::Storage &storage, ms::Gid gid, int n) {
  for (int i = 0; i < n; ++i) {
    auto acc = storage.Access(ms::WRITE);
    auto v = acc->FindVertex(gid, ms::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->AddLabel(acc->NameToLabel("L" + std::to_string(i))).has_value());
    Commit(*acc);
  }
}

void ExpectNoLabels(ms::Storage::Accessor &acc, ms::Gid gid, int n) {
  auto v = acc.FindVertex(gid, ms::View::OLD);
  ASSERT_TRUE(v.has_value());
  for (int i = 0; i < n; ++i) {
    EXPECT_FALSE(v->HasLabel(acc.NameToLabel("L" + std::to_string(i)), ms::View::OLD).value_or(true))
        << "the pinned snapshot must not see post-fork label L" << i;
  }
}

auto Val(int64_t value) { return std::optional{ms::PropertyValue(value)}; }

std::optional<ms::PropertyValue> ReadProp(ms::Storage::Accessor &acc, ms::Gid gid, ms::PropertyId prop) {
  auto v = acc.FindVertex(gid, ms::View::OLD);
  if (!v) return std::nullopt;
  auto p = v->GetProperty(prop, ms::View::OLD);
  if (!p) return std::nullopt;
  return *p;
}

template <typename Create>
void CommitIndex(ms::Storage &storage, Create &&create) {
  auto acc = storage.ReadOnlyAccess();
  ASSERT_TRUE(create(*acc).has_value());
  Commit(*acc);
}

// Metrics-backed storage (PERIODIC GC with a 1h interval, so GC runs only via Gc()): unreleased_delta_objects tells
// "retained" from "wrongly reclaimed".
class VersioningHistoricalAccessTest : public StorageV2GcMetricsTest {
 protected:
  auto *Mem() { return static_cast<ms::InMemoryStorage *>(storage.get()); }

  auto Unreleased() { return handles().unreleased_delta_objects.Value(); }

  std::optional<ms::PropertyValue> ReadPropAt(uint64_t fork_ts, ms::Gid gid, ms::PropertyId prop) {
    auto historical = Mem()->HistoricalAccess(fork_ts);
    if (!historical) return std::nullopt;
    return ReadProp(**historical, gid, prop);
  }
};

// HistoricalAccess must force SNAPSHOT_ISOLATION: under an inherited READ_COMMITTED/READ_UNCOMMITTED ambient level the
// post-fork commit would be visible.
TEST(VersioningHistoricalAccess, ReconstructsMainAsOfForkUnderAnyAmbientIsolation) {
  for (auto ambient_level : {ms::IsolationLevel::SNAPSHOT_ISOLATION,
                             ms::IsolationLevel::READ_COMMITTED,
                             ms::IsolationLevel::READ_UNCOMMITTED}) {
    SCOPED_TRACE(testing::Message() << "ambient isolation level = " << static_cast<int>(ambient_level));

    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    config.transaction.isolation_level = ambient_level;
    ms::InMemoryStorage storage(config);
    const auto prop = storage.NameToProperty("p");

    const auto gid = CommitNewVertex(storage, prop, 1);
    const uint64_t fork_ts = storage.RegisterForkPin();
    {
      auto acc = storage.Access(ms::WRITE);
      auto v = acc->FindVertex(gid, ms::View::OLD);
      ASSERT_TRUE(v.has_value());
      ASSERT_TRUE(v->SetProperty(prop, ms::PropertyValue(2)).has_value());
      (void)acc->CreateVertex();
      Commit(*acc);
    }

    {
      auto historical = storage.HistoricalAccess(fork_ts);
      ASSERT_TRUE(historical.has_value());
      EXPECT_EQ(ReadProp(**historical, gid, prop), Val(1)) << "must see main as of fork_ts, not the post-fork edit";
      EXPECT_EQ(CountVertices(**historical, ms::View::OLD), 1U) << "the post-fork vertex must not be visible";
    }
    {
      auto acc = storage.Access(ms::WRITE);
      EXPECT_EQ(ReadProp(*acc, gid, prop), Val(2));
      EXPECT_EQ(CountVertices(*acc, ms::View::OLD), 2U);
    }
    storage.ReleaseForkPin(fork_ts);
  }
}

// A never-pinned or released fork_ts is rejected even while another pin is live.
TEST_F(VersioningHistoricalAccessTest, RejectsUnpinnedForkTimestamp) {
  const auto prop = storage->NameToProperty("p");
  auto expect_rejected = [&](uint64_t fork_ts) {
    auto result = Mem()->HistoricalAccess(fork_ts);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ms::InMemoryStorage::HistoricalAccessError::ForkTimestampNotPinned);
  };

  const auto gid = CommitNewVertex(*storage, prop, 1);
  const uint64_t older = Mem()->RegisterForkPin();
  CommitSetProp(*storage, gid, prop, 2);
  const uint64_t newer = Mem()->RegisterForkPin();
  ASSERT_LT(older, newer);

  // Above every live pin, so a range check (fork_ts >= oldest pin) would wrongly accept it.
  expect_rejected(newer + 1);

  Mem()->ReleaseForkPin(older);
  expect_rejected(older);
  Mem()->ReleaseForkPin(newer);
}

// Releasing the older of two pins must keep the newer pin's history reconstructible; until then both are exact.
TEST_F(VersioningHistoricalAccessTest, NewerPinHistorySurvivesOlderPinRelease) {
  const auto prop = storage->NameToProperty("p");

  const auto gid = CommitNewVertex(*storage, prop, 1);
  const uint64_t pin_a = Mem()->RegisterForkPin();
  CommitSetProp(*storage, gid, prop, 2);
  const uint64_t pin_b = Mem()->RegisterForkPin();
  CommitSetProp(*storage, gid, prop, 3);

  Gc(*storage);
  EXPECT_EQ(ReadPropAt(pin_a, gid, prop), Val(1));
  EXPECT_EQ(ReadPropAt(pin_b, gid, prop), Val(2));

  Mem()->ReleaseForkPin(pin_a);
  Gc(*storage);
  EXPECT_EQ(ReadPropAt(pin_b, gid, prop), Val(2)) << "history needed for pin B must be retained";
  Mem()->ReleaseForkPin(pin_b);
}

// Transient historical readers must not mark the shared fork_ts finished in the commit log: RegisterForkPin does not
// advance timestamp_, so `real_reader` starts at fork_ts and would lose its deltas to GC.
TEST_F(VersioningHistoricalAccessTest, TransientReadsDoNotReleaseSharedForkTimestamp) {
  const auto gid = CommitNewVertex(*storage, storage->NameToProperty("p"), 1);
  const uint64_t fork_ts = Mem()->RegisterForkPin();

  auto real_reader = storage->Access(ms::READ);
  ASSERT_EQ(real_reader->GetStartTimestamp(), fork_ts)
      << "test precondition: the plain reader must start exactly at fork_ts";

  for (int i = 0; i < 5; ++i) {
    auto historical = Mem()->HistoricalAccess(fork_ts);
    ASSERT_TRUE(historical.has_value());
    ASSERT_TRUE((*historical)->FindVertex(gid, ms::View::OLD).has_value());
  }

  // Post-fork commits real_reader must undo through; it is older, so no fast-discard shortcut.
  CommitLabels(*storage, gid, 5);
  ASSERT_GT(Unreleased(), 0);

  // From here only commit_log_ state gates retention; the pin can no longer mask the regression.
  Mem()->ReleaseForkPin(fork_ts);
  Gc(*storage);
  EXPECT_GT(Unreleased(), 0) << "real_reader (start_timestamp == fork_ts, still open) needs this history regardless "
                                "of the fork pin; the transient historical reads must not have finalized it";
  ExpectNoLabels(*real_reader, gid, 5);

  real_reader.reset();
  Gc(*storage);
  EXPECT_EQ(0, Unreleased());
}

// Dropping the branch (ReleaseForkPin) while a historical reader is open must not let GC reclaim its history.
TEST_F(VersioningHistoricalAccessTest, DropDuringOpenReadDoesNotReclaim) {
  const auto gid = CommitNewVertex(*storage, storage->NameToProperty("p"), 1);
  const uint64_t fork_ts = Mem()->RegisterForkPin();
  CommitLabels(*storage, gid, 5);
  ASSERT_GT(Unreleased(), 0);

  auto historical = Mem()->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());

  Mem()->ReleaseForkPin(fork_ts);
  EXPECT_EQ(Mem()->OldestForkPin(), fork_ts) << "reader must self-pin fork_ts";
  Gc(*storage);
  EXPECT_GT(Unreleased(), 0) << "the reader's self-pin must keep the history alive after the branch pin is released";
  ExpectNoLabels(**historical, gid, 5);
  EXPECT_EQ(Mem()->OldestForkPin(), fork_ts);

  historical->reset();
  EXPECT_EQ(Mem()->OldestForkPin(), memgraph::storage::InMemoryStorage::kNoForkPin);
  Gc(*storage);
  EXPECT_EQ(0, Unreleased());
}

// A commit minting commit_ts == fork_ts (writer opened pre-fork, committed post-fork) must stay invisible at the pin,
// through later writes and GC; a '>=' off-by-one in the fast-discard gate would free the deltas.
TEST_F(VersioningHistoricalAccessTest, HistoricalReadStableAcrossCommitAtForkAndLaterWrites) {
  const auto prop = storage->NameToProperty("p");
  const auto gid = CommitNewVertex(*storage, prop, 1);
  auto expect_fork_state = [&](ms::Storage::Accessor &acc) {
    EXPECT_EQ(ReadProp(acc, gid, prop), Val(1));
    EXPECT_EQ(CountVertices(acc, ms::View::OLD), 1U);
  };

  auto writer = storage->Access(ms::WRITE);
  {
    auto v = writer->FindVertex(gid, ms::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->SetProperty(prop, ms::PropertyValue(2)).has_value());
    (void)writer->CreateVertex();
  }

  const uint64_t fork_ts = Mem()->RegisterForkPin();
  auto historical = Mem()->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  expect_fork_state(**historical);

  Commit(*writer);
  writer.reset();
  for (int64_t value = 3; value < 6; ++value) {
    expect_fork_state(**historical);
    CommitSetProp(*storage, gid, prop, value);
  }

  Gc(*storage);
  EXPECT_GT(Unreleased(), 0) << "the pin at P == C must keep the deltas";
  expect_fork_state(**historical);
  {
    auto fresh = Mem()->HistoricalAccess(fork_ts);
    ASSERT_TRUE(fresh.has_value());
    expect_fork_state(**fresh);
  }

  historical->reset();
  Mem()->ReleaseForkPin(fork_ts);
  Gc(*storage);
  EXPECT_EQ(0, Unreleased());
}

// Main writers commit while historical readers open and read at one pin and GC runs: every read returns the fork state.
TEST_F(VersioningHistoricalAccessTest, ConcurrentMainWritersAndHistoricalReadersSeeForkState) {
  const auto prop = storage->NameToProperty("p");
  constexpr int kVertices = 4;
  constexpr int kWriters = 2;
  constexpr int kReaders = 3;
  constexpr int kWriterIterations = 200;

  std::vector<ms::Gid> gids;
  {
    auto acc = storage->Access(ms::WRITE);
    for (int i = 0; i < kVertices; ++i) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.SetProperty(prop, ms::PropertyValue(i)).has_value());
      gids.push_back(v.Gid());
    }
    Commit(*acc);
  }
  const uint64_t fork_ts = Mem()->RegisterForkPin();

  std::atomic<bool> stop{false};
  std::atomic<int> mismatches{0};
  std::atomic<int> reads{0};
  std::vector<std::jthread> threads;
  for (int w = 0; w < kWriters; ++w) {
    threads.emplace_back([&, w] {
      for (int it = 0; it < kWriterIterations; ++it) {
        auto acc = storage->Access(ms::WRITE);
        // Disjoint vertices per writer, so no serialization conflicts.
        for (int i = w; i < kVertices; i += kWriters) {
          auto v = acc->FindVertex(gids[i], ms::View::OLD);
          if (!v || !v->SetProperty(prop, ms::PropertyValue(1000 + it)).has_value()) ++mismatches;
        }
        (void)acc->CreateVertex();
        if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) ++mismatches;
      }
    });
  }
  for (int r = 0; r < kReaders; ++r) {
    threads.emplace_back([&] {
      while (!stop.load(std::memory_order_acquire)) {
        auto historical = Mem()->HistoricalAccess(fork_ts);
        if (!historical) {
          ++mismatches;
          continue;
        }
        bool ok = CountVertices(**historical, ms::View::OLD) == kVertices;
        for (int i = 0; i < kVertices; ++i) ok = ok && ReadProp(**historical, gids[i], prop) == Val(i);
        if (!ok) ++mismatches;
        ++reads;
      }
    });
  }
  threads.emplace_back([&] {
    while (!stop.load(std::memory_order_acquire)) storage->FreeMemory();
  });
  for (int w = 0; w < kWriters; ++w) threads[w].join();
  stop.store(true, std::memory_order_release);
  threads.clear();

  EXPECT_EQ(mismatches.load(), 0);
  EXPECT_GT(reads.load(), 0);
  Mem()->ReleaseForkPin(fork_ts);
  EXPECT_EQ(Mem()->OldestForkPin(), memgraph::storage::InMemoryStorage::kNoForkPin);
}

// Full GC under a live pin: every GC horizon is clamped to it, and the historical label+property scan returns exactly
// the fork state in index order (ASC and DESC).
TEST_F(VersioningHistoricalAccessTest, FullGcUnderForkPinKeepsForkStateVisibleAndIndexed) {
  const auto label = storage->NameToLabel("Person");
  const auto age_prop = storage->NameToProperty("age");

  for (auto order : {ms::IndexOrder::ASC, ms::IndexOrder::DESC}) {
    CommitIndex(*storage, [&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{age_prop}}, order); });
  }

  // Created out of order so that gid order != age order.
  ms::Gid v20_gid;
  ms::Gid v40_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    for (int64_t age : {50, 20, 40, 10, 30}) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(label).has_value());
      ASSERT_TRUE(v.SetProperty(age_prop, ms::PropertyValue(age)).has_value());
      if (age == 20) v20_gid = v.Gid();
      if (age == 40) v40_gid = v.Gid();
    }
    Commit(*acc);
  }

  const uint64_t fork_ts = Mem()->RegisterForkPin();
  {
    auto acc = storage->Access(ms::WRITE);
    auto v20 = acc->FindVertex(v20_gid, ms::View::OLD);
    ASSERT_TRUE(v20.has_value());
    ASSERT_TRUE(v20->SetProperty(age_prop, ms::PropertyValue(999)).has_value());
    auto v5 = acc->CreateVertex();
    ASSERT_TRUE(v5.AddLabel(label).has_value());
    ASSERT_TRUE(v5.SetProperty(age_prop, ms::PropertyValue(5)).has_value());
    auto v40 = acc->FindVertex(v40_gid, ms::View::OLD);
    ASSERT_TRUE(v40.has_value());
    ASSERT_TRUE(acc->DetachDeleteVertex(&*v40).has_value());
    Commit(*acc);
  }
  Gc(*storage);

  auto historical = Mem()->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  auto &hist_acc = **historical;
  EXPECT_EQ(ReadProp(hist_acc, v20_gid, age_prop), Val(20)) << "GC must keep the pre-fork property value";
  EXPECT_TRUE(hist_acc.FindVertex(v40_gid, ms::View::OLD).has_value())
      << "GC must keep the vertex that was deleted after the fork";

  for (auto order : {ms::IndexOrder::ASC, ms::IndexOrder::DESC}) {
    SCOPED_TRACE(order == ms::IndexOrder::ASC ? "ASC" : "DESC");
    std::vector<int64_t> expected{10, 20, 30, 40, 50};
    if (order == ms::IndexOrder::DESC) std::ranges::reverse(expected);

    std::vector<int64_t> ages;
    for (auto v : hist_acc.Vertices(label,
                                    std::array{ms::PropertyPath{age_prop}},
                                    std::array{ms::PropertyValueRange::IsNotNull()},
                                    ms::View::OLD,
                                    order)) {
      auto p = v.GetProperty(age_prop, ms::View::OLD);
      ASSERT_TRUE(p.has_value());
      ages.push_back(p->ValueInt());
    }
    EXPECT_THAT(ages, ::testing::ElementsAreArray(expected))
        << "expected exactly the pre-fork ages in property order: 20 not 999, no 5, 40 present";
  }
  historical->reset();
  Mem()->ReleaseForkPin(fork_ts);
}

// Edge-type index scan through a historical accessor returns fork-state edges only.
TEST_F(VersioningHistoricalAccessTest, HistoricalEdgeTypeIndexScanReturnsForkState) {
  const auto knows_type = storage->NameToEdgeType("KNOWS");
  const auto likes_type = storage->NameToEdgeType("LIKES");
  CommitIndex(*storage, [&](auto &acc) { return acc.CreateIndex(knows_type); });

  // A-[:LIKES]->C is a distractor that must never appear in a KNOWS scan.
  ms::Gid a_gid;
  ms::Gid b_gid;
  ms::Gid c_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    auto b = acc->CreateVertex();
    auto c = acc->CreateVertex();
    a_gid = a.Gid();
    b_gid = b.Gid();
    c_gid = c.Gid();
    ASSERT_TRUE(acc->CreateEdge(&a, &b, knows_type).has_value());
    ASSERT_TRUE(acc->CreateEdge(&b, &c, knows_type).has_value());
    ASSERT_TRUE(acc->CreateEdge(&a, &c, likes_type).has_value());
    Commit(*acc);
  }

  const uint64_t fork_ts = Mem()->RegisterForkPin();
  {
    auto acc = storage->Access(ms::WRITE);
    auto a = acc->FindVertex(a_gid, ms::View::OLD);
    auto c = acc->FindVertex(c_gid, ms::View::OLD);
    ASSERT_TRUE(a.has_value() && c.has_value());
    ASSERT_TRUE(acc->CreateEdge(&*a, &*c, knows_type).has_value());

    auto out_edges = a->OutEdges(ms::View::OLD, {knows_type});
    ASSERT_TRUE(out_edges.has_value());
    bool deleted_ab = false;
    for (auto edge : out_edges->edges) {
      if (edge.ToVertex().Gid() == b_gid) {
        ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
        deleted_ab = true;
        break;
      }
    }
    ASSERT_TRUE(deleted_ab) << "setup bug: pre-fork A-[:KNOWS]->B edge not found for deletion";
    Commit(*acc);
  }

  auto historical = Mem()->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  std::vector<std::pair<uint64_t, uint64_t>> seen;
  for (auto edge : (*historical)->Edges(knows_type, ms::View::OLD)) {
    EXPECT_EQ(edge.EdgeType(), knows_type) << "scan must only yield :KNOWS edges, never the :LIKES distractor";
    seen.emplace_back(edge.FromVertex().Gid().AsUint(), edge.ToVertex().Gid().AsUint());
  }
  EXPECT_THAT(seen,
              ::testing::UnorderedElementsAre(std::pair{a_gid.AsUint(), b_gid.AsUint()},
                                              std::pair{b_gid.AsUint(), c_gid.AsUint()}))
      << "expected the 2 fork-time KNOWS edges: post-fork-deleted A->B visible, post-fork-created A->C absent";
  historical->reset();
  Mem()->ReleaseForkPin(fork_ts);
}

// An index committed after fork_ts is not ready for that reader (the fork state was never indexed by it), while plain
// accessors and a later-pinned reader use it; an index committed exactly at fork_ts (C == P, IsVisible is <=) is ready
// and serves the fork state, because it was populated with writers excluded.
TEST_F(VersioningHistoricalAccessTest, IndexVisibilityAtForkTimestampBoundary) {
  const auto label = storage->NameToLabel("L");
  const auto edge_type = storage->NameToEdgeType("E");
  const auto prop = storage->NameToProperty("p");
  const std::array paths{ms::PropertyPath{prop}};

  const uint64_t pin_before = Mem()->RegisterForkPin();
  CommitIndex(*storage, [&](auto &acc) { return acc.CreateIndex(label); });
  CommitIndex(*storage, [&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{prop}}); });
  CommitIndex(*storage, [&](auto &acc) { return acc.CreateIndex(edge_type); });
  CommitIndex(*storage, [&](auto &acc) { return acc.CreateIndex(edge_type, prop); });
  CommitIndex(*storage, [&](auto &acc) { return acc.CreateGlobalEdgeIndex(prop); });
  CommitIndex(*storage, [&](auto &acc) { return acc.CreateGlobalVertexIndex(prop); });
  const uint64_t pin_after = Mem()->RegisterForkPin();

  auto expect_all_ready = [&](ms::Storage::Accessor &acc, bool expected) {
    EXPECT_EQ(acc.LabelIndexReady(label), expected);
    EXPECT_EQ(acc.LabelPropertyIndexReady(label, paths), expected);
    EXPECT_EQ(acc.EdgeTypeIndexReady(edge_type), expected);
    EXPECT_EQ(acc.EdgeTypePropertyIndexReady(edge_type, prop), expected);
    EXPECT_EQ(acc.EdgePropertyIndexReady(prop), expected);
    EXPECT_EQ(acc.VertexPropertyIndexReady(prop), expected);
  };
  {
    SCOPED_TRACE("plain accessor");
    expect_all_ready(*storage->Access(ms::READ), true);
  }
  for (auto [pin, expected] : {std::pair{pin_before, false}, std::pair{pin_after, true}}) {
    SCOPED_TRACE(expected ? "historical reader pinned after the index commit"
                          : "historical reader pinned before the index commit");
    auto historical = Mem()->HistoricalAccess(pin);
    ASSERT_TRUE(historical.has_value());
    expect_all_ready(**historical, expected);
  }
  Mem()->ReleaseForkPin(pin_before);
  Mem()->ReleaseForkPin(pin_after);

  // Index committed exactly at fork_ts, on a fresh label/property.
  const auto label2 = storage->NameToLabel("L2");
  const auto prop2 = storage->NameToProperty("q");
  const std::array paths2{ms::PropertyPath{prop2}};
  ms::Gid v10_gid;
  ms::Gid v20_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    for (int64_t value : {20, 10}) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(label2).has_value());
      ASSERT_TRUE(v.SetProperty(prop2, ms::PropertyValue(value)).has_value());
      (value == 10 ? v10_gid : v20_gid) = v.Gid();
    }
    Commit(*acc);
  }

  uint64_t fork_ts = 0;
  {
    auto index_acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(index_acc->CreateIndex(label2, {ms::PropertyPath{prop2}}).has_value());
    fork_ts = Mem()->RegisterForkPin();
    Commit(*index_acc);
  }
  const uint64_t probe = Mem()->RegisterForkPin();
  ASSERT_EQ(probe, fork_ts + 1) << "test precondition: the index commit minted exactly fork_ts";
  Mem()->ReleaseForkPin(probe);

  {
    auto acc = storage->Access(ms::WRITE);
    auto v10 = acc->FindVertex(v10_gid, ms::View::OLD);
    ASSERT_TRUE(v10.has_value());
    ASSERT_TRUE(v10->SetProperty(prop2, ms::PropertyValue(999)).has_value());
    auto v5 = acc->CreateVertex();
    ASSERT_TRUE(v5.AddLabel(label2).has_value());
    ASSERT_TRUE(v5.SetProperty(prop2, ms::PropertyValue(5)).has_value());
    auto v20 = acc->FindVertex(v20_gid, ms::View::OLD);
    ASSERT_TRUE(v20.has_value());
    ASSERT_TRUE(acc->DetachDeleteVertex(&*v20).has_value());
    Commit(*acc);
  }

  auto historical = Mem()->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  auto &hist_acc = **historical;
  EXPECT_TRUE(hist_acc.LabelPropertyIndexReady(label2, paths2));
  std::vector<int64_t> values;
  for (auto v : hist_acc.Vertices(label2, paths2, std::array{ms::PropertyValueRange::IsNotNull()}, ms::View::OLD)) {
    values.push_back(v.GetProperty(prop2, ms::View::OLD).value_or(ms::PropertyValue()).ValueInt());
  }
  EXPECT_THAT(values, ::testing::ElementsAre(10, 20));
  historical->reset();
  Mem()->ReleaseForkPin(fork_ts);
}

// Writes through a historical accessor are refused (MVCC writes at the first delta, metadata writes at their entry
// points) and
// every accessor releases its self-pin exactly once, including a read-only commit followed by Abort.
TEST_F(VersioningHistoricalAccessTest, RefusesWritesAndReleasesSelfPin) {
  const auto label = storage->NameToLabel("L");
  const auto gid = CommitNewVertex(*storage, storage->NameToProperty("p"), 1);
  CommitIndex(*storage, [&](auto &acc) { return acc.CreateIndex(label); });
  const uint64_t fork_ts = Mem()->RegisterForkPin();

  {
    auto historical = Mem()->HistoricalAccess(fork_ts);
    ASSERT_TRUE(historical.has_value());
    EXPECT_THROW((void)(*historical)->CreateVertex(), ms::WriteInHistoricalAccessorException);
    auto v = (*historical)->FindVertex(gid, ms::View::OLD);
    ASSERT_TRUE(v.has_value());
    EXPECT_THROW((void)v->AddLabel(label), ms::WriteInHistoricalAccessorException);
  }
  {
    auto historical = Mem()->HistoricalAccess(fork_ts);
    ASSERT_TRUE(historical.has_value());
    EXPECT_THROW((*historical)->SetIndexStats(label, ms::LabelIndexStats{.count = 7, .avg_degree = 1.5}),
                 ms::WriteInHistoricalAccessorException);
    EXPECT_THROW((void)(*historical)->DeleteLabelIndexStats(label), ms::WriteInHistoricalAccessorException);
    EXPECT_THROW((void)(*historical)->DropIndex(label), ms::WriteInHistoricalAccessorException);
    const std::array<std::string, 1> enum_values{"a"};
    EXPECT_THROW((void)(*historical)->CreateEnum("E", enum_values), ms::WriteInHistoricalAccessorException);
  }
  EXPECT_EQ(Mem()->OldestForkPin(), fork_ts);
  {
    auto acc = storage->Access(ms::READ);
    EXPECT_TRUE(acc->LabelIndexReady(label)) << "a refused historical drop must not drop main's index";
    EXPECT_FALSE(acc->GetIndexStats(label).has_value()) << "a refused historical stats write must not reach main";
    EXPECT_TRUE(acc->ShowEnums().empty()) << "a refused historical enum write must not reach main";
    EXPECT_EQ(CountVertices(*acc, ms::View::OLD), 1U);
    auto v = acc->FindVertex(gid, ms::View::OLD);
    ASSERT_TRUE(v.has_value());
    EXPECT_FALSE(v->HasLabel(label, ms::View::OLD).value_or(true));
  }
  {
    auto reader = Mem()->HistoricalAccess(fork_ts);
    ASSERT_TRUE(reader.has_value());
    Commit(**reader);
    Mem()->ReleaseForkPin(fork_ts);
    EXPECT_EQ(Mem()->OldestForkPin(), memgraph::storage::InMemoryStorage::kNoForkPin)
        << "commit must release the self-pin";
    (*reader)->Abort();
  }
  EXPECT_EQ(Mem()->OldestForkPin(), memgraph::storage::InMemoryStorage::kNoForkPin)
      << "a refused historical accessor leaked its self-pin";
}

// A historical open that cannot get the shared lock in time throws and leaves the pins untouched.
TEST_F(VersioningHistoricalAccessTest, TimeoutThrowsAndKeepsPins) {
  const uint64_t fork_ts = Mem()->RegisterForkPin();
  {
    auto unique = storage->UniqueAccess();
    EXPECT_THROW((void)Mem()->HistoricalAccess(fork_ts, std::chrono::milliseconds{10}), ms::SharedAccessTimeout);
  }
  EXPECT_EQ(Mem()->OldestForkPin(), fork_ts) << "the failed open must release its self-pin";
  {
    auto historical = Mem()->HistoricalAccess(fork_ts, std::chrono::milliseconds{10});
    ASSERT_TRUE(historical.has_value());
  }
  Mem()->ReleaseForkPin(fork_ts);
  EXPECT_EQ(Mem()->OldestForkPin(), memgraph::storage::InMemoryStorage::kNoForkPin);
}

// Only IN_MEMORY_TRANSACTIONAL keeps MVCC history; elsewhere a historical read would silently see live data.
TEST_F(VersioningHistoricalAccessTest, RejectsNonTransactionalStorageMode) {
  const uint64_t fork_ts = Mem()->RegisterForkPin();
  Mem()->SetStorageMode(ms::StorageMode::IN_MEMORY_ANALYTICAL);

  auto result = Mem()->HistoricalAccess(fork_ts);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), ms::InMemoryStorage::HistoricalAccessError::StorageModeNotTransactional);
  EXPECT_EQ(Mem()->OldestForkPin(), fork_ts) << "the refused open kept its self-pin";
  Mem()->ReleaseForkPin(fork_ts);
  EXPECT_EQ(Mem()->OldestForkPin(), memgraph::storage::InMemoryStorage::kNoForkPin)
      << "the refused open leaked its self-pin";
}

// Point and vector indexes hold only main's current state: the point index is reported absent and vector search
// refused.
TEST_F(VersioningHistoricalAccessTest, PointAndVectorIndexesAreNotUsedByHistoricalReader) {
  const auto label = storage->NameToLabel("L");
  const auto loc = storage->NameToProperty("loc");
  const auto emb = storage->NameToProperty("emb");
  constexpr std::string_view kVectorIndex = "vidx";
  auto point = [](double x, double y) {
    return ms::PropertyValue(ms::Point2d{ms::CoordinateReferenceSystem::Cartesian_2d, x, y});
  };
  {
    auto unique_acc = storage->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreatePointIndex(label, loc).has_value());
    ASSERT_TRUE(unique_acc
                    ->CreateVectorIndex(ms::VectorIndexSpec{
                        .index_name = std::string{kVectorIndex},
                        .label_filter = ms::VectorLabelFilter{.mode = ms::VectorMatchMode::SINGLE, .ids = {label}},
                        .property = emb,
                        .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
                        .dimension = 2,
                        .resize_coefficient = 2,
                        .capacity = 10,
                        .scalar_kind = unum::usearch::scalar_kind_t::f32_k})
                    .has_value());
    Commit(*unique_acc);
  }
  ms::Gid a_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.AddLabel(label).has_value());
    ASSERT_TRUE(a.SetProperty(loc, point(0, 0)).has_value());
    Commit(*acc);
  }
  const uint64_t fork_ts = Mem()->RegisterForkPin();
  {
    auto acc = storage->Access(ms::WRITE);
    auto a = acc->FindVertex(a_gid, ms::View::OLD);
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(a->SetProperty(loc, point(9, 9)).has_value());
    auto b = acc->CreateVertex();
    ASSERT_TRUE(b.AddLabel(label).has_value());
    const auto index_id = acc->GetNameIdMapper()->NameToId(kVectorIndex);
    ASSERT_TRUE(b.SetProperty(emb,
                              ms::PropertyValue(ms::PropertyValue::VectorIndexIdData{
                                  .ids = memgraph::utils::small_vector<uint64_t>{index_id},
                                  .vector = memgraph::utils::small_vector<float>{1.0F, 0.0F}}))
                    .has_value());
    Commit(*acc);
  }

  EXPECT_TRUE(storage->Access(ms::READ)->PointIndexExists(label, loc));
  auto historical = Mem()->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  EXPECT_FALSE((*historical)->PointIndexExists(label, loc)) << "the point index only holds a's post-fork location";
  EXPECT_THROW((void)(*historical)->VectorIndexSearchOnNodes(std::string{kVectorIndex}, 10, {1.0F, 0.0F}),
               ms::VectorSearchException)
      << "the vector index would return b, created after the fork";
  historical->reset();
  Mem()->ReleaseForkPin(fork_ts);
}

}  // namespace
