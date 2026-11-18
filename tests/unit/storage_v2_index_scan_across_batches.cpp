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

// An index scan that a query opened before a periodic-commit batch boundary must not return an index entry a later
// batch of the same query added, whatever command the query is on: the scan sees the graph as of its own start, and
// MVCC decides the rest. Each index type gets the entry from an element that existed before the scan opened, since a
// new element is past the scan's gid bound anyway. (An edge's type never changes, so the edge-type index has no case.)

#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <vector>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/bound.hpp"

namespace ms = memgraph::storage;

namespace {

class IndexScanAcrossBatches : public ::testing::TestWithParam<ms::View> {
 protected:
  void SetUp() override {
    ms::Config config;
    config.salient.items.properties_on_edges = true;
    storage = std::make_unique<ms::InMemoryStorage>(config);
    auto acc = storage->Access(ms::WRITE);
    label = acc->NameToLabel("L");
    property = acc->NameToProperty("p");
    edge_type = acc->NameToEdgeType("T");
  }

  void Commit(std::unique_ptr<ms::Storage::Accessor> &acc) {
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  /// Two vertices with label L; the first has p, the second gets it in batch 2. Returns their gids.
  std::pair<ms::Gid, ms::Gid> TwoVertices(bool second_labelled) {
    auto acc = storage->Access(ms::WRITE);
    auto first = acc->CreateVertex();
    auto second = acc->CreateVertex();
    EXPECT_TRUE(first.AddLabel(label).has_value());
    if (second_labelled) EXPECT_TRUE(second.AddLabel(label).has_value());
    EXPECT_TRUE(first.SetProperty(property, ms::PropertyValue{1}).has_value());
    auto gids = std::pair{first.Gid(), second.Gid()};
    Commit(acc);
    return gids;
  }

  /// Two edges of type T; the first has p, the second gets it in batch 2. Returns their gids.
  std::pair<ms::Gid, ms::Gid> TwoEdges() {
    auto acc = storage->Access(ms::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    auto first = acc->CreateEdge(&from, &to, edge_type);
    auto second = acc->CreateEdge(&from, &to, edge_type);
    EXPECT_TRUE(first.has_value() && second.has_value());
    EXPECT_TRUE(first->SetProperty(property, ms::PropertyValue{1}).has_value());
    auto gids = std::pair{first->Gid(), second->Gid()};
    Commit(acc);
    return gids;
  }

  /// Opens a scan in batch 1, advances the command, commits batch 1; batch 2 runs `write` and commits; then the scan
  /// is read to its end.
  template <class TOpen, class TWrite>
  std::vector<ms::Gid> ScanAcrossBatches(TOpen open, TWrite write) {
    auto acc = storage->Access(ms::WRITE);
    auto iterable = open(*acc);
    acc->AdvanceCommand();
    EXPECT_TRUE(acc->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
    write(*acc);
    EXPECT_TRUE(acc->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
    std::vector<ms::Gid> seen;
    for (auto const &element : iterable) seen.push_back(element.Gid());
    acc->Abort();
    return seen;
  }

  std::unique_ptr<ms::Storage> storage;
  ms::LabelId label;
  ms::PropertyId property;
  ms::EdgeTypeId edge_type;
};

TEST_P(IndexScanAcrossBatches, LabelIndex) {
  {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(label).has_value());
    Commit(acc);
  }
  auto [first, second] = TwoVertices(false);
  auto seen =
      ScanAcrossBatches([&](auto &acc) { return acc.Vertices(label, GetParam()); },
                        [&](auto &acc) { ASSERT_TRUE(acc.FindVertex(second, ms::View::OLD)->AddLabel(label)); });
  EXPECT_EQ(seen, (std::vector{first}));
}

TEST_P(IndexScanAcrossBatches, LabelPropertyIndex) {
  {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(label, {ms::PropertyPath{property}}).has_value());
    Commit(acc);
  }
  auto [first, second] = TwoVertices(true);
  auto seen = ScanAcrossBatches(
      [&](auto &acc) {
        return acc.Vertices(
            label, std::array{ms::PropertyPath{property}}, std::array{ms::PropertyValueRange::IsNotNull()}, GetParam());
      },
      [&](auto &acc) {
        ASSERT_TRUE(acc.FindVertex(second, ms::View::OLD)->SetProperty(property, ms::PropertyValue{2}).has_value());
      });
  EXPECT_EQ(seen, (std::vector{first}));
}

TEST_P(IndexScanAcrossBatches, VertexPropertyIndex) {
  {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateGlobalVertexIndex(property).has_value());
    Commit(acc);
  }
  auto [first, second] = TwoVertices(true);
  auto seen = ScanAcrossBatches(
      [&](auto &acc) { return acc.Vertices(property, GetParam()); },
      [&](auto &acc) {
        ASSERT_TRUE(acc.FindVertex(second, ms::View::OLD)->SetProperty(property, ms::PropertyValue{2}).has_value());
      });
  EXPECT_EQ(seen, (std::vector{first}));
}

TEST_P(IndexScanAcrossBatches, EdgeTypePropertyIndex) {
  {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(edge_type, property).has_value());
    Commit(acc);
  }
  auto [first, second] = TwoEdges();
  auto seen = ScanAcrossBatches(
      [&](auto &acc) { return acc.Edges(edge_type, property, GetParam()); },
      [&](auto &acc) {
        ASSERT_TRUE(acc.FindEdge(second, ms::View::OLD)->SetProperty(property, ms::PropertyValue{2}).has_value());
      });
  EXPECT_EQ(seen, (std::vector{first}));
}

TEST_P(IndexScanAcrossBatches, EdgePropertyIndex) {
  {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateGlobalEdgeIndex(property).has_value());
    Commit(acc);
  }
  auto [first, second] = TwoEdges();
  auto seen = ScanAcrossBatches(
      [&](auto &acc) { return acc.Edges(property, GetParam()); },
      [&](auto &acc) {
        ASSERT_TRUE(acc.FindEdge(second, ms::View::OLD)->SetProperty(property, ms::PropertyValue{2}).has_value());
      });
  EXPECT_EQ(seen, (std::vector{first}));
}

INSTANTIATE_TEST_SUITE_P(Views, IndexScanAcrossBatches, ::testing::Values(ms::View::OLD, ms::View::NEW));

// A View::OLD scan writes, in its own batch, each element it returns, then commits the batch. The new entry carries
// the batch's start timestamp; the scan must not return the element a second time through it.
class OwnBatchWrite : public ::testing::Test {
 protected:
  void SetUp() override { Open(ms::StorageMode::IN_MEMORY_TRANSACTIONAL); }

  void Open(ms::StorageMode mode) {
    ms::Config config;
    config.salient.items.properties_on_edges = true;
    config.salient.storage_mode = mode;
    storage = std::make_unique<ms::InMemoryStorage>(config);
    auto acc = storage->Access(ms::WRITE);
    label = acc->NameToLabel("L");
    property = acc->NameToProperty("p");
    other_property = acc->NameToProperty("q");
    edge_type = acc->NameToEdgeType("T");
  }

  void Commit(std::unique_ptr<ms::Storage::Accessor> &acc) {
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  /// A write accessor armed as the query layer arms one for a query that commits in batches.
  std::unique_ptr<ms::Storage::Accessor> Armed() {
    auto acc = storage->Access(ms::WRITE);
    acc->ArmIndexStamps();
    return acc;
  }

  /// A vertex with label L and p = `value`, committed. Returns its gid.
  ms::Gid LabelledVertex(int value) {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    EXPECT_TRUE(vertex.AddLabel(label).has_value());
    EXPECT_TRUE(vertex.SetProperty(property, ms::PropertyValue{value}).has_value());
    auto const gid = vertex.Gid();
    Commit(acc);
    return gid;
  }

  template <class TCreate>
  void Index(TCreate create) {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(create(*acc));
    Commit(acc);
  }

  /// Vertices with label L and p = 1, 2, and one unindexed vertex for unrelated writes. Returns the L gids.
  std::vector<ms::Gid> Vertices() {
    auto acc = storage->Access(ms::WRITE);
    std::vector<ms::Gid> gids;
    for (auto value : {1, 2}) {
      auto vertex = acc->CreateVertex();
      EXPECT_TRUE(vertex.AddLabel(label).has_value());
      EXPECT_TRUE(vertex.SetProperty(property, ms::PropertyValue{value}).has_value());
      gids.push_back(vertex.Gid());
    }
    other = acc->CreateVertex().Gid();
    Commit(acc);
    return gids;
  }

  /// Edges of type T with p = 1, 2. Returns their gids.
  std::vector<ms::Gid> Edges() {
    auto acc = storage->Access(ms::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    std::vector<ms::Gid> gids;
    for (auto value : {1, 2}) {
      auto edge = acc->CreateEdge(&from, &to, edge_type);
      EXPECT_TRUE(edge.has_value());
      EXPECT_TRUE(edge->SetProperty(property, ms::PropertyValue{value}).has_value());
      gids.push_back(edge->Gid());
    }
    Commit(acc);
    return gids;
  }

  /// A write the scan does not index, so the batch has written something.
  void WriteOther(ms::Storage::Accessor &acc) {
    ASSERT_TRUE(acc.FindVertex(other, ms::View::NEW)->SetProperty(other_property, ms::PropertyValue{1}).has_value());
  }

  /// Reads the scan to its end; adds 10 to each element's p and commits the batch after each element.
  template <class TIterable>
  std::vector<ms::Gid> BumpEach(ms::Storage::Accessor &acc, TIterable &&iterable) {
    std::vector<ms::Gid> seen;
    for (auto it = iterable.begin(); it != iterable.end(); ++it) {
      auto element = *it;
      seen.push_back(element.Gid());
      auto const value = element.GetProperty(property, ms::View::NEW)->ValueInt();
      EXPECT_TRUE(element.SetProperty(property, ms::PropertyValue{value + 10}).has_value());
      EXPECT_TRUE(acc.PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    return seen;
  }

  auto LabelPropertyScan(ms::Storage::Accessor &acc) {
    return acc.Vertices(
        label, std::array{ms::PropertyPath{property}}, std::array{ms::PropertyValueRange::IsNotNull()}, ms::View::OLD);
  }

  std::unique_ptr<ms::Storage> storage;
  ms::LabelId label;
  ms::PropertyId property;
  ms::PropertyId other_property;
  ms::EdgeTypeId edge_type;
  ms::Gid other;
};

TEST_F(OwnBatchWrite, LabelPropertyIndex) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  auto acc = Armed();
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

TEST_F(OwnBatchWrite, EdgeTypePropertyIndex) {
  Index([&](auto &acc) { return acc.CreateIndex(edge_type, property).has_value(); });
  auto const gids = Edges();
  auto acc = Armed();
  EXPECT_EQ(BumpEach(*acc, acc->Edges(edge_type, property, ms::View::OLD)), gids);
  acc->Abort();
}

TEST_F(OwnBatchWrite, VertexPropertyIndex) {
  Index([&](auto &acc) { return acc.CreateGlobalVertexIndex(property).has_value(); });
  auto const gids = Vertices();
  auto acc = Armed();
  EXPECT_EQ(BumpEach(*acc, acc->Vertices(property, ms::View::OLD)), gids);
  acc->Abort();
}

TEST_F(OwnBatchWrite, EdgePropertyIndex) {
  Index([&](auto &acc) { return acc.CreateGlobalEdgeIndex(property).has_value(); });
  auto const gids = Edges();
  auto acc = Armed();
  EXPECT_EQ(BumpEach(*acc, acc->Edges(property, ms::View::OLD)), gids);
  acc->Abort();
}

TEST_F(OwnBatchWrite, ChunkedLabelPropertyIndex) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  auto acc = Armed();
  auto chunks = acc->ChunkedVertices(
      label, std::array{ms::PropertyPath{property}}, std::array{ms::PropertyValueRange::IsNotNull()}, ms::View::OLD, 1);
  ASSERT_EQ(chunks.size(), 1);
  EXPECT_EQ(BumpEach(*acc, chunks.get_chunk(0)), gids);
  acc->Abort();
}

TEST_F(OwnBatchWrite, ChunkedEdgeTypePropertyIndex) {
  Index([&](auto &acc) { return acc.CreateIndex(edge_type, property).has_value(); });
  auto const gids = Edges();
  auto acc = Armed();
  auto chunks = acc->ChunkedEdges(edge_type, property, ms::PropertyValueRange::IsNotNull(), ms::View::OLD, 1);
  ASSERT_EQ(chunks.size(), 1);
  EXPECT_EQ(BumpEach(*acc, chunks.get_chunk(0)), gids);
  acc->Abort();
}

// A later command is not a write: the batch's entries are still all newer than the scan.
TEST_F(OwnBatchWrite, LaterCommandWithoutAnEarlierWrite) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  auto acc = Armed();
  acc->AdvanceCommand();
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

TEST_F(OwnBatchWrite, AnalyticalMode) {
  Open(ms::StorageMode::IN_MEMORY_ANALYTICAL);
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  auto acc = Armed();
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

// A write and an advance in an earlier batch say nothing about the scan's batch.
TEST_F(OwnBatchWrite, AnEarlierBatchsWriteDoesNotCount) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  auto acc = Armed();
  WriteOther(*acc);
  acc->AdvanceCommand();
  ASSERT_TRUE(acc->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

// An element the batch created before the scan opened is the scan's to see, after a commit too. Analytical mode
// keeps no deltas, so there the advance raises a stamp without one.
TEST_F(OwnBatchWrite, AnElementTheBatchCreatedBeforeTheScanStaysVisible) {
  for (auto mode : {ms::StorageMode::IN_MEMORY_TRANSACTIONAL, ms::StorageMode::IN_MEMORY_ANALYTICAL}) {
    SCOPED_TRACE(static_cast<int>(mode));
    Open(mode);
    Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
    auto gids = Vertices();
    auto acc = Armed();
    auto created = acc->CreateVertex();
    ASSERT_TRUE(created.AddLabel(label).has_value());
    ASSERT_TRUE(created.SetProperty(property, ms::PropertyValue{5}).has_value());
    gids.push_back(created.Gid());
    acc->AdvanceCommand();
    auto iterable = LabelPropertyScan(*acc);
    std::vector<ms::Gid> seen;
    for (auto it = iterable.begin(); it != iterable.end(); ++it) {
      seen.push_back((*it).Gid());
      WriteOther(*acc);
      ASSERT_TRUE(acc->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    acc->Abort();
    EXPECT_EQ(seen, gids);
  }
}

// A write before the scan opened does not make the scan's own later entries visible (a write before a batched
// update): they carry the stamp the advance raised, the earlier write the batch start.
TEST_F(OwnBatchWrite, AWriteBeforeTheScanOpenedStillHidesItsLaterEntries) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  auto acc = Armed();
  WriteOther(*acc);
  acc->AdvanceCommand();
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

// T2: analytical mode has no deltas, and an advance there raises a stamp all the same.
TEST_F(OwnBatchWrite, AnAdvanceWithoutAWriteInAnalyticalMode) {
  Open(ms::StorageMode::IN_MEMORY_ANALYTICAL);
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  auto acc = Armed();
  acc->AdvanceCommand();
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

// T2b: an analytical write before the advance is seen once, and the scan's own later entries never.
TEST_F(OwnBatchWrite, AnAnalyticalWriteBeforeTheScanIsSeenOnce) {
  Open(ms::StorageMode::IN_MEMORY_ANALYTICAL);
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto gids = Vertices();
  auto acc = Armed();
  auto created = acc->CreateVertex();
  ASSERT_TRUE(created.AddLabel(label).has_value());
  ASSERT_TRUE(created.SetProperty(property, ms::PropertyValue{5}).has_value());
  gids.push_back(created.Gid());
  acc->AdvanceCommand();
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

// T3: an element the batch moved into the scanned range before the advance is seen once.
TEST_F(OwnBatchWrite, AnElementMovedIntoTheRangeBeforeTheScanIsSeenOnce) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto gids = Vertices();
  auto const moved = LabelledVertex(-1);
  auto acc = Armed();
  ASSERT_TRUE(acc->FindVertex(moved, ms::View::NEW)->SetProperty(property, ms::PropertyValue{3}).has_value());
  gids.push_back(moved);
  acc->AdvanceCommand();
  auto const positive =
      ms::PropertyValueRange::Bounded(memgraph::utils::MakeBoundExclusive(ms::PropertyValue{0}), std::nullopt);
  EXPECT_EQ(
      BumpEach(*acc, acc->Vertices(label, std::array{ms::PropertyPath{property}}, std::array{positive}, ms::View::OLD)),
      gids);
  acc->Abort();
}

// T4: another transaction that started after this batch and committed before the scan opened wrote an entry
// stamped between the batch start and the scan's stamp; the scan, which sees the graph as of its batch, hides it.
TEST_F(OwnBatchWrite, AForeignEntryBetweenTheBatchStartAndTheStampStaysHidden) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  ms::Gid foreign;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(vertex.AddLabel(label).has_value());
    foreign = vertex.Gid();
    Commit(acc);
  }
  auto acc = Armed();
  auto other = storage->Access(ms::WRITE);
  acc->AdvanceCommand();
  ASSERT_TRUE(other->FindVertex(foreign, ms::View::NEW)->SetProperty(property, ms::PropertyValue{3}).has_value());
  Commit(other);
  auto iterable = LabelPropertyScan(*acc);
  ASSERT_TRUE(acc->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
  std::vector<ms::Gid> seen;
  for (auto const &vertex : iterable) seen.push_back(vertex.Gid());
  acc->Abort();
  EXPECT_EQ(seen, gids);
}

// T6: entries carrying a stamp are visible to a transaction that starts after the commit.
TEST_F(OwnBatchWrite, ALaterTransactionSeesStampedEntries) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  std::vector<ms::Gid> gids;
  {
    auto acc = Armed();
    for (auto value : {1, 2}) {
      auto vertex = acc->CreateVertex();
      ASSERT_TRUE(vertex.AddLabel(label).has_value());
      ASSERT_TRUE(vertex.SetProperty(property, ms::PropertyValue{value}).has_value());
      gids.push_back(vertex.Gid());
      acc->AdvanceCommand();
    }
    Commit(acc);
  }
  auto acc = storage->Access(ms::WRITE);
  std::vector<ms::Gid> seen;
  for (auto const &vertex : LabelPropertyScan(*acc)) seen.push_back(vertex.Gid());
  EXPECT_EQ(seen, gids);
}

// T7: a stamp is a timestamp no transaction starts at; garbage collection must not wait for it to finish.
TEST_F(OwnBatchWrite, AStampDoesNotHoldBackGarbageCollection) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  ms::Gid gid;
  {
    auto acc = Armed();
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(vertex.AddLabel(label).has_value());
    ASSERT_TRUE(vertex.SetProperty(property, ms::PropertyValue{1}).has_value());
    gid = vertex.Gid();
    acc->AdvanceCommand();
    ASSERT_TRUE(vertex.SetProperty(property, ms::PropertyValue{2}).has_value());
    Commit(acc);
  }
  {
    auto acc = storage->Access(ms::WRITE);
    ASSERT_TRUE(acc->FindVertex(gid, ms::View::NEW)->SetProperty(property, ms::PropertyValue{3}).has_value());
    Commit(acc);
  }
  storage->FreeMemory();
  storage->FreeMemory();
  auto acc = storage->Access(ms::WRITE);
  EXPECT_EQ(acc->ApproximateVertexCount(label, std::array{ms::PropertyPath{property}}), 1);
}

// T8: an earlier query of an explicit transaction wrote before this query armed; its entry is seen once, and the
// scan's own later entries never.
TEST_F(OwnBatchWrite, AnEarlierCommandsWriteIsSeenOnceAfterArming) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto gids = Vertices();
  auto acc = storage->Access(ms::WRITE);
  auto created = acc->CreateVertex();
  ASSERT_TRUE(created.AddLabel(label).has_value());
  ASSERT_TRUE(created.SetProperty(property, ms::PropertyValue{5}).has_value());
  gids.push_back(created.Gid());
  acc->AdvanceCommand();
  acc->ArmIndexStamps();
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

// T9: arming at command 0 raises no stamp, so a View::NEW scan at command 0 still sees the batch's own writes.
TEST_F(OwnBatchWrite, ArmingAtCommandZeroRaisesNoStamp) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto gids = Vertices();
  auto acc = Armed();
  auto created = acc->CreateVertex();
  ASSERT_TRUE(created.AddLabel(label).has_value());
  ASSERT_TRUE(created.SetProperty(property, ms::PropertyValue{5}).has_value());
  gids.push_back(created.Gid());
  std::vector<ms::Gid> seen;
  for (auto const &vertex : acc->Vertices(label,
                                          std::array{ms::PropertyPath{property}},
                                          std::array{ms::PropertyValueRange::IsNotNull()},
                                          ms::View::NEW)) {
    seen.push_back(vertex.Gid());
  }
  acc->Abort();
  EXPECT_EQ(seen, gids);
}

// One case per index insert site, each through the index it feeds. `Setup` commits one element the scan returns
// and two targets without an entry; `Write` gives a target one (for the edge-type index, a new edge from it).
struct Ids {
  ms::LabelId label;
  ms::PropertyId p;
  ms::PropertyId q;
  ms::EdgeTypeId type;
};

using Accessor = ms::Storage::Accessor;
using Gids = std::array<ms::Gid, 3>;

/// Vertices for the vertex sites: the kept one gets `kept`, each target `target`.
template <class TKept, class TTarget>
Gids SetupVertices(Accessor &acc, TKept kept, TTarget target) {
  auto first = acc.CreateVertex();
  kept(first);
  auto second = acc.CreateVertex();
  target(second);
  auto third = acc.CreateVertex();
  target(third);
  return {first.Gid(), second.Gid(), third.Gid()};
}

/// Edges of type T for the edge property sites: the kept one has p, the targets do not.
Gids SetupEdges(Accessor &acc, Ids ids) {
  auto from = acc.CreateVertex();
  auto to = acc.CreateVertex();
  Gids gids;
  for (auto &gid : gids) {
    auto edge = acc.CreateEdge(&from, &to, ids.type);
    EXPECT_TRUE(edge.has_value());
    gid = edge->Gid();
  }
  EXPECT_TRUE(acc.FindEdge(gids[0], ms::View::NEW)->SetProperty(ids.p, ms::PropertyValue{1}).has_value());
  return gids;
}

auto LabelPropertyScanOf(Accessor &acc, Ids ids) {
  return acc.Vertices(
      ids.label, std::array{ms::PropertyPath{ids.p}}, std::array{ms::PropertyValueRange::IsNotNull()}, ms::View::OLD);
}

struct LabelSite {
  static constexpr bool kWritesBeforeOpen = false;

  static bool Index(Accessor &acc, Ids ids) { return acc.CreateIndex(ids.label).has_value(); }

  static Gids Setup(Accessor &acc, Ids ids) {
    return SetupVertices(acc, [&](auto &v) { EXPECT_TRUE(v.AddLabel(ids.label).has_value()); }, [](auto &) {});
  }

  static void Write(Accessor &acc, Ids ids, ms::Gid target) {
    ASSERT_TRUE(acc.FindVertex(target, ms::View::NEW)->AddLabel(ids.label).has_value());
  }

  static auto Open(Accessor &acc, Ids ids) { return acc.Vertices(ids.label, ms::View::OLD); }

  static uint64_t Count(Accessor &acc, Ids ids) { return acc.ApproximateVertexCount(ids.label); }
};

struct LabelPropertySetSite {
  static constexpr bool kWritesBeforeOpen = false;

  static bool Index(Accessor &acc, Ids ids) {
    return acc.CreateIndex(ids.label, {ms::PropertyPath{ids.p}}).has_value();
  }

  static Gids Setup(Accessor &acc, Ids ids) {
    return SetupVertices(
        acc,
        [&](auto &v) {
          EXPECT_TRUE(v.AddLabel(ids.label).has_value());
          EXPECT_TRUE(v.SetProperty(ids.p, ms::PropertyValue{1}).has_value());
        },
        [&](auto &v) { EXPECT_TRUE(v.AddLabel(ids.label).has_value()); });
  }

  static void Write(Accessor &acc, Ids ids, ms::Gid target) {
    ASSERT_TRUE(acc.FindVertex(target, ms::View::NEW)->SetProperty(ids.p, ms::PropertyValue{2}).has_value());
  }

  static auto Open(Accessor &acc, Ids ids) { return LabelPropertyScanOf(acc, ids); }

  static uint64_t Count(Accessor &acc, Ids ids) {
    return acc.ApproximateVertexCount(ids.label, std::array{ms::PropertyPath{ids.p}});
  }
};

struct LabelPropertyAddLabelSite : LabelPropertySetSite {
  static Gids Setup(Accessor &acc, Ids ids) {
    return SetupVertices(
        acc,
        [&](auto &v) {
          EXPECT_TRUE(v.AddLabel(ids.label).has_value());
          EXPECT_TRUE(v.SetProperty(ids.p, ms::PropertyValue{1}).has_value());
        },
        [&](auto &v) { EXPECT_TRUE(v.SetProperty(ids.p, ms::PropertyValue{2}).has_value()); });
  }

  static void Write(Accessor &acc, Ids ids, ms::Gid target) {
    ASSERT_TRUE(acc.FindVertex(target, ms::View::NEW)->AddLabel(ids.label).has_value());
  }
};

struct CompositeSite {
  static constexpr bool kWritesBeforeOpen = false;

  static bool Index(Accessor &acc, Ids ids) {
    return acc.CreateIndex(ids.label, {ms::PropertyPath{ids.p}, ms::PropertyPath{ids.q}}).has_value();
  }

  static Gids Setup(Accessor &acc, Ids ids) {
    auto labelled = [&](auto &v) {
      EXPECT_TRUE(v.AddLabel(ids.label).has_value());
      EXPECT_TRUE(v.SetProperty(ids.p, ms::PropertyValue{1}).has_value());
    };
    return SetupVertices(
        acc,
        [&](auto &v) {
          labelled(v);
          EXPECT_TRUE(v.SetProperty(ids.q, ms::PropertyValue{1}).has_value());
        },
        labelled);
  }

  static void Write(Accessor &acc, Ids ids, ms::Gid target) {
    ASSERT_TRUE(acc.FindVertex(target, ms::View::NEW)->SetProperty(ids.q, ms::PropertyValue{2}).has_value());
  }

  static auto Open(Accessor &acc, Ids ids) {
    return acc.Vertices(ids.label,
                        std::array{ms::PropertyPath{ids.p}, ms::PropertyPath{ids.q}},
                        std::array{ms::PropertyValueRange::IsNotNull(), ms::PropertyValueRange::IsNotNull()},
                        ms::View::OLD);
  }

  static uint64_t Count(Accessor &acc, Ids ids) {
    return acc.ApproximateVertexCount(ids.label, std::array{ms::PropertyPath{ids.p}, ms::PropertyPath{ids.q}});
  }
};

struct VertexPropertySite {
  static constexpr bool kWritesBeforeOpen = false;

  static bool Index(Accessor &acc, Ids ids) { return acc.CreateGlobalVertexIndex(ids.p).has_value(); }

  static Gids Setup(Accessor &acc, Ids ids) {
    return SetupVertices(
        acc, [&](auto &v) { EXPECT_TRUE(v.SetProperty(ids.p, ms::PropertyValue{1}).has_value()); }, [](auto &) {});
  }

  static void Write(Accessor &acc, Ids ids, ms::Gid target) {
    ASSERT_TRUE(acc.FindVertex(target, ms::View::NEW)->SetProperty(ids.p, ms::PropertyValue{2}).has_value());
  }

  static auto Open(Accessor &acc, Ids ids) { return acc.Vertices(ids.p, ms::View::OLD); }

  static uint64_t Count(Accessor &acc, Ids ids) { return acc.ApproximateVertexCount(ids.p); }
};

struct EdgeTypePropertySite {
  static constexpr bool kWritesBeforeOpen = false;

  static bool Index(Accessor &acc, Ids ids) { return acc.CreateIndex(ids.type, ids.p).has_value(); }

  static Gids Setup(Accessor &acc, Ids ids) { return SetupEdges(acc, ids); }

  static void Write(Accessor &acc, Ids ids, ms::Gid target) {
    ASSERT_TRUE(acc.FindEdge(target, ms::View::NEW)->SetProperty(ids.p, ms::PropertyValue{2}).has_value());
  }

  static auto Open(Accessor &acc, Ids ids) { return acc.Edges(ids.type, ids.p, ms::View::OLD); }

  static uint64_t Count(Accessor &acc, Ids ids) { return acc.ApproximateEdgeCount(ids.type, ids.p); }
};

struct EdgePropertySite : EdgeTypePropertySite {
  static bool Index(Accessor &acc, Ids ids) { return acc.CreateGlobalEdgeIndex(ids.p).has_value(); }

  static auto Open(Accessor &acc, Ids ids) { return acc.Edges(ids.p, ms::View::OLD); }

  static uint64_t Count(Accessor &acc, Ids ids) { return acc.ApproximateEdgeCount(ids.p); }
};

// An edge's type never changes, so the only entry is the one a new edge makes; a new edge made after the scan
// opened is past its gid bound, so this site writes at the scan's own command, before the open.
struct EdgeTypeSite {
  static constexpr bool kWritesBeforeOpen = true;

  static bool Index(Accessor &acc, Ids ids) { return acc.CreateIndex(ids.type).has_value(); }

  static Gids Setup(Accessor &acc, Ids ids) {
    auto from = acc.CreateVertex();
    auto edge = acc.CreateEdge(&from, &from, ids.type);
    EXPECT_TRUE(edge.has_value());
    return {edge->Gid(), acc.CreateVertex().Gid(), acc.CreateVertex().Gid()};
  }

  static void Write(Accessor &acc, Ids ids, ms::Gid target) {
    auto vertex = acc.FindVertex(target, ms::View::NEW);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(acc.CreateEdge(&*vertex, &*vertex, ids.type).has_value());
  }

  static auto Open(Accessor &acc, Ids ids) { return acc.Edges(ids.type, ms::View::OLD); }

  static uint64_t Count(Accessor &acc, Ids ids) { return acc.ApproximateEdgeCount(ids.type); }
};

template <class TSite>
class EachInsertSite : public OwnBatchWrite {
 protected:
  Ids ids() const { return {label, property, other_property, edge_type}; }

  /// Creates the site's index and commits its setup.
  Gids Prepare() {
    Index([&](auto &acc) { return TSite::Index(acc, ids()); });
    auto acc = storage->Access(ms::WRITE);
    auto const gids = TSite::Setup(*acc, ids());
    Commit(acc);
    return gids;
  }

  uint64_t Count() {
    auto acc = storage->Access(ms::WRITE);
    return TSite::Count(*acc, ids());
  }
};

using Sites = ::testing::Types<LabelSite, LabelPropertySetSite, LabelPropertyAddLabelSite, CompositeSite,
                               VertexPropertySite, EdgeTypePropertySite, EdgePropertySite, EdgeTypeSite>;
TYPED_TEST_SUITE(EachInsertSite, Sites);

// T1 per site: after an earlier write and an advance, the scan's own entry is not seen once its batch commits.
TYPED_TEST(EachInsertSite, AnEntryOfTheScansCommandIsNotSeenAfterACommit) {
  auto const [kept, target, unused] = this->Prepare();
  auto acc = this->Armed();
  acc->CreateVertex();
  acc->AdvanceCommand();
  if constexpr (TypeParam::kWritesBeforeOpen) TypeParam::Write(*acc, this->ids(), target);
  auto iterable = TypeParam::Open(*acc, this->ids());
  if constexpr (!TypeParam::kWritesBeforeOpen) TypeParam::Write(*acc, this->ids(), target);
  ASSERT_TRUE(acc->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
  std::vector<ms::Gid> seen;
  for (auto const &element : iterable) seen.push_back(element.Gid());
  acc->Abort();
  EXPECT_EQ(seen, (std::vector{kept}));
}

// T5 per site: an abort removes the entries of every command of the batch.
TYPED_TEST(EachInsertSite, AnAbortRemovesTheEntriesOfEveryCommand) {
  auto const [kept, first, second] = this->Prepare();
  auto const before = this->Count();
  {
    auto acc = this->Armed();
    TypeParam::Write(*acc, this->ids(), first);
    acc->AdvanceCommand();
    TypeParam::Write(*acc, this->ids(), second);
    acc->Abort();
  }
  EXPECT_EQ(this->Count(), before);
}

// T5 across a commit: the batch after a commit writes its own start, which the abort removes.
TYPED_TEST(EachInsertSite, AnAbortAfterABatchCommitRemovesTheNewBatchsEntries) {
  auto const [kept, first, second] = this->Prepare();
  auto const before = this->Count();
  {
    auto acc = this->Armed();
    acc->AdvanceCommand();
    ASSERT_TRUE(acc->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
    TypeParam::Write(*acc, this->ids(), first);
    acc->Abort();
  }
  EXPECT_EQ(this->Count(), before);
}

}  // namespace
