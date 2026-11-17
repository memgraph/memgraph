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
  auto acc = storage->Access(ms::WRITE);
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

TEST_F(OwnBatchWrite, EdgeTypePropertyIndex) {
  Index([&](auto &acc) { return acc.CreateIndex(edge_type, property).has_value(); });
  auto const gids = Edges();
  auto acc = storage->Access(ms::WRITE);
  EXPECT_EQ(BumpEach(*acc, acc->Edges(edge_type, property, ms::View::OLD)), gids);
  acc->Abort();
}

TEST_F(OwnBatchWrite, VertexPropertyIndex) {
  Index([&](auto &acc) { return acc.CreateGlobalVertexIndex(property).has_value(); });
  auto const gids = Vertices();
  auto acc = storage->Access(ms::WRITE);
  EXPECT_EQ(BumpEach(*acc, acc->Vertices(property, ms::View::OLD)), gids);
  acc->Abort();
}

TEST_F(OwnBatchWrite, EdgePropertyIndex) {
  Index([&](auto &acc) { return acc.CreateGlobalEdgeIndex(property).has_value(); });
  auto const gids = Edges();
  auto acc = storage->Access(ms::WRITE);
  EXPECT_EQ(BumpEach(*acc, acc->Edges(property, ms::View::OLD)), gids);
  acc->Abort();
}

TEST_F(OwnBatchWrite, ChunkedLabelPropertyIndex) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  auto acc = storage->Access(ms::WRITE);
  auto chunks = acc->ChunkedVertices(
      label, std::array{ms::PropertyPath{property}}, std::array{ms::PropertyValueRange::IsNotNull()}, ms::View::OLD, 1);
  ASSERT_EQ(chunks.size(), 1);
  EXPECT_EQ(BumpEach(*acc, chunks.get_chunk(0)), gids);
  acc->Abort();
}

TEST_F(OwnBatchWrite, ChunkedEdgeTypePropertyIndex) {
  Index([&](auto &acc) { return acc.CreateIndex(edge_type, property).has_value(); });
  auto const gids = Edges();
  auto acc = storage->Access(ms::WRITE);
  auto chunks = acc->ChunkedEdges(edge_type, property, ms::PropertyValueRange::IsNotNull(), ms::View::OLD, 1);
  ASSERT_EQ(chunks.size(), 1);
  EXPECT_EQ(BumpEach(*acc, chunks.get_chunk(0)), gids);
  acc->Abort();
}

// A later command is not a write: the batch's entries are still all newer than the scan.
TEST_F(OwnBatchWrite, LaterCommandWithoutAnEarlierWrite) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  auto acc = storage->Access(ms::WRITE);
  acc->AdvanceCommand();
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

TEST_F(OwnBatchWrite, AnalyticalMode) {
  Open(ms::StorageMode::IN_MEMORY_ANALYTICAL);
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  auto acc = storage->Access(ms::WRITE);
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

// A write and an advance in an earlier batch say nothing about the scan's batch.
TEST_F(OwnBatchWrite, AnEarlierBatchsWriteDoesNotCount) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  auto acc = storage->Access(ms::WRITE);
  WriteOther(*acc);
  acc->AdvanceCommand();
  ASSERT_TRUE(acc->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

// An element the batch created before the scan opened is the scan's to see, after a commit too. Analytical mode
// keeps no deltas, so there every advance counts as an earlier write.
TEST_F(OwnBatchWrite, AnElementTheBatchCreatedBeforeTheScanStaysVisible) {
  for (auto mode : {ms::StorageMode::IN_MEMORY_TRANSACTIONAL, ms::StorageMode::IN_MEMORY_ANALYTICAL}) {
    SCOPED_TRACE(static_cast<int>(mode));
    Open(mode);
    Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
    auto gids = Vertices();
    auto acc = storage->Access(ms::WRITE);
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

// Known residual of the per-scan flag: once the batch wrote before the scan opened, the scan cannot tell its own
// later entries from earlier ones (a bookkeeping write before a batched update). A per-entry stamp fixes it.
TEST_F(OwnBatchWrite, DISABLED_AWriteBeforeTheScanOpenedStillHidesItsLaterEntries) {
  Index([&](auto &acc) { return acc.CreateIndex(label, {ms::PropertyPath{property}}).has_value(); });
  auto const gids = Vertices();
  auto acc = storage->Access(ms::WRITE);
  WriteOther(*acc);
  acc->AdvanceCommand();
  EXPECT_EQ(BumpEach(*acc, LabelPropertyScan(*acc)), gids);
  acc->Abort();
}

}  // namespace
