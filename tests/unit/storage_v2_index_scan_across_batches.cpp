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

// A View::OLD scan at command 0 writes, in its own batch, an element it already returned. The new entry carries the
// batch's start timestamp; after the batch commits, the scan must not return that element a second time.
TEST_P(IndexScanAcrossBatches, OwnBatchWriteOfAReturnedVertex) {
  if (GetParam() != ms::View::OLD) GTEST_SKIP() << "a View::NEW scan sees its own write";
  {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(label, {ms::PropertyPath{property}}).has_value());
    Commit(acc);
  }
  std::vector<ms::Gid> gids;
  {
    auto acc = storage->Access(ms::WRITE);
    for (auto value : {1, 2}) {
      auto vertex = acc->CreateVertex();
      ASSERT_TRUE(vertex.AddLabel(label).has_value());
      ASSERT_TRUE(vertex.SetProperty(property, ms::PropertyValue{value}).has_value());
      gids.push_back(vertex.Gid());
    }
    Commit(acc);
  }
  auto acc = storage->Access(ms::WRITE);
  auto iterable = acc->Vertices(
      label, std::array{ms::PropertyPath{property}}, std::array{ms::PropertyValueRange::IsNotNull()}, ms::View::OLD);
  std::vector<ms::Gid> seen;
  for (auto it = iterable.begin(); it != iterable.end(); ++it) {
    auto vertex = *it;
    seen.push_back(vertex.Gid());
    auto const old_value = vertex.GetProperty(property, ms::View::NEW)->ValueInt();
    ASSERT_TRUE(vertex.SetProperty(property, ms::PropertyValue{old_value + 10}).has_value());
    ASSERT_TRUE(acc->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  acc->Abort();
  EXPECT_EQ(seen, gids);
}

TEST_P(IndexScanAcrossBatches, OwnBatchWriteOfAReturnedEdge) {
  if (GetParam() != ms::View::OLD) GTEST_SKIP() << "a View::NEW scan sees its own write";
  {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(edge_type, property).has_value());
    Commit(acc);
  }
  std::vector<ms::Gid> gids;
  {
    auto acc = storage->Access(ms::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    for (auto value : {1, 2}) {
      auto edge = acc->CreateEdge(&from, &to, edge_type);
      ASSERT_TRUE(edge.has_value());
      ASSERT_TRUE(edge->SetProperty(property, ms::PropertyValue{value}).has_value());
      gids.push_back(edge->Gid());
    }
    Commit(acc);
  }
  auto acc = storage->Access(ms::WRITE);
  auto iterable = acc->Edges(edge_type, property, ms::View::OLD);
  std::vector<ms::Gid> seen;
  for (auto it = iterable.begin(); it != iterable.end(); ++it) {
    auto edge = *it;
    seen.push_back(edge.Gid());
    auto const old_value = edge.GetProperty(property, ms::View::NEW)->ValueInt();
    ASSERT_TRUE(edge.SetProperty(property, ms::PropertyValue{old_value + 10}).has_value());
    ASSERT_TRUE(acc->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  acc->Abort();
  EXPECT_EQ(seen, gids);
}

INSTANTIATE_TEST_SUITE_P(Views, IndexScanAcrossBatches, ::testing::Values(ms::View::OLD, ms::View::NEW));

}  // namespace
