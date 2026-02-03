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

#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace ms = memgraph::storage;
namespace r = ranges;
namespace rv = ranges::views;
using ::testing::UnorderedElementsAreArray;

namespace {

// Unordered comparison of the actual edges with the expected edges
void CompareEdges(auto const &actual_edges, std::span<ms::EdgeTypeId const> expected_edges) {
  ASSERT_EQ(actual_edges.edges.size(), expected_edges.size());
  auto const actual_edge_types =
      actual_edges.edges | rv::transform([](auto &&actual_edge) { return actual_edge.EdgeType(); }) | r::to_vector;
  EXPECT_THAT(actual_edge_types, UnorderedElementsAreArray(expected_edges));
}

}  // unnamed namespace

TEST(StorageV2NonSequentialIsolation, IsolationWorksWithSingleEdgeEdgeWriteConflict) {
  std::unique_ptr<ms::Storage> storage(std::make_unique<ms::InMemoryStorage>(ms::Config{}));

  ms::Gid v1_gid, v2_gid;

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto tx1 = storage->Access(memgraph::storage::WRITE);
  auto v1_1 = tx1->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_1.has_value());
  {
    // tx1 finds no edges on v1, as there aren't any edges yet
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  auto tx2 = storage->Access(memgraph::storage::WRITE);
  auto v1_2 = tx2->FindVertex(v1_gid, ms::View::OLD);
  auto v2_2 = tx2->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_2.has_value() && v2_2.has_value());
  ASSERT_TRUE(tx2->CreateEdge(&*v1_2, &*v2_2, tx2->NameToEdgeType("Edge2")).has_value());

  {
    // Uncommited tx2 can see the Edge2 it created
    auto edges = v1_2->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx2->NameToEdgeType("Edge2")});
  }

  auto tx3 = storage->Access(memgraph::storage::WRITE);
  auto v1_3 = tx3->FindVertex(v1_gid, ms::View::OLD);
  auto v2_3 = tx3->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_3.has_value() && v2_3.has_value());
  ASSERT_TRUE(tx3->CreateEdge(&*v1_3, &*v2_3, tx3->NameToEdgeType("Edge3")).has_value());

  {
    // Uncommited tx3 can see the Edge3 it created, but not the uncommited Edge2
    // created by tx2
    auto edges = v1_3->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx3->NameToEdgeType("Edge3")});
  }

  auto tx4 = storage->Access(memgraph::storage::WRITE);
  auto v1_4 = tx4->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_4.has_value());
  {
    // tx4 started before the transactions above have committed and cannot see
    // the uncommitted edges
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  // tx2 now commits.
  ASSERT_TRUE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  tx2.reset();

  {
    // tx1 still sees no edges
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx3 still only sees its edge, despite tx2 having committed
    auto edges = v1_3->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx3->NameToEdgeType("Edge3")});
  }

  {
    // tx4 still sees no edges
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  auto tx5 = storage->Access(memgraph::storage::WRITE);
  auto v1_5 = tx5->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_5.has_value());
  {
    // tx5 started after tx2 committed. This means it can see tx2's committed
    // edge, but not tx3's uncommitted edge.
    auto edges = v1_5->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx5->NameToEdgeType("Edge2")});
  }

  // tx3 now commits
  ASSERT_TRUE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  tx3.reset();

  {
    // tx1 still sees no edges
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx4 still sees no edges
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx5 still sees the edge committed by tx2
    auto edges = v1_5->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx5->NameToEdgeType("Edge2")});
  }

  {
    // A transaction started after both write transactions have committed
    // can see both edges.
    auto tx6 = storage->Access(memgraph::storage::WRITE);
    auto v1_6 = tx6->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1_6.has_value());
    auto edges = v1_6->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx6->NameToEdgeType("Edge2"), tx6->NameToEdgeType("Edge3")});
  }
}

TEST(StorageV2NonSequentialIsolation, IsolationWorksWithMultipleEdgeEdgeWriteConflicts) {
  std::unique_ptr<ms::Storage> storage(std::make_unique<ms::InMemoryStorage>(ms::Config{}));

  ms::Gid v1_gid, v2_gid;

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto tx0 = storage->Access(memgraph::storage::WRITE);
  auto v1_0 = tx0->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_0.has_value());

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    ASSERT_TRUE(acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType("Edge1")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  auto tx1 = storage->Access(memgraph::storage::WRITE);
  auto v1_1 = tx1->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_1.has_value());
  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  auto tx2 = storage->Access(memgraph::storage::WRITE);
  auto v1_2 = tx2->FindVertex(v1_gid, ms::View::OLD);
  auto v2_2 = tx2->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_2.has_value() && v2_2.has_value());
  ASSERT_TRUE(tx2->CreateEdge(&*v1_2, &*v2_2, tx2->NameToEdgeType("Edge2a")).has_value());

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // Uncommited tx2 can see the committed Edge1 and Edge2a it created
    auto edges = v1_2->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx2->NameToEdgeType("Edge1"), tx2->NameToEdgeType("Edge2a")});
  }

  auto tx3 = storage->Access(memgraph::storage::WRITE);
  auto v1_3 = tx3->FindVertex(v1_gid, ms::View::OLD);
  auto v2_3 = tx3->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_3.has_value() && v2_3.has_value());
  ASSERT_TRUE(tx3->CreateEdge(&*v1_3, &*v2_3, tx3->NameToEdgeType("Edge3a")).has_value());

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // Uncommited tx2 can see the committed Edge1 and Edge2a it created
    auto edges = v1_2->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx2->NameToEdgeType("Edge1"), tx2->NameToEdgeType("Edge2a")});
  }

  {
    // Uncommited tx3 can see the committed Edge1, and Edge3a it created,
    // but not the uncommited Edge2a created by tx2
    auto edges = v1_3->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx3->NameToEdgeType("Edge1"), tx3->NameToEdgeType("Edge3a")});
  }

  // Create another two edges, giving us a non-sequential delta chain of:
  // (v1)-[tx3*]-[tx2*]-[tx3*]-[tx2*]-[tx1]
  // where * indicates an uncommited delta.
  ASSERT_TRUE(tx2->CreateEdge(&*v1_2, &*v2_2, tx2->NameToEdgeType("Edge2b")).has_value());
  ASSERT_TRUE(tx3->CreateEdge(&*v1_3, &*v2_3, tx3->NameToEdgeType("Edge3b")).has_value());

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // Uncommited tx2 can see Edge1, it's own Edge2a and Edge2b, but not the
    // uncommitted edges created by tx3
    auto edges = v1_2->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(
        edges.value(),
        std::array{tx2->NameToEdgeType("Edge1"), tx2->NameToEdgeType("Edge2a"), tx2->NameToEdgeType("Edge2b")});
  }

  {
    // Uncommited tx3 can see Edge1, it's own Edge3a and Edge3b, but not the
    // uncommitted edges created by tx2
    auto edges = v1_3->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(
        edges.value(),
        std::array{tx3->NameToEdgeType("Edge1"), tx3->NameToEdgeType("Edge3a"), tx3->NameToEdgeType("Edge3b")});
  }

  auto tx4 = storage->Access(memgraph::storage::WRITE);
  auto v1_4 = tx4->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_4.has_value());
  {
    // tx4 started before the transactions above have committed and cannot see
    // the uncommitted edges, but can still see the committed Edge1
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx4->NameToEdgeType("Edge1")});
  }

  // tx2 now commits.
  ASSERT_TRUE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  tx2.reset();

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 still sees no Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // tx3 still only sees the committed Edge1 and its own Edge3s, despite tx2
    // having committed
    auto edges = v1_3->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 3);
    CompareEdges(
        edges.value(),
        std::array{tx3->NameToEdgeType("Edge1"), tx3->NameToEdgeType("Edge3a"), tx3->NameToEdgeType("Edge3b")});
  }

  {
    // tx4 still sees Edge1
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx4->NameToEdgeType("Edge1")});
  }

  auto tx5 = storage->Access(memgraph::storage::WRITE);
  auto v1_5 = tx5->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_5.has_value());
  {
    // tx5 started after tx2 committed. This means it can see tx2's committed
    // edge, but not tx3's uncommitted edge.
    auto edges = v1_5->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(
        edges.value(),
        std::array{tx5->NameToEdgeType("Edge1"), tx5->NameToEdgeType("Edge2a"), tx5->NameToEdgeType("Edge2b")});
  }

  // tx3 now commits
  ASSERT_TRUE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  tx3.reset();

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 still sees Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // tx4 still sees Edge1
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx4->NameToEdgeType("Edge1")});
  }

  {
    // tx5 started after tx2 committed. This means it can see tx2's committed
    // edge, but not tx3's uncommitted edge.
    auto edges = v1_5->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(
        edges.value(),
        std::array{tx5->NameToEdgeType("Edge1"), tx5->NameToEdgeType("Edge2a"), tx5->NameToEdgeType("Edge2b")});
  }

  {
    // A transaction started after both write transactions have committed
    // can see all the edges.
    auto tx6 = storage->Access(memgraph::storage::WRITE);
    auto v1_6 = tx6->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1_6.has_value());
    auto edges = v1_6->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(),
                 std::array{tx6->NameToEdgeType("Edge1"),
                            tx6->NameToEdgeType("Edge2a"),
                            tx6->NameToEdgeType("Edge2b"),
                            tx6->NameToEdgeType("Edge3a"),
                            tx6->NameToEdgeType("Edge3b")});
  }
}

TEST(StorageV2NonSequentialIsolation, IsolationWorksWithAbortedNonSequentialTransactions) {
  std::unique_ptr<ms::Storage> storage(std::make_unique<ms::InMemoryStorage>(ms::Config{}));

  ms::Gid v1_gid, v2_gid;

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto tx0 = storage->Access(memgraph::storage::WRITE);
  auto v1_0 = tx0->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_0.has_value());

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    ASSERT_TRUE(acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType("Edge1")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  auto tx1 = storage->Access(memgraph::storage::WRITE);
  auto v1_1 = tx1->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_1.has_value());
  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  auto tx2 = storage->Access(memgraph::storage::WRITE);
  auto v1_2 = tx2->FindVertex(v1_gid, ms::View::OLD);
  auto v2_2 = tx2->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_2.has_value() && v2_2.has_value());

  auto tx3 = storage->Access(memgraph::storage::WRITE);
  auto v1_3 = tx3->FindVertex(v1_gid, ms::View::OLD);
  auto v2_3 = tx3->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_3.has_value() && v2_3.has_value());

  ASSERT_TRUE(tx2->CreateEdge(&*v1_2, &*v2_2, tx2->NameToEdgeType("Edge2a")).has_value());
  ASSERT_TRUE(tx3->CreateEdge(&*v1_3, &*v2_3, tx3->NameToEdgeType("Edge3a")).has_value());
  ASSERT_TRUE(tx2->CreateEdge(&*v1_2, &*v2_2, tx2->NameToEdgeType("Edge2b")).has_value());
  ASSERT_TRUE(tx3->CreateEdge(&*v1_3, &*v2_3, tx3->NameToEdgeType("Edge3b")).has_value());

  auto tx4 = storage->Access(memgraph::storage::WRITE);
  auto v1_4 = tx4->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_4.has_value());
  {
    // tx4 can only see Edge1, the only edge committed before it started
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx4->NameToEdgeType("Edge1")});
  }

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  tx3->Abort();
  tx3.reset();

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // tx2 can see the committed Edge1 and Edge2a and Edge2b it created
    auto edges = v1_2->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(
        edges.value(),
        std::array{tx2->NameToEdgeType("Edge1"), tx2->NameToEdgeType("Edge2a"), tx2->NameToEdgeType("Edge2b")});
  }

  {
    // tx4 should still only see Edge1
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx4->NameToEdgeType("Edge1")});
  }

  tx2->Abort();
  tx2.reset();

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // tx4 should still only see Edge1
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx4->NameToEdgeType("Edge1")});
  }

  {
    // New transaction only sees Edge1, as every other edge creation was
    // aborted
    auto tx6 = storage->Access(memgraph::storage::WRITE);
    auto v1_6 = tx6->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1_6.has_value());
    auto edges = v1_6->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    CompareEdges(edges.value(), std::array{tx6->NameToEdgeType("Edge1")});
  }
}

TEST(StorageV2NonSequentialIsolation, AbortedEdgesNotVisibleDuringAbort) {
  std::unique_ptr<ms::Storage> storage(std::make_unique<ms::InMemoryStorage>(ms::Config{}));

  ms::Gid v1_gid, v2_gid;

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    constexpr int kNumEdges = 500;
    constexpr int kNumIterations = 10;
    std::atomic<bool> test_finished{false};

    auto create_edges_then_abort = [&](int writer_id) {
      for (int iteration = 0; iteration < kNumIterations; ++iteration) {
        auto acc = storage->Access(memgraph::storage::WRITE);
        auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
        auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
        ASSERT_TRUE(v1.has_value());
        ASSERT_TRUE(v2.has_value());

        for (int i = 0; i < kNumEdges; ++i) {
          auto const edge_name = std::format("E{}_{}", writer_id, i);
          auto edge = acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType(edge_name));
          ASSERT_TRUE(edge.has_value());
        }

        acc->Abort();
        acc.reset();

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    };

    std::jthread writer1([&]() {
      create_edges_then_abort(1);
      test_finished.store(true, std::memory_order_release);
    });

    std::jthread writer2([&]() { create_edges_then_abort(2); });
    std::jthread writer3([&]() { create_edges_then_abort(3); });
    std::jthread writer4([&]() { create_edges_then_abort(4); });

    std::jthread reader([&]() {
      while (!test_finished.load(std::memory_order_acquire)) {
        auto acc = storage->Access(memgraph::storage::WRITE);
        auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
        ASSERT_TRUE(v1.has_value());
        auto edges = v1->OutEdges(ms::View::OLD);
        if (edges.has_value()) {
          int edge_count = edges->edges.size();
          ASSERT_EQ(edge_count, 0);
        }
      }
    });
  }

  // New transactions should still see no edges
  {
    auto tx_final = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx_final->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    auto edges = v1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    ASSERT_EQ(edges->edges.size(), 0);
  }
}

TEST(StorageV2NonSequentialIsolation, ConcurrentEdgeCreationSucceeds) {
  std::unique_ptr<ms::Storage> storage(std::make_unique<ms::InMemoryStorage>(ms::Config{}));

  ms::Gid v1_gid, v2_gid;

  // Setup: Create two vertices
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Tx_1: Create an edge but don't commit
  auto tx1 = storage->Access(memgraph::storage::WRITE);
  auto v1_tx1 = tx1->FindVertex(v1_gid, ms::View::OLD);
  auto v2_tx1 = tx1->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_tx1.has_value());
  ASSERT_TRUE(v2_tx1.has_value());

  auto edge_type_1 = tx1->NameToEdgeType("TYPE1");
  auto edge1 = tx1->CreateEdge(&*v1_tx1, &*v2_tx1, edge_type_1);
  ASSERT_TRUE(edge1.has_value());

  // Tx_2: Try to create another edge while Tx_1's edge is uncommitted
  // This should SUCCEED because both operations are commutative
  auto tx2 = storage->Access(memgraph::storage::WRITE);
  auto v1_tx2 = tx2->FindVertex(v1_gid, ms::View::OLD);
  auto v2_tx2 = tx2->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_tx2.has_value());
  ASSERT_TRUE(v2_tx2.has_value());

  auto edge_type_2 = tx2->NameToEdgeType("TYPE2");
  auto edge2 = tx2->CreateEdge(&*v1_tx2, &*v2_tx2, edge_type_2);
  ASSERT_TRUE(edge2.has_value());

  ASSERT_TRUE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  ASSERT_TRUE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

TEST(StorageV2NonSequentialIsolation, EdgeCreationFailsAfterLabelChange) {
  std::unique_ptr<ms::Storage> storage(std::make_unique<ms::InMemoryStorage>(ms::Config{}));

  ms::Gid v1_gid, v2_gid;

  // Setup: Create two vertices
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Tx_1: Add label, then create an edge
  auto tx1 = storage->Access(memgraph::storage::WRITE);
  auto v1_tx1 = tx1->FindVertex(v1_gid, ms::View::OLD);
  auto v2_tx1 = tx1->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_tx1.has_value());
  ASSERT_TRUE(v2_tx1.has_value());

  auto label = tx1->NameToLabel("LABEL1");
  ASSERT_TRUE(v1_tx1->AddLabel(label).has_value());

  auto edge_type_1 = tx1->NameToEdgeType("TYPE1");
  auto edge1 = tx1->CreateEdge(&*v1_tx1, &*v2_tx1, edge_type_1);
  ASSERT_TRUE(edge1.has_value());

  // Tx_2: Try to create another edge while Tx_1 has uncommitted label change + edge
  // This is SERIALIZATION_ERROR because Tx_1 has uncommited non-commutative operations
  auto tx2 = storage->Access(memgraph::storage::WRITE);
  auto v1_tx2 = tx2->FindVertex(v1_gid, ms::View::OLD);
  auto v2_tx2 = tx2->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_tx2.has_value());
  ASSERT_TRUE(v2_tx2.has_value());

  auto edge_type_2 = tx2->NameToEdgeType("TYPE2");
  auto edge2 = tx2->CreateEdge(&*v1_tx2, &*v2_tx2, edge_type_2);
  ASSERT_FALSE(edge2.has_value());
  ASSERT_EQ(edge2.error(), ms::Error::SERIALIZATION_ERROR);

  tx1->Abort();
  tx2->Abort();
}

TEST(StorageV2NonSequentialIsolation, BlockingOperationRejectedWithCommittedHeadAndUncommittedDownstream) {
  std::unique_ptr<ms::Storage> storage(std::make_unique<ms::InMemoryStorage>(ms::Config{}));

  ms::Gid v1_gid, v2_gid;

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto tx1 = storage->Access(memgraph::storage::WRITE);
  auto v1_tx1 = tx1->FindVertex(v1_gid, ms::View::OLD);
  auto v2_tx1 = tx1->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_tx1.has_value() && v2_tx1.has_value());
  auto edge1 = tx1->CreateEdge(&*v1_tx1, &*v2_tx1, tx1->NameToEdgeType("TYPE1"));
  ASSERT_TRUE(edge1.has_value());

  auto tx2 = storage->Access(memgraph::storage::WRITE);
  auto v1_tx2 = tx2->FindVertex(v1_gid, ms::View::OLD);
  auto v2_tx2 = tx2->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_tx2.has_value() && v2_tx2.has_value());
  auto edge2 = tx2->CreateEdge(&*v1_tx2, &*v2_tx2, tx2->NameToEdgeType("TYPE2"));
  ASSERT_TRUE(edge2.has_value());

  ASSERT_TRUE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  tx1.reset();

  auto tx3 = storage->Access(memgraph::storage::WRITE);
  auto v1_tx3 = tx3->FindVertex(v1_gid, ms::View::OLD);
  auto v2_tx3 = tx3->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_tx3.has_value() && v2_tx3.has_value());
  auto edge3 = tx3->CreateEdge(&*v1_tx3, &*v2_tx3, tx3->NameToEdgeType("TYPE3"));
  ASSERT_TRUE(edge3.has_value());

  ASSERT_TRUE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  tx3.reset();

  auto tx4 = storage->Access(memgraph::storage::WRITE);
  auto v1_tx4 = tx4->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_tx4.has_value());

  // This should fail with a serialization error. Although the head delta is
  // committed, we have uncommitted deltas downstream and so cannot prepend a
  // sequential delta.
  auto label = tx4->NameToLabel("Label1");
  auto res = v1_tx4->AddLabel(label);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), ms::Error::SERIALIZATION_ERROR);

  tx2->Abort();
  tx4->Abort();
}
