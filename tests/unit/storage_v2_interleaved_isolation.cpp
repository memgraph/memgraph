// Copyright 2025 Memgraph Ltd.
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
using ::testing::Contains;

namespace {

// Unordered comparison of the actual edges with the expected edges
void CompareEdges(auto const &actual_edges, std::span<ms::EdgeTypeId const> expected_edges) {
  ASSERT_EQ(actual_edges.edges.size(), expected_edges.size());
  auto actual_edge_types =
      actual_edges.edges | rv::transform([](auto &&actual_edge) { return actual_edge.EdgeType(); }) | r::to_vector;
  for (auto &&expected_edge : expected_edges) {
    EXPECT_TRUE(ranges::contains(actual_edge_types, expected_edge)) << expected_edge.AsUint();
  }
}

}  // unnamed namespace

TEST(StorageV2InterleavedIsolation, IsolationWorksWithSingleEdgeEdgeWriteConflict) {
  std::unique_ptr<ms::Storage> storage(std::make_unique<ms::InMemoryStorage>(ms::Config{}));

  ms::Gid v1_gid, v2_gid;

  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto tx1 = storage->Access();
  auto v1_1 = tx1->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_1.has_value());
  {
    // tx1 finds no edges on v1, as there aren't any edges yet
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  auto tx2 = storage->Access();
  auto v1_2 = tx2->FindVertex(v1_gid, ms::View::OLD);
  auto v2_2 = tx2->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_2.has_value() && v2_2.has_value());
  ASSERT_TRUE(tx2->CreateEdge(&*v1_2, &*v2_2, tx2->NameToEdgeType("Edge2")).HasValue());

  {
    // Uncommited tx2 can see the Edge2 it created
    auto edges = v1_2->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx2->NameToEdgeType("Edge2")});
  }

  auto tx3 = storage->Access();
  auto v1_3 = tx3->FindVertex(v1_gid, ms::View::OLD);
  auto v2_3 = tx3->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_3.has_value() && v2_3.has_value());
  ASSERT_TRUE(tx3->CreateEdge(&*v1_3, &*v2_3, tx3->NameToEdgeType("Edge3")).HasValue());

  {
    // Uncommited tx3 can see the Edge3 it created, but not the uncommited Edge2
    // created by tx2
    auto edges = v1_3->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx3->NameToEdgeType("Edge3")});
  }

  auto tx4 = storage->Access();
  auto v1_4 = tx4->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_4.has_value());
  {
    // tx4 started before the transactions above have committed and cannot see
    // the uncommitted edges
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  // tx2 now commits.
  ASSERT_FALSE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  tx2.reset();

  {
    // tx1 still sees no edges
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx3 still only sees its edge, despite tx2 having committed
    auto edges = v1_3->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx3->NameToEdgeType("Edge3")});
  }

  {
    // tx4 still sees no edges
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  auto tx5 = storage->Access();
  auto v1_5 = tx5->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_5.has_value());
  {
    // tx5 started after tx2 committed. This means it can see tx2's committed
    // edge, but not tx3's uncommitted edge.
    auto edges = v1_5->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx5->NameToEdgeType("Edge2")});
  }

  // tx3 now commits
  ASSERT_FALSE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  tx3.reset();

  {
    // tx1 still sees no edges
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx4 still sees no edges
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx5 still sees the edge committed by tx2
    auto edges = v1_5->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx5->NameToEdgeType("Edge2")});
  }

  {
    // A transaction started after both write transactions have committed
    // can see both edges.
    auto tx6 = storage->Access();
    auto v1_6 = tx6->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1_6.has_value());
    auto edges = v1_6->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx6->NameToEdgeType("Edge2"), tx6->NameToEdgeType("Edge3")});
  }
}

TEST(StorageV2InterleavedIsolation, IsolationWorksWithMultipleEdgeEdgeWriteConflicts) {
  std::unique_ptr<ms::Storage> storage(std::make_unique<ms::InMemoryStorage>(ms::Config{}));

  ms::Gid v1_gid, v2_gid;

  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto tx0 = storage->Access();
  auto v1_0 = tx0->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_0.has_value());

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    auto acc = storage->Access();
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    ASSERT_TRUE(acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType("Edge1")).HasValue());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  auto tx1 = storage->Access();
  auto v1_1 = tx1->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_1.has_value());
  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  auto tx2 = storage->Access();
  auto v1_2 = tx2->FindVertex(v1_gid, ms::View::OLD);
  auto v2_2 = tx2->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_2.has_value() && v2_2.has_value());
  ASSERT_TRUE(tx2->CreateEdge(&*v1_2, &*v2_2, tx2->NameToEdgeType("Edge2a")).HasValue());

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // Uncommited tx2 can see the committed Edge1 and Edge2a it created
    auto edges = v1_2->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx2->NameToEdgeType("Edge1"), tx2->NameToEdgeType("Edge2a")});
  }

  auto tx3 = storage->Access();
  auto v1_3 = tx3->FindVertex(v1_gid, ms::View::OLD);
  auto v2_3 = tx3->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_3.has_value() && v2_3.has_value());
  ASSERT_TRUE(tx3->CreateEdge(&*v1_3, &*v2_3, tx3->NameToEdgeType("Edge3a")).HasValue());

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // Uncommited tx2 can see the committed Edge1 and Edge2a it created
    auto edges = v1_2->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx2->NameToEdgeType("Edge1"), tx2->NameToEdgeType("Edge2a")});
  }

  {
    // Uncommited tx3 can see the committed Edge1, and Edge3a it created,
    // but not the uncommited Edge2a created by tx2
    auto edges = v1_3->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx3->NameToEdgeType("Edge1"), tx3->NameToEdgeType("Edge3a")});
  }

  // Create another two edges, giving us an interleaved delta chain of:
  // (v1)-[tx3*]-[tx2*]-[tx3*]-[tx2*]-[tx1]
  // where * indicates an uncommited delta.
  ASSERT_TRUE(tx2->CreateEdge(&*v1_2, &*v2_2, tx2->NameToEdgeType("Edge2b")).HasValue());
  ASSERT_TRUE(tx3->CreateEdge(&*v1_3, &*v2_3, tx3->NameToEdgeType("Edge3b")).HasValue());

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // Uncommited tx2 can see Edge1, it's own Edge2a and Edge2b, but not the
    // uncommitted edges created by tx3
    auto edges = v1_2->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx2->NameToEdgeType("Edge1"), tx2->NameToEdgeType("Edge2a"),
                                              tx2->NameToEdgeType("Edge2b")});
  }

  {
    // Uncommited tx3 can see Edge1, it's own Edge3a and Edge3b, but not the
    // uncommitted edges created by tx2
    auto edges = v1_3->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx3->NameToEdgeType("Edge1"), tx3->NameToEdgeType("Edge3a"),
                                              tx3->NameToEdgeType("Edge3b")});
  }

  auto tx4 = storage->Access();
  auto v1_4 = tx4->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_4.has_value());
  {
    // tx4 started before the transactions above have committed and cannot see
    // the uncommitted edges, but can still see the committed Edge1
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx4->NameToEdgeType("Edge1")});
  }

  // tx2 now commits.
  ASSERT_FALSE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  tx2.reset();

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 still sees no Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // tx3 still only sees the committed Edge1 and its own Edge3s, despite tx2
    // having committed
    auto edges = v1_3->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 3);
    CompareEdges(edges.GetValue(), std::array{tx3->NameToEdgeType("Edge1"), tx3->NameToEdgeType("Edge3a"),
                                              tx3->NameToEdgeType("Edge3b")});
  }

  {
    // tx4 still sees Edge1
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx4->NameToEdgeType("Edge1")});
  }

  auto tx5 = storage->Access();
  auto v1_5 = tx5->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_5.has_value());
  {
    // tx5 started after tx2 committed. This means it can see tx2's committed
    // edge, but not tx3's uncommitted edge.
    auto edges = v1_5->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx5->NameToEdgeType("Edge1"), tx5->NameToEdgeType("Edge2a"),
                                              tx5->NameToEdgeType("Edge2b")});
  }

  // tx3 now commits
  ASSERT_FALSE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  tx3.reset();

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 still sees Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // tx4 still sees Edge1
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx4->NameToEdgeType("Edge1")});
  }

  {
    // tx5 started after tx2 committed. This means it can see tx2's committed
    // edge, but not tx3's uncommitted edge.
    auto edges = v1_5->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx5->NameToEdgeType("Edge1"), tx5->NameToEdgeType("Edge2a"),
                                              tx5->NameToEdgeType("Edge2b")});
  }

  {
    // A transaction started after both write transactions have committed
    // can see all the edges.
    auto tx6 = storage->Access();
    auto v1_6 = tx6->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1_6.has_value());
    auto edges = v1_6->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(),
                 std::array{tx6->NameToEdgeType("Edge1"), tx6->NameToEdgeType("Edge2a"), tx6->NameToEdgeType("Edge2b"),
                            tx6->NameToEdgeType("Edge3a"), tx6->NameToEdgeType("Edge3b")});
  }
}

TEST(StorageV2InterleavedIsolation, IsolationWorksWithAbortedInterleavedTransactions) {
  std::unique_ptr<ms::Storage> storage(std::make_unique<ms::InMemoryStorage>(ms::Config{}));

  ms::Gid v1_gid, v2_gid;

  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto tx0 = storage->Access();
  auto v1_0 = tx0->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_0.has_value());

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    auto acc = storage->Access();
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    ASSERT_TRUE(acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType("Edge1")).HasValue());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  auto tx1 = storage->Access();
  auto v1_1 = tx1->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_1.has_value());
  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  auto tx2 = storage->Access();
  auto v1_2 = tx2->FindVertex(v1_gid, ms::View::OLD);
  auto v2_2 = tx2->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_2.has_value() && v2_2.has_value());

  auto tx3 = storage->Access();
  auto v1_3 = tx3->FindVertex(v1_gid, ms::View::OLD);
  auto v2_3 = tx3->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1_3.has_value() && v2_3.has_value());

  ASSERT_TRUE(tx2->CreateEdge(&*v1_2, &*v2_2, tx2->NameToEdgeType("Edge2a")).HasValue());
  ASSERT_TRUE(tx3->CreateEdge(&*v1_3, &*v2_3, tx3->NameToEdgeType("Edge3a")).HasValue());
  ASSERT_TRUE(tx2->CreateEdge(&*v1_2, &*v2_2, tx2->NameToEdgeType("Edge2b")).HasValue());
  ASSERT_TRUE(tx3->CreateEdge(&*v1_3, &*v2_3, tx3->NameToEdgeType("Edge3b")).HasValue());

  auto tx4 = storage->Access();
  auto v1_4 = tx4->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1_4.has_value());
  {
    // tx4 can only see Edge1, the only edge committed before it started
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx4->NameToEdgeType("Edge1")});
  }

  tx3->Abort();
  tx3.reset();

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // tx2 can see the committed Edge1 and Edge2a and Edge2b it created
    auto edges = v1_2->OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx2->NameToEdgeType("Edge1"), tx2->NameToEdgeType("Edge2a"),
                                              tx2->NameToEdgeType("Edge2b")});
  }

  {
    // tx4 should still only see Edge1
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx4->NameToEdgeType("Edge1")});
  }

  tx2->Abort();
  tx2.reset();

  {
    // tx0 sees no edges
    auto edges = v1_0->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx1 finds committed Edge1
    auto edges = v1_1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx1->NameToEdgeType("Edge1")});
  }

  {
    // tx4 should still only see Edge1
    auto edges = v1_4->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx4->NameToEdgeType("Edge1")});
  }

  {
    // New transaction only sees Edge1, as every other edge creation was
    // aborted
    auto tx6 = storage->Access();
    auto v1_6 = tx6->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1_6.has_value());
    auto edges = v1_6->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    CompareEdges(edges.GetValue(), std::array{tx6->NameToEdgeType("Edge1")});
  }
}
