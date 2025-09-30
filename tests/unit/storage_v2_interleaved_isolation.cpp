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

TEST(StorageV2InterleavedIsolation, IsolationWorksWithSingleEdgeEdgeWriteConflict) {
  std::unique_ptr<memgraph::storage::Storage> storage(
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{}));

  memgraph::storage::Gid v1_gid, v2_gid;

  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto tx1 = storage->Access();
  auto v1_1 = tx1->FindVertex(v1_gid, memgraph::storage::View::OLD);
  ASSERT_TRUE(v1_1.has_value());
  {
    // tx1 finds no edges on v1, as there aren't any edges yet
    auto edges = v1_1->OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  auto tx2 = storage->Access();
  auto v1_2 = tx2->FindVertex(v1_gid, memgraph::storage::View::OLD);
  auto v2_2 = tx2->FindVertex(v2_gid, memgraph::storage::View::OLD);
  ASSERT_TRUE(v1_2.has_value() && v2_2.has_value());
  ASSERT_TRUE(tx2->CreateEdge(&*v1_2, &*v2_2, tx2->NameToEdgeType("Edge2")).HasValue());

  {
    // Uncommited tx2 can see the Edge2 it just created
    auto edges = v1_2->OutEdges(memgraph::storage::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    ASSERT_EQ(edges->edges.size(), 1);
    EXPECT_EQ(edges->edges[0].EdgeType(), tx2->NameToEdgeType("Edge2"));
  }

  auto tx3 = storage->Access();
  auto v1_3 = tx3->FindVertex(v1_gid, memgraph::storage::View::OLD);
  auto v2_3 = tx3->FindVertex(v2_gid, memgraph::storage::View::OLD);
  ASSERT_TRUE(v1_3.has_value() && v2_3.has_value());
  ASSERT_TRUE(tx3->CreateEdge(&*v1_3, &*v2_3, tx3->NameToEdgeType("Edge3")).HasValue());

  {
    // Uncommited tx3 can see the Edge3 it just created, but not the
    // uncommited Edge2 created by tx2
    auto edges = v1_3->OutEdges(memgraph::storage::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 1);
    EXPECT_EQ(edges->edges[0].EdgeType(), tx3->NameToEdgeType("Edge3"));
  }

  auto tx4 = storage->Access();
  auto v1_4 = tx4->FindVertex(v1_gid, memgraph::storage::View::OLD);
  ASSERT_TRUE(v1_4.has_value());
  {
    // tx4 started before the transactions above have committed and cannot see
    // the uncommitted edges
    auto edges = v1_4->OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  // tx2 now commits.
  ASSERT_FALSE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  tx2.reset();

  {
    // tx1 still sees no edges
    auto edges = v1_1->OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx3 still only sees its edge, despite tx2 having committed
    auto edges = v1_3->OutEdges(memgraph::storage::View::NEW);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 1);
    EXPECT_EQ(edges->edges[0].EdgeType(), tx3->NameToEdgeType("Edge3"));
  }

  {
    // tx4 still sees no edges
    auto edges = v1_4->OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  auto tx5 = storage->Access();
  auto v1_5 = tx5->FindVertex(v1_gid, memgraph::storage::View::OLD);
  ASSERT_TRUE(v1_5.has_value());
  {
    // tx5 started after tx2 committed. This means it can see tx2's committed
    // edge, but not tx3's uncommitted edge.
    auto edges = v1_5->OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    ASSERT_EQ(edges->edges.size(), 1);
    EXPECT_EQ(edges->edges[0].EdgeType(), tx5->NameToEdgeType("Edge2"));
  }

  // tx3 now commits
  ASSERT_FALSE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  tx3.reset();

  {
    // tx1 still sees no edges
    auto edges = v1_1->OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx4 still sees no edges
    auto edges = v1_4->OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 0);
  }

  {
    // tx5 still sees the edge committed by tx2
    auto edges = v1_5->OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 1);
    EXPECT_EQ(edges->edges[0].EdgeType(), tx5->NameToEdgeType("Edge2"));
  }

  {
    // A transaction started after both write transactions have committed
    // can see both edges.
    auto tx6 = storage->Access();
    auto v1_6 = tx6->FindVertex(v1_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_6.has_value());
    auto edges = v1_6->OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(edges.HasValue());
    EXPECT_EQ(edges->edges.size(), 2);
  }
}
