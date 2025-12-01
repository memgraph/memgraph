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

#include <gtest/gtest.h>

#include "planner/core/union_find.hpp"

namespace memgraph::planner::core {

TEST(UnionFind, InitAndMergeSets) {
  UnionFind uf;

  auto id1 = uf.MakeSet();
  auto id2 = uf.MakeSet();
  auto id3 = uf.MakeSet();

  EXPECT_EQ(uf.Size(), 3);
  EXPECT_EQ(uf.ComponentCount(), 3);
  EXPECT_FALSE(uf.Connected(id1, id2));
  EXPECT_FALSE(uf.Connected(id2, id3));
  EXPECT_FALSE(uf.Connected(id1, id3));

  uf.UnionSets(id1, id2);

  EXPECT_EQ(uf.Size(), 3);
  EXPECT_EQ(uf.ComponentCount(), 2);
  EXPECT_TRUE(uf.Connected(id1, id2));
  EXPECT_FALSE(uf.Connected(id2, id3));
  EXPECT_FALSE(uf.Connected(id1, id3));

  auto root = uf.UnionSets(id2, id3);

  EXPECT_EQ(uf.Size(), 3);
  EXPECT_EQ(uf.ComponentCount(), 1);
  EXPECT_EQ(uf.Find(id1), root);
  EXPECT_EQ(uf.Find(id2), root);
  EXPECT_EQ(uf.Find(id3), root);
}

TEST(UnionFind, BulkUnionMultipleSets) {
  UnionFind uf;
  UnionFindContext ctx;

  // Create three separate components
  auto a1 = uf.MakeSet();
  auto a2 = uf.MakeSet();
  uf.UnionSets(a1, a2);

  auto b1 = uf.MakeSet();
  auto b2 = uf.MakeSet();
  uf.UnionSets(b1, b2);

  auto c1 = uf.MakeSet();
  auto c2 = uf.MakeSet();
  uf.UnionSets(c1, c2);

  EXPECT_EQ(uf.ComponentCount(), 3);
  EXPECT_TRUE(uf.Find(a1) == uf.Find(a2));
  EXPECT_TRUE(uf.Find(b1) == uf.Find(b2));
  EXPECT_TRUE(uf.Find(c1) == uf.Find(c2));

  // Bulk union representatives from each component
  std::vector<UnionFind::id_t> representatives = {a1, b1, c1};
  uf.UnionSets(representatives, ctx);

  // All should now be in one component
  EXPECT_EQ(uf.ComponentCount(), 1);
  EXPECT_TRUE(uf.Connected(a1, b1));
  EXPECT_TRUE(uf.Connected(b1, c1));
  EXPECT_TRUE(uf.Connected(a2, b2));
  EXPECT_TRUE(uf.Connected(b2, c2));
}

TEST(UnionFind, UnionWithSelf) {
  UnionFind uf;

  auto id1 = uf.MakeSet();
  auto id2 = uf.MakeSet();

  EXPECT_EQ(uf.ComponentCount(), 2);
  auto root = uf.UnionSets(id1, id1);

  EXPECT_EQ(uf.Find(id1), root);
  EXPECT_NE(uf.Find(id2), root);
  EXPECT_EQ(uf.ComponentCount(), 2);
}

TEST(UnionFind, ClearResetsStructure) {
  UnionFind uf;

  auto id1 = uf.MakeSet();
  auto id2 = uf.MakeSet();
  uf.UnionSets(id1, id2);

  EXPECT_EQ(uf.Size(), 2);
  EXPECT_EQ(uf.ComponentCount(), 1);

  uf.Clear();

  EXPECT_EQ(uf.Size(), 0);
  EXPECT_EQ(uf.ComponentCount(), 0);
}

}  // namespace memgraph::planner::core