#include <stdlib.h>
#include <iostream>

#include "gtest/gtest.h"

#include "union_find.hpp"

void ExpectFully(UnionFind<> &uf, bool connected, int from = 0, int to = -1) {
  if (to == -1) to = uf.size();

  for (int i = from; i < to; i++)
    for (int j = from; j < to; j++)
      if (i != j) EXPECT_EQ(uf.find(i, j), connected);
}

TEST(UnionFindTest, InitialSizeTest) {
  for (int i = 0; i < 10; i++) {
    UnionFind<> uf(i);
    EXPECT_EQ(i, uf.size());
  }
}

TEST(UnionFindTest, ModifiedSizeTest) {
  UnionFind<> uf(10);
  EXPECT_EQ(10, uf.size());

  uf.connect(0, 0);
  EXPECT_EQ(10, uf.size());

  uf.connect(0, 1);
  EXPECT_EQ(9, uf.size());

  uf.connect(2, 3);
  EXPECT_EQ(8, uf.size());

  uf.connect(0, 2);
  EXPECT_EQ(7, uf.size());

  uf.connect(1, 3);
  EXPECT_EQ(7, uf.size());
}

TEST(UnionFindTest, Disconectivity) {
  UnionFind<> uf(10);
  ExpectFully(uf, false);
}

TEST(UnionFindTest, ConnectivityAlongChain) {
  UnionFind<> uf(10);
  for (unsigned int i = 1; i < uf.size(); i++) uf.connect(i - 1, i);
  ExpectFully(uf, true);
}

TEST(UnionFindTest, ConnectivityOnTree) {
  UnionFind<> uf(10);
  ExpectFully(uf, false);

  uf.connect(0, 1);
  uf.connect(0, 2);
  ExpectFully(uf, true, 0, 3);
  ExpectFully(uf, false, 2);

  uf.connect(2, 3);
  ExpectFully(uf, true, 0, 4);
  ExpectFully(uf, false, 3);
}

TEST(UnionFindTest, DisjointChains) {
  UnionFind<> uf(30);
  for (int i = 0; i < 30; i++) uf.connect(i, i % 10 == 0 ? i : i - 1);

  for (int i = 0; i < 30; i++)
    for (int j = 0; j < 30; j++)
      EXPECT_EQ(uf.find(i, j), (j - (j % 10)) == (i - (i % 10)));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
