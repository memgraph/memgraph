#include <algorithm>
#include <iterator>
#include <vector>

#include "gtest/gtest.h"

#include "data_structures/concurrent/skiplist.hpp"
#include "database/indexes/index_common.hpp"

template <class TIterable>
int Count(TIterable &collection) {
  int ret = 0;
  for (__attribute__((unused)) auto it : collection) ret += 1;
  return ret;
}

TEST(SkipListSuffix, EmptyRange) {
  SkipList<int> V;
  auto access = V.access();
  auto r1 = IndexUtils::SkipListSuffix<typename SkipList<int>::Iterator, int,
                                       SkipList<int>>(access.begin(),
                                                      std::move(access));
  EXPECT_EQ(Count(r1), 0);
}

TEST(SkipListSuffix, NonEmptyRange) {
  SkipList<int> V;
  auto access = V.access();
  access.insert(1);
  access.insert(5);
  access.insert(3);
  auto r1 = IndexUtils::SkipListSuffix<typename SkipList<int>::Iterator, int,
                                       SkipList<int>>(access.begin(),
                                                      std::move(access));
  EXPECT_EQ(Count(r1), 3);
  auto iter = r1.begin();
  EXPECT_EQ(*iter, 1);
  ++iter;
  EXPECT_EQ(*iter, 3);
  ++iter;
  EXPECT_EQ(*iter, 5);
}
