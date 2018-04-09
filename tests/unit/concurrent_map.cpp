#include <iostream>

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "data_structures/concurrent/concurrent_map.hpp"

using concurrent_map_t = ConcurrentMap<int, int>;

template <typename TAccessor>
void print_skiplist(const TAccessor &access) {
  DLOG(INFO) << "Map now has: ";
  for (auto &kv : access)
    DLOG(INFO) << fmt::format("    ({}, {})", kv.first, kv.second);
}

TEST(ConcurrentMapSkiplist, Mix) {
  concurrent_map_t skiplist;
  auto accessor = skiplist.access();

  // insert 10
  EXPECT_TRUE(accessor.insert(1, 10).second);

  // try insert 10 again (should fail)
  EXPECT_FALSE(accessor.insert(1, 10).second);

  // insert 20
  EXPECT_TRUE(accessor.insert(2, 20).second);

  print_skiplist(accessor);

  // value at key 3 shouldn't exist
  EXPECT_TRUE(accessor.find(3) == accessor.end());

  // value at key 2 should exist
  EXPECT_TRUE(accessor.find(2) != accessor.end());

  // at key 2 is 20 (true)
  EXPECT_EQ(accessor.find(2)->second, 20);

  // removed existing (1)
  EXPECT_TRUE(accessor.remove(1));

  // removed non-existing (3)
  EXPECT_FALSE(accessor.remove(3));

  // insert (1, 10)
  EXPECT_TRUE(accessor.insert(1, 10).second);

  // insert (4, 40)
  EXPECT_TRUE(accessor.insert(4, 40).second);

  print_skiplist(accessor);
}

TEST(ConcurrentMapSkiplist, ConstFind) {
  ConcurrentMap<int, int> map;
  {
    auto access = map.access();
    for (int i = 0; i < 10; ++i) access.insert(i, i);
  }
  {
    const auto &const_map = map;
    auto access = const_map.access();
    auto it = access.find(4);
    EXPECT_NE(it, access.end());
    it = access.find(12);
    EXPECT_EQ(it, access.end());
  }
}
