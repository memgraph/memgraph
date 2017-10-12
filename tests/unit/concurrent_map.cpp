#include <iostream>

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "data_structures/concurrent/concurrent_map.hpp"

using concurrent_map_t = ConcurrentMap<int, int>;

void print_skiplist(const concurrent_map_t::Accessor<false> &map) {
  DLOG(INFO) << "Map now has: ";
  for (auto &kv : map)
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

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
