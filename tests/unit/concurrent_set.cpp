#include <iostream>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "data_structures/concurrent/concurrent_set.hpp"
#include "utils/assert.hpp"

void print_skiplist(const ConcurrentSet<int>::Accessor &skiplist) {
  DLOG(INFO) << "Skiplist set now has:";
  for (auto &item : skiplist) DLOG(INFO) << item;
}

TEST(ConcurrentSet, Mix) {
  ConcurrentSet<int> set;

  auto accessor = set.access();

  // added non-existing 1? (true)
  EXPECT_TRUE(accessor.insert(1).second);

  // added already existing 1? (false)
  EXPECT_FALSE(accessor.insert(1).second);

  // added non-existing 2? (true)
  EXPECT_TRUE(accessor.insert(2).second);

  // item 3 doesn't exist? (true)
  EXPECT_EQ(accessor.find(3), accessor.end());

  // item 3 exists? (false)
  EXPECT_FALSE(accessor.contains(3));

  // item 2 exists? (true)
  EXPECT_NE(accessor.find(2), accessor.end());

  // find item 2
  EXPECT_EQ(*accessor.find(2), 2);

  // removed existing 1? (true)
  EXPECT_TRUE(accessor.remove(1));

  // try to remove non existing element
  EXPECT_FALSE(accessor.remove(3));

  // add 1 again
  EXPECT_TRUE(accessor.insert(1).second);

  // add 4
  EXPECT_TRUE(accessor.insert(4).second);

  print_skiplist(accessor);
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
