#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <vector>

#include "data_structures/concurrent/skiplist.hpp"

TEST(SkipList, Access) {
  SkipList<int> input;
  {
    auto accessor = input.access();
    accessor.insert(1);
    accessor.insert(2);
    accessor.insert(3);
  }

  auto accessor = input.access();
  std::vector<int> results;
  for (auto it = accessor.begin(); it != accessor.end(); ++it)
    results.push_back(*it);

  EXPECT_THAT(results, testing::ElementsAre(1, 2, 3));
}

TEST(SkipList, ConstAccess) {
  SkipList<int> input;
  {
    auto accessor = input.access();
    accessor.insert(1);
    accessor.insert(2);
    accessor.insert(3);
  }

  const SkipList<int> &skiplist = input;
  auto accessor = skiplist.caccess();

  std::vector<int> results;
  for (auto it = accessor.begin(); it != accessor.end(); ++it)
    results.push_back(*it);

  EXPECT_THAT(results, testing::ElementsAre(1, 2, 3));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
