#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <vector>

#include "data_structures/concurrent/concurrent_map.hpp"

TEST(ConcurrentMap, Access) {
  ConcurrentMap<int, int> input;
  {
    auto accessor = input.access();
    accessor.insert(1, 1);
    accessor.insert(2, 2);
    accessor.insert(3, 3);
  }

  auto accessor = input.access();
  std::vector<int> results;
  for (auto it = accessor.begin(); it != accessor.end(); ++it)
    results.push_back(it->first);

  EXPECT_THAT(results, testing::ElementsAre(1, 2, 3));
}

TEST(ConcurrentMap, ConstAccess) {
  ConcurrentMap<int, int> input;
  {
    auto accessor = input.access();
    accessor.insert(1, 1);
    accessor.insert(2, 2);
    accessor.insert(3, 3);
  }

  const ConcurrentMap<int, int> &map = input;
  auto accessor = map.access();

  std::vector<int> results;
  for (auto it = accessor.begin(); it != accessor.end(); ++it)
    results.push_back(it->first);

  EXPECT_THAT(results, testing::ElementsAre(1, 2, 3));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
