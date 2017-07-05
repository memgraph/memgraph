#include <algorithm>
#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "data_structures/concurrent/skiplist.hpp"
#include "utils/assert.hpp"

/* The following tests validate the SkipList::position_and_count estimation
 * functionality. That function has a tunable speed vs. accuracy. The tests
 * here test the absolutely-accurate parameterization, as well as the default
 * one that should be optimal parametrization. As such the tests are
 * stochastic and defined to validate generally acceptable behavior in
 * a vast majority of cases. The probability of test failure due to
 * stochasticity should be extremely small, but isn't zero.
 */

auto SkiplistRange(int count) {
  auto sl = std::make_unique<SkipList<int>>();
  auto access = sl->access();
  for (int i = 0; i < count; i++) access.insert(i);
  return sl;
}

auto Median(std::vector<int> &elements) {
  auto elem_size = elements.size();
  debug_assert(elem_size > 0, "Provide some elements to get median!");
  std::sort(elements.begin(), elements.end());
  if (elem_size % 2)
    return elements[elem_size / 2];
  else
    return (elements[elem_size / 2 - 1] + elements[elem_size / 2]) / 2;
}

auto Less(int granularity) {
  return [granularity](const int &a, const int &b) {
    return a / granularity < b / granularity;
  };
}

auto Equal(int granularity) {
  return [granularity](const int &a, const int &b) {
    return a / granularity == b / granularity;
  };
}

#define EXPECT_ABS_POS_COUNT(granularity, position, expected_position, \
                             expected_count)                           \
  {                                                                    \
    auto sl = SkiplistRange(10000);                                    \
    auto position_and_count = sl->access().position_and_count(         \
        position, Less(granularity), Equal(granularity), 1000, 0);     \
    EXPECT_EQ(position_and_count.first, expected_position);            \
    EXPECT_EQ(position_and_count.second, expected_count);              \
  }

TEST(SkiplistPosAndCount, AbsoluteAccuracy) {
  EXPECT_ABS_POS_COUNT(1, 42, 42, 1);
  EXPECT_ABS_POS_COUNT(3, 42, 42, 3);
  EXPECT_ABS_POS_COUNT(10, 42, 40, 10);
}

#define EXPECT_POS_COUNT(skiplist_size, position, expected_count,            \
                         position_error_margin, count_error_margin)          \
  {                                                                          \
    std::vector<int> pos_errors;                                             \
    std::vector<int> count_errors;                                           \
                                                                             \
    for (int i = 0; i < 30; i++) {                                           \
      auto sl = SkiplistRange(skiplist_size);                                \
      auto position_count = sl->access().position_and_count(position);       \
      pos_errors.push_back(std::abs((long)position_count.first - position)); \
      count_errors.push_back(                                                \
          std::abs((long)position_count.second - expected_count));           \
    }                                                                        \
    EXPECT_LE(Median(pos_errors), position_error_margin);                    \
    EXPECT_LE(Median(count_errors), count_error_margin);                     \
  }

TEST(SkiplistPosAndCount, DefaultSpeedAndAccuracy) {
  EXPECT_POS_COUNT(5000, 42, 1, 20, 3);
  EXPECT_POS_COUNT(5000, 2500, 1, 100, 3);
  EXPECT_POS_COUNT(5000, 4500, 1, 200, 3);

  // for an item greater then all list elements the returned
  // estimations are always absolutely accurate
  EXPECT_POS_COUNT(5000, 5000, 0, 0, 0);
}
