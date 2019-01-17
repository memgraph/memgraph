#include <cmath>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "utils/math.hpp"

TEST(UtilsMath, Log2) {
  for (uint64_t i = 1; i < 1000000; ++i) {
    ASSERT_EQ(utils::Log2(i), static_cast<uint64_t>(log2(i)));
  }
}
