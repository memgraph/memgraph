#include <list>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "utils/algorithm.hpp"

using vec = std::vector<std::string>;

using namespace std::string_literals;
using namespace utils;


TEST(Algorithm, Reversed) {
  EXPECT_EQ(Reversed(""s), ""s);
  EXPECT_EQ(Reversed("abc"s), "cba"s);
  EXPECT_EQ(Reversed(std::vector<int>({1, 2, 3, 4})),
            std::vector<int>({4, 3, 2, 1}));
  EXPECT_EQ(Reversed(std::list<std::string>({"ab"s, "cd"s})),
            std::list<std::string>({"cd"s, "ab"s}));
}
