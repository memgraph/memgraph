#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "utils/string.hpp"

using vec = std::vector<std::string>;

TEST(String, SplitNoLimit) {
  EXPECT_EQ(utils::Split("aba", "a"), vec({"", "b", ""}));
  EXPECT_EQ(utils::Split("aba", "b"), vec({"a", "a"}));
  EXPECT_EQ(utils::Split("abba", "b"), vec({"a", "", "a"}));
  EXPECT_EQ(utils::Split("aba", "c"), vec{"aba"});
}

TEST(String, RSplitNoLimit) {
  // Tests same like for Split
  EXPECT_EQ(utils::RSplit("aba", "a"), vec({"", "b", ""}));
  EXPECT_EQ(utils::RSplit("aba", "b"), vec({"a", "a"}));
  EXPECT_EQ(utils::RSplit("abba", "b"), vec({"a", "", "a"}));
  EXPECT_EQ(utils::RSplit("aba", "c"), vec{"aba"});
}

TEST(String, SplitWithLimit) {
  EXPECT_EQ(utils::Split("a.b.c.d", ".", 0), vec({"a.b.c.d"}));
  EXPECT_EQ(utils::Split("a.b.c.d", ".", 1), vec({"a", "b.c.d"}));
  EXPECT_EQ(utils::Split("a.b.c.d", ".", 2), vec({"a", "b", "c.d"}));
  EXPECT_EQ(utils::Split("a.b.c.d", ".", 100), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(utils::Split("a.b.c.d", ".", -1), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(utils::Split("a.b.c.d", ".", -2), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(utils::Split("a.b.c.d", ".", -100), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(utils::Split("a..b..c", ".", 1), vec({"a", ".b..c"}));
  EXPECT_EQ(utils::Split("a..b..c", ".", 2), vec({"a","", "b..c"}));
}

TEST(String, RSplitWithLimit) {
  EXPECT_EQ(utils::RSplit("a.b.c.d", ".", 0), vec({"a.b.c.d"}));
  EXPECT_EQ(utils::RSplit("a.b.c.d", ".", 1), vec({"a.b.c", "d"}));
  EXPECT_EQ(utils::RSplit("a.b.c.d", ".", 2), vec({"a.b", "c", "d"}));
  EXPECT_EQ(utils::RSplit("a.b.c.d", ".", 100), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(utils::RSplit("a.b.c.d", ".", -1), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(utils::RSplit("a.b.c.d", ".", -2), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(utils::RSplit("a.b.c.d", ".", -100), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(utils::RSplit("a..b..c", ".", 1), vec({"a..b.", "c"}));
  EXPECT_EQ(utils::RSplit("a..b..c", ".", 2), vec({"a..b","", "c"}));
}

TEST(String, SplitWhistespace) {
  EXPECT_EQ(utils::Split(" "), vec({}));
  EXPECT_EQ(utils::Split("  a  b  "), vec({"a", "b"}));
}
