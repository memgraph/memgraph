#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "utils/exceptions.hpp"
#include "utils/string.hpp"

using vec = std::vector<std::string>;

using namespace utils;

TEST(String, Trim) {
  EXPECT_EQ(Trim(" \t\n\r ab\r\n\t ab \r\t "), "ab\r\n\t ab");
  EXPECT_EQ(Trim(" \t\n\r"), "");
  EXPECT_EQ(Trim("run()"), "run()");
  EXPECT_EQ(Trim(""), "");
}

TEST(String, ToLowerCase) {
  EXPECT_EQ(ToLowerCase("MemGraph"), "memgraph");
  EXPECT_EQ(ToLowerCase(" ( Mem Graph ) "), " ( mem graph ) ");
}

TEST(String, ToUpperCase) {
  EXPECT_EQ(ToUpperCase("MemGraph"), "MEMGRAPH");
  EXPECT_EQ(ToUpperCase(" ( Mem Graph ) "), " ( MEM GRAPH ) ");
  // ToUpperCase ignores unicode.
  EXPECT_EQ(ToUpperCase("\u0161memgraph"), "\u0161MEMGRAPH");
}

TEST(String, Join) {
  EXPECT_EQ(Join({}, " "), "");
  EXPECT_EQ(Join({"mem", "gra", "ph"}, ""), "memgraph");
  EXPECT_EQ(Join({"mirko", "slavko", "pero"}, ", "), "mirko, slavko, pero");
  EXPECT_EQ(Join({"", "abc", "", "def", ""}, " "), " abc  def ");
}

TEST(String, Replace) {
  EXPECT_EQ(Replace("ab.abab.", "ab", "cd"), "cd.cdcd.");
  EXPECT_EQ(Replace("ababab", "ab", ""), "");
  EXPECT_EQ(Replace("ababccab.", "ab", ""), "cc.");
  EXPECT_EQ(Replace("aabb", "ab", ""), "ab");
  EXPECT_EQ(Replace("ababcab.", "ab", "abab"), "ababababcabab.");
}

TEST(String, SplitNoLimit) {
  EXPECT_EQ(Split("aba", "a"), vec({"", "b", ""}));
  EXPECT_EQ(Split("aba", "b"), vec({"a", "a"}));
  EXPECT_EQ(Split("abba", "b"), vec({"a", "", "a"}));
  EXPECT_EQ(Split("aba", "c"), vec{"aba"});
}

TEST(String, RSplitNoLimit) {
  // Tests same like for Split
  EXPECT_EQ(RSplit("aba", "a"), vec({"", "b", ""}));
  EXPECT_EQ(RSplit("aba", "b"), vec({"a", "a"}));
  EXPECT_EQ(RSplit("abba", "b"), vec({"a", "", "a"}));
  EXPECT_EQ(RSplit("aba", "c"), vec{"aba"});
}

TEST(String, SplitWithLimit) {
  EXPECT_EQ(Split("a.b.c.d", ".", 0), vec({"a.b.c.d"}));
  EXPECT_EQ(Split("a.b.c.d", ".", 1), vec({"a", "b.c.d"}));
  EXPECT_EQ(Split("a.b.c.d", ".", 2), vec({"a", "b", "c.d"}));
  EXPECT_EQ(Split("a.b.c.d", ".", 100), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(Split("a.b.c.d", ".", -1), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(Split("a.b.c.d", ".", -2), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(Split("a.b.c.d", ".", -100), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(Split("a..b..c", ".", 1), vec({"a", ".b..c"}));
  EXPECT_EQ(Split("a..b..c", ".", 2), vec({"a", "", "b..c"}));
}

TEST(String, RSplitWithLimit) {
  EXPECT_EQ(RSplit("a.b.c.d", ".", 0), vec({"a.b.c.d"}));
  EXPECT_EQ(RSplit("a.b.c.d", ".", 1), vec({"a.b.c", "d"}));
  EXPECT_EQ(RSplit("a.b.c.d", ".", 2), vec({"a.b", "c", "d"}));
  EXPECT_EQ(RSplit("a.b.c.d", ".", 100), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(RSplit("a.b.c.d", ".", -1), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(RSplit("a.b.c.d", ".", -2), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(RSplit("a.b.c.d", ".", -100), vec({"a", "b", "c", "d"}));
  EXPECT_EQ(RSplit("a..b..c", ".", 1), vec({"a..b.", "c"}));
  EXPECT_EQ(RSplit("a..b..c", ".", 2), vec({"a..b", "", "c"}));
}

TEST(String, SplitWhitespace) {
  EXPECT_EQ(Split(" "), vec({}));
  EXPECT_EQ(Split("  a  b  "), vec({"a", "b"}));
}

TEST(String, ParseDouble) {
  EXPECT_EQ(ParseDouble(".5"), 0.5);
  EXPECT_EQ(ParseDouble("5"), 5.0);
  EXPECT_EQ(ParseDouble("1.5"), 1.5);
  EXPECT_EQ(ParseDouble("15e-1"), 1.5);
  EXPECT_THROW(ParseDouble("1.5a"), BasicException);
}

TEST(String, StartsWith) {
  EXPECT_TRUE(StartsWith("memgraph", "mem"));
  EXPECT_TRUE(StartsWith("memgraph", ""));
  EXPECT_TRUE(StartsWith("", ""));
  EXPECT_TRUE(StartsWith("memgraph", "memgraph"));
  EXPECT_FALSE(StartsWith("memgraph", "MEM"));
  EXPECT_FALSE(StartsWith("memgrap", "memgraph"));
}

TEST(String, EndsWith) {
  EXPECT_TRUE(EndsWith("memgraph", "graph"));
  EXPECT_TRUE(EndsWith("memgraph", ""));
  EXPECT_TRUE(EndsWith("", ""));
  EXPECT_TRUE(EndsWith("memgraph", "memgraph"));
  EXPECT_FALSE(EndsWith("memgraph", "GRAPH"));
  EXPECT_FALSE(EndsWith("memgraph", "the memgraph"));
}

TEST(String, RandomString) {
  EXPECT_EQ(RandomString(0).size(), 0);
  EXPECT_EQ(RandomString(1).size(), 1);
  EXPECT_EQ(RandomString(42).size(), 42);

  std::set<std::string> string_set;
  for (int i = 0 ; i < 20 ; ++i)
    string_set.emplace(RandomString(256));

  EXPECT_EQ(string_set.size(), 20);
}
