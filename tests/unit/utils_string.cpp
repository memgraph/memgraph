// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "utils/exceptions.hpp"
#include "utils/string.hpp"

using vec = std::vector<std::string>;

using namespace memgraph::utils;

TEST(String, LTrim) {
  EXPECT_EQ(LTrim(" \t\n\r ab\r\n\t ab \r\t "), "ab\r\n\t ab \r\t ");
  EXPECT_EQ(LTrim(" \t\n\r"), "");
  EXPECT_EQ(LTrim("run()"), "run()");
  EXPECT_EQ(LTrim("run()", "rn"), "un()");
  EXPECT_EQ(LTrim(""), "");
}

TEST(String, RTrim) {
  EXPECT_EQ(RTrim(" \t\n\r ab\r\n\t ab \r\t "), " \t\n\r ab\r\n\t ab");
  EXPECT_EQ(RTrim(" \t\n\r"), "");
  EXPECT_EQ(RTrim("run()"), "run()");
  EXPECT_EQ(RTrim("run()", "u()"), "run");
  EXPECT_EQ(RTrim(""), "");
}

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

TEST(String, IEquals) {
  EXPECT_TRUE(IEquals("", ""));
  EXPECT_FALSE(IEquals("", "fdasfa"));
  EXPECT_TRUE(IEquals("abcv", "AbCV"));
  EXPECT_FALSE(IEquals("abcv", "AbCd"));
}

TEST(String, RandomString) {
  EXPECT_EQ(RandomString(0).size(), 0);
  EXPECT_EQ(RandomString(1).size(), 1);
  EXPECT_EQ(RandomString(42).size(), 42);

  std::set<std::string, std::less<>> string_set;
  for (int i = 0; i < 20; ++i) string_set.emplace(RandomString(256));

  EXPECT_EQ(string_set.size(), 20);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(String, Substr) {
  const std::string string("memgraph");
  EXPECT_EQ(Substr(string), string.substr());
  EXPECT_EQ(Substr(string, string.size()), string.substr(string.size()));
  EXPECT_THROW(string.substr(string.size() + 1), std::out_of_range);
  EXPECT_TRUE(Substr(string, string.size() + 1).empty());
  EXPECT_EQ(Substr(string, 1, string.size()), string.substr(1, string.size()));
  EXPECT_EQ(Substr(string, 0, string.size()), string.substr(0, string.size()));
  EXPECT_EQ(Substr(string, 0, string.size() + 1), string.substr(0, string.size() + 1));
  EXPECT_EQ(Substr(string, 0, string.size() - 1), string.substr(0, string.size() - 1));
  EXPECT_EQ(Substr(string, string.size() - 1, 1), string.substr(string.size() - 1, 1));
  EXPECT_EQ(Substr(string, string.size() - 1, 2), string.substr(string.size() - 1, 2));
}

TEST(String, DoubleToString) {
  EXPECT_EQ(DoubleToString(0), "0");
  EXPECT_EQ(DoubleToString(1), "1");
  EXPECT_EQ(DoubleToString(1234567890123456), "1234567890123456");
  EXPECT_EQ(DoubleToString(static_cast<double>(12345678901234567)), "12345678901234568");
  EXPECT_EQ(DoubleToString(0.5), "0.5");
  EXPECT_EQ(DoubleToString(1.0), "1");
  EXPECT_EQ(DoubleToString(5.8), "5.8");
  EXPECT_EQ(DoubleToString(1.01234000), "1.01234");
  EXPECT_EQ(DoubleToString(1.036837585345), "1.036837585345");
  EXPECT_EQ(DoubleToString(103.6837585345), "103.683758534500001");
  EXPECT_EQ(DoubleToString(1.01234567890123456789), "1.012345678901235");
  EXPECT_EQ(DoubleToString(1234567.01234567891234567), "1234567.012345678871498");
  EXPECT_EQ(DoubleToString(0.00001), "0.00001");
  EXPECT_EQ(DoubleToString(0.00000000000001), "0.00000000000001");
  EXPECT_EQ(DoubleToString(0.000000000000001), "0.000000000000001");
  EXPECT_EQ(DoubleToString(0.0000000000000001), "0");
}
