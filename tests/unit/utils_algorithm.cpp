// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <list>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "utils/algorithm.hpp"

using vec = std::vector<std::string>;

using namespace std::string_literals;
using namespace memgraph::utils;

TEST(Algorithm, PrintIterable) {
  // Checks both variants of the function (using iterators and collections)
  auto check = [](const std::vector<int> &iterable, const std::string &expected_output) {
    {
      std::ostringstream oss;
      PrintIterable(oss, iterable, ", ");
      EXPECT_EQ(oss.str(), expected_output);
    }
    {
      auto streamer = [](auto &stream, const auto &item) { stream << item; };
      std::ostringstream oss;
      PrintIterable(&oss, iterable.begin(), iterable.end(), ", ", streamer);
      EXPECT_EQ(oss.str(), expected_output);
    }
  };

  check(std::vector<int>{1, 2, 3, 4}, "1, 2, 3, 4");
  check(std::vector<int>{1}, "1");
  check(std::vector<int>{}, "");

  {
    // Use custom streamer
    auto map_streamer = [](auto &stream, const auto &item) { stream << item.first << ": " << item.second; };
    std::ostringstream oss;
    std::map<std::string, std::string> map;
    map.emplace("a", "x");
    map.emplace("b", "y");
    PrintIterable(&oss, map.begin(), map.end(), ", ", map_streamer);
    EXPECT_EQ(oss.str(), "a: x, b: y");
  }
}

TEST(Algorithm, Reversed) {
  EXPECT_EQ(Reversed(""s), ""s);
  EXPECT_EQ(Reversed("abc"s), "cba"s);
  EXPECT_EQ(Reversed(std::vector<int>({1, 2, 3, 4})), std::vector<int>({4, 3, 2, 1}));
  EXPECT_EQ(Reversed(std::list<std::string>({"ab"s, "cd"s})), std::list<std::string>({"cd"s, "ab"s}));
}
