// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <array>
#include <string>
#include <string_view>

#include "gtest/gtest.h"

#include "versioning/name.hpp"

using memgraph::versioning::ValidateBranchName;

namespace {
constexpr std::string_view kEmptyMsg = "must not be empty";
constexpr std::string_view kReservedMsg = "reserved branch name";
constexpr std::string_view kLengthMsg = "at most 63 characters";
constexpr std::string_view kAtMsg = "must not contain '@'";
constexpr std::string_view kEndsMsg = "start and end with an alphanumeric";
constexpr std::string_view kCharsetMsg = "may contain only letters, digits, '.', '_' and '-'";

struct Rejection {
  std::string_view input;
  std::string_view message_part;
};
}  // namespace

TEST(VersioningName, InvalidNamesAreRejectedByTheMatchingRule) {
  constexpr std::array<Rejection, 29> kRejections{{
      {"", kEmptyMsg},
      {"main", kReservedMsg},
      {"a.", kEndsMsg},
      {".hidden", kEndsMsg},
      {".", kEndsMsg},
      {"..x", kEndsMsg},
      {"feature-", kEndsMsg},
      {"_x", kEndsMsg},
      {"x_", kEndsMsg},
      {"a b", kCharsetMsg},
      {"a/b", kCharsetMsg},
      {"/", kCharsetMsg},
      {"\\", kCharsetMsg},
      {"a\\", kCharsetMsg},
      {"a/b/c", kCharsetMsg},
      {"release/1.2", kCharsetMsg},
      {"release\\1.2", kCharsetMsg},
      {"a:b", kCharsetMsg},
      {"a`", kCharsetMsg},
      {"a{", kCharsetMsg},
      {"a[", kCharsetMsg},
      {"a/", kCharsetMsg},
      {"a:", kCharsetMsg},
      {"a?b", kCharsetMsg},
      {"a*b", kCharsetMsg},
      {"@", kAtMsg},
      {"a@b", kAtMsg},
      {"shop@branch", kAtMsg},
      {"@main", kAtMsg},  // '@' is checked before the alphanumeric-ends rule
  }};
  for (const auto &[input, message_part] : kRejections) {
    const auto error = ValidateBranchName(input);
    ASSERT_TRUE(error.has_value()) << "expected '" << input << "' to be rejected";
    EXPECT_NE(error->find(message_part), std::string::npos) << "input '" << input << "' got message: " << *error;
  }
}

TEST(VersioningName, ValidNamesAreAccepted) {
  // Only exact lowercase "main" is reserved.
  for (const std::string_view input : {"feature-1",
                                       "my_branch",
                                       "release_2026_07",
                                       "m",
                                       "mainly-not-main",
                                       "Main",
                                       "MAIN",
                                       "release.1.2",
                                       "a1",
                                       "b-1.c_2",
                                       "a",
                                       "z",
                                       "A",
                                       "Z",
                                       "0",
                                       "9",
                                       "zZ0-9Aa"}) {
    EXPECT_EQ(ValidateBranchName(input), std::nullopt) << "input '" << input << "'";
  }
}

TEST(VersioningName, NameLengthIsBoundedAt63) {
  std::string ok(63, 'x');
  ok.front() = 'a';
  ok.back() = '9';
  EXPECT_EQ(ValidateBranchName(ok), std::nullopt) << "input '" << ok << "'";

  std::string too_long = ok + 'x';
  ASSERT_EQ(too_long.size(), 64);
  const auto error = ValidateBranchName(too_long);
  ASSERT_TRUE(error.has_value()) << "expected the 64-char name to be rejected";
  EXPECT_NE(error->find(kLengthMsg), std::string::npos) << "got message: " << *error;
}
