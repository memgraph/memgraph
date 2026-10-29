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

// A branch name must be non-empty, not `main`, not start with `.`, and contain no `/`, `\` or `@`.

namespace {
constexpr std::string_view kEmptyMsg = "must not be empty";
constexpr std::string_view kReservedMsg = "reserved branch name";
constexpr std::string_view kDotMsg = "must not start with '.'";
constexpr std::string_view kSeparatorMsg = "must not contain '/', '\\' or '@'";

struct Rejection {
  std::string_view input;
  std::string_view message_part;
};
}  // namespace

TEST(VersioningName, InvalidNamesAreRejectedByTheMatchingRule) {
  constexpr std::array<Rejection, 17> kRejections{{
      {"", kEmptyMsg},
      {"main", kReservedMsg},
      {".hidden", kDotMsg},
      {".", kDotMsg},
      {"..x", kDotMsg},
      {"/", kSeparatorMsg},
      {"\\", kSeparatorMsg},
      {"a/", kSeparatorMsg},
      {"/a", kSeparatorMsg},
      {"a\\", kSeparatorMsg},
      {"a/b/c", kSeparatorMsg},
      {"release/1.2", kSeparatorMsg},
      {"release\\1.2", kSeparatorMsg},
      {"@", kSeparatorMsg},
      {"a@b", kSeparatorMsg},
      {"shop@branch", kSeparatorMsg},
      {".a/b", kDotMsg},  // leading dot is checked before separators
  }};
  for (const auto &[input, message_part] : kRejections) {
    const auto error = ValidateBranchName(input);
    ASSERT_TRUE(error.has_value()) << "expected '" << input << "' to be rejected";
    EXPECT_NE(error->find(message_part), std::string::npos) << "input '" << input << "' got message: " << *error;
  }
}

TEST(VersioningName, ValidNamesAreAccepted) {
  // "Main"/"MAIN": only exact lowercase "main" is reserved. "mainly-not-main": contains but != "main".
  for (const std::string_view input : {"feature-1",
                                       "my_branch",
                                       "release_2026_07",
                                       "m",
                                       "mainly-not-main",
                                       "Main",
                                       "MAIN",
                                       "release.1.2",
                                       "a.",
                                       "a b"}) {
    EXPECT_EQ(ValidateBranchName(input), std::nullopt) << "input '" << input << "'";
  }
}
