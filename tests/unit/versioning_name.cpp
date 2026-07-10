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

#include "gtest/gtest.h"

#include "versioning/name.hpp"

using memgraph::versioning::ValidateBranchName;

// specs/graph-versioning.md §4.1: "A version name ... must be non-empty, cannot be `main`
// (reserved), cannot start with `.`, and cannot contain `/` or `\`."

TEST(VersioningName, ValidNamesAreAccepted) {
  EXPECT_EQ(ValidateBranchName("feature-1"), std::nullopt);
  EXPECT_EQ(ValidateBranchName("my_branch"), std::nullopt);
  EXPECT_EQ(ValidateBranchName("release_2026_07"), std::nullopt);
  EXPECT_EQ(ValidateBranchName("m"), std::nullopt);
  EXPECT_EQ(ValidateBranchName("mainly-not-main"), std::nullopt);  // contains but != "main"
}

TEST(VersioningName, EmptyIsRejected) { EXPECT_NE(ValidateBranchName(""), std::nullopt); }

TEST(VersioningName, ReservedMainIsRejected) { EXPECT_NE(ValidateBranchName("main"), std::nullopt); }

TEST(VersioningName, MainIsCaseSensitiveSoMainCapitalizedIsAccepted) {
  // Only the exact lowercase "main" is reserved per the spec; this documents the boundary rather
  // than prescribing product behaviour for case-insensitive filesystems/clients.
  EXPECT_EQ(ValidateBranchName("Main"), std::nullopt);
  EXPECT_EQ(ValidateBranchName("MAIN"), std::nullopt);
}

TEST(VersioningName, LeadingDotIsRejected) {
  EXPECT_NE(ValidateBranchName(".hidden"), std::nullopt);
  EXPECT_NE(ValidateBranchName("."), std::nullopt);
}

TEST(VersioningName, DotElsewhereIsAccepted) { EXPECT_EQ(ValidateBranchName("release.1.2"), std::nullopt); }

TEST(VersioningName, ForwardSlashIsRejected) {
  EXPECT_NE(ValidateBranchName("release/1.2"), std::nullopt);
  EXPECT_NE(ValidateBranchName("a/b/c"), std::nullopt);
}

TEST(VersioningName, BackslashIsRejected) { EXPECT_NE(ValidateBranchName("release\\1.2"), std::nullopt); }

TEST(VersioningName, ErrorMessagesAreNonEmpty) {
  for (auto invalid : {"", "main", ".x", "a/b", "a\\b"}) {
    auto error = ValidateBranchName(invalid);
    ASSERT_TRUE(error.has_value()) << "expected '" << invalid << "' to be rejected";
    EXPECT_FALSE(error->empty());
  }
}
