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

#include "versioning/gate.hpp"

using memgraph::versioning::CheckVersioningGate;
using memgraph::versioning::GateError;
using memgraph::versioning::GateErrorMessage;

TEST(VersioningGate, ExhaustiveTruthTableHonoursPrecedence) {
  // Precedence: flag, then license, then storage mode, then commit-lock narrowing (first failure wins).
  struct Case {
    bool flag;
    bool license;
    bool storage;
    std::optional<GateError> expected;  // when commit_lock_narrowing is false
  };

  constexpr Case kCases[] = {
      {true, true, true, std::nullopt},
      {true, true, false, GateError::kUnsupportedStorageMode},
      {true, false, true, GateError::kNoLicense},
      {true, false, false, GateError::kNoLicense},
      {false, true, true, GateError::kDisabled},
      {false, true, false, GateError::kDisabled},
      {false, false, true, GateError::kDisabled},
      {false, false, false, GateError::kDisabled},
  };
  for (const auto &c : kCases) {
    for (const bool narrowing : {false, true}) {
      // Narrowing only surfaces when every earlier check passed.
      const auto expected = (narrowing && !c.expected) ? std::optional{GateError::kCommitLockNarrowing} : c.expected;
      EXPECT_EQ(CheckVersioningGate(c.flag, c.license, c.storage, narrowing), expected)
          << "flag=" << c.flag << " license=" << c.license << " storage=" << c.storage << " narrowing=" << narrowing;
    }
  }
}

TEST(VersioningGate, ErrorMessagesAreExact) {
  ASSERT_EQ(GateErrorMessage(GateError::kDisabled),
            "Versioning is disabled. Start Memgraph with --versioning-enabled=true to use branch queries.");
  ASSERT_EQ(GateErrorMessage(GateError::kNoLicense),
            "Graph versioning is an enterprise feature. A valid Memgraph Enterprise License is required.");
  ASSERT_EQ(GateErrorMessage(GateError::kUnsupportedStorageMode),
            "Graph versioning requires IN_MEMORY_TRANSACTIONAL storage mode. It is not available in "
            "IN_MEMORY_ANALYTICAL or ON_DISK_TRANSACTIONAL.");
  ASSERT_EQ(GateErrorMessage(GateError::kCommitLockNarrowing),
            "Graph versioning is not supported with --experimental-enabled=commit-lock-narrowing.");
}
