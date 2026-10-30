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

#include <utility>

#include "flags/experimental.hpp"
#include "versioning/gate.hpp"

using memgraph::versioning::CheckVersioningGate;
using memgraph::versioning::GateError;
using memgraph::versioning::GateErrorMessage;
using memgraph::versioning::GateOperation;
using memgraph::versioning::GateState;

// Precedence follows the spec's prerequisites: flag, license, storage mode, durability, commit-lock narrowing (first
// failure wins). Only kCreate needs the license and durability; kObserve (SHOW, USE, DROP) needs neither.
TEST(VersioningGate, ExhaustiveTruthTableHonoursPrecedence) {
  for (const auto op : {GateOperation::kObserve, GateOperation::kCreate}) {
    const bool creating = op == GateOperation::kCreate;
    for (unsigned bits = 0; bits < 32; ++bits) {
      const GateState state{.flag_enabled = (bits & 1U) != 0,
                            .enterprise_valid = (bits & 2U) != 0,
                            .is_in_memory_transactional = (bits & 4U) != 0,
                            .durable = (bits & 8U) != 0,
                            .commit_lock_narrowing = (bits & 16U) != 0};
      std::optional<GateError> expected;
      if (!state.flag_enabled) {
        expected = GateError::kDisabled;
      } else if (creating && !state.enterprise_valid) {
        expected = GateError::kNoLicense;
      } else if (!state.is_in_memory_transactional) {
        expected = GateError::kUnsupportedStorageMode;
      } else if (creating && !state.durable) {
        expected = GateError::kNotDurable;
      } else if (state.commit_lock_narrowing) {
        expected = GateError::kCommitLockNarrowing;
      }
      EXPECT_EQ(CheckVersioningGate(op, state), expected) << "creating=" << creating << " bits=" << bits;
    }
  }
}

TEST(VersioningGate, UnlicensedObserveIsAllowedButCreateIsNot) {
  const GateState state{.flag_enabled = true,
                        .enterprise_valid = false,
                        .is_in_memory_transactional = true,
                        .durable = true,
                        .commit_lock_narrowing = false};
  EXPECT_EQ(CheckVersioningGate(GateOperation::kObserve, state), std::nullopt);
  EXPECT_EQ(CheckVersioningGate(GateOperation::kCreate, state), GateError::kNoLicense);
}

TEST(VersioningGate, ErrorMessagesAreExact) {
  ASSERT_EQ(
      GateErrorMessage(GateError::kDisabled),
      "Graph versioning is disabled. Start Memgraph with --experimental-enabled=versioning to use branch queries.");
  ASSERT_EQ(GateErrorMessage(GateError::kNoLicense),
            "Graph versioning is an enterprise feature. A valid Memgraph Enterprise License is required.");
  ASSERT_EQ(GateErrorMessage(GateError::kUnsupportedStorageMode),
            "Graph versioning requires IN_MEMORY_TRANSACTIONAL storage mode. It is not available in "
            "IN_MEMORY_ANALYTICAL or ON_DISK_TRANSACTIONAL.");
  ASSERT_EQ(GateErrorMessage(GateError::kNotDurable),
            "Graph versioning requires the WAL (PERIODIC_SNAPSHOT_WITH_WAL) and --data-recovery-on-startup=true.");
  ASSERT_EQ(GateErrorMessage(GateError::kCommitLockNarrowing),
            "Graph versioning is not supported with --experimental-enabled=commit-lock-narrowing.");
}

TEST(VersioningExperimentalFlag, VersioningIsItsOwnExperimentsBit) {
  using memgraph::flags::Experiments;
  EXPECT_TRUE(memgraph::flags::ValidExperimentalFlag("versioning"));
  EXPECT_EQ(memgraph::flags::ReadExperimental("versioning"), Experiments::VERSIONING);

  const auto both = memgraph::flags::ReadExperimental("commit-lock-narrowing,versioning");
  EXPECT_EQ(std::to_underlying(both),
            std::to_underlying(Experiments::COMMIT_LOCK_NARROWING) | std::to_underlying(Experiments::VERSIONING));

  memgraph::flags::SetExperimental(memgraph::flags::ReadExperimental("planner-v2"));
  EXPECT_FALSE(memgraph::flags::AreExperimentsEnabled(Experiments::VERSIONING));
  memgraph::flags::SetExperimental(memgraph::flags::ReadExperimental("versioning"));
  EXPECT_TRUE(memgraph::flags::AreExperimentsEnabled(Experiments::VERSIONING));
  memgraph::flags::SetExperimental(Experiments::NONE);
}
