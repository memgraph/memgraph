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

#include <optional>
#include <string>
#include <utility>

#include "flags/experimental.hpp"
#include "versioning/gate.hpp"

using memgraph::versioning::BranchesDurable;
using memgraph::versioning::CheckVersioningGate;
using memgraph::versioning::GateError;
using memgraph::versioning::GateErrorMessage;
using memgraph::versioning::GateOperation;
using memgraph::versioning::GateState;
using memgraph::versioning::ValidateStartupConfig;

TEST(VersioningGate, ExhaustiveTruthTableHonoursPrecedence) {
  for (const auto op : {GateOperation::kObserve, GateOperation::kCreate}) {
    const bool creating = op == GateOperation::kCreate;
    for (unsigned bits = 0; bits < 8; ++bits) {
      const GateState state{.flag_enabled = (bits & 1U) != 0,
                            .enterprise_valid = (bits & 2U) != 0,
                            .is_in_memory_transactional = (bits & 4U) != 0};
      std::optional<GateError> expected;
      if (!state.flag_enabled) {
        expected = GateError::kDisabled;
      } else if (creating && !state.enterprise_valid) {
        expected = GateError::kNoLicense;
      } else if (!state.is_in_memory_transactional) {
        expected = GateError::kUnsupportedStorageMode;
      }
      const auto result = CheckVersioningGate(op, state);
      EXPECT_EQ(result ? std::nullopt : std::optional{result.error()}, expected)
          << "creating=" << creating << " bits=" << bits;
    }
  }
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
}

TEST(VersioningGate, BranchesDurableNeedsWalAndRecoveryOnStartup) {
  using SnapshotWalMode = memgraph::storage::Config::Durability::SnapshotWalMode;
  for (const auto mode :
       {SnapshotWalMode::DISABLED, SnapshotWalMode::PERIODIC_SNAPSHOT, SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL}) {
    for (const bool recover : {false, true}) {
      memgraph::storage::Config config;
      config.durability.snapshot_wal_mode = mode;
      config.durability.recover_on_startup = recover;
      EXPECT_EQ(BranchesDurable(config), recover && mode == SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL);
    }
  }
}

TEST(VersioningGate, ValidateStartupConfigNamesTheSettingThatCannotHostBranches) {
  using SnapshotWalMode = memgraph::storage::Config::Durability::SnapshotWalMode;
  using memgraph::storage::StorageMode;
  const auto config_with = [](SnapshotWalMode wal, bool recover) {
    memgraph::storage::Config config;
    config.durability.snapshot_wal_mode = wal;
    config.durability.recover_on_startup = recover;
    return config;
  };
  const auto durable = config_with(SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL, true);
  EXPECT_TRUE(ValidateStartupConfig(StorageMode::IN_MEMORY_TRANSACTIONAL, durable).has_value());

  for (const auto mode : {StorageMode::IN_MEMORY_ANALYTICAL, StorageMode::ON_DISK_TRANSACTIONAL}) {
    const auto error = ValidateStartupConfig(mode, durable);
    ASSERT_FALSE(error.has_value());
    EXPECT_NE(error.error().find("IN_MEMORY_TRANSACTIONAL"), std::string::npos) << error.error();
    EXPECT_NE(error.error().find("--experimental-enabled=versioning"), std::string::npos) << error.error();
  }
  for (const auto &config : {config_with(SnapshotWalMode::DISABLED, true),
                             config_with(SnapshotWalMode::PERIODIC_SNAPSHOT, true),
                             config_with(SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL, false)}) {
    const auto error = ValidateStartupConfig(StorageMode::IN_MEMORY_TRANSACTIONAL, config);
    ASSERT_FALSE(error.has_value());
    EXPECT_NE(error.error().find("--data-recovery-on-startup=true"), std::string::npos) << error.error();
    EXPECT_NE(error.error().find("PERIODIC_SNAPSHOT_WITH_WAL"), std::string::npos) << error.error();
  }
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
  memgraph::flags::SetExperimental(memgraph::flags::ReadExperimental("commit-lock-narrowing"));
  EXPECT_FALSE(memgraph::flags::AreExperimentsEnabled(Experiments::VERSIONING));
  EXPECT_TRUE(memgraph::flags::AreExperimentsEnabled(Experiments::COMMIT_LOCK_NARROWING));
  memgraph::flags::SetExperimental(memgraph::flags::ReadExperimental("versioning"));
  EXPECT_TRUE(memgraph::flags::AreExperimentsEnabled(Experiments::VERSIONING));
  EXPECT_FALSE(memgraph::flags::AreExperimentsEnabled(Experiments::COMMIT_LOCK_NARROWING));
  memgraph::flags::SetExperimental(Experiments::NONE);
}
