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

TEST(VersioningGate, AllowedWhenEverythingSatisfied) {
  ASSERT_EQ(CheckVersioningGate(/*flag_enabled=*/true,
                                /*enterprise_valid=*/true,
                                /*is_in_memory_transactional=*/true,
                                /*wal_enabled=*/true),
            std::nullopt);
}

TEST(VersioningGate, DisabledWhenFlagOff) {
  ASSERT_EQ(CheckVersioningGate(/*flag_enabled=*/false,
                                /*enterprise_valid=*/true,
                                /*is_in_memory_transactional=*/true,
                                /*wal_enabled=*/true),
            GateError::kDisabled);
}

TEST(VersioningGate, FlagOffWinsOverAllOtherSimultaneousFailures) {
  ASSERT_EQ(CheckVersioningGate(/*flag_enabled=*/false,
                                /*enterprise_valid=*/false,
                                /*is_in_memory_transactional=*/false,
                                /*wal_enabled=*/false),
            GateError::kDisabled);
}

TEST(VersioningGate, NoLicenseWhenEnterpriseInvalid) {
  ASSERT_EQ(CheckVersioningGate(/*flag_enabled=*/true,
                                /*enterprise_valid=*/false,
                                /*is_in_memory_transactional=*/true,
                                /*wal_enabled=*/true),
            GateError::kNoLicense);
}

TEST(VersioningGate, UnsupportedStorageModeWhenNotInMemoryTransactional) {
  ASSERT_EQ(CheckVersioningGate(/*flag_enabled=*/true,
                                /*enterprise_valid=*/true,
                                /*is_in_memory_transactional=*/false,
                                /*wal_enabled=*/true),
            GateError::kUnsupportedStorageMode);
}

TEST(VersioningGate, WalDisabledWhenWalOff) {
  ASSERT_EQ(CheckVersioningGate(/*flag_enabled=*/true,
                                /*enterprise_valid=*/true,
                                /*is_in_memory_transactional=*/true,
                                /*wal_enabled=*/false),
            GateError::kWalDisabled);
}

TEST(VersioningGate, ErrorMessagesMatchSpecVerbatim) {
  ASSERT_EQ(GateErrorMessage(GateError::kDisabled),
            "Versioning is disabled. Start Memgraph with --versioning-enabled=true to use branch queries.");
  ASSERT_EQ(GateErrorMessage(GateError::kNoLicense),
            "Graph versioning is an enterprise feature. A valid Memgraph Enterprise License is required.");
  ASSERT_EQ(GateErrorMessage(GateError::kUnsupportedStorageMode),
            "Graph versioning requires IN_MEMORY_TRANSACTIONAL storage mode. It is not available in "
            "IN_MEMORY_ANALYTICAL or ON_DISK_TRANSACTIONAL.");
  ASSERT_EQ(GateErrorMessage(GateError::kWalDisabled),
            "Graph versioning requires durability with WAL enabled (--storage-wal-enabled=true). Enable "
            "periodic snapshots + WAL to use branches.");
}
