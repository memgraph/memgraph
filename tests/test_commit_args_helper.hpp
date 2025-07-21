// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include "storage/v2/commit_args.hpp"
#include "storage/v2/database_access.hpp"

namespace memgraph::tests {

// Test-only implementation of DatabaseProtector for unit tests
// This is a minimal implementation that satisfies the interface requirements
struct TestDatabaseProtector : storage::DatabaseProtector {
  auto clone() const -> storage::DatabaseProtectorPtr override { return std::make_unique<TestDatabaseProtector>(); }
};

// Helper function for unit tests to create CommitArgs for MAIN with a dummy protector
// Use this when replication is not involved (which is most unit tests)
inline auto MakeMainCommitArgs() -> storage::CommitArgs {
  // Create a test database protector
  // This is safe for tests that don't involve replication
  auto dummy_protector = std::make_unique<TestDatabaseProtector>();
  return storage::CommitArgs::make_main(std::move(dummy_protector));
}

}  // namespace memgraph::tests
