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

#pragma once

#include <cstdint>

#include "storage/v2/timestamp_types.hpp"

namespace memgraph::storage {

constexpr uint64_t kTimestampInitialId = 0;
constexpr uint64_t kTransactionInitialId = 1ULL << 63U;
/// Largest value still in the committed-timestamp space. Use as a saturating ts
/// for reads that want "everything committed, nothing populating/in-flight":
/// IsVisible(commit_ts) returns true for every committed entry and false for
/// the kPopulating sentinel (which equals kTransactionInitialId).
constexpr CommitTimestamp kLargestCommittedTimestamp = CommitTimestamp::FromUint(kTransactionInitialId - 1);
/// Where the commit-timestamp space begins. Recovery publishes a schema object at it so that every
/// transaction sees it, there being no earlier boundary to read at.
constexpr CommitTimestamp kInitialCommitTimestamp = CommitTimestamp::FromUint(kTimestampInitialId);

}  // namespace memgraph::storage
