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
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "storage/v2/durability/durability.hpp"
#include "storage/v2/inmemory/storagefwd.hpp"
#include "versioning/branch_ledger.hpp"

namespace memgraph::dbms {

// Durable ts up to which main must be recovered so every branch's fork base can be rebuilt: fork_ts - 1, since readers
// at a fork_ts see commits with ts < fork_ts. nullopt when the ledger holds no branch.
std::optional<uint64_t> BranchRecoveryBound(const versioning::LedgerContents &ledger);

// Replays `tail` into `storage` as MVCC txns with WAL appends suppressed, registering a fork pin per (ascending)
// `fork_ts` right before the first txn with durable ts >= it (after all if none), then bumps the clock past the last
// durable ts and to at least the newest `fork_ts`. Returns the pins in `fork_ts` order, caller-owned. On failure no pin
// is held and `storage` is partially replayed, so it must be discarded.
std::expected<std::vector<uint64_t>, std::string> ReplayHistoryTail(storage::InMemoryStorage &storage,
                                                                    const storage::durability::RecoveryTail &tail,
                                                                    std::span<const uint64_t> fork_ts);

}  // namespace memgraph::dbms
