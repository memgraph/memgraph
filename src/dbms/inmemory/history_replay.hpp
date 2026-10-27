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

// Durable timestamp up to which a restart must recover main so that every branch's fork base can be rebuilt: readers at
// a fork_ts see exactly the commits with ts < fork_ts, and on main the MVCC commit ts is the durable ts. nullopt when
// the ledger holds no branch.
std::optional<uint64_t> BranchRecoveryBound(const versioning::LedgerContents &ledger);

// Replays `tail` (the WAL files left out of a bounded recovery) into `storage` as MVCC transactions, WAL appends
// suppressed, registering a fork pin for each entry of `fork_ts` right before the first transaction whose durable
// timestamp is >= it (after all of them if none is). `fork_ts` must be ascending. Then moves the storage clock past
// the last durable timestamp so new commits get fresh ids.
//
// Returns the pins in the order of `fork_ts`; the caller owns them. On failure no pin is held and `storage` is left
// partially replayed, so it must be discarded.
std::expected<std::vector<uint64_t>, std::string> ReplayHistoryTail(storage::InMemoryStorage &storage,
                                                                    const storage::durability::RecoveryTail &tail,
                                                                    std::span<const uint64_t> fork_ts);

}  // namespace memgraph::dbms
