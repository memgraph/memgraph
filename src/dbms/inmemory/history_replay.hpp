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

#ifdef MG_ENTERPRISE

#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "storage/v2/durability/durability.hpp"
#include "storage/v2/inmemory/storagefwd.hpp"
#include "versioning/branch_ledger.hpp"

namespace memgraph::storage::durability {
struct WalInfo;
}  // namespace memgraph::storage::durability

namespace memgraph::dbms {

// Applies the transactions of `wal_path` newer than the last durable timestamp as commits that do not append to the
// WAL, calling `on_txn_begin` with each one's durable timestamp before its first delta. False when the file cannot be
// read or replay consumes a delta count other than `wal_info.num_deltas`; throws on a decode failure.
bool ReplayWalFile(storage::InMemoryStorage &storage, std::filesystem::path const &wal_path,
                   storage::durability::WalInfo const &wal_info, std::function<void(uint64_t)> on_txn_begin);

// Durable ts up to which main must be recovered to rebuild every branch's base state: the earliest branch's recorded
// fork_ts - 1 (clamped at 0), since a fork sees commits with ts < fork_ts. nullopt when the ledger has no branch.
std::optional<uint64_t> BranchRecoveryBound(const versioning::LedgerContents &ledger);

// Replays `tail` into the freshly recovered `storage` as MVCC txns committed without a WAL append, registering a fork
// pin per `fork_ts` right before the first txn with durable ts >= it (after all if none), then bumps the clock past the
// last durable ts and to at least the newest `fork_ts`. Returns the pins (local MVCC timestamps, not durable ones) in
// `fork_ts` order, caller-owned. `fork_ts` must be ascending (duplicates allowed) and past `tail.reached_timestamp`.
// Precondition: called while the Database is being constructed, before `storage` is published (no sessions, triggers,
// streams or TTL). Periodic snapshots are paused for its duration; GC may run, fork pins protect the history.
// Nothing else may write to `storage` until it returns. On failure no pin is held and `storage` is partially
// replayed, so it must be discarded.
std::expected<std::vector<uint64_t>, std::string> ReplayHistoryTail(storage::InMemoryStorage &storage,
                                                                    const storage::durability::RecoveryTail &tail,
                                                                    std::span<const uint64_t> fork_ts);

}  // namespace memgraph::dbms

#endif
