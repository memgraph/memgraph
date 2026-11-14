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
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>

#include "storage/v2/inmemory/storagefwd.hpp"
#include "versioning/branch_ledger.hpp"

namespace memgraph::versioning {

class BranchCore;
class VersionStore;

// Replays the branch WAL in `wal_dir` into `core` (just built with the recorded gid floors, no session yet) once
// main is recovered to the fork point. Fork-object records go through the BranchContext write resolver as the live
// write did; a fork object main no longer has is an error. Afterwards the diff clock is past every replayed commit
// and WAL numbering continues. Files with no readable header or whole transaction are skipped, an unfinalized file
// is replayed to its last whole transaction; a seq-num gap or repeat, a CRC or summary failure in a finalized file
// or an inapplicable record is an error. No file is renamed, deleted or written. Never throws.
[[nodiscard]] std::expected<void, std::string> ReplayBranchWal(std::shared_ptr<BranchCore> core,
                                                               const std::filesystem::path &wal_dir);

// Startup only, after VersionStore::Restore, main recovered to the fork pins: replays (ReplayBranchWal) every restored
// branch with WAL files; a damaged one is marked unavailable (VersionStore::MarkUnavailable) and warned about, its
// files, pin and ledger entry kept, and the start goes on. A branch in `unrecoverable` (FindHistoryShortfalls) is set
// aside without reading its files. Raises main's gid counters to every ledger ceiling. Never throws.
void RecoverBranches(VersionStore &store, storage::InMemoryStorage &main,
                     const std::unordered_map<uint64_t, std::string> &unrecoverable);

// The branches whose fork base main did not recover, by number with the reason. `durable_at_fork[i]` is the last
// durable ts main recovered below the fork of `ledger.branches[i]` (ReplayHistoryTail); a branch is short when main had
// committed more by the fork. Branches whose ledger predates that record are never short.
[[nodiscard]] std::unordered_map<uint64_t, std::string> FindHistoryShortfalls(
    const LedgerContents &ledger, std::span<const uint64_t> durable_at_fork);

// Startup only, with the branches in `ledger` hidden (versioning off): raises main's gid counters to the gid ceilings
// the ledger records and moves main's clock past the newest fork point, so commits made meanwhile stay above every
// fork. Reads no branch file. Never throws.
void KeepMainClearOfHiddenBranches(const LedgerContents &ledger, storage::InMemoryStorage &main);

// KeepMainClearOfHiddenBranches' counter raise for the branches `store` holds, for an instance that becomes MAIN: a
// snapshot or WAL loaded while it was a REPLICA may have set the counters below a branch's gids.
void RaiseGidCountersForBranches(const VersionStore &store, storage::InMemoryStorage &main);

}  // namespace memgraph::versioning
