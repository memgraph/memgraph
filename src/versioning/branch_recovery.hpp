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
#include <string>
#include <string_view>
#include <vector>

#include "storage/v2/inmemory/storagefwd.hpp"
#include "versioning/branch_ledger.hpp"

namespace memgraph::versioning {

class BranchCore;
class VersionStore;

// Replays the branch WAL in `wal_dir` (an existing directory) into `core`, which was just built with the branch's
// recorded gid floors and is not yet used by any session. Main must already be recovered up to the branch's fork point
// (Database does that before any branch is restored): a record on a fork object is applied to main@fork_ts through the
// BranchContext write resolver, exactly as the live write was, so the stubs, change filters and fork-delete counts come
// back with it. Records on branch-native objects are the diff engine's stock operations. A fork object the main
// history no longer has is an error, never silently skipped. Afterwards the diff engine's clock is past every replayed
// commit and its next WAL file continues the sequence numbering.
//
// WAL files are read the way main's recovery reads its own: a file with no readable header or no complete transaction
// is skipped (never renamed), an unfinalized file is replayed up to its last whole transaction, and a missing file in
// the sequence-number chain, a finalized file that fails its CRC, or a record that does not apply is an error. Nothing
// is dropped, renamed or rewritten. Never throws.
[[nodiscard]] std::expected<void, std::string> ReplayBranchWal(std::shared_ptr<BranchCore> core,
                                                               const std::filesystem::path &wal_dir);

// A branch whose WAL did not replay at startup.
struct BranchRecoveryFailure {
  std::string name;
  uint64_t number{0};
  std::string reason;
};

// Startup only, after VersionStore::Restore and with main recovered up to the fork pins: builds and replays (see
// ReplayBranchWal) the core of every restored branch that has WAL files and registers it with `store`; a branch with
// none is built empty on its first checkout. Returns the branches that could not be replayed, empty on success; the
// caller must refuse to start then (see DescribeRecoveryRefusal), because serving without such a branch would let main
// move past it. Nothing is dropped, renamed or rewritten. Finally raises main's gid counters past every recorded floor
// and every native gid of every replayed branch. Never throws.
[[nodiscard]] std::vector<BranchRecoveryFailure> RecoverBranches(VersionStore &store, storage::InMemoryStorage &main);

// The message for a startup refused over `failures`: per branch the replay error and its directory, WAL directory and
// the ledger file, then the steps that let the instance start again.
[[nodiscard]] std::string DescribeRecoveryRefusal(std::string_view database,
                                                  const std::filesystem::path &storage_directory,
                                                  const std::vector<BranchRecoveryFailure> &failures);

// Startup only, with the branches in `ledger` hidden (versioning off): raises main's gid counters like RecoverBranches
// and moves main's clock past the newest fork point, so commits made meanwhile stay above every fork. An unreadable
// WAL contributes what was read before the damage. Never throws.
void KeepMainClearOfHiddenBranches(const LedgerContents &ledger, storage::InMemoryStorage &main);

// KeepMainClearOfHiddenBranches' counter raise for the branches `store` holds, for an instance that becomes MAIN: a
// snapshot or WAL loaded while it was a REPLICA may have set the counters below a branch's floor. Never throws.
void RaiseGidCountersForBranches(const VersionStore &store, storage::InMemoryStorage &main);

}  // namespace memgraph::versioning
