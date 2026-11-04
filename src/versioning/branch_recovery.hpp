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

#include <expected>
#include <string>
#include <vector>

#include "storage/v2/inmemory/storagefwd.hpp"
#include "versioning/branch_ledger.hpp"

namespace memgraph::versioning {

class VersionStore;

// `branches` names only the branches whose contents failed; it is empty when the failure is not tied to one branch.
struct BranchRecoveryFailure {
  std::vector<std::string> branches;
  std::string reason;
};

// Startup only, after VersionStore::Restore and with main fully recovered (fork pins held). Replays each restored
// branch's diff-engine WAL into a core built with its recorded gid floors, rebuilds the filters, touched sets and
// `branched()` bits derived from the overlay, and registers the core with `store`. Then raises main's gid counters
// above every recorded floor and branch-native gid so new objects never collide with a branch's.
//
// WAL files are read the way main's recovery reads its own: a file with no readable header or no complete transaction
// is skipped (never renamed), an unfinalized file is replayed up to its last whole transaction, and a missing file in
// the sequence-number chain, a finalized file that fails its CRC, or a WAL that does not replay or cannot be listed
// makes the result an error naming each affected branch and why. Nothing is dropped, renamed or rewritten; the caller
// refuses to start, since keeping the branch would show main@fork as its contents. Main's gid counters are raised even
// on failure. Never throws.
[[nodiscard]] std::expected<void, BranchRecoveryFailure> RecoverBranchContents(VersionStore &store,
                                                                               storage::InMemoryStorage &main);

// Startup only, with the branches in `ledger` hidden: raises main's gid counters above every recorded floor and every
// gid a hidden branch's WAL created. An unreadable WAL contributes what was read before the damage. Never throws.
void RaiseGidCountersForHiddenBranches(const LedgerContents &ledger, storage::InMemoryStorage &main);

}  // namespace memgraph::versioning
