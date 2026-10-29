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

#include "storage/v2/inmemory/storagefwd.hpp"
#include "versioning/branch_ledger.hpp"

namespace memgraph::versioning {

class VersionStore;

// Startup only, after VersionStore::Restore and with main fully recovered (fork pins held). Replays each restored
// branch's diff-engine WAL into a core built with its recorded gid floors, rebuilds the filters, touched sets and
// `branched()` bits derived from the overlay, and registers the core with `store`. Then raises main's gid counters
// above every recorded floor and branch-native gid so new objects never collide with a branch's.
//
// WAL files are read the way main's recovery reads its own: a file with no readable header or no complete transaction
// is skipped (never renamed), an unfinalized file is replayed up to its last whole transaction, and a missing file in
// the sequence-number chain, a finalized file that fails its CRC, or a WAL that does not replay or cannot be listed is
// logged and the branch dropped (registry, ledger, fork pin), since keeping it would show main@fork as its contents.
// Other branches and main are unaffected, and main's gid counters are raised even if a branch fails. Never throws.
void RecoverBranchContents(VersionStore &store, storage::InMemoryStorage &main);

// Startup only, with the branches in `ledger` hidden: raises main's gid counters above every recorded floor and every
// gid a hidden branch's WAL created. An unreadable WAL contributes what was read before the damage. Never throws.
void RaiseGidCountersForHiddenBranches(const LedgerContents &ledger, storage::InMemoryStorage &main);

}  // namespace memgraph::versioning
