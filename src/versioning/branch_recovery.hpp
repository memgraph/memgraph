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

namespace memgraph::versioning {

class VersionStore;

// Startup only, after VersionStore::Restore and with main fully recovered (fork pins held). For every restored branch
// whose directory holds a diff-engine WAL: builds the branch's core with its recorded gid floors, replays the WAL into
// the diff engine and the overlay store, rebuilds the filters, touched sets and `branched()` bits derived from the
// overlay, and registers the core with `store`. Then raises main's gid counters above every recorded floor and every
// branch-native gid so new objects never collide with a branch's.
//
// A branch whose WAL cannot be replayed is logged and dropped (registry, ledger and fork pin); other branches and main
// are unaffected. Never throws.
void RecoverBranchContents(VersionStore &store, storage::InMemoryStorage &main);

}  // namespace memgraph::versioning
