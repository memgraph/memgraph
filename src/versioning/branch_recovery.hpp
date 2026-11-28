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

#include "storage/v2/inmemory/storagefwd.hpp"

namespace memgraph::versioning {

class BranchCore;
class VersionStore;

// One past the highest native gid the replay created, 0 where it created none. The caller raises main's id counters to
// at least these before the database serves, so main never hands out a gid the branch uses.
struct NativeGidEnds {
  uint64_t vertex{0};
  uint64_t edge{0};
};

// Replays the branch WAL in `wal_dir` into `core` (just built from the branch's ForkPoint, no session yet) once main is
// recovered to the fork point. Base-object records go through the BranchContext write resolver as the live write did;
// a base object main no longer has is an error. Native objects keep their recorded gids. Afterwards the diff clock is
// past every replayed commit and WAL numbering continues. An unfinalized file with no readable header or whole
// transaction is skipped, one with some is replayed to its last whole transaction; a seq-num gap or repeat, an
// unreadable finalized file, a CRC failure or an inapplicable record is an error. A branch whose only files are
// unreadable starts as forked, with a warning. No file is renamed, deleted or written. Never throws.
[[nodiscard]] std::expected<NativeGidEnds, std::string> ReplayBranchWal(std::shared_ptr<BranchCore> core,
                                                                        const std::filesystem::path &wal_dir);

// Startup only, after VersionStore::Restore and before the database serves: replays every available branch that has WAL
// files into a core the store keeps, then raises main's id counters to at least the native gids each replay created.
// A branch that cannot be recovered (damaged files, a replay error, a native gid main already holds) is marked
// unavailable (VersionStore::MarkUnavailable) and the start goes on; main is never changed by a failing branch. A
// missing branch or WAL directory is such a failure (CREATE BRANCH makes both); a branch with no WAL file has nothing
// to replay. Never throws.
void RecoverBranches(VersionStore &store, storage::InMemoryStorage &main);

}  // namespace memgraph::versioning
