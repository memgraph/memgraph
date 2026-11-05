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
#include <filesystem>
#include <memory>
#include <string>

namespace memgraph::versioning {

class BranchCore;

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

}  // namespace memgraph::versioning
