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
#include <string>

namespace memgraph::versioning {

class BranchCore;

// Replays the branch WAL in `wal_dir` (an existing directory) into `core`, which was just built with the branch's
// recorded gid floors and is not yet used by any session. Branch-native vertices and edges are recreated through the
// diff engine; an endpoint that is a fork vertex resolves to its anchor, as BranchContext::CowVertex does. Afterwards
// the diff engine's clock is past every replayed commit and its next WAL file continues the sequence numbering.
//
// WAL files are read the way main's recovery reads its own: a file with no readable header or no complete transaction
// is skipped (never renamed), an unfinalized file is replayed up to its last whole transaction, and a missing file in
// the sequence-number chain, a finalized file that fails its CRC, or a delta that does not apply is an error. Nothing
// is dropped, renamed or rewritten. Never throws.
[[nodiscard]] std::expected<void, std::string> ReplayBranchWal(BranchCore &core, const std::filesystem::path &wal_dir);

}  // namespace memgraph::versioning
