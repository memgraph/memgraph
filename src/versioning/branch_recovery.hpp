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

// Replays the branch WAL in `wal_dir` into `core` (just built with the recorded gid floors, no session yet) once
// main is recovered to the fork point. Fork-object records go through the BranchContext write resolver as the live
// write did; a fork object main no longer has is an error. Afterwards the diff clock is past every replayed commit
// and WAL numbering continues. Files with no readable header or whole transaction are skipped, an unfinalized file
// is replayed to its last whole transaction; a seq-num gap or repeat, a CRC or summary failure in a finalized file
// or an inapplicable record is an error. No file is renamed, deleted or written. Never throws.
[[nodiscard]] std::expected<void, std::string> ReplayBranchWal(std::shared_ptr<BranchCore> core,
                                                               const std::filesystem::path &wal_dir);

}  // namespace memgraph::versioning
