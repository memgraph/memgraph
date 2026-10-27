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
#include <optional>
#include <string>

namespace memgraph::versioning {

// Gid floors are unset until the branch's core is first built.
struct BranchInfo {
  // Never reused, even across drops; main is implicit version 1, the first branch is 2.
  uint64_t number;
  std::string parent;
  // GC fork-pin timestamp, held until the branch is dropped; drives visibility.
  uint64_t fork_ts;
  // fork_ts as persisted in the ledger. Differs from fork_ts only after startup restore, where fork_ts is the pin
  // taken on the rebuilt history. Ledger writes and the retention floor use this one.
  uint64_t recorded_fork_ts;
  std::optional<std::string> description;
  std::optional<uint64_t> vertex_gid_floor;
  std::optional<uint64_t> edge_gid_floor;
};

}  // namespace memgraph::versioning
