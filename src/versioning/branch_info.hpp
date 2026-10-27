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

// `number` is never reused, even across drops (main is implicit version 1, the first branch is 2).
// `fork_ts` is the GC fork-pin timestamp, held until the branch is dropped.
// The gid floors are unset until the branch's core is first built.
// `recorded_fork_ts` is the fork_ts as recorded in the ledger: equal to `fork_ts` for a branch created in this run, but
// for a branch restored at startup `fork_ts` is the pin taken on the rebuilt history. Every ledger write and the
// retention floor use `recorded_fork_ts`; visibility uses `fork_ts`.
struct BranchInfo {
  uint64_t number;
  std::string parent;
  uint64_t fork_ts;
  uint64_t recorded_fork_ts;
  std::optional<std::string> description;
  std::optional<uint64_t> vertex_gid_floor;
  std::optional<uint64_t> edge_gid_floor;
};

}  // namespace memgraph::versioning
