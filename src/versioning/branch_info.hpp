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
// `fork_durable_ts` is main's last durable commit timestamp read together with `fork_ts`; unlike `fork_ts` it is
// meaningful across restarts. The gid floors are unset until the branch's core is first built.
struct BranchInfo {
  uint64_t number;
  std::string parent;
  uint64_t fork_ts;
  std::optional<std::string> description;
  uint64_t fork_durable_ts{0};
  std::optional<uint64_t> vertex_gid_floor;
  std::optional<uint64_t> edge_gid_floor;
};

}  // namespace memgraph::versioning
