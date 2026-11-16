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

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>

namespace memgraph::versioning {

enum class GidKind : uint8_t { kVertex, kEdge };

struct BranchInfo {
  // Never reused, even across drops; main is implicit version 1, the first branch is 2.
  uint64_t number;
  std::string parent;
  // This process's MVCC fork-pin timestamp, held until the branch is dropped; drives visibility and GC.
  uint64_t fork_ts;
  // The fork point in main's durable timestamps; the ledger persists only this. Equal to fork_ts until a restart:
  // replay recommits main's tail at fresh MVCC timestamps, so the rebuilt history's pin differs. Defaults to fork_ts
  // so an omitted initializer cannot persist 0.
  uint64_t durable_fork_ts{fork_ts};
  // Unset until the branch's core is first built.
  std::optional<uint64_t> vertex_gid_floor;
  std::optional<uint64_t> edge_gid_floor;
  // Every native gid the branch has taken is below these, and they are durable before the gid is used. Absent (the
  // floor stands in) until the branch takes its first native gid.
  std::optional<uint64_t> vertex_gid_ceiling;
  std::optional<uint64_t> edge_gid_ceiling;
  // Wall-clock creation time, microseconds since the Unix epoch.
  std::optional<uint64_t> created_at_us;
};

// The gid at or above which the ledger knows no object of the branch: its ceiling, or its floor while it has taken no
// native gid, or 0 while its floors are unset.
inline uint64_t GidsHeldBelow(const BranchInfo &info, GidKind kind) {
  const auto &floor = kind == GidKind::kVertex ? info.vertex_gid_floor : info.edge_gid_floor;
  const auto &ceiling = kind == GidKind::kVertex ? info.vertex_gid_ceiling : info.edge_gid_ceiling;
  return std::max(floor.value_or(0), ceiling.value_or(0));
}

}  // namespace memgraph::versioning
