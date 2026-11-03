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
#include <expected>
#include <functional>
#include <optional>
#include <string>

namespace memgraph::versioning {

enum class GidKind : uint8_t { kVertex, kEdge };

// Makes every native gid of the given kind below `through` durably the branch's and returns the ceiling now in force.
using GidReserver = std::function<std::expected<uint64_t, std::string>(GidKind, uint64_t through)>;

// Approximate vertex and edge counts, as Storage::Accessor::ApproximateVertexCount/ApproximateEdgeCount report them.
struct ObjectCounts {
  uint64_t vertices{0};
  uint64_t edges{0};

  friend bool operator==(const ObjectCounts &, const ObjectCounts &) = default;
};

// A branch's fork pin and main's counts read with it.
struct ForkPoint {
  uint64_t fork_ts;
  ObjectCounts counts;
};

struct BranchInfo {
  // Never reused, even across drops; main is implicit version 1, the first branch is 2.
  uint64_t number;
  std::string parent;
  // GC fork-pin timestamp, held until the branch is dropped; drives visibility.
  uint64_t fork_ts;
  // fork_ts as persisted in the ledger; differs from fork_ts only after startup restore, where fork_ts is the pin
  // taken on the rebuilt history. Defaults to fork_ts so an omitted initializer cannot persist 0.
  uint64_t recorded_fork_ts{fork_ts};
  // Unset until the branch's core is first built.
  std::optional<uint64_t> vertex_gid_floor;
  std::optional<uint64_t> edge_gid_floor;
  // Every native gid the branch has taken is below these, and they are durable before the gid is used. Absent (the
  // floor stands in) until the branch takes its first native gid.
  std::optional<uint64_t> vertex_gid_ceiling;
  std::optional<uint64_t> edge_gid_ceiling;
  // Wall-clock creation time, microseconds since the Unix epoch.
  std::optional<uint64_t> created_at_us;
  // Main's counts at the fork, the base of the branch's counts. Absent in ledgers written before they were recorded.
  std::optional<ObjectCounts> fork_counts;
};

// The gid at or above which the ledger knows no object of the branch: its ceiling, or its floor while it has taken no
// native gid, or 0 while its floors are unset.
inline uint64_t GidsHeldBelow(const BranchInfo &info, GidKind kind) {
  const auto &floor = kind == GidKind::kVertex ? info.vertex_gid_floor : info.edge_gid_floor;
  const auto &ceiling = kind == GidKind::kVertex ? info.vertex_gid_ceiling : info.edge_gid_ceiling;
  return std::max(floor.value_or(0), ceiling.value_or(0));
}

// The durable timestamp the branch still needs recoverable: one below its recorded fork, clamped at 0.
inline uint64_t RecoverableBelow(const BranchInfo &info) {
  return info.recorded_fork_ts == 0 ? 0 : info.recorded_fork_ts - 1;
}

}  // namespace memgraph::versioning
