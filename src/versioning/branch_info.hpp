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
#include <optional>
#include <string>
#include <string_view>

namespace memgraph::versioning {

// Ok if `name` is a valid branch name, else a user-facing error.
// Valid: 1..63 chars of [A-Za-z0-9._-], alphanumeric at both ends. Exact "main" is reserved;
// '@' is rejected as the <database>@<branch> separator.
std::expected<void, std::string> ValidateBranchName(std::string_view name);

enum class GidKind : uint8_t { kVertex, kEdge };

struct BranchInfo {
  // Never reused, even across drops. 1 is main (implicit, never stored), so the first branch is 2.
  uint64_t id;
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
  // Validate rejects a ceiling below the floor, so the ceiling, when set, is the larger.
  return ceiling.value_or(floor.value_or(0));
}

}  // namespace memgraph::versioning
