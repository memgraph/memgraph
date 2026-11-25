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
#include <string>
#include <string_view>

namespace memgraph::versioning {

// Ok if `name` is a valid branch name, else a user-facing error.
// Valid: 1..63 chars of [A-Za-z0-9._-], alphanumeric at both ends. Exact "main" is reserved;
// '@' is rejected as the <database>@<branch> separator.
std::expected<void, std::string> ValidateBranchName(std::string_view name);

// Approximate counts, as Storage::Accessor::ApproximateVertexCount/ApproximateEdgeCount report them.
struct ObjectCounts {
  uint64_t vertices{0};
  uint64_t edges{0};

  friend bool operator==(const ObjectCounts &, const ObjectCounts &) = default;
};

// What a branch forked from; persisted in the ledger, every field required.
struct ForkPoint {
  // Upper bound of the fork in main's durable timestamps: every commit below it is in the fork. Equals the MVCC pin at
  // CREATE; after a restart the pin is retaken on the rebuilt history and differs.
  uint64_t durable_fork_ts{0};
  // Main's last durable commit when the pin was taken (lower bound): main recovering less lost history the branch
  // forked from.
  uint64_t main_durable_ts{0};
  // Main's id counters right after the pin; every gid visible at the fork is below them.
  uint64_t vertex_gid_floor{0};
  uint64_t edge_gid_floor{0};
  // Main's approximate counts at the fork, the base of the branch's counts.
  ObjectCounts counts;

  friend bool operator==(const ForkPoint &, const ForkPoint &) = default;
};

// One ledger entry, minus the name.
struct BranchInfo {
  // Never reused, even across drops. 1 is main (implicit, never stored), so the first branch is 2.
  uint64_t id{0};
  ForkPoint fork;
  // Wall clock, microseconds since the Unix epoch.
  uint64_t created_at_us{0};
};

}  // namespace memgraph::versioning
