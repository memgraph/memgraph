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

struct BranchInfo {
  // Never reused, even across drops. 1 is main (implicit, never stored), so the first branch is 2.
  uint64_t id;
  // This process's MVCC fork-pin timestamp, held until the branch is dropped; drives visibility and GC.
  uint64_t fork_ts;
  // The fork point in main's durable timestamps; the ledger persists only this. Equal to fork_ts until a restart:
  // replay recommits main's tail at fresh MVCC timestamps, so the rebuilt history's pin differs. Defaults to fork_ts
  // so an omitted initializer cannot persist 0.
  uint64_t durable_fork_ts{fork_ts};
  // Main's id counters read right after the fork pin; every gid visible at the fork is below them.
  uint64_t vertex_gid_floor{0};
  uint64_t edge_gid_floor{0};
  // Wall-clock creation time, microseconds since the Unix epoch.
  std::optional<uint64_t> created_at_us;
};

}  // namespace memgraph::versioning
