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

#ifdef MG_ENTERPRISE

#include <cstdint>
#include <string>
#include <vector>

namespace memgraph::coordination {

struct RaftFollower {
  std::string name;
  uint64_t lag_entries;
  double last_response_seconds;
};

struct RaftStatus {
  uint64_t term;
  bool has_leader;
  // Empty unless this coordinator is the leader: only the leader tracks its followers' progress.
  std::vector<RaftFollower> followers;
};

}  // namespace memgraph::coordination

#endif
