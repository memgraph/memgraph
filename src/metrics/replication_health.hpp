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
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace memgraph::metrics {

struct ReplicaHealth {
  std::string replica;
  std::string database;
  // Every state the replica can be in, each flagged true when it is the current one.
  std::vector<std::pair<std::string_view, bool>> states;
  int64_t txns_behind;
};

struct ReplicationHealth {
  bool is_main;
  bool writeable;
  uint64_t registered_replicas;
  std::vector<ReplicaHealth> replicas;
};

}  // namespace memgraph::metrics
