// Copyright 2024 Memgraph Ltd.
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
#include <map>
#include <stdexcept>
#include <string>

namespace memgraph::replication_coordination_glue {

enum class ReplicationMode : std::uint8_t { SYNC, ASYNC };

inline auto ReplicationModeToString(ReplicationMode mode) -> std::string {
  switch (mode) {
    case ReplicationMode::SYNC:
      return "SYNC";
    case ReplicationMode::ASYNC:
      return "ASYNC";
  }
  throw std::invalid_argument("Invalid replication mode");
}

inline auto ReplicationModeFromString(std::string_view mode) -> ReplicationMode {
  if (mode == "SYNC") {
    return ReplicationMode::SYNC;
  }
  if (mode == "ASYNC") {
    return ReplicationMode::ASYNC;
  }
  throw std::invalid_argument("Invalid replication mode");
}

}  // namespace memgraph::replication_coordination_glue
