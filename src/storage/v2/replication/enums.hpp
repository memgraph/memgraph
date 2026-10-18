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
#include <array>
#include <cstdint>
#include <string_view>

namespace memgraph::storage::replication {

enum class ReplicaState : std::uint8_t { READY, REPLICATING, RECOVERY, MAYBE_BEHIND, DIVERGED_FROM_MAIN };

inline constexpr std::array kReplicaStates{ReplicaState::READY,
                                           ReplicaState::REPLICATING,
                                           ReplicaState::RECOVERY,
                                           ReplicaState::MAYBE_BEHIND,
                                           ReplicaState::DIVERGED_FROM_MAIN};

// The status users see for a replica, in SHOW REPLICAS and in metrics.
constexpr auto ReplicaStatusName(ReplicaState const state) -> std::string_view {
  switch (state) {
    using enum ReplicaState;
    case READY:
      return "ready";
    case REPLICATING:
      return "replicating";
    case RECOVERY:
      return "recovery";
    case MAYBE_BEHIND:
      return "invalid";
    case DIVERGED_FROM_MAIN:
      return "diverged";
  }
}

}  // namespace memgraph::storage::replication
