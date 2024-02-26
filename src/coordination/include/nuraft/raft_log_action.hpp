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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_exceptions.hpp"

#include <cstdint>
#include <string>

namespace memgraph::coordination {

enum class RaftLogAction : uint8_t {
  REGISTER_REPLICATION_INSTANCE,
  UNREGISTER_REPLICATION_INSTANCE,
  SET_INSTANCE_AS_MAIN,
  SET_INSTANCE_AS_REPLICA
};

inline auto ParseRaftLogAction(std::string_view action) -> RaftLogAction {
  if (action == "register") {
    return RaftLogAction::REGISTER_REPLICATION_INSTANCE;
  }
  if (action == "unregister") {
    return RaftLogAction::UNREGISTER_REPLICATION_INSTANCE;
  }
  if (action == "promote") {
    return RaftLogAction::SET_INSTANCE_AS_MAIN;
  }
  if (action == "demote") {
    return RaftLogAction::SET_INSTANCE_AS_REPLICA;
  }
  throw InvalidRaftLogActionException("Invalid Raft log action: {}.", action);
}

}  // namespace memgraph::coordination
#endif
