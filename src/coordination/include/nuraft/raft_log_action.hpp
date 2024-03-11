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

#include "json/json.hpp"

namespace memgraph::coordination {

enum class RaftLogAction : uint8_t {
  REGISTER_REPLICATION_INSTANCE,
  UNREGISTER_REPLICATION_INSTANCE,
  SET_INSTANCE_AS_MAIN,
  SET_INSTANCE_AS_REPLICA,
  UPDATE_UUID
};

NLOHMANN_JSON_SERIALIZE_ENUM(RaftLogAction, {
                                                {RaftLogAction::REGISTER_REPLICATION_INSTANCE, "register"},
                                                {RaftLogAction::UNREGISTER_REPLICATION_INSTANCE, "unregister"},
                                                {RaftLogAction::SET_INSTANCE_AS_MAIN, "promote"},
                                                {RaftLogAction::SET_INSTANCE_AS_REPLICA, "demote"},
                                                {RaftLogAction::UPDATE_UUID, "update_uuid"},
                                            })

}  // namespace memgraph::coordination
#endif
