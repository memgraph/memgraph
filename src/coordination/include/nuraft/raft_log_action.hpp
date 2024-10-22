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

enum class RaftLogAction : uint8_t { OPEN_LOCK, UPDATE_CLUSTER_STATE, CLOSE_LOCK };

NLOHMANN_JSON_SERIALIZE_ENUM(RaftLogAction, {{RaftLogAction::OPEN_LOCK, "open_lock"},
                                             {RaftLogAction::UPDATE_CLUSTER_STATE, "update_cluster_state"},
                                             {RaftLogAction::CLOSE_LOCK, "close_lock"}})

}  // namespace memgraph::coordination
#endif
