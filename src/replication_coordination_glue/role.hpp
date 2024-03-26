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

#include "json/json.hpp"

namespace memgraph::replication_coordination_glue {

// TODO: figure out a way of ensuring that usage of this type is never uninitialized/defaulted incorrectly to MAIN
enum class ReplicationRole : uint8_t { MAIN, REPLICA };

NLOHMANN_JSON_SERIALIZE_ENUM(ReplicationRole, {{ReplicationRole::MAIN, "main"}, {ReplicationRole::REPLICA, "replica"}})

}  // namespace memgraph::replication_coordination_glue
