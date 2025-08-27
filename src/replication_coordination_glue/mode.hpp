// Copyright 2025 Memgraph Ltd.
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
#include <string>

#include <nlohmann/json_fwd.hpp>

using namespace std::string_view_literals;

namespace memgraph::replication_coordination_glue {

enum class ReplicationMode : std::uint8_t { SYNC, ASYNC, STRICT_SYNC };

// JSON serialization functions
void to_json(nlohmann::json &j, const ReplicationMode &mode);
void from_json(const nlohmann::json &j, ReplicationMode &mode);

}  // namespace memgraph::replication_coordination_glue
