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

#include "replication_coordination_glue/mode.hpp"

#include <array>
#include <nlohmann/json.hpp>
#include <range/v3/algorithm/find.hpp>
#include <string_view>

namespace memgraph::replication_coordination_glue {

namespace {
constexpr std::array<std::pair<ReplicationMode, std::string_view>, 3> kModeMapping{
    {{ReplicationMode::SYNC, "sync"},
     {ReplicationMode::ASYNC, "async"},
     {ReplicationMode::STRICT_SYNC, "strict_sync"}}};
}  // namespace

void to_json(nlohmann::json &j, const ReplicationMode &mode) {
  const auto *it = ranges::find(kModeMapping, mode, &std::pair<ReplicationMode, std::string_view>::first);
  j = std::string(it != kModeMapping.end() ? it->second : kModeMapping.front().second);
}

void from_json(const nlohmann::json &j, ReplicationMode &mode) {
  const auto value = j.get<std::string>();
  const auto *it = ranges::find(kModeMapping, value, &std::pair<ReplicationMode, std::string_view>::second);
  mode = it != kModeMapping.end() ? it->first : kModeMapping.front().first;
}

}  // namespace memgraph::replication_coordination_glue
