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

#include "replication_coordination_glue/role.hpp"

#include <array>
#include <nlohmann/json.hpp>
#include <range/v3/algorithm/find.hpp>
#include <string_view>

namespace memgraph::replication_coordination_glue {

namespace {
constexpr std::array<std::pair<ReplicationRole, std::string_view>, 2> kRoleMapping{
    {{ReplicationRole::MAIN, "main"}, {ReplicationRole::REPLICA, "replica"}}};
}  // namespace

void to_json(nlohmann::json &j, const ReplicationRole &role) {
  const auto *it = ranges::find(kRoleMapping, role, &std::pair<ReplicationRole, std::string_view>::first);
  j = std::string(it != kRoleMapping.end() ? it->second : kRoleMapping.front().second);
}

void from_json(const nlohmann::json &j, ReplicationRole &role) {
  const auto value = j.get<std::string>();
  const auto *it = ranges::find(kRoleMapping, value, &std::pair<ReplicationRole, std::string_view>::second);
  role = it != kRoleMapping.end() ? it->first : kRoleMapping.front().first;
}

}  // namespace memgraph::replication_coordination_glue
