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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_instance_context.hpp"
#include "utils/logging.hpp"

static constexpr std::string_view kBoltServer{"bolt_server"};
static constexpr std::string_view kManagementServer{"management_server"};

namespace memgraph::coordination {
void to_json(nlohmann::json &j, CoordinatorInstanceContext const &context) {
  j = nlohmann::json{{kBoltServer, context.bolt_server}, {kManagementServer, context.management_server}};
}
void from_json(nlohmann::json const &j, CoordinatorInstanceContext &context) {
  context.bolt_server = j.at("bolt_server").get<std::string>();
  context.management_server = j.at("management_server").get<std::string>();
}

auto DeserializeRaftContext(std::string const &user_ctx) -> std::map<uint32_t, CoordinatorInstanceContext> {
  if (user_ctx.empty()) {
    return {};
  }
  std::map<uint32_t, CoordinatorInstanceContext> servers;
  try {
    auto const parsedJson = nlohmann::json::parse(user_ctx);
    // Deserialize JSON object into the map
    for (auto it = parsedJson.begin(); it != parsedJson.end(); ++it) {
      int const server_id = std::stoi(it.key());                          // Convert string keys back to int
      servers[server_id] = it.value().get<CoordinatorInstanceContext>();  // Use from_json
    }
  } catch (nlohmann::json::exception const &e) {
    MG_ASSERT(false, "Error when parsing context of raft servers {}.", e.what());
  }
  return servers;
}

auto SerializeRaftContext(std::map<uint32_t, CoordinatorInstanceContext> const &servers) -> std::string {
  if (servers.empty()) {
    return "";
  }

  nlohmann::json j;
  for (auto const &[server_id, server_context] : servers) {
    j[std::to_string(server_id)] = server_context;
  }
  return j.dump();
}

}  // namespace memgraph::coordination

#endif
