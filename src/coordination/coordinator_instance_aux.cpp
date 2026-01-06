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

module;

#include <nlohmann/json.hpp>

module memgraph.coordination.coordinator_instance_aux;

#ifdef MG_ENTERPRISE

static constexpr std::string_view kId{"id"};
static constexpr std::string_view kManagementServer{"management_server"};
static constexpr std::string_view kCoordinatorServer{"coordinator_server"};

namespace memgraph::coordination {
void to_json(nlohmann::json &j, CoordinatorInstanceAux const &context) {
  j = nlohmann::json{{kId.data(), context.id},
                     {kManagementServer.data(), context.management_server},
                     {kCoordinatorServer.data(), context.coordinator_server}};
}

void from_json(nlohmann::json const &j, CoordinatorInstanceAux &context) {
  context.id = j.at(kId.data()).get<int32_t>();
  context.management_server = j.at(kManagementServer.data()).get<std::string>();
  context.coordinator_server = j.at(kCoordinatorServer.data()).get<std::string>();
}

}  // namespace memgraph::coordination

#endif
