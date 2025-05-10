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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_instance_context.hpp"
#include "utils/logging.hpp"

#include <nlohmann/json.hpp>

static constexpr std::string_view kId{"id"};
static constexpr std::string_view kBoltServer{"bolt_server"};

namespace memgraph::coordination {
void to_json(nlohmann::json &j, CoordinatorInstanceContext const &context) {
  j = nlohmann::json{{kId.data(), context.id}, {kBoltServer.data(), context.bolt_server}};
}
void from_json(nlohmann::json const &j, CoordinatorInstanceContext &context) {
  context.id = j.at(kId.data()).get<int32_t>();
  context.bolt_server = j.at(kBoltServer.data()).get<std::string>();
}

}  // namespace memgraph::coordination

#endif
