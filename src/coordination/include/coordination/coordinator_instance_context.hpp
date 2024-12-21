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

#include <cstdint>
#include <string>

#include "json/json.hpp"

#pragma once

#ifdef MG_ENTERPRISE

namespace memgraph::coordination {
struct CoordinatorInstanceContext {
  uint32_t id;
  std::string bolt_server;
  std::string management_server;

  friend bool operator==(CoordinatorInstanceContext const &, CoordinatorInstanceContext const &) = default;
};

void to_json(nlohmann::json &j, CoordinatorInstanceContext const &context);
void from_json(nlohmann::json const &j, CoordinatorInstanceContext &context);

auto DeserializeRaftContext(std::string const &user_ctx) -> std::map<uint32_t, CoordinatorInstanceContext>;
auto SerializeRaftContext(std::map<uint32_t, CoordinatorInstanceContext> const &servers) -> std::string;

}  // namespace memgraph::coordination

#endif
