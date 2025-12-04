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

#include <cstdint>
#include <string>

#include <nlohmann/json_fwd.hpp>

export module memgraph.coordination.coordinator_instance_context;

#ifdef MG_ENTERPRISE

export namespace memgraph::coordination {
// Context saved about each coordinator in Raft logs (app log)
struct CoordinatorInstanceContext {
  int32_t id;
  std::string bolt_server;

  friend bool operator==(CoordinatorInstanceContext const &, CoordinatorInstanceContext const &) = default;
};

void to_json(nlohmann::json &j, CoordinatorInstanceContext const &context);
void from_json(nlohmann::json const &j, CoordinatorInstanceContext &context);
}  // namespace memgraph::coordination

#endif
