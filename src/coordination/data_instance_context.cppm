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

#include "replication_coordination_glue/role.hpp"
#include "utils/uuid.hpp"

#include <nlohmann/json_fwd.hpp>

export module memgraph.coordination.data_instance_context;

import memgraph.coordination.coordinator_communication_config;

#ifdef MG_ENTERPRISE

export namespace memgraph::coordination {

// Context saved about each data instance in Raft logs (app log)
struct DataInstanceContext {
  DataInstanceConfig config;
  replication_coordination_glue::ReplicationRole status;

  // for replica this is main uuid of current main
  // for "main" main this same as current_main_id_
  // when replica is down and comes back up we reset uuid of main replica is listening to
  // so we need to send swap uuid again
  // For MAIN we don't enable writing until cluster is in healthy state
  utils::UUID instance_uuid;

  friend auto operator==(DataInstanceContext const &lhs, DataInstanceContext const &rhs) -> bool = default;
};

void to_json(nlohmann::json &j, DataInstanceContext const &instance_state);
void from_json(nlohmann::json const &j, DataInstanceContext &instance_state);

}  // namespace memgraph::coordination

#endif
