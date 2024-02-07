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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_config.hpp"
#include "coordination/coordinator_instance_status.hpp"
#include "coordination/coordinator_state.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"

#include <vector>

namespace memgraph::dbms {

class DbmsHandler;

class CoordinatorHandler {
 public:
  explicit CoordinatorHandler(coordination::CoordinatorState &coordinator_state);

  auto RegisterInstance(coordination::CoordinatorClientConfig config)
      -> coordination::RegisterInstanceCoordinatorStatus;

  auto SetInstanceToMain(std::string instance_name) -> coordination::SetInstanceToMainCoordinatorStatus;

  auto ShowInstances() const -> std::vector<coordination::CoordinatorInstanceStatus>;

 private:
  coordination::CoordinatorState &coordinator_state_;
};

}  // namespace memgraph::dbms
#endif
