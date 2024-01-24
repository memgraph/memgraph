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

#include "coordination/register_main_replica_coordinator_status.hpp"
#ifdef MG_ENTERPRISE

#include "dbms/coordinator_handler.hpp"

#include "dbms/dbms_handler.hpp"

namespace memgraph::dbms {

CoordinatorHandler::CoordinatorHandler(DbmsHandler &dbms_handler) : dbms_handler_(dbms_handler) {}

auto CoordinatorHandler::RegisterInstance(memgraph::coordination::CoordinatorClientConfig config)
    -> coordination::RegisterInstanceCoordinatorStatus {
  return dbms_handler_.CoordinatorState().RegisterInstance(config);
}

auto CoordinatorHandler::SetInstanceToMain(std::string instance_name)
    -> coordination::SetInstanceToMainCoordinatorStatus {
  return dbms_handler_.CoordinatorState().SetInstanceToMain(std::move(instance_name));
}

auto CoordinatorHandler::ShowReplicasOnCoordinator() const -> std::vector<coordination::CoordinatorInstanceStatus> {
  return dbms_handler_.CoordinatorState().ShowReplicas();
}

auto CoordinatorHandler::ShowMainOnCoordinator() const -> std::optional<coordination::CoordinatorInstanceStatus> {
  return dbms_handler_.CoordinatorState().ShowMain();
}

}  // namespace memgraph::dbms

#endif
