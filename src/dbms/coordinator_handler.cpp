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

#include "dbms/coordinator_handler.hpp"

#include "dbms/dbms_handler.hpp"

namespace memgraph::dbms {

CoordinatorHandler::CoordinatorHandler(DbmsHandler &dbms_handler) : dbms_handler_(dbms_handler) {}

auto CoordinatorHandler::RegisterReplicaOnCoordinator(const coordination::CoordinatorClientConfig &config)
    -> coordination::RegisterMainReplicaCoordinatorStatus {
  return dbms_handler_.CoordinatorState().RegisterReplica(config);
}

auto CoordinatorHandler::RegisterMainOnCoordinator(const memgraph::coordination::CoordinatorClientConfig &config)
    -> coordination::RegisterMainReplicaCoordinatorStatus {
  return dbms_handler_.CoordinatorState().RegisterMain(config);
}

auto CoordinatorHandler::ShowReplicasOnCoordinator() const -> std::vector<coordination::CoordinatorEntityInfo> {
  return dbms_handler_.CoordinatorState().ShowReplicas();
}

auto CoordinatorHandler::PingReplicasOnCoordinator() const -> std::unordered_map<std::string_view, bool> {
  return dbms_handler_.CoordinatorState().PingReplicas();
}

auto CoordinatorHandler::ShowMainOnCoordinator() const -> std::optional<coordination::CoordinatorEntityInfo> {
  return dbms_handler_.CoordinatorState().ShowMain();
}

auto CoordinatorHandler::PingMainOnCoordinator() const -> std::optional<coordination::CoordinatorEntityHealthInfo> {
  return dbms_handler_.CoordinatorState().PingMain();
}

auto CoordinatorHandler::DoFailover() const -> coordination::DoFailoverStatus {
  return dbms_handler_.CoordinatorState().DoFailover();
}

}  // namespace memgraph::dbms

#endif
