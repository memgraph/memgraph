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

auto CoordinatorHandler::RegisterReplicaOnCoordinator(const memgraph::coordination::CoordinatorClientConfig &config)
    -> utils::BasicResult<RegisterMainReplicaCoordinatorStatus> {
  auto instance_client = dbms_handler_.CoordinatorState().RegisterReplica(config);
  using repl_status = memgraph::coordination::RegisterMainReplicaCoordinatorStatus;
  using dbms_status = memgraph::dbms::RegisterMainReplicaCoordinatorStatus;
  if (instance_client.HasError()) switch (instance_client.GetError()) {
      case memgraph::coordination::RegisterMainReplicaCoordinatorStatus::NOT_COORDINATOR:
        MG_ASSERT(false, "Only coordinator instance can register main and replica!");
        return {};
      case repl_status::NAME_EXISTS:
        return dbms_status::NAME_EXISTS;
      case repl_status::END_POINT_EXISTS:
        return dbms_status::END_POINT_EXISTS;
      case repl_status::COULD_NOT_BE_PERSISTED:
        return dbms_status::COULD_NOT_BE_PERSISTED;
      case repl_status::SUCCESS:
        break;
    }

  instance_client.GetValue()->StartFrequentCheck();
  return {};
}

auto CoordinatorHandler::RegisterMainOnCoordinator(const memgraph::coordination::CoordinatorClientConfig &config)
    -> utils::BasicResult<RegisterMainReplicaCoordinatorStatus> {
  auto instance_client = dbms_handler_.CoordinatorState().RegisterMain(config);
  if (instance_client.HasError()) switch (instance_client.GetError()) {
      case memgraph::coordination::RegisterMainReplicaCoordinatorStatus::NOT_COORDINATOR:
        MG_ASSERT(false, "Only coordinator instance can register main and replica!");
      case memgraph::coordination::RegisterMainReplicaCoordinatorStatus::NAME_EXISTS:
        return memgraph::dbms::RegisterMainReplicaCoordinatorStatus::NAME_EXISTS;
      case memgraph::coordination::RegisterMainReplicaCoordinatorStatus::END_POINT_EXISTS:
        return memgraph::dbms::RegisterMainReplicaCoordinatorStatus::END_POINT_EXISTS;
      case memgraph::coordination::RegisterMainReplicaCoordinatorStatus::COULD_NOT_BE_PERSISTED:
        return memgraph::dbms::RegisterMainReplicaCoordinatorStatus::COULD_NOT_BE_PERSISTED;
      case memgraph::coordination::RegisterMainReplicaCoordinatorStatus::SUCCESS:
        break;
    }

  instance_client.GetValue()->StartFrequentCheck();
  return {};
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

auto CoordinatorHandler::DoFailover() const -> DoFailoverStatus {
  auto status = dbms_handler_.CoordinatorState().DoFailover();
  switch (status) {
    case memgraph::coordination::DoFailoverStatus::ALL_REPLICAS_DOWN:
      return memgraph::dbms::DoFailoverStatus::ALL_REPLICAS_DOWN;
    case memgraph::coordination::DoFailoverStatus::SUCCESS:
      return memgraph::dbms::DoFailoverStatus::SUCCESS;
    case memgraph::coordination::DoFailoverStatus::MAIN_ALIVE:
      return memgraph::dbms::DoFailoverStatus::MAIN_ALIVE;
  }
}

}  // namespace memgraph::dbms

#endif
