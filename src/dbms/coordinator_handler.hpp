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

#include "utils/result.hpp"

#include "coordination/coordinator_config.hpp"
#include "coordination/coordinator_entity_info.hpp"
#include "coordination/failover_status.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"

#include <cstdint>
#include <optional>
#include <vector>

namespace memgraph::dbms {

class DbmsHandler;

class CoordinatorHandler {
 public:
  explicit CoordinatorHandler(DbmsHandler &dbms_handler);

  auto RegisterReplicaOnCoordinator(const coordination::CoordinatorClientConfig &config)
      -> coordination::RegisterMainReplicaCoordinatorStatus;

  auto RegisterMainOnCoordinator(const coordination::CoordinatorClientConfig &config)
      -> coordination::RegisterMainReplicaCoordinatorStatus;

  auto ShowReplicasOnCoordinator() const -> std::vector<coordination::CoordinatorEntityInfo>;

  auto ShowMainOnCoordinator() const -> std::optional<coordination::CoordinatorEntityInfo>;

  auto PingReplicasOnCoordinator() const -> std::unordered_map<std::string_view, bool>;

  auto PingMainOnCoordinator() const -> std::optional<coordination::CoordinatorEntityHealthInfo>;

  auto DoFailover() const -> coordination::DoFailoverStatus;

 private:
  DbmsHandler &dbms_handler_;
};

}  // namespace memgraph::dbms
#endif
