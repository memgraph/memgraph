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

#include "coordination/coordinator_client.hpp"
#include "coordination/coordinator_client_info.hpp"
#include "coordination/coordinator_data.hpp"
#include "coordination/coordinator_instance_status.hpp"
#include "coordination/coordinator_server.hpp"
#include "coordination/failover_status.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"
#include "rpc/server.hpp"
#include "utils/result.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"

#include <list>
#include <variant>

namespace memgraph::coordination {

class CoordinatorState {
 public:
  CoordinatorState();
  ~CoordinatorState() = default;

  CoordinatorState(const CoordinatorState &) = delete;
  CoordinatorState &operator=(const CoordinatorState &) = delete;

  CoordinatorState(CoordinatorState &&) noexcept = delete;
  CoordinatorState &operator=(CoordinatorState &&) noexcept = delete;

  [[nodiscard]] auto RegisterReplica(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus;

  [[nodiscard]] auto RegisterMain(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus;

  auto ShowReplicas() const -> std::vector<CoordinatorInstanceStatus>;

  auto ShowMain() const -> std::optional<CoordinatorInstanceStatus>;

  // The client code must check that the server exists before calling this method.
  auto GetCoordinatorServer() const -> CoordinatorServer &;

  [[nodiscard]] auto DoFailover() -> DoFailoverStatus;

 private:
  std::variant<CoordinatorData, CoordinatorMainReplicaData> data_;
};

}  // namespace memgraph::coordination
#endif
