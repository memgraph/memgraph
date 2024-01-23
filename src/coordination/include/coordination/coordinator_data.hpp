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

#include "coordination/coordinator_instance.hpp"
#include "coordination/coordinator_instance_status.hpp"
#include "coordination/coordinator_server.hpp"
#include "coordination/failover_status.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"
#include "utils/rw_lock.hpp"

#include <list>

namespace memgraph::coordination {
class CoordinatorData {
 public:
  CoordinatorData();

  [[nodiscard]] auto DoFailover() -> DoFailoverStatus;

  [[nodiscard]] auto RegisterReplica(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus;
  [[nodiscard]] auto RegisterMain(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus;

  auto ShowReplicas() const -> std::vector<CoordinatorInstanceStatus>;
  auto ShowMain() const -> std::optional<CoordinatorInstanceStatus>;

 private:
  mutable utils::RWLock coord_data_lock_{utils::RWLock::Priority::READ};
  HealthCheckCallback main_succ_cb_, main_fail_cb_, replica_succ_cb_, replica_fail_cb_;
  std::list<CoordinatorInstance> registered_instances_;
};

struct CoordinatorMainReplicaData {
  std::unique_ptr<CoordinatorServer> coordinator_server_;
};

}  // namespace memgraph::coordination
#endif
