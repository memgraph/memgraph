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
#include "replication_coordination_glue/role.hpp"

namespace memgraph::coordination {

class CoordinatorData;
using HealthCheckCallback = std::function<void(CoordinatorData *, std::string_view)>;

struct CoordinatorInstance {
  // TODO: (andi) Capture by const reference functions
  CoordinatorInstance(CoordinatorData *data, CoordinatorClientConfig config, HealthCheckCallback succ_cb,
                      HealthCheckCallback fail_cb, replication_coordination_glue::ReplicationRole replication_role)
      : client_(data, config, succ_cb, fail_cb),
        client_info_(config.instance_name, config.ip_address + ":" + std::to_string(config.port)),
        replication_role_(replication_role) {}

  CoordinatorInstance(CoordinatorInstance const &other) = delete;
  CoordinatorInstance &operator=(CoordinatorInstance const &other) = delete;
  CoordinatorInstance(CoordinatorInstance &&other) noexcept = delete;
  CoordinatorInstance &operator=(CoordinatorInstance &&other) noexcept = delete;
  ~CoordinatorInstance() = default;

  auto IsReplica() const -> bool {
    return replication_role_ == replication_coordination_glue::ReplicationRole::REPLICA;
  }
  auto IsMain() const -> bool { return replication_role_ == replication_coordination_glue::ReplicationRole::MAIN; }

  CoordinatorClient client_;
  CoordinatorClientInfo client_info_;
  replication_coordination_glue::ReplicationRole replication_role_;

  // TODO: (andi) Make this better
  friend bool operator==(CoordinatorInstance const &first, CoordinatorInstance const &second) {
    return first.client_ == second.client_ && first.replication_role_ == second.replication_role_;
  }
};

}  // namespace memgraph::coordination
#endif
