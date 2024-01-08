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

#include "replication/coordinator_client.hpp"
#include "replication/coordinator_entity_info.hpp"
#include "replication/coordinator_server.hpp"
#include "replication/register_replica_error.hpp"
#include "utils/result.hpp"

#include <list>

namespace memgraph::replication {

#ifdef MG_ENTERPRISE
struct CoordinatorState {
  CoordinatorState();
  ~CoordinatorState() = default;

  CoordinatorState(const CoordinatorState &) = delete;
  CoordinatorState &operator=(const CoordinatorState &) = delete;

  CoordinatorState(CoordinatorState &&other) noexcept
      : registered_replicas_(std::move(other.registered_replicas_)),
        registered_main_(std::move(other.registered_main_)) {}

  CoordinatorState &operator=(CoordinatorState &&other) noexcept {
    if (this == &other) {
      return *this;
    }
    registered_replicas_ = std::move(other.registered_replicas_);
    registered_main_ = std::move(other.registered_main_);
    return *this;
  }

  utils::BasicResult<RegisterReplicaError, CoordinatorClient *> RegisterReplica(const ReplicationClientConfig &config);

  /// TODO: (andi) Introduce RegisterMainError
  utils::BasicResult<RegisterReplicaError, CoordinatorClient *> RegisterMain(const ReplicationClientConfig &config);

  std::vector<CoordinatorEntityInfo> ShowReplicas() const;

  std::optional<CoordinatorEntityInfo> ShowMain() const;

  std::list<CoordinatorClient> registered_replicas_;
  std::unique_ptr<CoordinatorClient> registered_main_;
  std::unique_ptr<CoordinatorServer> coordinator_server_;
};
#endif

}  // namespace memgraph::replication
