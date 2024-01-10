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
#include <variant>

namespace memgraph::replication {

#ifdef MG_ENTERPRISE
class CoordinatorState {
 public:
  CoordinatorState();
  ~CoordinatorState() = default;

  CoordinatorState(const CoordinatorState &) = delete;
  CoordinatorState &operator=(const CoordinatorState &) = delete;

  CoordinatorState(CoordinatorState &&other) noexcept : data_(std::move(other.data_)) {}

  CoordinatorState &operator=(CoordinatorState &&other) noexcept {
    if (this == &other) {
      return *this;
    }
    data_ = std::move(other.data_);
    return *this;
  }

  utils::BasicResult<RegisterMainReplicaCoordinatorStatus, CoordinatorClient *> RegisterReplica(
      const CoordinatorClientConfig &config);

  /// TODO: (andi) Introduce RegisterMainError
  utils::BasicResult<RegisterMainReplicaCoordinatorStatus, CoordinatorClient *> RegisterMain(
      const CoordinatorClientConfig &config);

  std::vector<CoordinatorEntityInfo> ShowReplicas() const;

  std::unordered_map<std::string, bool> PingReplicas() const;

  std::optional<CoordinatorEntityInfo> ShowMain() const;

  std::optional<CoordinatorEntityHealthInfo> PingMain() const;

 private:
  struct CoordinatorData {
    std::list<CoordinatorClient> registered_replicas_;
    std::unique_ptr<CoordinatorClient> registered_main_;
  };

  struct CoordinatorMainReplicaData {
    std::unique_ptr<CoordinatorServer> coordinator_server_;
  };

  std::variant<CoordinatorData, CoordinatorMainReplicaData> data_;
};
#endif

}  // namespace memgraph::replication
