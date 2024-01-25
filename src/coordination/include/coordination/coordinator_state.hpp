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
#include "coordination/coordinator_entity_info.hpp"
#include "coordination/coordinator_server.hpp"
#include "rpc/server.hpp"
#include "utils/result.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"

#include <list>
#include <variant>

namespace memgraph::coordination {

enum class RegisterMainReplicaCoordinatorStatus : uint8_t {
  NAME_EXISTS,
  END_POINT_EXISTS,
  COULD_NOT_BE_PERSISTED,
  NOT_COORDINATOR,
  SUCCESS
};

enum class DoFailoverStatus : uint8_t { SUCCESS, ALL_REPLICAS_DOWN, MAIN_ALIVE, CLUSTER_UNINITIALIZED };

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

  auto RegisterReplica(const CoordinatorClientConfig &config)
      -> utils::BasicResult<RegisterMainReplicaCoordinatorStatus, CoordinatorClient *>;

  auto RegisterMain(const CoordinatorClientConfig &config)
      -> utils::BasicResult<RegisterMainReplicaCoordinatorStatus, CoordinatorClient *>;

  auto ShowReplicas() const -> std::vector<CoordinatorEntityInfo>;

  auto PingReplicas() const -> std::unordered_map<std::string_view, bool>;

  auto ShowMain() const -> std::optional<CoordinatorEntityInfo>;

  auto PingMain() const -> std::optional<CoordinatorEntityHealthInfo>;

  // The client code must check that the server exists before calling this method.
  auto GetCoordinatorServer() const -> CoordinatorServer &;

  auto DoFailover() -> DoFailoverStatus;

 private:
  // TODO: Data is not thread safe

  // Coordinator stores registered replicas and main
  struct CoordinatorData {
    std::list<CoordinatorClient> registered_replicas_;
    std::unique_ptr<CoordinatorClient> registered_main_;
  };

  // Data which each main and replica stores
  struct CoordinatorMainReplicaData {
    std::unique_ptr<CoordinatorServer> coordinator_server_;
  };

  std::variant<CoordinatorData, CoordinatorMainReplicaData> data_;
};

}  // namespace memgraph::coordination
#endif
