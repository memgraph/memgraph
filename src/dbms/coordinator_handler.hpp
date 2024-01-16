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

#include <cstdint>
#include <optional>
#include <vector>

namespace memgraph::coordination {
struct CoordinatorEntityInfo;
struct CoordinatorEntityHealthInfo;
struct CoordinatorClientConfig;
}  // namespace memgraph::coordination

namespace memgraph::dbms {

enum class RegisterMainReplicaCoordinatorStatus : uint8_t {
  NAME_EXISTS,
  END_POINT_EXISTS,
  COULD_NOT_BE_PERSISTED,
  NOT_COORDINATOR,
  SUCCESS
};

enum class DoFailoverStatus : uint8_t { SUCCESS, ALL_REPLICAS_DOWN, MAIN_ALIVE };

class DbmsHandler;

class CoordinatorHandler {
 public:
  explicit CoordinatorHandler(DbmsHandler &dbms_handler);

  auto RegisterReplicaOnCoordinator(const memgraph::coordination::CoordinatorClientConfig &config)
      -> utils::BasicResult<RegisterMainReplicaCoordinatorStatus>;

  auto RegisterMainOnCoordinator(const memgraph::coordination::CoordinatorClientConfig &config)
      -> utils::BasicResult<RegisterMainReplicaCoordinatorStatus>;

  auto ShowReplicasOnCoordinator() const -> std::vector<memgraph::coordination::CoordinatorEntityInfo>;

  auto ShowMainOnCoordinator() const -> std::optional<memgraph::coordination::CoordinatorEntityInfo>;

  auto PingReplicasOnCoordinator() const -> std::unordered_map<std::string_view, bool>;

  auto PingMainOnCoordinator() const -> std::optional<memgraph::coordination::CoordinatorEntityHealthInfo>;

  auto DoFailover() const -> DoFailoverStatus;

 private:
  DbmsHandler &dbms_handler_;
};

}  // namespace memgraph::dbms
#endif
