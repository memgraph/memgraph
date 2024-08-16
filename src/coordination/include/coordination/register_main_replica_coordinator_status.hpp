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

#include <cstdint>

namespace memgraph::coordination {

enum class RegisterInstanceCoordinatorStatus : uint8_t {
  NAME_EXISTS,
  MGMT_ENDPOINT_EXISTS,
  REPL_ENDPOINT_EXISTS,
  NOT_COORDINATOR,
  NOT_LEADER,
  RPC_FAILED,
  RAFT_LOG_ERROR,
  SUCCESS,
  LOCK_OPENED,
  FAILED_TO_OPEN_LOCK,
  FAILED_TO_CLOSE_LOCK
};

enum class UnregisterInstanceCoordinatorStatus : uint8_t {
  NO_INSTANCE_WITH_NAME,
  IS_MAIN,
  NOT_COORDINATOR,
  RPC_FAILED,
  NOT_LEADER,
  RAFT_LOG_ERROR,
  SUCCESS,
  LOCK_OPENED,
  FAILED_TO_OPEN_LOCK,
  FAILED_TO_CLOSE_LOCK
};

enum class SetInstanceToMainCoordinatorStatus : uint8_t {
  NO_INSTANCE_WITH_NAME,
  MAIN_ALREADY_EXISTS,
  NOT_COORDINATOR,
  NOT_LEADER,
  RAFT_LOG_ERROR,
  COULD_NOT_PROMOTE_TO_MAIN,
  SWAP_UUID_FAILED,
  SUCCESS,
  LOCK_OPENED,
  FAILED_TO_OPEN_LOCK,
  ENABLE_WRITING_FAILED,
  FAILED_TO_CLOSE_LOCK
};

enum class AddCoordinatorInstanceStatus : uint8_t {
  SUCCESS,
  ID_ALREADY_EXISTS,
  BOLT_ENDPOINT_ALREADY_EXISTS,
  COORDINATOR_ENDPOINT_ALREADY_EXISTS
};

enum class DemoteInstanceCoordinatorStatus : uint8_t {
  NO_INSTANCE_WITH_NAME,
  NOT_LEADER,
  RPC_FAILED,
  RAFT_LOG_ERROR,
  SUCCESS,
  LOCK_OPENED,
  FAILED_TO_OPEN_LOCK,
  FAILED_TO_CLOSE_LOCK,
  NOT_COORDINATOR
};

enum class ReconcileClusterStateStatus : uint8_t { SUCCESS, FAIL, SHUTTING_DOWN };

}  // namespace memgraph::coordination
#endif
