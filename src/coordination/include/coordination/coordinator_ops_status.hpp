// Copyright 2025 Memgraph Ltd.
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

namespace memgraph::coordination {

enum class YieldLeadershipStatus { SUCCESS = 0, NOT_LEADER };
enum class SetCoordinatorSettingStatus : uint8_t { SUCCESS = 0, RAFT_LOG_ERROR, UNKNOWN_SETTING };

enum class RegisterInstanceCoordinatorStatus : uint8_t {
  NAME_EXISTS = 0,
  MGMT_ENDPOINT_EXISTS,
  REPL_ENDPOINT_EXISTS,
  NOT_COORDINATOR,
  NOT_LEADER,
  RPC_FAILED,
  RAFT_LOG_ERROR,
  SUCCESS,
  LOCK_OPENED,
  FAILED_TO_OPEN_LOCK,
  FAILED_TO_CLOSE_LOCK,
  STRICT_SYNC_AND_SYNC_FORBIDDEN,
};

enum class UnregisterInstanceCoordinatorStatus : uint8_t {
  NO_INSTANCE_WITH_NAME = 0,
  IS_MAIN,
  NO_MAIN,
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
  NO_INSTANCE_WITH_NAME = 0,
  MAIN_ALREADY_EXISTS,
  NOT_COORDINATOR,
  NOT_LEADER,
  RAFT_LOG_ERROR,
  COULD_NOT_PROMOTE_TO_MAIN,
  SUCCESS,
  LOCK_OPENED,
  FAILED_TO_OPEN_LOCK,
  ENABLE_WRITING_FAILED,
  FAILED_TO_CLOSE_LOCK
};

enum class AddCoordinatorInstanceStatus : uint8_t {
  SUCCESS = 0,
  ID_ALREADY_EXISTS,
  MGMT_ENDPOINT_ALREADY_EXISTS,
  COORDINATOR_ENDPOINT_ALREADY_EXISTS
};

enum class RemoveCoordinatorInstanceStatus : uint8_t {
  SUCCESS = 0,
  NO_SUCH_ID,
};

enum class DemoteInstanceCoordinatorStatus : uint8_t {
  NO_INSTANCE_WITH_NAME = 0,
  NOT_LEADER,
  RPC_FAILED,
  RAFT_LOG_ERROR,
  SUCCESS,
  LOCK_OPENED,
  FAILED_TO_OPEN_LOCK,
  FAILED_TO_CLOSE_LOCK,
  NOT_COORDINATOR
};

enum class ReconcileClusterStateStatus : uint8_t { SUCCESS = 0, FAIL, SHUTTING_DOWN, NOT_LEADER_ANYMORE };

}  // namespace memgraph::coordination
#endif
