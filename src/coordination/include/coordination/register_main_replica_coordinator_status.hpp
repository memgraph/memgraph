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
  ENDPOINT_EXISTS,
  NOT_COORDINATOR,
  RPC_FAILED,
  NOT_LEADER,
  RAFT_COULD_NOT_ACCEPT,
  RAFT_COULD_NOT_APPEND,
  SUCCESS
};

enum class SetInstanceToMainCoordinatorStatus : uint8_t {
  NO_INSTANCE_WITH_NAME,
  NOT_COORDINATOR,
  SUCCESS,
  COULD_NOT_PROMOTE_TO_MAIN,
  SWAP_UUID_FAILED
};

}  // namespace memgraph::coordination
#endif
