// Copyright 2023 Memgraph Ltd.
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
#include <cstdint>

namespace memgraph::storage::replication {
enum class ReplicationRole : uint8_t { MAIN, REPLICA };

enum class ReplicationMode : std::uint8_t { SYNC, ASYNC };

enum class ReplicaState : std::uint8_t { READY, REPLICATING, RECOVERY, INVALID };

enum class RegistrationMode : std::uint8_t { MUST_BE_INSTANTLY_VALID, CAN_BE_INVALID };
}  // namespace memgraph::storage::replication
