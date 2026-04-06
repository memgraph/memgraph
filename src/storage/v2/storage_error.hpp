// Copyright 2026 Memgraph Ltd.
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

#include "storage/v2/constraints/constraint_violation.hpp"

#include <fmt/core.h>

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace memgraph::storage {

// --- Internal start-txn error types (used by CollectStartTxnErrors) ---

struct FailedToConnectErr {};

struct FailedToGetAsyncRpcLock {};

struct GenericRpcError {};

struct ReplicaNotInSyncErr {};

struct ReplicaDivergedErr {};

using StartTxnReplicationError =
    std::variant<FailedToConnectErr, FailedToGetAsyncRpcLock, GenericRpcError, ReplicaNotInSyncErr, ReplicaDivergedErr>;

// --- Unified replication error types ---

enum class ReplicaFailureReason : uint8_t {
  NOT_IN_SYNC,         // FailedToConnectErr, ReplicaNotInSyncErr, SOCKET_FAILED_TO_CONNECT
  FAILED_TO_GET_LOCK,  // FailedToGetAsyncRpcLock
  RPC_ERROR,           // GenericRpcError, GENERIC_ERROR
  DIVERGED,            // ReplicaDivergedErr
  TIMEOUT,             // TIMEOUT_ERROR
};

struct ReplicaFailure {
  std::string name;
  std::string mode;  // "SYNC", "STRICT_SYNC", "ASYNC"
  ReplicaFailureReason reason;
};

struct ReplicationError {
  std::vector<ReplicaFailure> failures;
  bool transaction_committed;  // true = committed on main, false = aborted
};

auto ReplicaFailureReasonToString(ReplicaFailureReason reason) -> std::string;

auto FormatReplicationError(ReplicationError const &error) -> std::string;

struct ReplicaShouldNotWriteError {};

struct PersistenceError {};  // TODO: Generalize and add to InMemory durability as well (currently durability just
                             // asserts and terminated if failed)

struct IndexDefinitionError {};

struct IndexDefinitionCancelationError {};

struct IndexDefinitionAlreadyExistsError {};

struct IndexDefinitionConfigError {};

struct ConstraintsPersistenceError {};

struct SerializationError {};

inline bool operator==(const SerializationError & /*err1*/, const SerializationError & /*err2*/) { return true; }

using StorageManipulationError = std::variant<ConstraintViolation, ReplicationError, SerializationError,
                                              PersistenceError, ReplicaShouldNotWriteError>;

using StorageIndexDefinitionError = std::variant<IndexDefinitionError, IndexDefinitionAlreadyExistsError,
                                                 IndexDefinitionConfigError, IndexDefinitionCancelationError>;

struct ConstraintDefinitionError {};

using StorageExistenceConstraintDefinitionError = std::variant<ConstraintViolation, ConstraintDefinitionError>;

using StorageExistenceConstraintDroppingError = ConstraintDefinitionError;

using StorageUniqueConstraintDefinitionError = std::variant<ConstraintViolation, ConstraintDefinitionError>;

using StorageTypeConstraintDefinitionError = std::variant<ConstraintViolation, ConstraintDefinitionError>;

using StorageTypeConstraintDroppingError = ConstraintDefinitionError;

}  // namespace memgraph::storage
