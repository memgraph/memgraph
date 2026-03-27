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
#include "storage/v2/replication/enums.hpp"

#include <string>
#include <variant>
#include <vector>

namespace memgraph::storage {

struct FailedToConnectErr {};

struct FailedToGetAsyncRpcLock {};

struct GenericRpcError {};

struct ReplicaNotInSyncErr {};

struct ReplicaDivergedErr {};

using StartTxnReplicationError =
    std::variant<FailedToConnectErr, FailedToGetAsyncRpcLock, GenericRpcError, ReplicaNotInSyncErr, ReplicaDivergedErr>;

struct StartTxnReplicationFailure {
  std::string replica_name;
  std::string mode;
  StartTxnReplicationError error;
};

struct StartTxnReplicationErrors {
  std::vector<StartTxnReplicationFailure> failures;
  bool two_phase_commit_aborted{false};
};

struct TimeoutReplicationError {
  std::vector<std::string> replica_names;
  bool transaction_aborted{false};
};

struct SyncReplicationError {};

struct StrictSyncReplicationError {};

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

using StorageManipulationError =
    std::variant<ConstraintViolation, SyncReplicationError, StrictSyncReplicationError, SerializationError,
                 PersistenceError, ReplicaShouldNotWriteError, TimeoutReplicationError, StartTxnReplicationErrors>;

using StorageIndexDefinitionError = std::variant<IndexDefinitionError, IndexDefinitionAlreadyExistsError,
                                                 IndexDefinitionConfigError, IndexDefinitionCancelationError>;

struct ConstraintDefinitionError {};

using StorageExistenceConstraintDefinitionError = std::variant<ConstraintViolation, ConstraintDefinitionError>;

using StorageExistenceConstraintDroppingError = ConstraintDefinitionError;

using StorageUniqueConstraintDefinitionError = std::variant<ConstraintViolation, ConstraintDefinitionError>;

using StorageTypeConstraintDefinitionError = std::variant<ConstraintViolation, ConstraintDefinitionError>;

using StorageTypeConstraintDroppingError = ConstraintDefinitionError;

}  // namespace memgraph::storage
