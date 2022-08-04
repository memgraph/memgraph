// Copyright 2022 Memgraph Ltd.
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

#include "storage/v2/constraints.hpp"

#include <variant>

namespace memgraph::storage {

struct ReplicationError {};

using StorageDataManipulationError = std::variant<ConstraintViolation, ReplicationError>;

struct IndexDefinitionError {};
using StorageIndexDefinitionError = std::variant<IndexDefinitionError, ReplicationError>;

enum class ConstraintDefinitionError {
  EXISTENT_CONSTRAINT,  //#NoCommit remove, not needed
  NONEXISTENT_CONSTRAINT,
};

using StorageExistenceConstraintDefinitionError =
    std::variant<ConstraintViolation, ConstraintDefinitionError, ReplicationError>;

using StorageExistenceConstraintDroppingError = std::variant<ConstraintDefinitionError, ReplicationError>;

using StorageUniqueConstraintDefinitionError = std::variant<ConstraintViolation, ReplicationError>;

using StorageUniqueConstraintDroppingError = std::variant<ReplicationError>;

}  // namespace memgraph::storage
