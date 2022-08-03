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

enum class ReplicationError {
  UNABLE_TO_SYNC_REPLICATE,
};

struct StorageDataManipulationError {
  std::variant<ConstraintViolation, ReplicationError> error;

  bool operator==(const StorageDataManipulationError &) const = default;
};

enum class IndexDefinitionError {
  EXISTENT_INDEX,
  NONEXISTENT_INDEX,
};

struct StorageIndexDefinitionError {
  std::variant<IndexDefinitionError, ReplicationError> error;

  bool operator==(const StorageIndexDefinitionError &) const = default;
};

enum class ConstraintDefinitionError {
  EXISTENT_CONSTRAINT,
  NONEXISTENT_CONSTRAINT,
};

struct StorageExistenceConstraintDefinitionError {
  std::variant<ConstraintViolation, ConstraintDefinitionError, ReplicationError> error;

  bool operator==(const StorageExistenceConstraintDefinitionError &) const = default;
};

struct StorageExistenceConstraintDroppingError {
  std::variant<ConstraintDefinitionError, ReplicationError> error;

  bool operator==(const StorageExistenceConstraintDroppingError &) const = default;
};

struct StorageUniqueConstraintDefinitionError {
  std::variant<ConstraintViolation, ReplicationError> error;

  bool operator==(const StorageUniqueConstraintDefinitionError &) const = default;
};

struct StorageUniqueConstraintDroppingError {
  std::variant<ReplicationError> error;

  bool operator==(const StorageUniqueConstraintDroppingError &) const = default;
};

}  // namespace memgraph::storage
