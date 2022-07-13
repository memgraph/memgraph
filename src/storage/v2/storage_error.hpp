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

// #NoCommit from /home/jeremy/dev/memgraph/src/query/metadata.hpp, see how to avoid duplication
enum class DataDefinitionError {
  EXISTANT_INDEX,
  EXISTANT_CONSTRAINT,
  NONEXISTANT_INDEX,
  NONEXISTANT_CONSTRAINT,
};

struct StorageIndexDefinitionError {
  std::variant<DataDefinitionError, ReplicationError> error;

  bool operator==(const StorageIndexDefinitionError &) const = default;
};

struct StorageExistenceConstraintDefinitionError {
  std::variant<ConstraintViolation, DataDefinitionError, ReplicationError> error;

  bool operator==(const StorageExistenceConstraintDefinitionError &) const = default;
};

struct StorageExistenceConstraintDroppingError {
  std::variant<DataDefinitionError, ReplicationError> error;

  bool operator==(const StorageExistenceConstraintDroppingError &) const = default;
};

struct StorageUniqueConstraintDefinitionError {
  std::variant<ConstraintViolation, ReplicationError> error;

  bool operator==(const StorageUniqueConstraintDefinitionError &) const = default;
};

}  // namespace memgraph::storage
