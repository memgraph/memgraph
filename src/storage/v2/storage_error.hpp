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

#include "storage/v2/constraints/constraints.hpp"

#include <iterator>
#include <variant>

namespace memgraph::storage {

struct ReplicationError {};

struct SerializationError {};
inline bool operator==(const SerializationError &, const SerializationError &) { return true; }

using StorageDataManipulationError = std::variant<ConstraintViolation, ReplicationError, SerializationError>;

struct IndexDefinitionError {};
using StorageIndexDefinitionError = std::variant<IndexDefinitionError, ReplicationError>;

struct ConstraintDefinitionError {};

using StorageExistenceConstraintDefinitionError =
    std::variant<ConstraintViolation, ConstraintDefinitionError, ReplicationError>;

using StorageExistenceConstraintDroppingError = std::variant<ConstraintDefinitionError, ReplicationError>;

using StorageUniqueConstraintDefinitionError =
    std::variant<ConstraintViolation, ConstraintDefinitionError, ReplicationError>;

using StorageUniqueConstraintDroppingError = std::variant<ReplicationError>;

}  // namespace memgraph::storage
