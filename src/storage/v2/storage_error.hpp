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

#include "storage/v2/constraints/constraints.hpp"

#include <iterator>
#include <variant>

namespace memgraph::storage {

struct ReplicationError {};
struct PersistenceError {};  // TODO: Generalize and add to InMemory durability as well (currently durability just
                             // asserts and terminated if failed)

struct IndexDefinitionError {};

struct ConstraintsPersistenceError {};

struct SerializationError {};
inline bool operator==(const SerializationError & /*err1*/, const SerializationError & /*err2*/) { return true; }

using StorageManipulationError =
    std::variant<ConstraintViolation, ReplicationError, SerializationError, PersistenceError>;

using StorageIndexDefinitionError = IndexDefinitionError;

struct ConstraintDefinitionError {};

using StorageExistenceConstraintDefinitionError = std::variant<ConstraintViolation, ConstraintDefinitionError>;

using StorageExistenceConstraintDroppingError = ConstraintDefinitionError;

using StorageUniqueConstraintDefinitionError = std::variant<ConstraintViolation, ConstraintDefinitionError>;

}  // namespace memgraph::storage
