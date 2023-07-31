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

#include <set>

#include "storage/v2/id_types.hpp"

namespace memgraph::storage {

struct ConstraintViolation {
  enum class Type {
    EXISTENCE,
    UNIQUE,
  };

  Type type;
  LabelId label;

  // While multiple properties are supported by unique constraints, the
  // `properties` set will always have exactly one element in the case of
  // existence constraint violation.
  std::set<PropertyId> properties;
};

inline bool operator==(const ConstraintViolation &lhs, const ConstraintViolation &rhs) {
  return lhs.type == rhs.type && lhs.label == rhs.label && lhs.properties == rhs.properties;
}

}  // namespace memgraph::storage
