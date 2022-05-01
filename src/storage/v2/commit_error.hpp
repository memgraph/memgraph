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

#include <optional>

#include "storage/v2/constraints.hpp"

namespace memgraph::storage {

struct CommitError {
  enum class Type {
    CONSTRAINT_VIOLATION,
    UNABLE_TO_REPLICATE,
  };
  Type type;

  std::optional<ConstraintViolation> maybe_constraint_violation;
};

bool operator==(const CommitError &lhs, const CommitError &rhs);

}  // namespace memgraph::storage
