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

#include "storage/v2/constraints/existence_constraints.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"

namespace memgraph::storage {

struct Constraints {
  /// TODO: andi Pass storage mode here
  Constraints()
      : existence_constraints_(std::make_unique<ExistenceConstraints>()),
        unique_constraints_(std::make_unique<InMemoryUniqueConstraints>()) {}

  Constraints(const Constraints &) = delete;
  Constraints(Constraints &&) = delete;
  Constraints &operator=(const Constraints &) = delete;
  Constraints &operator=(Constraints &&) = delete;
  ~Constraints() = default;

  std::unique_ptr<ExistenceConstraints> existence_constraints_;
  std::unique_ptr<UniqueConstraints> unique_constraints_;
};

}  // namespace memgraph::storage
