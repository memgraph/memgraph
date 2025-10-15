// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/constraints/type_constraints.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"

namespace memgraph::storage {

Constraints::Constraints(const Config &config) {
  std::invoke([this, config]() {
    existence_constraints_ = std::make_unique<ExistenceConstraints>();
    type_constraints_ = std::make_unique<TypeConstraints>();
    unique_constraints_ = std::make_unique<InMemoryUniqueConstraints>();
  });
}

void Constraints::AbortEntries(std::span<Vertex const *const> const vertices,
                               uint64_t const exact_start_timestamp) const {
  static_cast<InMemoryUniqueConstraints *>(unique_constraints_.get())->AbortEntries(vertices, exact_start_timestamp);
}

void Constraints::DropGraphClearConstraints() const {
  static_cast<InMemoryUniqueConstraints *>(unique_constraints_.get())->DropGraphClearConstraints();
  existence_constraints_->DropGraphClearConstraints();
  type_constraints_->DropGraphClearConstraints();
}

bool Constraints::HasTypeConstraints() const { return !type_constraints_->empty(); }

}  // namespace memgraph::storage
