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

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/constraints/type_constraints.hpp"
#include "storage/v2/disk/unique_constraints.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"

namespace memgraph::storage {

Constraints::Constraints(const Config &config, StorageMode storage_mode) {
  std::invoke([this, config, storage_mode]() {
    existence_constraints_ = std::make_unique<ExistenceConstraints>();
    type_constraints_ = std::make_unique<TypeConstraints>();
    switch (storage_mode) {
      case StorageMode::IN_MEMORY_TRANSACTIONAL:
      case StorageMode::IN_MEMORY_ANALYTICAL:
        unique_constraints_ = std::make_unique<InMemoryUniqueConstraints>();
        break;
      case StorageMode::ON_DISK_TRANSACTIONAL:
        unique_constraints_ = std::make_unique<DiskUniqueConstraints>(config);
        break;
      case StorageMode::N:
        __builtin_unreachable();
    }
  });
}

void Constraints::DropGraphClearConstraints() const {
  // DROP GRAPH can only happen for IN_MEMORY so it safe to assume this cast
  static_cast<InMemoryUniqueConstraints *>(unique_constraints_.get())->DropGraphClearConstraints();
  existence_constraints_->DropGraphClearConstraints();
  type_constraints_->DropGraphClearConstraints();
}

}  // namespace memgraph::storage
