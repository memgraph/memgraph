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

#include <utility>

#include "storage/v2/constraints/type_constraints.hpp"
#include "storage/v2/disk/unique_constraints.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"

namespace memgraph::storage {

Constraints::Constraints(const Config &config, StorageMode storage_mode,
                         metrics::DatabaseMetricHandles *metric_handles) {
  std::invoke([this, config, storage_mode, metric_handles]() {
    existence_constraints_ = std::make_unique<ExistenceConstraints>(metric_handles);
    type_constraints_ = std::make_unique<TypeConstraints>(metric_handles);
    switch (storage_mode) {
      case StorageMode::IN_MEMORY_TRANSACTIONAL:
      case StorageMode::IN_MEMORY_ANALYTICAL:
        unique_constraints_ = std::make_unique<InMemoryUniqueConstraints>(metric_handles);
        break;
      case StorageMode::ON_DISK_TRANSACTIONAL:
        unique_constraints_ = std::make_unique<DiskUniqueConstraints>(config, metric_handles);
        break;
      case StorageMode::N:
        std::unreachable();
    }
  });
  // Build composite outside the outer lock — see Indices::Indices ctor.
  auto snapshot = std::make_shared<ActiveConstraints>(existence_constraints_->GetActiveConstraints(),
                                                      unique_constraints_->GetActiveConstraints(),
                                                      type_constraints_->GetActiveConstraints());
  active_constraints_.WithLock([&](ActiveConstraintsPtr &ac) { ac = std::move(snapshot); });
}

void Constraints::DropGraphClearConstraints() {
  // DROP GRAPH can only happen for IN_MEMORY so it safe to assume this cast
  static_cast<InMemoryUniqueConstraints *>(unique_constraints_.get())->DropGraphClearConstraints();
  existence_constraints_->DropGraphClearConstraints();
  type_constraints_->DropGraphClearConstraints();
  auto snapshot = std::make_shared<ActiveConstraints>(existence_constraints_->GetActiveConstraints(),
                                                      unique_constraints_->GetActiveConstraints(),
                                                      type_constraints_->GetActiveConstraints());
  active_constraints_.WithLock([&](ActiveConstraintsPtr &ac) { ac = std::move(snapshot); });
}

}  // namespace memgraph::storage
