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

#pragma once

#include <cstdint>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "storage/v2/constraints/active_constraints.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/transaction_constants.hpp"
#include "versioning/branch_engine.hpp"

namespace memgraph::versioning {

// The diff engine mirrors no constraints (D12): a branch is checked against main's current constraint set. This is a
// snapshot of it, listing all committed constraints.
class MainConstraints {
 public:
  using ExistenceList = std::vector<std::pair<storage::LabelId, storage::PropertyId>>;
  using TypeList = std::vector<std::tuple<storage::LabelId, storage::PropertyId, storage::TypeConstraintKind>>;

  explicit MainConstraints(BranchContext &ctx) : active_{ctx.core()->main().GetActiveConstraints()} {}

  ExistenceList Existence() const { return active_->existence_->ListConstraints(kAllCommitted); }

  // Empty (no allocation beyond the shared_ptr copy) when main has no type constraints.
  TypeList Types() const {
    if (active_->type_->empty()) return {};
    return active_->type_->ListConstraints(kAllCommitted);
  }

 private:
  static constexpr uint64_t kAllCommitted = storage::kTransactionInitialId - 1;
  storage::ActiveConstraintsPtr active_;
};

}  // namespace memgraph::versioning
