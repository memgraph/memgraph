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

#include <memory>
#include <utility>

#include "storage/v2/constraints/existence_constraints.hpp"
#include "storage/v2/constraints/type_constraints.hpp"
#include "storage/v2/constraints/unique_constraints.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

struct Vertex;
struct Transaction;

struct ActiveConstraints {
  ActiveConstraints() = delete;

  explicit ActiveConstraints(std::shared_ptr<ExistenceConstraints::ActiveConstraints> existence,
                             std::shared_ptr<UniqueConstraints::ActiveConstraints> unique,
                             std::shared_ptr<TypeConstraints::ActiveConstraints> type)
      : existence_{std::move(existence)}, unique_{std::move(unique)}, type_{std::move(type)} {}

  /// Returns a new ActiveConstraints with one field replaced, identified by
  /// pointer-to-member. Mirrors `ActiveIndices::With<Member>` -- keeps layout
  /// knowledge in one place and avoids the positional-argument foot-gun of the
  /// 3-arg ctor.
  template <auto Member, class X>
  [[nodiscard]] std::shared_ptr<ActiveConstraints const> With(X x) const {
    auto next = *this;
    next.*Member = std::move(x);
    return std::make_shared<ActiveConstraints>(std::move(next));
  }

  // Related to collection and validation
  bool empty() const { return existence_->empty() && unique_->empty() && type_->empty(); }

  std::shared_ptr<ExistenceConstraints::ActiveConstraints> existence_;
  std::shared_ptr<UniqueConstraints::ActiveConstraints> unique_;
  std::shared_ptr<TypeConstraints::ActiveConstraints> type_;
};

using ActiveConstraintsPtr = std::shared_ptr<ActiveConstraints const>;
using ActiveConstraintsStore = utils::Synchronized<ActiveConstraintsPtr, utils::WritePrioritizedRWLock>;

}  // namespace memgraph::storage
