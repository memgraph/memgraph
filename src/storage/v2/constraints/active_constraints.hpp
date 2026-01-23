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

namespace memgraph::storage {

struct Vertex;
struct Transaction;

struct ActiveConstraints {
  ActiveConstraints() = delete;
  explicit ActiveConstraints(std::unique_ptr<ExistenceConstraints::ActiveConstraints> existence,
                             std::unique_ptr<UniqueConstraints::ActiveConstraints> unique,
                             std::unique_ptr<TypeConstraints::ActiveConstraints> type)
      : existence_{std::move(existence)}, unique_{std::move(unique)}, type_{std::move(type)} {}

  // Related to collection and validation
  bool empty() const { return existence_->empty() && unique_->empty() && type_->empty(); }

  std::unique_ptr<ExistenceConstraints::ActiveConstraints> existence_;
  std::unique_ptr<UniqueConstraints::ActiveConstraints> unique_;
  std::unique_ptr<TypeConstraints::ActiveConstraints> type_;
};

}  // namespace memgraph::storage
