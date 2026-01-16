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

#include "storage/v2/config.hpp"
#include "storage/v2/constraints/existence_constraints.hpp"
#include "storage/v2/constraints/type_constraints.hpp"
#include "storage/v2/constraints/unique_constraints.hpp"
#include "storage/v2/storage_mode.hpp"

namespace memgraph::storage {

struct Constraints {
  Constraints(const Config &config, StorageMode storage_mode);

  Constraints(const Constraints &) = delete;
  Constraints(Constraints &&) = delete;
  Constraints &operator=(const Constraints &) = delete;
  Constraints &operator=(Constraints &&) = delete;
  ~Constraints() = default;

  void DropGraphClearConstraints() const;
  /// Remove all POPULATING constraints (for abort handling).
  /// Since schema transactions are exclusive, any POPULATING constraint belongs to the current transaction.
  void AbortPopulating() const;
  bool HasTypeConstraints() const;

  std::unique_ptr<ExistenceConstraints> existence_constraints_;
  std::unique_ptr<UniqueConstraints> unique_constraints_;
  std::unique_ptr<TypeConstraints> type_constraints_;

  bool empty() const {
    return existence_constraints_->empty() && unique_constraints_->empty() && type_constraints_->empty();
  }
};

}  // namespace memgraph::storage
