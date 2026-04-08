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

#include "storage/v2/constraints/active_constraints.hpp"

namespace memgraph::storage {

struct ActiveConstraintsUpdater {
  explicit ActiveConstraintsUpdater(ActiveConstraintsStore &active_constraints)
      : active_constraints_(active_constraints) {}

  void operator()(std::shared_ptr<ExistenceConstraints::ActiveConstraints> const &x) const;
  void operator()(std::shared_ptr<UniqueConstraints::ActiveConstraints> const &x) const;
  void operator()(std::shared_ptr<TypeConstraints::ActiveConstraints> const &x) const;

 private:
  ActiveConstraintsStore &active_constraints_;
};

}  // namespace memgraph::storage
