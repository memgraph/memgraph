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

#include "storage/v2/constraints/active_constraints_updater.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

// ActiveConstraints::With* factory methods — each returns a new snapshot with one field replaced.

ActiveConstraintsPtr ActiveConstraints::WithExistence(
    std::shared_ptr<ExistenceConstraints::ActiveConstraints> x) const {
  return std::make_shared<ActiveConstraints>(std::move(x), unique_, type_);
}

ActiveConstraintsPtr ActiveConstraints::WithUnique(std::shared_ptr<UniqueConstraints::ActiveConstraints> x) const {
  return std::make_shared<ActiveConstraints>(existence_, std::move(x), type_);
}

ActiveConstraintsPtr ActiveConstraints::WithType(std::shared_ptr<TypeConstraints::ActiveConstraints> x) const {
  return std::make_shared<ActiveConstraints>(existence_, unique_, std::move(x));
}

// ActiveConstraintsUpdater::operator() overloads — delegate to the With* factory methods.

void ActiveConstraintsUpdater::operator()(std::shared_ptr<ExistenceConstraints::ActiveConstraints> const &x) const {
  active_constraints_.WithLock([&](ActiveConstraintsPtr &ac) {
    MG_ASSERT(ac, "ActiveConstraints must be initialized before updating. Was Storage fully constructed?");
    ac = ac->WithExistence(x);
  });
}

void ActiveConstraintsUpdater::operator()(std::shared_ptr<UniqueConstraints::ActiveConstraints> const &x) const {
  active_constraints_.WithLock([&](ActiveConstraintsPtr &ac) {
    MG_ASSERT(ac, "ActiveConstraints must be initialized before updating. Was Storage fully constructed?");
    ac = ac->WithUnique(x);
  });
}

void ActiveConstraintsUpdater::operator()(std::shared_ptr<TypeConstraints::ActiveConstraints> const &x) const {
  active_constraints_.WithLock([&](ActiveConstraintsPtr &ac) {
    MG_ASSERT(ac, "ActiveConstraints must be initialized before updating. Was Storage fully constructed?");
    ac = ac->WithType(x);
  });
}

}  // namespace memgraph::storage
