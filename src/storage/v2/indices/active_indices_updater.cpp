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

#include "storage/v2/indices/active_indices_updater.hpp"
#include "storage/v2/indices/active_indices.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

// ActiveIndices::With* factory methods — each returns a new snapshot with one field replaced.

ActiveIndicesPtr ActiveIndices::WithLabel(std::shared_ptr<LabelIndexActiveIndices> x) const {
  return std::make_shared<ActiveIndices>(
      std::move(x), label_properties_, edge_type_, edge_type_properties_, edge_property_);
}

ActiveIndicesPtr ActiveIndices::WithLabelProperties(std::shared_ptr<LabelPropertyIndexActiveIndices> x) const {
  return std::make_shared<ActiveIndices>(label_, std::move(x), edge_type_, edge_type_properties_, edge_property_);
}

ActiveIndicesPtr ActiveIndices::WithEdgeType(std::shared_ptr<EdgeTypeIndexActiveIndices> x) const {
  return std::make_shared<ActiveIndices>(
      label_, label_properties_, std::move(x), edge_type_properties_, edge_property_);
}

ActiveIndicesPtr ActiveIndices::WithEdgeTypeProperties(std::shared_ptr<EdgeTypePropertyIndexActiveIndices> x) const {
  return std::make_shared<ActiveIndices>(label_, label_properties_, edge_type_, std::move(x), edge_property_);
}

ActiveIndicesPtr ActiveIndices::WithEdgeProperty(std::shared_ptr<EdgePropertyIndexActiveIndices> x) const {
  return std::make_shared<ActiveIndices>(label_, label_properties_, edge_type_, edge_type_properties_, std::move(x));
}

// ActiveIndicesUpdater::operator() overloads — delegate to the With* factory methods.

void ActiveIndicesUpdater::operator()(std::shared_ptr<LabelIndexActiveIndices> const &x) const {
  active_indices_.WithLock([&](ActiveIndicesPtr &ai) {
    MG_ASSERT(ai, "ActiveIndices must be initialized before updating. Was Storage fully constructed?");
    ai = ai->WithLabel(x);
  });
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<LabelPropertyIndexActiveIndices> const &x) const {
  active_indices_.WithLock([&](ActiveIndicesPtr &ai) {
    MG_ASSERT(ai, "ActiveIndices must be initialized before updating. Was Storage fully constructed?");
    ai = ai->WithLabelProperties(x);
  });
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<EdgeTypeIndexActiveIndices> const &x) const {
  active_indices_.WithLock([&](ActiveIndicesPtr &ai) {
    MG_ASSERT(ai, "ActiveIndices must be initialized before updating. Was Storage fully constructed?");
    ai = ai->WithEdgeType(x);
  });
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<EdgeTypePropertyIndexActiveIndices> const &x) const {
  active_indices_.WithLock([&](ActiveIndicesPtr &ai) {
    MG_ASSERT(ai, "ActiveIndices must be initialized before updating. Was Storage fully constructed?");
    ai = ai->WithEdgeTypeProperties(x);
  });
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<EdgePropertyIndexActiveIndices> const &x) const {
  active_indices_.WithLock([&](ActiveIndicesPtr &ai) {
    MG_ASSERT(ai, "ActiveIndices must be initialized before updating. Was Storage fully constructed?");
    ai = ai->WithEdgeProperty(x);
  });
}

}  // namespace memgraph::storage
