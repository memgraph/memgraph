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

void memgraph::storage::ActiveIndicesUpdater::operator()(std::shared_ptr<LabelIndexActiveIndices> const &x) const {
  active_indices_.WithLock([&](std::shared_ptr<ActiveIndices const> &ai) {
    MG_ASSERT(ai, "ActiveIndices must be initialized before updating. Was Storage fully constructed?");
    ai = std::make_shared<ActiveIndices>(
        x, ai->label_properties_, ai->edge_type_, ai->edge_type_properties_, ai->edge_property_);
  });
}

void memgraph::storage::ActiveIndicesUpdater::operator()(
    std::shared_ptr<LabelPropertyIndexActiveIndices> const &x) const {
  active_indices_.WithLock([&](std::shared_ptr<ActiveIndices const> &ai) {
    MG_ASSERT(ai, "ActiveIndices must be initialized before updating. Was Storage fully constructed?");
    ai = std::make_shared<ActiveIndices>(ai->label_, x, ai->edge_type_, ai->edge_type_properties_, ai->edge_property_);
  });
}

void memgraph::storage::ActiveIndicesUpdater::operator()(std::shared_ptr<EdgeTypeIndexActiveIndices> const &x) const {
  active_indices_.WithLock([&](std::shared_ptr<ActiveIndices const> &ai) {
    MG_ASSERT(ai, "ActiveIndices must be initialized before updating. Was Storage fully constructed?");
    ai = std::make_shared<ActiveIndices>(
        ai->label_, ai->label_properties_, x, ai->edge_type_properties_, ai->edge_property_);
  });
}

void memgraph::storage::ActiveIndicesUpdater::operator()(
    std::shared_ptr<EdgeTypePropertyIndexActiveIndices> const &x) const {
  active_indices_.WithLock([&](std::shared_ptr<ActiveIndices const> &ai) {
    MG_ASSERT(ai, "ActiveIndices must be initialized before updating. Was Storage fully constructed?");
    ai = std::make_shared<ActiveIndices>(ai->label_, ai->label_properties_, ai->edge_type_, x, ai->edge_property_);
  });
}

void memgraph::storage::ActiveIndicesUpdater::operator()(
    std::shared_ptr<EdgePropertyIndexActiveIndices> const &x) const {
  active_indices_.WithLock([&](std::shared_ptr<ActiveIndices const> &ai) {
    MG_ASSERT(ai, "ActiveIndices must be initialized before updating. Was Storage fully constructed?");
    ai = std::make_shared<ActiveIndices>(
        ai->label_, ai->label_properties_, ai->edge_type_, ai->edge_type_properties_, x);
  });
}
