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

void memgraph::storage::ActiveIndicesUpdater::operator()(std::shared_ptr<LabelIndexActiveIndices> const &x) const {
  active_indices_.WithLock([&](std::shared_ptr<ActiveIndices const> &ai) {
    if (ai) {
      ai = std::make_shared<ActiveIndices>(
          std::move(x), ai->label_properties_, ai->edge_type_, ai->edge_type_properties_, ai->edge_property_);
    } else {
      ai = std::make_shared<ActiveIndices>(std::move(x), nullptr, nullptr, nullptr, nullptr);
    }
  });
}

void memgraph::storage::ActiveIndicesUpdater::operator()(
    std::shared_ptr<LabelPropertyIndex::ActiveIndices> const &x) const {
  active_indices_.WithLock([&](std::shared_ptr<ActiveIndices const> &ai) {
    if (ai) {
      ai = std::make_shared<ActiveIndices>(
          ai->label_, std::move(x), ai->edge_type_, ai->edge_type_properties_, ai->edge_property_);
    } else {
      ai = std::make_shared<ActiveIndices>(nullptr, std::move(x), nullptr, nullptr, nullptr);
    }
  });
}

void memgraph::storage::ActiveIndicesUpdater::operator()(std::shared_ptr<EdgeTypeIndex::ActiveIndices> const &x) const {
  active_indices_.WithLock([&](std::shared_ptr<ActiveIndices const> &ai) {
    if (ai) {
      ai = std::make_shared<ActiveIndices>(
          ai->label_, ai->label_properties_, std::move(x), ai->edge_type_properties_, ai->edge_property_);
    } else {
      ai = std::make_shared<ActiveIndices>(nullptr, nullptr, std::move(x), nullptr, nullptr);
    }
  });
}

void memgraph::storage::ActiveIndicesUpdater::operator()(
    std::shared_ptr<EdgeTypePropertyIndex::ActiveIndices> const &x) const {
  active_indices_.WithLock([&](std::shared_ptr<ActiveIndices const> &ai) {
    if (ai) {
      ai = std::make_shared<ActiveIndices>(
          ai->label_, ai->label_properties_, ai->edge_type_, std::move(x), ai->edge_property_);
    } else {
      ai = std::make_shared<ActiveIndices>(nullptr, nullptr, nullptr, std::move(x), nullptr);
    }
  });
}

void memgraph::storage::ActiveIndicesUpdater::operator()(
    std::shared_ptr<EdgePropertyIndex::ActiveIndices> const &x) const {
  active_indices_.WithLock([&](std::shared_ptr<ActiveIndices const> &ai) {
    if (ai) {
      ai = std::make_shared<ActiveIndices>(
          ai->label_, ai->label_properties_, ai->edge_type_, ai->edge_type_properties_, std::move(x));
    } else {
      ai = std::make_shared<ActiveIndices>(nullptr, nullptr, nullptr, nullptr, std::move(x));
    }
  });
}
