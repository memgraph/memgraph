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

namespace {
constexpr auto kUninitMsg = "ActiveIndices must be initialized before updating. Was Storage fully constructed?";

template <auto Member, class X>
void Publish(ActiveIndicesStore &store, X const &x) {
  store.WithLock([&](ActiveIndicesPtr &ai) {
    MG_ASSERT(ai, kUninitMsg);
    ai = ai->With<Member>(x);
  });
}
}  // namespace

void ActiveIndicesUpdater::operator()(std::shared_ptr<LabelIndexActiveIndices> const &x) const {
  Publish<&ActiveIndices::label_>(active_indices_, x);
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<LabelPropertyIndexActiveIndices> const &x) const {
  Publish<&ActiveIndices::label_properties_>(active_indices_, x);
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<EdgeTypeIndexActiveIndices> const &x) const {
  Publish<&ActiveIndices::edge_type_>(active_indices_, x);
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<EdgeTypePropertyIndexActiveIndices> const &x) const {
  Publish<&ActiveIndices::edge_type_properties_>(active_indices_, x);
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<EdgePropertyIndexActiveIndices> const &x) const {
  Publish<&ActiveIndices::edge_property_>(active_indices_, x);
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<TextIndexActiveIndices> const &x) const {
  Publish<&ActiveIndices::text_>(active_indices_, x);
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<TextEdgeIndexActiveIndices> const &x) const {
  Publish<&ActiveIndices::text_edge_>(active_indices_, x);
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<PointIndexActiveIndices const> const &x) const {
  Publish<&ActiveIndices::point_>(active_indices_, x);
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<VectorIndexActiveIndices const> const &x) const {
  Publish<&ActiveIndices::vector_>(active_indices_, x);
}

void ActiveIndicesUpdater::operator()(std::shared_ptr<VectorEdgeIndexActiveIndices const> const &x) const {
  Publish<&ActiveIndices::vector_edge_>(active_indices_, x);
}

}  // namespace memgraph::storage
