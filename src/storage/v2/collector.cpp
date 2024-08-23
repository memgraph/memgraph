// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/collector.hpp"

#include "storage/v2/property_store.hpp"
#include "storage/v2/vertex.hpp"

void memgraph::storage::Collector::UpdateOnAddLabel(memgraph::storage::LabelId label,
                                                    memgraph::storage::Vertex *vertex) {
  if (tracked_changes_.empty()) return;

  constexpr auto all_point_types = std::array{PropertyStoreType::POINT_2D, PropertyStoreType::POINT_3D};
  for (auto prop : vertex->properties.PropertiesOfTypes(all_point_types)) {
    auto k = LabelPropKey{label, prop};
    auto it = tracked_changes_.find(k);
    if (it != tracked_changes_.end()) {
      it->second.insert(vertex);
    }
  }
}

void memgraph::storage::Collector::UpdateOnRemoveLabel(memgraph::storage::LabelId label,
                                                       memgraph::storage::Vertex *vertex) {
  if (tracked_changes_.empty()) return;

  constexpr auto all_point_types = std::array{PropertyStoreType::POINT_2D, PropertyStoreType::POINT_3D};
  for (auto prop : vertex->properties.PropertiesOfTypes(all_point_types)) {
    auto k = LabelPropKey{label, prop};
    auto it = tracked_changes_.find(k);
    if (it != tracked_changes_.end()) {
      it->second.insert(vertex);
    }
  }
}

void memgraph::storage::Collector::UpdateOnSetProperty(memgraph::storage::PropertyId prop_id,
                                                       memgraph::storage::PropertyValue const &old_value,
                                                       memgraph::storage::PropertyValue const &new_value,
                                                       memgraph::storage::Vertex *vertex) {
  if (tracked_changes_.empty()) return;
  if (!(old_value.IsPoint2d() || old_value.IsPoint3d() || new_value.IsPoint2d() || new_value.IsPoint3d())) return;

  for (auto label : vertex->labels) {
    auto k = LabelPropKey{label, prop_id};
    auto it = tracked_changes_.find(k);
    if (it != tracked_changes_.end()) {
      it->second.insert(vertex);
    }
  }
}
bool memgraph::storage::Collector::AnyTrackedChanges() const {
  auto all_tracked_changes = tracked_changes_ | std::views::values;
  auto has_any_change = [](std::unordered_set<Vertex *> const &tracked) { return !tracked.empty(); };
  return std::ranges::any_of(all_tracked_changes, has_any_change);
}
