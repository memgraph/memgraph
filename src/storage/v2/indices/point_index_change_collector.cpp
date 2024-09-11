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

#include "storage/v2/indices/point_index_change_collector.hpp"

#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

void PointIndexChangeCollector::UpdateOnChangeLabel(LabelId label, Vertex const *vertex) {
  if (current_changes_.empty()) return;

  constexpr auto all_point_types = std::array{PropertyStoreType::POINT};
  for (auto prop : vertex->properties.PropertiesOfTypes(all_point_types)) {
    auto k = LabelPropKey{label, prop};
    auto it = current_changes_.find(k);
    if (it != current_changes_.end()) {
      it->second.insert(vertex);
    }
  }
}

void PointIndexChangeCollector::UpdateOnSetProperty(PropertyId prop_id, PropertyValue const &old_value,
                                                    PropertyValue const &new_value, Vertex const *vertex) {
  if (current_changes_.empty()) return;
  if (!(old_value.IsPoint2d() || old_value.IsPoint3d() || new_value.IsPoint2d() || new_value.IsPoint3d())) return;

  for (auto label : vertex->labels) {
    auto k = LabelPropKey{label, prop_id};
    auto it = current_changes_.find(k);
    if (it != current_changes_.end()) {
      it->second.insert(vertex);
    }
  }
}

auto PointIndexChangeCollector::CurrentChanges() const -> TrackedChanges const & { return current_changes_; }

auto PointIndexChangeCollector::PreviousChanges() const -> TrackedChanges const & { return previous_changes_; }

void PointIndexChangeCollector::ArchiveCurrentChanges() { previous_changes_.MergeIntoAndClearOther(current_changes_); }

PointIndexChangeCollector::PointIndexChangeCollector(PointIndexContext &ctx)
    : current_changes_{std::invoke([&]() {
        auto rng = ctx.IndexKeys() | std::views::transform([](auto key) {
                     return std::pair{key, absl::flat_hash_set<Vertex const *>{}};
                   });
        return TrackedChanges{rng.begin(), rng.end()};
      })},
      /// Note: this is a copy of current_changes_ so that it has the same keys
      previous_changes_{current_changes_} {}
}  // namespace memgraph::storage
