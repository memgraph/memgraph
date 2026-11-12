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

#include <algorithm>
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

#include <functional>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <boost/unordered/unordered_flat_set.hpp>
#include "absl/hash/hash.h"

namespace memgraph::storage {

struct Vertex;
struct PointIndexContext;

using ChangedVertices = boost::unordered_flat_set<Vertex const *, absl::Hash<Vertex const *>>;

struct TrackedChanges {
  TrackedChanges() = default;

  template <typename It>
  TrackedChanges(It b, It e) : data(b, e) {}

  bool AnyChanges() const {
    auto all_tracked_changes = data | std::views::values;
    auto has_any_change = [](ChangedVertices const &tracked) { return !tracked.empty(); };
    return std::ranges::any_of(all_tracked_changes, has_any_change);
  }

  void MergeIntoAndClearOther(TrackedChanges &other) {
    // Iterate over other's keys to allow dynamic key insertion into this
    for (auto &[k, other_set] : other.data) {
      auto &my_set = data[k];  // Insert key if missing
      if (my_set.empty()) {
        my_set.swap(other_set);
      } else {
        // Not merge(): a throwing insert inside merge drops the element from both sets.
        my_set.insert(other_set.begin(), other_set.end());
        other_set.clear();
      }
    }
  }

  auto begin() const { return data.begin(); }

  auto end() const { return data.end(); }

  bool empty() const { return data.empty(); }

  auto find(LabelPropKey key) { return data.find(key); }

  auto find(LabelPropKey key) const { return data.find(key); }

 private:
  std::unordered_map<LabelPropKey, ChangedVertices> data;
};

struct PointIndexChangeCollector {
  PointIndexChangeCollector() = default;

  explicit PointIndexChangeCollector(PointIndexContext &ctx);

  void UpdateOnChangeLabel(LabelId label, Vertex const *vertex);

  void UpdateOnSetProperty(PropertyId prop_id, const PropertyValue &old_value, const PropertyValue &new_value,
                           Vertex const *vertex);

  auto CurrentChanges() const -> TrackedChanges const &;

  auto PreviousChanges() const -> TrackedChanges const &;

  void ArchiveCurrentChanges();

  void UpdateOnVertexDelete(Vertex *vertex);

 private:
  TrackedChanges current_changes_;
  mutable std::optional<TrackedChanges> previous_changes_;  // Lazy initialization
};
}  // namespace memgraph::storage
