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

#pragma once

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

#include <functional>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace memgraph::storage {

struct Vertex;

struct Collector {
  Collector() = default;

  explicit Collector(std::ranges::view auto keys)
      : tracked_changes_{std::invoke([&]() {
          auto rng = keys | std::views::transform([](auto key) {
                       return std::pair{key, std::unordered_set<Vertex *>{}};
                     });
          return std::unordered_map<LabelPropKey, std::unordered_set<Vertex *>>{rng.begin(), rng.end()};
        })} {}

  void UpdateOnAddLabel(LabelId label, Vertex *vertex);
  void UpdateOnRemoveLabel(LabelId label, Vertex *vertex);

  void UpdateOnSetProperty(PropertyId prop_id, const PropertyValue &old_value, const PropertyValue &new_value,
                           Vertex *vertex);

  bool AnyTrackedChanges() const;

 private:
  std::unordered_map<LabelPropKey, std::unordered_set<Vertex *>> tracked_changes_;
};
}  // namespace memgraph::storage
