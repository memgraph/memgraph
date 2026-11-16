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

#include "storage/v2/indices/ordered_property_paths.hpp"

#include <range/v3/all.hpp>

#include "utils/logging.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

OrderedPropertyPaths::OrderedPropertyPaths(std::vector<PropertyPath> paths) : sorted_{std::move(paths)} {
  auto inverse_permutation = rv::iota(std::size_t{}, sorted_.size()) | r::to_vector;
  // Sorting the two as one range permutes both together, so the paths end up sorted and each one's
  // original position travels with it.
  r::sort(rv::zip(inverse_permutation, sorted_), std::less{}, [](auto const &value) -> decltype(auto) {
    return std::get<1>(value);
  });
  position_lookup_ = std::move(inverse_permutation);
  // A store is read one forward pass, so a path named twice is searched for the second time behind
  // the reader and answers Null: a wrong answer for the slot rather than a refusal. Sorting has put
  // any repeat next to its first occurrence.
  DMG_ASSERT(r::adjacent_find(sorted_) == sorted_.end(), "A property may be read only once in one pass");
}

auto OrderedPropertyPaths::FromPropertyIds(std::span<PropertyId const> properties) -> OrderedPropertyPaths {
  auto paths = std::vector<PropertyPath>{};
  paths.reserve(properties.size());
  for (auto const property : properties) paths.emplace_back(property);
  return OrderedPropertyPaths{std::move(paths)};
}

}  // namespace memgraph::storage
