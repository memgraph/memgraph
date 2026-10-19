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
}

}  // namespace memgraph::storage
