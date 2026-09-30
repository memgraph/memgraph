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

#include "query/relations/comparability.hpp"

#include <algorithm>

namespace memgraph::query::relations::comparability {

std::optional<std::partial_ordering> CompareOfLists(TypedValue::TVector const &a, TypedValue::TVector const &b) {
  // Only the common prefix is compared; past it, the length decides. So
  // `[1] < [1, null]` is true: the null is never read.
  auto const shared = std::min(a.size(), b.size());
  for (auto at = std::size_t{0}; at != shared; ++at) {
    auto const element = Compare(a[at], b[at]);

    // Undecided element (Null, unlike types, NaN): the lists are undecided too.
    // A NaN is unordered as a scalar, but the specification counts it incomparable,
    // and inside a list that makes the pair Null.
    if (!element || *element == std::partial_ordering::unordered) return std::nullopt;

    if (std::is_neq(*element)) return element;
  }
  return a.size() <=> b.size();
}

}  // namespace memgraph::query::relations::comparability
