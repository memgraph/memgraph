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
  // Pairwise from the start, settling on the first position the two differ at.
  // Only the elements both lists have are read: where one runs out, the length
  // decides, and a shorter list comes first whatever the longer one holds next.
  // That is what leaves `[1] < [1, null]` decided while `[1, 2] >= [1, null]` is
  // not, since the second reads an element against the null and the first does
  // not reach it.
  auto const shared = std::min(a.size(), b.size());
  for (auto at = std::size_t{0}; at != shared; ++at) {
    auto const element = Compare(a[at], b[at]);

    // An element pair this relation cannot decide leaves the two lists
    // undecided: whether that position was the deciding one is itself unknown.
    if (!element) return std::nullopt;

    // An element pair it places nowhere, which is a NaN, leaves the two lists
    // placed nowhere. That is a different answer from an undecided one: all four
    // comparisons read it as false rather than as Null.
    if (*element == std::partial_ordering::unordered) return std::partial_ordering::unordered;

    if (std::is_neq(*element)) return element;
  }
  return a.size() <=> b.size();
}

}  // namespace memgraph::query::relations::comparability
