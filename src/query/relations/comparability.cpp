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
#include <vector>

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

std::optional<std::partial_ordering> CompareOfVectorRefs(const TypedValue &a, const TypedValue &b) {
  thread_local std::vector<float> floats_a;
  thread_local std::vector<float> floats_b;
  a.MaterializeVectorRefInto(floats_a);
  b.MaterializeVectorRefInto(floats_b);

  auto const shared = std::min(floats_a.size(), floats_b.size());
  for (auto at = std::size_t{0}; at != shared; ++at) {
    auto const element = floats_a[at] <=> floats_b[at];
    if (element == std::partial_ordering::unordered) return std::nullopt;
    if (std::is_neq(element)) return element;
  }
  return floats_a.size() <=> floats_b.size();
}

std::optional<std::partial_ordering> CompareOfVectorRefAndOther(const TypedValue &a, const TypedValue &b) {
  auto const a_is_embedding = a.type() == TypedValue::Type::VectorRef;
  auto const &other = a_is_embedding ? b : a;
  if (other.type() != TypedValue::Type::List) return std::nullopt;

  // Materialized with the default allocator so the transient list is not held by the query's arena.
  auto const list = (a_is_embedding ? a : b).MaterializeVectorRef(TypedValue::allocator_type{});
  return a_is_embedding ? CompareOfLists(list.UnsafeValueList(), other.UnsafeValueList())
                        : CompareOfLists(other.UnsafeValueList(), list.UnsafeValueList());
}

}  // namespace memgraph::query::relations::comparability
