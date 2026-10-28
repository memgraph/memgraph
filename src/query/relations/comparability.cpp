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
#include <span>

#include "utils/logging.hpp"

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

namespace {

// What Compare answers for a double against a list element: only a number is placed against it.
std::optional<std::partial_ordering> CompareDoubleAndElement(double d, const TypedValue &element) {
  if (element.IsDouble()) return d <=> element.UnsafeValueDouble();
  if (element.IsInt()) return ReversedOrder(PlaceIntegerAgainstDouble(element.UnsafeValueInt(), d));
  return std::nullopt;
}

// CompareOfLists over the embedding's floats and a plain list, without building the list of doubles.
std::optional<std::partial_ordering> CompareOfFloatsAndList(std::span<const float> floats,
                                                            TypedValue::TVector const &list, bool floats_first) {
  auto const shared = std::min(floats.size(), list.size());
  for (auto at = std::size_t{0}; at != shared; ++at) {
    auto const element = CompareDoubleAndElement(static_cast<double>(floats[at]), list[at]);
    if (!element || *element == std::partial_ordering::unordered) return std::nullopt;
    if (std::is_neq(*element)) return floats_first ? *element : ReversedOrder(*element);
  }
  return floats_first ? floats.size() <=> list.size() : list.size() <=> floats.size();
}

}  // namespace

std::optional<std::partial_ordering> CompareWithVectorRef(const TypedValue &a, const TypedValue &b) {
  DMG_ASSERT(a.IsVectorRef() || b.IsVectorRef(), "CompareWithVectorRef takes at least one embedding");
  if (a.IsVectorRef() && b.IsVectorRef()) {
    auto const floats_a = a.ReadVectorRef();
    auto const floats_b = b.ReadVectorRef();
    auto const shared = std::min(floats_a.size(), floats_b.size());
    for (auto at = std::size_t{0}; at != shared; ++at) {
      auto const element = floats_a[at] <=> floats_b[at];
      if (element == std::partial_ordering::unordered) return std::nullopt;
      if (std::is_neq(element)) return element;
    }
    return floats_a.size() <=> floats_b.size();
  }

  auto const a_is_embedding = a.IsVectorRef();
  auto const &embedding = a_is_embedding ? a : b;
  auto const &other = a_is_embedding ? b : a;
  // A VectorRef never nests, so a plain list is the only other value an embedding orders against.
  if (!other.IsPlainList()) return std::nullopt;
  return CompareOfFloatsAndList(embedding.ReadVectorRef(), other.UnsafeValueList(), a_is_embedding);
}

}  // namespace memgraph::query::relations::comparability
