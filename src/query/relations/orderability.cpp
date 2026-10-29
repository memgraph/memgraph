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

#include "query/relations/orderability.hpp"

#include <algorithm>
#include <optional>
#include <span>

#include "utils/logging.hpp"

import memgraph.storage.property_value;

namespace memgraph::query::relations::orderability {

namespace {

/// The run the stored order keeps a value of this type in, for the types a
/// store holds. Nothing for the rest, which are the graph types and a function:
/// a store never holds one, so the stored order says nothing about where they go.
///
/// The switch names every type, so one added to the value has to be answered
/// for here before this compiles.
constexpr std::optional<storage::Stretch> StoredRunOf(TypedValue::Type type) {
  switch (type) {
    using enum TypedValue::Type;
    case Null:
      return storage::StretchOf(storage::PropertyValueType::Null);
    case Bool:
      return storage::StretchOf(storage::PropertyValueType::Bool);
    case Int:
      return storage::StretchOf(storage::PropertyValueType::Int);
    case Double:
      return storage::StretchOf(storage::PropertyValueType::Double);
    case String:
      return storage::StretchOf(storage::PropertyValueType::String);
    case List:
    case VectorRef:
      return storage::StretchOf(storage::PropertyValueType::List);
    case Map:
      return storage::StretchOf(storage::PropertyValueType::Map);
    case Date:
    case LocalTime:
    case LocalDateTime:
    case Duration:
      return storage::StretchOf(storage::PropertyValueType::TemporalData);
    case ZonedDateTime:
      return storage::StretchOf(storage::PropertyValueType::ZonedTemporalData);
    case Enum:
      return storage::StretchOf(storage::PropertyValueType::Enum);
    case Point2d:
      return storage::StretchOf(storage::PropertyValueType::Point2d);
    case Point3d:
      return storage::StretchOf(storage::PropertyValueType::Point3d);

    case Vertex:
    case Edge:
    case VirtualEdge:
    case VirtualNode:
    case Path:
    case Graph:
    case VirtualGraph:
    case Function:
      return std::nullopt;
  }
  return std::nullopt;
}

/// Whether a sort reads a column in an order the stored one can be walked in.
///
/// A plan is allowed to drop a sort because a scan already walked the column,
/// which holds only where the two orders agree. They are not the same order: a
/// sort tells the four temporal kinds apart where a store keeps them in one
/// run, so the sort's order refines the stored one. What may not happen is the
/// two disagreeing, which is what this asks.
///
/// Asked of the tables rather than of drawn values, so that a type seated
/// wrongly fails to build rather than waiting for a law to draw the pair.
constexpr bool ASortRefinesTheStoredOrder() {
  for (auto first = 0U; first != TypedValue::kTypeCount; ++first) {
    auto const one_run = StoredRunOf(static_cast<TypedValue::Type>(first));
    if (!one_run) continue;
    for (auto second = 0U; second != TypedValue::kTypeCount; ++second) {
      auto const other_run = StoredRunOf(static_cast<TypedValue::Type>(second));
      if (!other_run) continue;

      auto const sorted = detail::kPositions[first] <=> detail::kPositions[second];
      auto const stored = *one_run <=> *other_run;

      // A store putting one before the other settles where a sort puts them.
      if (std::is_lt(stored) && !std::is_lt(sorted)) return false;
      if (std::is_gt(stored) && !std::is_gt(sorted)) return false;
      // One run holds both, so a sort may still tell them apart, but it may not
      // put them in one place unless the store does too.
      if (std::is_eq(sorted) && !std::is_eq(stored)) return false;
    }
  }
  return true;
}

static_assert(ASortRefinesTheStoredOrder(),
              "A sort and the stored order place two types differently, so a scan standing in for a sort would hand "
              "back a column in an order the sort would not");

/// Elementwise as the list of doubles the floats stand for, each pair going through Compare so that a NaN,
/// an Int, a Null or any other element is placed exactly as it is in a list.
template <bool ListFirst>
std::partial_ordering CompareFloatsWithList(std::span<const float> floats, TypedValue::TVector const &list) {
  auto const common = std::min(floats.size(), list.size());
  for (auto i = 0UZ; i != common; ++i) {
    TypedValue const as_double{static_cast<double>(floats[i])};
    auto const order = ListFirst ? Compare(list[i], as_double) : Compare(as_double, list[i]);
    if (!std::is_eq(order)) return order;
  }
  return ListFirst ? list.size() <=> floats.size() : floats.size() <=> list.size();
}

}  // namespace

std::partial_ordering CompareOfLists(TypedValue::TVector const &a, TypedValue::TVector const &b) {
  return std::lexicographical_compare_three_way(
      a.begin(), a.end(), b.begin(), b.end(), [](TypedValue const &x, TypedValue const &y) { return Compare(x, y); });
}

std::partial_ordering CompareOfVectorRefs(TypedValue const &a, TypedValue const &b) {
  DMG_ASSERT(a.IsVectorRef() && b.IsVectorRef(), "Both sides must be VectorRef");
  auto const floats_a = a.ReadVectorRef();
  auto const floats_b = b.ReadVectorRef();
  return std::lexicographical_compare_three_way(
      floats_a.begin(), floats_a.end(), floats_b.begin(), floats_b.end(), [](float x, float y) {
        return PlaceDoubles(x, y);
      });
}

std::partial_ordering CompareOfTiedTypes(TypedValue const &a, TypedValue const &b) {
  if (a.IsVectorRef() && b.IsPlainList()) return CompareFloatsWithList<false>(a.ReadVectorRef(), b.UnsafeValueList());
  DMG_ASSERT(a.IsPlainList() && b.IsVectorRef(), "Only a VectorRef against a List shares a position with unlike types");
  return CompareFloatsWithList<true>(b.ReadVectorRef(), a.UnsafeValueList());
}

}  // namespace memgraph::query::relations::orderability
