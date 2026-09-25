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
#include <utility>
#include <vector>

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

}  // namespace

std::partial_ordering CompareOfLists(TypedValue::TVector const &a, TypedValue::TVector const &b) {
  return std::lexicographical_compare_three_way(
      a.begin(), a.end(), b.begin(), b.end(), [](TypedValue const &x, TypedValue const &y) { return Compare(x, y); });
}

std::partial_ordering CompareOfMaps(TypedValue::TMap const &a, TypedValue::TMap const &b) {
  // The map holding fewer entries comes first whatever its keys are, which is
  // where a stored pair of unequal size parts.
  if (auto const size = a.size() <=> b.size(); std::is_neq(size)) return size;

  // A map keeps its entries in the order its keys' names sort in, so one walk
  // reads them in that order. A store keeps the same map by the identifiers
  // those names were interned as and is walked back into this order to compare,
  // so the two layers place a pair of maps alike.
  //
  // One entry at a time, each key and then the value under it. Reading every
  // key before any value would part a pair at a later entry and place the two
  // the other way round.
  for (auto one = a.begin(), other = b.begin(); one != a.end(); ++one, ++other) {
    if (auto const key = one->first <=> other->first; std::is_neq(key)) return key;
    if (auto const value = Compare(one->second, other->second); std::is_neq(value)) return value;
  }
  return std::partial_ordering::equivalent;
}

}  // namespace memgraph::query::relations::orderability
