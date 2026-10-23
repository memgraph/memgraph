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

#include "query/relations/equality.hpp"

#include <algorithm>
#include <cmath>
#include <vector>

namespace memgraph::query::relations::equality {

namespace {

/// Whether any value held within this one, however deeply nested, answers the
/// predicate. A container is walked; anything else is asked directly.
///
/// The walk is written once because the questions asked of it differ only in
/// the leaf: a caller wanting two of them answered would otherwise walk the
/// same value twice.
bool AnyValueWithin(const TypedValue &value, auto const &holds) {
  switch (value.type()) {
    case TypedValue::Type::List:
      return std::ranges::any_of(value.UnsafeValueList(),
                                 [&](auto const &element) { return AnyValueWithin(element, holds); });
    case TypedValue::Type::Map:
      return std::ranges::any_of(value.UnsafeValueMap(),
                                 [&](auto const &entry) { return AnyValueWithin(entry.second, holds); });
    case TypedValue::Type::VectorRef: {
      // Read as the list of doubles it materializes to, without making one.
      thread_local std::vector<float> floats;
      value.MaterializeVectorRefInto(floats);
      return std::ranges::any_of(floats,
                                 [&](float element) { return holds(TypedValue(static_cast<double>(element))); });
    }
    default:
      return holds(value);
  }
}

constexpr auto kIsNull = [](const TypedValue &value) { return value.IsNull(); };

/// The two values equality does not hold equal to themselves, for its two
/// reasons: a Null leaves the pair undecided, a NaN answers false.
///
/// A point carries its coordinates as doubles and compares them together, so
/// one holding a NaN is no more equal to itself than the NaN is. Storage spells
/// this question separately, and answers a point the same way.
constexpr auto kIsUndecidable = [](const TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Null:
      return true;
    case TypedValue::Type::Double:
      return std::isnan(value.UnsafeValueDouble());
    case TypedValue::Type::Point2d: {
      auto const &point = value.UnsafeValuePoint2d();
      return std::isnan(point.x()) || std::isnan(point.y());
    }
    case TypedValue::Type::Point3d: {
      auto const &point = value.UnsafeValuePoint3d();
      return std::isnan(point.x()) || std::isnan(point.y()) || std::isnan(point.z());
    }
    default:
      return false;
  }
};

}  // namespace

bool HoldsANull(const TypedValue &value) {
  // An embedding is floats, so there is nothing to materialize to learn it holds no Null.
  if (value.type() == TypedValue::Type::VectorRef) return false;
  return AnyValueWithin(value, kIsNull);
}

bool EqualsItself(const TypedValue &value) { return !AnyValueWithin(value, kIsUndecidable); }

TypedValue EqualOfLists(TypedValue::TVector const &a, TypedValue::TVector const &b, TypedValue::allocator_type alloc) {
  // A list is equal only to a list of the same length holding equal elements,
  // so `2 = [2]` is false however deeply either side is nested. Neo4j differs
  // here, answering true for `2 = [2]` and for `[[2]] = [[[[[[2]]]]]]`.
  if (a.size() != b.size()) return TypedValue(false, alloc);

  // A part that differs settles the whole, whatever the rest holds: `[null, 1]`
  // and `[null, 2]` differ however the first element turns out. So a difference
  // answers at once, and an undecided part survives only to the end.
  auto undecided = false;
  for (size_t i = 0; i != a.size(); ++i) {
    auto const element = Equal(a[i], b[i]);
    if (element.IsNull()) {
      undecided = true;
    } else if (!element.UnsafeValueBool()) {
      return TypedValue(false, alloc);
    }
  }
  return undecided ? TypedValue(alloc) : TypedValue(true, alloc);
}

TypedValue EqualOfMaps(TypedValue::TMap const &a, TypedValue::TMap const &b, TypedValue::allocator_type alloc) {
  if (a.size() != b.size()) return TypedValue(false, alloc);

  auto undecided = false;
  for (auto const &[key, value_a] : a) {
    // Which keys a map holds is known, so a key the other side lacks settles it
    // however the values would have compared.
    auto const found = b.find(key);
    if (found == b.end()) return TypedValue(false, alloc);
    auto const value = Equal(value_a, found->second);
    if (value.IsNull()) {
      undecided = true;
    } else if (!value.UnsafeValueBool()) {
      return TypedValue(false, alloc);
    }
  }
  return undecided ? TypedValue(alloc) : TypedValue(true, alloc);
}

TypedValue EqualOfVectorRefs(const TypedValue &a, const TypedValue &b, TypedValue::allocator_type alloc) {
  thread_local std::vector<float> floats_a;
  thread_local std::vector<float> floats_b;
  a.MaterializeVectorRefInto(floats_a);
  b.MaterializeVectorRefInto(floats_b);
  return TypedValue(floats_a == floats_b, alloc);
}

TypedValue EqualOfVectorRefAndOther(const TypedValue &a, const TypedValue &b, TypedValue::allocator_type alloc) {
  // Only a list can be equal to an embedding. It is materialized with the default allocator so that
  // the transient list is freed here rather than held by the query's arena.
  auto const &embedding = a.type() == TypedValue::Type::VectorRef ? a : b;
  auto const &other = a.type() == TypedValue::Type::VectorRef ? b : a;
  if (other.type() != TypedValue::Type::List) return TypedValue(false, alloc);
  auto const list = embedding.MaterializeVectorRef(TypedValue::allocator_type{});
  return EqualOfLists(list.UnsafeValueList(), other.UnsafeValueList(), alloc);
}

}  // namespace memgraph::query::relations::equality
