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

/// @file
/// Equivalence: one of the four relations openCypher defines over values, the
/// one DISTINCT and grouping read.
///
/// It differs from equality over two values. A Null it holds equivalent to a
/// Null, where equality answers Null and decides nothing. A NaN it holds
/// equivalent to a NaN, following the order, where equality reads one as
/// different from itself. A container is walked element by element for the same
/// reason: deriving the container cases from equality would inherit the Null it
/// propagates and read that as a difference.
#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>

#include "query/relations/equality.hpp"
#include "query/typed_value.hpp"

namespace memgraph::query::relations::equivalence {

/// The two a set or a map keyed by this relation is built with.
///
/// A structure names them where it is declared, which is the only place the
/// choice between the four relations is visible: reaching for the standard
/// equality instead would silently key it by a different one. They live beside
/// the relation rather than on the value, so a caller cannot name them without
/// including the header that defines what they answer.
struct KeyEqual {
  bool operator()(TypedValue const &left, TypedValue const &right) const;
};

struct Hasher {
  size_t operator()(TypedValue const &value) const;
};

/// Whether two coordinates are the same coordinate.
///
/// A point holds doubles, and every NaN is one value here as it is anywhere
/// else, which the comparison a double is given by default does not say.
inline bool AlikeAsCoordinates(double lhs, double rhs) { return lhs == rhs || (std::isnan(lhs) && std::isnan(rhs)); }

/// Whether two values are equivalent.
inline bool Equivalent(const TypedValue &lhs, const TypedValue &rhs) {
  // Equivalence parts from equality over a Null and over a NaN, each of which
  // it holds equivalent to itself, and containers hold that of their elements
  // too. Deriving the container cases from equality would instead inherit the
  // Null it propagates and read it as not equivalent, so they are walked here.
  // Only a pair of the same type can differ that way, which lets every other
  // pair reach equality below through a single comparison; this is the hot
  // path, since it is the one a hash set of query values takes on every probe.
  if (lhs.type() == rhs.type()) {
    switch (lhs.type()) {
      using enum TypedValue::Type;
      case Null:
        return true;
      // Answering directly rather than through equality, which would build and
      // destroy a whole query value to carry one bit back.
      case Bool:
        return lhs.UnsafeValueBool() == rhs.UnsafeValueBool();
      case Int:
        return lhs.UnsafeValueInt() == rhs.UnsafeValueInt();
      case Double: {
        auto const lhs_value = lhs.UnsafeValueDouble();
        auto const rhs_value = rhs.UnsafeValueDouble();
        // Equivalence holds two values alike exactly where the order places
        // them alike, and the order places every NaN together. IEEE equality
        // separates them, itself included, so they are held here. The test is
        // reached only once the values differ, which no pair of numbers does.
        return lhs_value == rhs_value || (std::isnan(lhs_value) && std::isnan(rhs_value));
      }
      case String:
        return lhs.UnsafeValueString() == rhs.UnsafeValueString();
      case List: {
        const auto &list_lhs = lhs.UnsafeValueList();
        const auto &list_rhs = rhs.UnsafeValueList();
        return list_lhs.size() == list_rhs.size() &&
               std::equal(
                   list_lhs.begin(), list_lhs.end(), list_rhs.begin(), [](const TypedValue &l, const TypedValue &r) {
                     return Equivalent(l, r);
                   });
      }
      case Map: {
        const auto &map_lhs = lhs.UnsafeValueMap();
        const auto &map_rhs = rhs.UnsafeValueMap();
        if (map_lhs.size() != map_rhs.size()) return false;
        return std::ranges::all_of(map_lhs, [&](const auto &kv) {
          auto const it = map_rhs.find(kv.first);
          return it != map_rhs.end() && Equivalent(kv.second, it->second);
        });
      }
      case Point2d: {
        auto const &point_lhs = lhs.UnsafeValuePoint2d();
        auto const &point_rhs = rhs.UnsafeValuePoint2d();
        return point_lhs.crs() == point_rhs.crs() && AlikeAsCoordinates(point_lhs.x(), point_rhs.x()) &&
               AlikeAsCoordinates(point_lhs.y(), point_rhs.y());
      }
      case Point3d: {
        auto const &point_lhs = lhs.UnsafeValuePoint3d();
        auto const &point_rhs = rhs.UnsafeValuePoint3d();
        return point_lhs.crs() == point_rhs.crs() && AlikeAsCoordinates(point_lhs.x(), point_rhs.x()) &&
               AlikeAsCoordinates(point_lhs.y(), point_rhs.y()) && AlikeAsCoordinates(point_lhs.z(), point_rhs.z());
      }
      default:
        break;
    }
  } else if (lhs.IsNull() || rhs.IsNull()) {
    return false;
  }

  // Reached directly rather than through the operator, which is written out of
  // line: this is a hash set's per-probe comparison and pays for a call it
  // cannot see.
  TypedValue equality_result = equality::Equal(lhs, rhs);
  DMG_ASSERT(equality_result.type() == TypedValue::Type::Bool || equality_result.type() == TypedValue::Type::Null,
             "Equality between two TypedValues must result in either Null or Bool");
  return equality_result.type() == TypedValue::Type::Bool && equality_result.ValueBool();
}

/// A hash agreeing with Equivalent: two equivalent values hash alike.
///
/// Declared beside the relation it has to agree with, since a change to one
/// that is not made to the other is silent until a lookup misses.
///
/// @throw TypedValueException for a value no hash is defined over, which is a
///        Function, a Graph or a VirtualGraph. A container keyed by this
///        relation raises when handed one.
size_t Hash(const TypedValue &value);

// The presentation surface. The comparison is defined here rather than out of
// line in a source file so that a caller including this header reaches the
// relation without a call, which a hash set's per-probe comparison pays for on
// every probe. The hash cannot follow it: it is built from a module the header
// would have to import to define it here, so only its dispatch is inlined.

inline bool KeyEqual::operator()(TypedValue const &left, TypedValue const &right) const {
  return Equivalent(left, right);
}

inline size_t Hasher::operator()(TypedValue const &value) const { return Hash(value); }

}  // namespace memgraph::query::relations::equivalence

namespace memgraph::query {

/// Lets a hash container from the standard-adjacent libraries key a value by
/// equivalence. Found by argument lookup, so a caller reaches it only by
/// including this header, and keys the container by the relation named here
/// rather than by whichever one it happened to reach.
template <typename H>
H AbslHashValue(H h, TypedValue const &value) {
  return H::combine(std::move(h), relations::equivalence::Hash(value));
}

}  // namespace memgraph::query
