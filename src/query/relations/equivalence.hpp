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
/// one DISTINCT and grouping read, and the one a hash container is keyed by.
///
/// It is two-valued where equality is three-valued, and it holds alike whatever
/// equality does not hold equal to itself: a Null, which equality leaves
/// undecided, and a NaN, which equality answers false for. Either reaches this
/// relation directly, inside a list or a map, or as a point's coordinate.
#pragma once

#include <cmath>
#include <cstddef>
#include <limits>

#include "query/relations/equality.hpp"
#include "query/typed_value.hpp"

namespace memgraph::query::relations::equivalence {

/// The two cases that walk what they hold, and so reach this relation again.
///
/// Out of line so that Equivalent does not call itself. A compiler will not
/// inline a function that recurses, whichever case reaches the recursion.
///
/// Each takes what it walks rather than the values holding it, so that neither
/// can be handed a pair of unlike things.
bool EquivalentOfLists(TypedValue::TVector const &a, TypedValue::TVector const &b);
bool EquivalentOfMaps(TypedValue::TMap const &a, TypedValue::TMap const &b);

/// Reads a pair of containers equality did not decide equal.
///
/// @pre `a` is a list or a map. Only those two hold a value equality declines
/// to decide, other than a Null or a NaN standing on its own.
bool EquivalentOfContainers(const TypedValue &a, const TypedValue &b);

/// Reads a pair of points equality did not decide equal. A point holds its
/// coordinates as doubles and compares them together, so one holding a NaN is no
/// more equal to itself than the NaN is.
///
/// @pre `a` is a two- or three-dimensional point.
bool EquivalentOfPoints(const TypedValue &a, const TypedValue &b);

/// The types equality declines to decide against themselves, as a bit per type,
/// so that ruling a type out is one test rather than one per type.
///
/// One bit per type is only a mask while the types fit the word holding it.
static_assert(TypedValue::kTypeCount <= std::numeric_limits<unsigned>::digits, "More types than a bit each fits in");
inline constexpr unsigned kDeclinedOver =
    (1U << static_cast<unsigned>(TypedValue::Type::Double)) | (1U << static_cast<unsigned>(TypedValue::Type::Point2d)) |
    (1U << static_cast<unsigned>(TypedValue::Type::Point3d)) | (1U << static_cast<unsigned>(TypedValue::Type::List)) |
    (1U << static_cast<unsigned>(TypedValue::Type::Map));

inline bool Equivalent(const TypedValue &lhs, const TypedValue &rhs) {
  if (lhs.IsNull() || rhs.IsNull()) return lhs.IsNull() && rhs.IsNull();

  // Equality deciding a pair equal decides this too, and is the answer a hash
  // lookup gets on the key it is looking for. Every hash lookup reaches here, so
  // that case is all that is left inline.
  TypedValue const equality_result = equality::Equal(lhs, rhs);
  if (equality_result.type() == TypedValue::Type::Bool && equality_result.UnsafeValueBool()) [[likely]] {
    return true;
  }

  // Anything else is equality declining rather than deciding: a Null leaves the
  // pair undecided and a NaN answers false against everything, itself included.
  // Taking either answer would leave a value holding one not equivalent to
  // itself, and a hash container would never find such a key again.
  //
  // A Null, a NaN or a container holding one is all that equality declines
  // over, and the left value's type rules the first two out without reading
  // either value. A probe that misses on any other type is then one test past
  // the cost of equality alone, which is what it was before.
  if ((kDeclinedOver >> static_cast<unsigned>(lhs.type()) & 1U) == 0U) [[likely]]
    return false;

  if (lhs.type() == TypedValue::Type::Double) {
    return std::isnan(lhs.UnsafeValueDouble()) && rhs.type() == TypedValue::Type::Double &&
           std::isnan(rhs.UnsafeValueDouble());
  }

  if (lhs.type() == TypedValue::Type::Point2d || lhs.type() == TypedValue::Type::Point3d) {
    return EquivalentOfPoints(lhs, rhs);
  }

  // Equality answering Null says a Null sits inside and it declined. A false
  // answer stands unless a NaN sits inside, which one walk over the leaves
  // rules out, and that is cheaper than walking the pair against each other.
  if (equality_result.type() == TypedValue::Type::Bool && equality::EqualsItself(lhs)) return false;
  return EquivalentOfContainers(lhs, rhs);
}

/// A hash agreeing with Equivalent: two equivalent values hash alike.
///
/// Declared beside the relation it has to agree with, since a change to one
/// that is not made to the other is silent until a lookup misses. Defined out
/// of line, because it is built from a collection hash this header would
/// otherwise have to pull in.
size_t Hash(const TypedValue &value);

}  // namespace memgraph::query::relations::equivalence
