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
/// It is two-valued where equality is three-valued: a Null it holds equivalent
/// to a Null, where equality answers Null and decides nothing.
#pragma once

#include <cstddef>

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

/// Reads a pair equality could not decide, which is a container holding a Null.
///
/// @pre Both hold the same type of container. Equality answering Null over a
/// pair that is not itself Null establishes exactly that.
bool EquivalentOfContainersHoldingANull(const TypedValue &a, const TypedValue &b);

inline bool Equivalent(const TypedValue &lhs, const TypedValue &rhs) {
  if (lhs.IsNull() || rhs.IsNull()) return lhs.IsNull() && rhs.IsNull();

  // Equality decides this wherever it can, which is everywhere no Null sits
  // inside either value. Where it cannot it says so, and only then is the
  // container walked: collapsing that Null to false would make a value holding
  // one not equivalent to itself, and a hash container would never find such a
  // key again. Every hash lookup reaches this, so the deciding case is all that
  // is left here and the walk is reached through one call.
  TypedValue const equality_result = equality::Equal(lhs, rhs);
  if (equality_result.type() == TypedValue::Type::Bool) [[likely]] {
    return equality_result.UnsafeValueBool();
  }
  return EquivalentOfContainersHoldingANull(lhs, rhs);
}

/// A hash agreeing with Equivalent: two equivalent values hash alike.
///
/// Declared beside the relation it has to agree with, since a change to one
/// that is not made to the other is silent until a lookup misses. Defined out
/// of line, because it is built from a collection hash this header would
/// otherwise have to pull in.
size_t Hash(const TypedValue &value);

}  // namespace memgraph::query::relations::equivalence
