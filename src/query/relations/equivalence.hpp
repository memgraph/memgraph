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
/// Out of line so that Equivalent does not call itself, and because equality answers Null for these
/// where equivalence has to answer a bool, so they cannot be deferred to it.
bool EquivalentOfContainers(const TypedValue &lhs, const TypedValue &rhs);

inline bool Equivalent(const TypedValue &lhs, const TypedValue &rhs) {
  if (lhs.IsNull() || rhs.IsNull()) return lhs.IsNull() && rhs.IsNull();
  // A container has to be walked here rather than by equality, which reads a Null it holds as
  // undecided and answers Null. Equivalence decides: two Nulls in the same place are the same value.
  if (lhs.type() == rhs.type() && (lhs.type() == TypedValue::Type::List || lhs.type() == TypedValue::Type::Map)) {
    return EquivalentOfContainers(lhs, rhs);
  }
  TypedValue equality_result = equality::Equal(lhs, rhs);
  DMG_ASSERT(equality_result.type() == TypedValue::Type::Bool || equality_result.type() == TypedValue::Type::Null,
             "Equality between two TypedValues must result in either Null or Bool");
  return equality_result.type() == TypedValue::Type::Bool && equality_result.ValueBool();
}

/// A hash agreeing with Equivalent: two equivalent values hash alike.
///
/// Declared beside the relation it has to agree with, since a change to one
/// that is not made to the other is silent until a lookup misses. Defined out
/// of line, because it is built from a collection hash this header would
/// otherwise have to pull in.
size_t Hash(const TypedValue &value);

}  // namespace memgraph::query::relations::equivalence
