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

// The laws the relations' consumers rely on, asked of drawn values rather than
// of written ones.
//
// A hand-written case asks about the value its author thought of. These ask
// about whatever the generator draws, which is the point: the first of them
// below is the law that was false until recently, and one written value would
// have settled it just as well had anyone written that value.

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <cmath>
#include <compare>
#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <vector>

#include "query/exceptions.hpp"
#include "query/relations/equality.hpp"
#include "query/relations/equivalence.hpp"
#include "query/relations/orderability.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/point.hpp"
#include "tests/property_based/typed_value_generators.hpp"

using memgraph::query::TypedValue;

namespace equality = memgraph::query::relations::equality;
namespace equivalence = memgraph::query::relations::equivalence;
namespace orderability = memgraph::query::relations::orderability;
namespace generators = memgraph::test::generators;

namespace {

/// Whether equality decided the pair, rather than leaving it open with a Null.
bool Decided(TypedValue const &answer) { return answer.type() == TypedValue::Type::Bool; }

/// The largest integer a double holds exactly. Above it the two stop agreeing,
/// so an integer and its double are no longer the same value.
constexpr std::int64_t kExactlyRepresentable = std::int64_t{1} << 53;

/// A value equivalent to the one given, written a different way wherever there
/// is one: a NaN spelled with other bits, a whole number held as an integer
/// rather than a double, a container rebuilt from rebuilt elements.
///
/// Equivalence has to hold such a pair alike and the hash has to agree, which
/// together are what lets a hash container find a key stored by one route when
/// it is looked up by the other.
TypedValue BuiltAnotherWay(TypedValue const &value) {
  switch (value.type()) {
    case TypedValue::Type::Double: {
      auto const held = value.ValueDouble();
      if (std::isnan(held)) return TypedValue(-std::numeric_limits<double>::quiet_NaN());

      auto whole = 0.0;
      auto const is_whole = std::modf(held, &whole) == 0.0;
      if (is_whole && std::abs(held) <= static_cast<double>(kExactlyRepresentable)) {
        return TypedValue(static_cast<std::int64_t>(held));
      }
      return TypedValue(held);
    }
    case TypedValue::Type::Int: {
      auto const held = value.ValueInt();
      if (std::abs(held) <= kExactlyRepresentable) return TypedValue(static_cast<double>(held));
      return TypedValue(held);
    }
    case TypedValue::Type::Point2d: {
      auto const point = value.ValuePoint2d();
      auto const respelled = [](double coordinate) {
        return std::isnan(coordinate) ? -std::numeric_limits<double>::quiet_NaN() : coordinate;
      };
      return TypedValue(memgraph::storage::Point2d{point.crs(), respelled(point.x()), respelled(point.y())});
    }
    case TypedValue::Type::List: {
      auto rebuilt = std::vector<TypedValue>{};
      for (auto const &element : value.ValueList()) rebuilt.emplace_back(BuiltAnotherWay(element));
      return TypedValue(std::move(rebuilt));
    }
    case TypedValue::Type::Map: {
      auto rebuilt = std::map<std::string, TypedValue>{};
      for (auto const &entry : value.ValueMap()) rebuilt.emplace(entry.first, BuiltAnotherWay(entry.second));
      return TypedValue(std::move(rebuilt));
    }
    default:
      return value;
  }
}

}  // namespace

RC_GTEST_PROP(Equivalence, HoldsEveryValueEquivalentToItself, ()) {
  // What a hash container needs of the relation it is keyed by. A key not
  // equivalent to itself can be stored and never found again.
  auto const value = *generators::AnyTypedValue();
  RC_ASSERT(equivalence::Equivalent(value, value));
}

RC_GTEST_PROP(Equivalence, IsSymmetric, ()) {
  auto const left = *generators::AnyTypedValue();
  auto const right = *generators::AnyTypedValue();
  RC_ASSERT(equivalence::Equivalent(left, right) == equivalence::Equivalent(right, left));
}

RC_GTEST_PROP(Equivalence, SendsAPairBuiltTwoWaysToOneHash, ()) {
  // The half of the contract a lookup fails on rather than answers wrongly: a
  // pair the relation holds alike that hashes apart is a key in two buckets.
  //
  // The pair is built rather than drawn and filtered. Two values drawn
  // independently are almost never equivalent, so a precondition would discard
  // nearly every case and the law would be established over the handful left.
  auto const value = *generators::AnyTypedValue();
  auto const other = BuiltAnotherWay(value);

  RC_ASSERT(equivalence::Equivalent(value, other));
  RC_ASSERT(equivalence::Hash(value) == equivalence::Hash(other));
}

RC_GTEST_PROP(Orderability, PlacesEveryPairItDoesNotRefuse, ()) {
  // A sort needs a total order over what it is handed. Answering "unordered"
  // leaves a pair in neither position, and a sort reading that as neither less
  // nor greater treats the two as interchangeable, which is not a strict weak
  // ordering and leaves the sort undefined rather than merely oddly arranged.
  //
  // Drawing both sides freely covers breadth and little depth: a type is one of
  // fifteen, so the pair of doubles this law is really about turns up in well
  // under one draw in a hundred. The property below draws that pair every time.
  auto const left = *generators::AnyTypedValue();
  auto const right = *generators::AnyTypedValue();

  try {
    auto const placed = orderability::Compare(left, right);
    RC_ASSERT(placed != std::partial_ordering::unordered);
  } catch (memgraph::query::QueryRuntimeException const &) {
    // Refusing the pair outright is the other allowed answer.
  }
}

RC_GTEST_PROP(Orderability, PlacesEveryPairOfNumbers, ()) {
  // Every draw is a double, and the shapes make a NaN a common one, so this
  // reaches the pair with no IEEE order on most cases rather than on none.
  auto const left = *generators::TypedValueOfType(TypedValue::Type::Double, 0);
  auto const right = *generators::TypedValueOfType(TypedValue::Type::Double, 0);

  RC_ASSERT(orderability::Compare(left, right) != std::partial_ordering::unordered);
}

RC_GTEST_PROP(Orderability, GivesOnePositionToWhateverEquivalenceHoldsAlike, ()) {
  // The rule tying the two relations together: two values are equivalent
  // exactly where they share a position under orderability. Were they to
  // disagree, a sort keyed by one would not agree with a grouping keyed by the
  // other over the same column.
  //
  // The pair is built equivalent rather than drawn and filtered, for the reason
  // the hash law above gives.
  auto const value = *generators::AnyTypedValue();
  auto const other = BuiltAnotherWay(value);
  RC_ASSERT(equivalence::Equivalent(value, other));

  try {
    RC_ASSERT(std::is_eq(orderability::Compare(value, other)));
  } catch (memgraph::query::QueryRuntimeException const &) {
    // This relation refuses some types outright, and says so rather than
    // placing them.
  }
}

RC_GTEST_PROP(Equality, IsSymmetricInAllThreeAnswers, ()) {
  auto const left = *generators::AnyTypedValue();
  auto const right = *generators::AnyTypedValue();
  auto const forwards = equality::Equal(left, right);
  auto const backwards = equality::Equal(right, left);

  RC_ASSERT(forwards.type() == backwards.type());
  if (Decided(forwards)) RC_ASSERT(forwards.ValueBool() == backwards.ValueBool());
}

RC_GTEST_PROP(Equality, AnswersEqualsItselfExactlyWhereItDecidesAValueAgainstItself, ()) {
  // `EqualsItself` is read where a lookup has to know whether equality can be
  // trusted about a value, so it has to agree with what equality actually does.
  auto const value = *generators::AnyTypedValue();
  auto const answer = equality::Equal(value, value);
  auto const decided_true = Decided(answer) && answer.ValueBool();

  RC_ASSERT(equality::EqualsItself(value) == decided_true);
}

RC_GTEST_PROP(Equality, LeavesEquivalenceToDecideWhatItDeclines, ()) {
  // Where equality cannot decide a value against itself, equivalence still has
  // to, and this is the pair of relations meeting: the values equality declines
  // over are exactly the ones equivalence is doing its own work for.
  auto const value = *generators::AnyTypedValue();
  RC_PRE(!equality::EqualsItself(value));
  RC_ASSERT(equivalence::Equivalent(value, value));
}
