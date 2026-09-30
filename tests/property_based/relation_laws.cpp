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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <vector>

#include "query/exceptions.hpp"
#include "query/relations/comparability.hpp"
#include "query/relations/equality.hpp"
#include "query/relations/equivalence.hpp"
#include "query/relations/orderability.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/point.hpp"
#include "tests/property_based/typed_value_generators.hpp"

using memgraph::query::TypedValue;

namespace comparability = memgraph::query::relations::comparability;
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
/// The same NaN, spelled with other bits.
///
/// Equivalence holds every NaN alike, so respelling one is how a law reaches a
/// pair that is one value built two ways.
double ANaNSpeltOtherwise(double held) { return std::isnan(held) ? -std::numeric_limits<double>::quiet_NaN() : held; }

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
      return TypedValue(
          memgraph::storage::Point2d{point.crs(), ANaNSpeltOtherwise(point.x()), ANaNSpeltOtherwise(point.y())});
    }
    case TypedValue::Type::Point3d: {
      auto const point = value.ValuePoint3d();
      return TypedValue(memgraph::storage::Point3d{
          point.crs(), ANaNSpeltOtherwise(point.x()), ANaNSpeltOtherwise(point.y()), ANaNSpeltOtherwise(point.z())});
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

/// Whether a sort would put the first value before the second.
///
/// This, rather than the relation's own three-way answer, is what a sort is
/// handed, and the laws below are the ones it has to obey for a sort to be
/// defined at all. Reading an unplaced pair as "neither first" is exactly how
/// the comparator in the query layer reads one.
bool Before(TypedValue const &a, TypedValue const &b) { return std::is_lt(orderability::Compare(a, b)); }

/// Whether neither value comes before the other, which is what a sort treats as
/// the two being interchangeable.
bool SharesAPosition(TypedValue const &a, TypedValue const &b) { return !Before(a, b) && !Before(b, a); }

/// The types orderability places without any chance of refusing a pair.
///
/// The two containers are left out, because a pair of either may be refused: a
/// pair of maps outright, and a pair of lists once it reaches a pair of elements
/// that is. Each has properties of its own rather than a place in a law that may
/// not throw.
std::vector<TypedValue::Type> PlaceableScalarTypes() {
  auto types = generators::GraphFreeTypes();
  std::erase_if(types, [](auto type) { return type == TypedValue::Type::List || type == TypedValue::Type::Map; });
  return types;
}

}  // namespace

RC_GTEST_PROP(Orderability, PutsNoValueBeforeItself, ()) {
  // Irreflexivity. A sort given a comparator saying a value precedes itself may
  // read past the end of its own range.
  //
  // Drawn freely for breadth. The value that breaks this law is a NaN, which
  // arrives here in about one draw in three hundred, so the numbers property
  // below asks the same law where the answer is dense enough to rely on.
  auto const value = *generators::AnyTypedValue();
  try {
    RC_ASSERT(!Before(value, value));
  } catch (memgraph::query::QueryRuntimeException const &) {
    // Refusing the pair outright is the other allowed answer.
  }
}

RC_GTEST_PROP(Orderability, PutsOnlyOneOfAPairFirst, ()) {
  // Asymmetry. Both directions answering true would let a sort swap a pair
  // forever.
  auto const type = *rc::gen::elementOf(PlaceableScalarTypes());
  auto const a = *generators::TypedValueOfType(type, 0);
  auto const b = *generators::TypedValueOfType(type, 0);

  if (Before(a, b)) RC_ASSERT(!Before(b, a));
  if (Before(b, a)) RC_ASSERT(!Before(a, b));
}

RC_GTEST_PROP(Orderability, KeepsComingFirstTransitive, ()) {
  // Transitivity of the order itself.
  auto const type = *rc::gen::elementOf(PlaceableScalarTypes());
  auto const a = *generators::TypedValueOfType(type, 0);
  auto const b = *generators::TypedValueOfType(type, 0);
  auto const c = *generators::TypedValueOfType(type, 0);

  if (Before(a, b) && Before(b, c)) RC_ASSERT(Before(a, c));
}

RC_GTEST_PROP(Orderability, KeepsSharingAPositionTransitive, ()) {
  // The law a NaN broke, and the one that is easy to miss: a strict weak
  // ordering needs the values that share a position to form groups. A NaN
  // sharing a position with every number, while no two numbers share one with
  // each other, is exactly this law failing.
  auto const type = *rc::gen::elementOf(PlaceableScalarTypes());
  auto const a = *generators::TypedValueOfType(type, 0);
  auto const b = *generators::TypedValueOfType(type, 0);
  auto const c = *generators::TypedValueOfType(type, 0);

  if (SharesAPosition(a, b) && SharesAPosition(b, c)) RC_ASSERT(SharesAPosition(a, c));
}

RC_GTEST_PROP(Orderability, KeepsBothLawsOverNumbersOfEitherType, ()) {
  // Every law above, asked again where the values that break them are common.
  // A NaN is a fraction of the doubles rather than a fraction of the doubles
  // among fifteen types, and an integer against a double reaches the arm it was
  // unplaced in. A law asked only in the broad properties is a law asked mostly
  // about strings and dates.
  auto const number = [] {
    return rc::gen::mapcat(rc::gen::element(TypedValue::Type::Int, TypedValue::Type::Double),
                           [](auto type) { return generators::TypedValueOfType(type, 0); });
  };
  auto const a = *number();
  auto const b = *number();
  auto const c = *number();

  RC_ASSERT(!Before(a, a));
  if (Before(a, b)) RC_ASSERT(!Before(b, a));
  if (Before(a, b) && Before(b, c)) RC_ASSERT(Before(a, c));
  if (SharesAPosition(a, b) && SharesAPosition(b, c)) RC_ASSERT(SharesAPosition(a, c));
}

TEST(RelationLaws, ReachesTheAntecedentsTheConditionalLawsRestOn) {
  // A law shaped "where the antecedent holds, assert the consequent" is silent
  // over a run that never meets the antecedent, and says so nowhere: the case
  // is not discarded, so nothing counts it and nothing complains. The rate is
  // therefore measured here, drawing triples the way the properties draw them.
  constexpr auto kTriples = 20'000;
  constexpr auto kLeastShare = 0.02;

  auto const types = PlaceableScalarTypes();
  auto ordered_chains = 0;
  auto shared_chains = 0;
  auto by_type_ordered = std::map<TypedValue::Type, int>{};
  auto by_type_shared = std::map<TypedValue::Type, int>{};

  for (auto draw = 0; draw < kTriples; ++draw) {
    auto const type = types[static_cast<std::size_t>(draw) % types.size()];
    auto const generator = generators::TypedValueOfType(type, 0);
    auto const at = [&](int offset) {
      auto const seed = static_cast<std::uint64_t>(draw) * 3 + static_cast<std::uint64_t>(offset);
      return generator(rc::Random(seed), rc::kNominalSize).value();
    };
    auto const a = at(0);
    auto const b = at(1);
    auto const c = at(2);

    if (Before(a, b) && Before(b, c)) {
      ++ordered_chains;
      ++by_type_ordered[type];
    }
    if (SharesAPosition(a, b) && SharesAPosition(b, c)) {
      ++shared_chains;
      ++by_type_shared[type];
    }
  }

  auto const ordered_share = static_cast<double>(ordered_chains) / kTriples;
  auto const shared_share = static_cast<double>(shared_chains) / kTriples;

  EXPECT_GT(ordered_share, kLeastShare) << "the transitivity law is asked on " << ordered_share * 100
                                        << "% of draws, which is too few to establish it";
  EXPECT_GT(shared_share, kLeastShare) << "the sharing-a-position law is asked on " << shared_share * 100
                                       << "% of draws, which is too few to establish it";

  // Both laws have to be reached by more than one type, or a single type's
  // values are carrying a law stated over all of them.
  EXPECT_GT(by_type_ordered.size(), 1U) << "only one type ever puts three values in order";
  EXPECT_GT(by_type_shared.size(), 1U) << "only one type ever shares a position three ways";
}

TEST(RelationLaws, ReachesTheAntecedentOverTriplesOfUnlikeTypes) {
  // The same measurement for the law asked with each type drawn on its own. A
  // chain of three there turns on where the types sit rather than on the values,
  // so a run meeting it rarely would leave the seating unchecked.
  constexpr auto kTriples = 20'000;
  constexpr auto kLeastShare = 0.02;

  auto const types = PlaceableScalarTypes();
  auto ordered_chains = 0;

  for (auto draw = 0; draw < kTriples; ++draw) {
    auto const at = [&](int offset) {
      // The type is drawn the way the property draws it rather than stepped
      // through by hand, so that what is measured is what is asked.
      auto const seed = static_cast<std::uint64_t>(draw) * 3 + static_cast<std::uint64_t>(offset);
      auto const type = rc::gen::elementOf(types)(rc::Random(seed), rc::kNominalSize).value();
      return generators::TypedValueOfType(type, 0)(rc::Random(seed), rc::kNominalSize).value();
    };
    auto const a = at(0);
    auto const b = at(1);
    auto const c = at(2);

    if (Before(a, b) && Before(b, c)) ++ordered_chains;
  }

  auto const ordered_share = static_cast<double>(ordered_chains) / kTriples;
  EXPECT_GT(ordered_share, kLeastShare) << "the transitivity law over unlike types is asked on " << ordered_share * 100
                                        << "% of draws, which is too few to establish it";
}

RC_GTEST_PROP(Orderability, KeepsBothLawsOverValuesOfUnlikeTypes, ()) {
  // Every law above, asked again with each value's type drawn on its own. The
  // laws before this one fix one type and draw three values of it, so none of
  // them reads where the types sit relative to each other, which is the half of
  // the order a column holding more than one type is sorted by.
  //
  // Sharing a position is the exception and stays with the numbers property: two
  // unlike types share one only where they are Int and Double, which three
  // independent draws reach in well under one case in a hundred.
  auto const types = PlaceableScalarTypes();
  auto const value = [&types] {
    return rc::gen::mapcat(rc::gen::elementOf(types), [](auto type) { return generators::TypedValueOfType(type, 0); });
  };
  auto const a = *value();
  auto const b = *value();
  auto const c = *value();

  RC_ASSERT(!Before(a, a));
  if (Before(a, b)) RC_ASSERT(!Before(b, a));
  if (Before(a, b) && Before(b, c)) RC_ASSERT(Before(a, c));
}

RC_GTEST_PROP(Orderability, PutsAnIntegerAndADoubleOnOneSideOfEveryOtherValue, ()) {
  // The law the seating has to obey whatever order the types are put in: an
  // integer and a double holding the same number are equal, so a third value
  // cannot fall between them. One that did would sort a column into an order a
  // comparison over the same column contradicts.
  //
  // Asked here rather than left to the property above, where it would be asked
  // almost never. Breaking it costs transitivity only on a triple drawn as a
  // double, then a value seated between the two numeric types, then an integer,
  // which three free draws reach about once in two thousand. The pair is built
  // equal for the same reason: two free draws are almost never the same number.
  auto const whole = *rc::gen::inRange<int64_t>(-1'000, 1'000);
  auto const as_integer = TypedValue(whole);
  auto const as_double = TypedValue(static_cast<double>(whole));

  auto const types = PlaceableScalarTypes();
  auto const third =
      *rc::gen::mapcat(rc::gen::elementOf(types), [](auto type) { return generators::TypedValueOfType(type, 0); });

  RC_ASSERT(Before(as_integer, third) == Before(as_double, third));
  RC_ASSERT(Before(third, as_integer) == Before(third, as_double));
}

RC_GTEST_PROP(Orderability, TellsApartTwoIntegersThatReachOneDouble, ()) {
  // Above the point where the doubles stop being spaced one apart, an integer and its
  // neighbour reach the same double. Both relations have to keep the two apart and place
  // each against that double, or a value equality holds equal to one of them is held equal
  // to the other, and sharing a position stops being transitive.
  //
  // Drawn at the boundary on purpose. A value drawn freely is almost never large enough, so
  // this law asked over the broad generator would be asked about nothing at all.
  auto const step = *rc::gen::inRange<int64_t>(0, 1 << 20);
  auto const whole = kExactlyRepresentable + step * 2;  // even, so still exactly a double
  auto const lower = TypedValue(whole);
  auto const higher = TypedValue(whole + 1);  // odd, so no double carries it
  auto const reached = TypedValue(static_cast<double>(whole));

  RC_ASSERT(std::is_eq(orderability::Compare(lower, reached)));
  RC_ASSERT(std::is_gt(orderability::Compare(higher, reached)));
  RC_ASSERT(std::is_lt(orderability::Compare(reached, higher)));
  RC_ASSERT(std::is_lt(orderability::Compare(lower, higher)));

  // Equality answers the same pair, and has to answer it the same way.
  RC_ASSERT(equality::Equal(lower, reached).ValueBool());
  RC_ASSERT(!equality::Equal(higher, reached).ValueBool());
}

RC_GTEST_PROP(Orderability, AgreesWithComparabilityWhereverComparabilityAnswers, ()) {
  // The two relations differ in which pairs they answer for, not in what they
  // answer. Where comparability gives an order, a sort keyed by orderability has
  // to agree with a filter written with `<`, or the same column reads one way
  // and sorts another.
  auto const type = *rc::gen::elementOf(PlaceableScalarTypes());
  auto const a = *generators::TypedValueOfType(type, 0);
  auto const b = *generators::TypedValueOfType(type, 0);

  auto const compared = comparability::Compare(a, b);
  if (!compared || *compared == std::partial_ordering::unordered) return;

  RC_ASSERT(orderability::Compare(a, b) == *compared);
}

RC_GTEST_PROP(Equality, LetsADifferenceSettleAPairThatAlsoHoldsANull, ()) {
  // Equality answers Null only where it cannot decide. A difference decides,
  // whatever else the pair holds, so a Null beside one must not hide it: a list
  // differing in its first element is unequal even with a Null in its second.
  auto const differing = *rc::gen::suchThat(rc::gen::pair(generators::AnyTypedValue(1), generators::AnyTypedValue(1)),
                                            [](auto const &pair) {
                                              return Decided(equality::Equal(pair.first, pair.second)) &&
                                                     !equality::Equal(pair.first, pair.second).ValueBool();
                                            });

  auto const left = TypedValue(std::vector<TypedValue>{differing.first, TypedValue()});
  auto const right = TypedValue(std::vector<TypedValue>{differing.second, TypedValue()});

  auto const answer = equality::Equal(left, right);
  RC_ASSERT(Decided(answer));
  RC_ASSERT(!answer.ValueBool());
}

RC_GTEST_PROP(Equivalence, IsTransitive, ()) {
  // A hash container groups by this relation, so values it holds alike have to
  // form groups: two keys each equivalent to a third must land in one bucket
  // together, not merely each with the third.
  auto const type = *rc::gen::elementOf(PlaceableScalarTypes());
  auto const a = *generators::TypedValueOfType(type, 0);
  auto const b = *generators::TypedValueOfType(type, 0);
  auto const c = *generators::TypedValueOfType(type, 0);

  if (equivalence::Equivalent(a, b) && equivalence::Equivalent(b, c)) RC_ASSERT(equivalence::Equivalent(a, c));
}

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
