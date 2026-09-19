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

// The laws the storage-side relations obey, asked over generated values.
//
// Nothing outside storage observes the stored order or the stretch a bound
// fences a range to, so no other database and no query can say whether either
// is right. What is left is what they have to be true of on their own: the
// order has to be an order, and the fence has to hold all and only the values
// a comparison against the bound can answer for.

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <compare>
#include <cstdint>
#include <limits>
#include <map>
#include <utility>
#include <variant>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/property_value_utils.hpp"
#include "storage/v2/temporal.hpp"
#include "tests/property_based/value_generators.hpp"
#include "tests/unit/value_shapes.hpp"

using memgraph::storage::AreComparable;
using memgraph::storage::LowerBoundComparableWith;
using memgraph::storage::PropertyValue;
using memgraph::storage::PropertyValueType;
using memgraph::storage::TemporalData;
using memgraph::storage::TemporalType;
using memgraph::storage::UpperBoundComparableWith;

namespace generators = memgraph::test::generators;
namespace shapes = memgraph::test::shapes;

namespace {

/// Whether the value is a NaN held directly, rather than inside a container.
///
/// Only a NaN held directly matters to a fence. One inside a list is ordered by
/// the elements around it like any other element, and the list it is in sits in
/// the stretch every list sits in.
bool IsANaN(PropertyValue const &value) { return value.IsDouble() && std::isnan(value.ValueDouble()); }

/// Whether an ordered comparison between the two can be true.
///
/// Two answers are not true: a pair of unlike types answers Null, and every
/// comparison against a NaN is false in both directions. A range built from a
/// bound must reach neither, so both are what the fence is there to exclude.
bool AComparisonCanHold(PropertyValue const &bound, PropertyValue const &value) {
  return AreComparable(bound, value) && !IsANaN(bound) && !IsANaN(value);
}

/// Whether the stored order places the value inside the stretch a range built
/// from this bound is fenced to.
bool InsideTheStretchOf(PropertyValue const &bound, PropertyValue const &value) {
  return memgraph::storage::IsValueIncludedByLowerBound(value, LowerBoundComparableWith(bound)) &&
         memgraph::storage::IsValueIncludedByUpperBound(value, UpperBoundComparableWith(bound));
}

/// The four kinds that share one stored type, ordered by which of the four
/// before anything else. Each is its own stretch, and the pair of one against
/// another is the pair a fence that reads the type alone lets through.
constexpr std::array kTemporalKinds = {
    TemporalType::Date, TemporalType::LocalTime, TemporalType::LocalDateTime, TemporalType::Duration};

/// Whether the stored order puts the first value before the second.
bool Before(PropertyValue const &a, PropertyValue const &b) { return std::is_lt(a <=> b); }

/// Whether neither value comes before the other, which is the position a sorted
/// container keeps them both at.
bool SharesAPosition(PropertyValue const &a, PropertyValue const &b) { return !Before(a, b) && !Before(b, a); }

/// The same list held in the other representation.
///
/// A list of numbers is packed and any other list is boxed, so a list that can
/// be packed has two spellings and the order has to place them alike.
PropertyValue HeldTheOtherWay(PropertyValue const &value) {
  auto elements = std::vector<PropertyValue>{};
  switch (value.type()) {
    case PropertyValueType::IntList:
      for (auto const element : value.ValueIntList()) elements.emplace_back(static_cast<std::int64_t>(element));
      return PropertyValue(std::move(elements));
    case PropertyValueType::DoubleList:
      for (auto const element : value.ValueDoubleList()) elements.emplace_back(element);
      return PropertyValue(std::move(elements));
    case PropertyValueType::NumericList:
      for (auto const &element : value.ValueNumericList()) {
        if (auto const *whole = std::get_if<int>(&element)) {
          elements.emplace_back(static_cast<std::int64_t>(*whole));
        } else {
          elements.emplace_back(std::get<double>(element));
        }
      }
      return PropertyValue(std::move(elements));
    default:
      break;
  }
  if (value.type() != PropertyValueType::List) return value;

  auto packable_ints = true;
  auto doubles = true;
  auto numbers = true;
  for (auto const &element : value.ValueList()) {
    auto const packable_int = element.IsInt() && memgraph::storage::FitsAPackedList(element.ValueInt());
    packable_ints &= packable_int;
    doubles &= element.IsDouble();
    numbers &= packable_int || element.IsDouble();
  }
  auto list = PropertyValue::list_t{};
  for (auto const &element : value.ValueList()) list.emplace_back(element);
  if (packable_ints) return PropertyValue(memgraph::storage::IntListTag{}, std::move(list));
  if (doubles) return PropertyValue(memgraph::storage::DoubleListTag{}, std::move(list));
  if (numbers) return PropertyValue(memgraph::storage::NumericListTag{}, std::move(list));
  return value;
}

/// A value equivalent to the one given, written a different way wherever there
/// is one: a NaN spelled with other bits, a list held in the other
/// representation, a container rebuilt from rebuilt elements.
///
/// The order has to place such a pair at one position, or an index entry stored
/// by one route is not found when it is looked up by the other.
PropertyValue WrittenAnotherWay(PropertyValue const &value) {
  switch (value.type()) {
    case PropertyValueType::Double: {
      auto const held = value.ValueDouble();
      if (!std::isnan(held)) return value;
      return PropertyValue(std::bit_cast<double>(std::bit_cast<std::uint64_t>(held) ^ 0x7));
    }
    case PropertyValueType::IntList:
    case PropertyValueType::DoubleList:
    case PropertyValueType::NumericList:
      return HeldTheOtherWay(value);
    case PropertyValueType::List: {
      auto held_the_other_way = HeldTheOtherWay(value);
      if (held_the_other_way.type() != PropertyValueType::List) return held_the_other_way;
      // Nothing in it is a number, so the list has no other representation and
      // what is left to write another way is what is inside it.
      auto elements = std::vector<PropertyValue>{};
      for (auto const &element : value.ValueList()) elements.emplace_back(WrittenAnotherWay(element));
      return PropertyValue(std::move(elements));
    }
    case PropertyValueType::Map: {
      auto rebuilt = PropertyValue::map_t{};
      for (auto const &[key, held] : value.ValueMap()) rebuilt.emplace(key, WrittenAnotherWay(held));
      return PropertyValue(std::move(rebuilt));
    }
    default:
      return value;
  }
}

/// Whether the value holds a vector index id, at any depth.
///
/// The stored form of one keeps the ids and drops the coordinates, so a value
/// holding one is the single thing the store does not read back unchanged. It
/// is excluded from the round-trip laws by name and asked about on its own.
bool HoldsAVectorIndexId(PropertyValue const &value) {
  switch (value.type()) {
    case PropertyValueType::VectorIndexId:
      return true;
    case PropertyValueType::List:
      return std::ranges::any_of(value.ValueList(), HoldsAVectorIndexId);
    case PropertyValueType::Map:
      return std::ranges::any_of(value.ValueMap(), [](auto const &entry) { return HoldsAVectorIndexId(entry.second); });
    default:
      return false;
  }
}

rc::Gen<PropertyValue> TemporalOfKind(TemporalType kind) {
  return rc::gen::map(rc::gen::arbitrary<std::int64_t>(),
                      [kind](std::int64_t microseconds) { return PropertyValue(TemporalData{kind, microseconds}); });
}

}  // namespace

RC_GTEST_PROP(Stretch, HoldsEveryValueAComparisonAnswersFor, ()) {
  // A value a comparison against the bound can answer for that lies outside the
  // fence is a row the index scan never reaches and the unindexed query does,
  // which is the standing invariant failing.
  auto const bound = *generators::AnyValue();
  auto const value = *generators::AnyValue();

  if (AComparisonCanHold(bound, value)) RC_ASSERT(InsideTheStretchOf(bound, value));
}

RC_GTEST_PROP(Stretch, HoldsNothingAComparisonDeclines, ()) {
  // The other half, and the one a kind sharing a stored type with three others
  // breaks: a value whose comparison against the bound answers Null is inside
  // the fence, and the scan returns it having asked nothing else.
  //
  // A NaN is asked about by type here, unlike the half above. What the fence
  // owns is the values it cannot tell apart by where they sit; whether a range
  // may be built from a bound no comparison answers for is settled before one
  // is built.
  auto const bound = *generators::AnyValue();
  auto const value = *generators::AnyValue();

  if (InsideTheStretchOf(bound, value)) RC_ASSERT(AreComparable(bound, value));
}

RC_GTEST_PROP(Stretch, FencesEachTemporalKindFromTheOtherThree, ()) {
  // The same pair of laws where the values that break them are every draw
  // rather than a fraction of one type. Drawn freely, two temporal values land
  // on the same kind about a quarter of the time and on the pair of kinds that
  // sit next to each other far less often than that.
  auto const bound_kind = *rc::gen::elementOf(kTemporalKinds);
  auto const value_kind = *rc::gen::elementOf(kTemporalKinds);
  auto const bound = *TemporalOfKind(bound_kind);
  auto const value = *TemporalOfKind(value_kind);

  RC_ASSERT(InsideTheStretchOf(bound, value) == (bound_kind == value_kind));
}

RC_GTEST_PROP(Stretch, KeepsANaNOutOfEveryNumericRange, ()) {
  // Every comparison against a NaN is false, so a range with a numeric bound
  // must stop below where the NaNs are kept. The numbers are the only stretch
  // ending anywhere but at the next type, and this is what that gap is for.
  auto const number = *generators::ValueOfType(*rc::gen::element(PropertyValueType::Int, PropertyValueType::Double), 0);
  auto const nan = PropertyValue(std::numeric_limits<double>::quiet_NaN());

  RC_ASSERT(!InsideTheStretchOf(number, nan));
}

RC_GTEST_PROP(StoredOrder, PutsNoValueBeforeItself, ()) {
  // Irreflexivity. An index is a sorted container, and a comparison saying a
  // value precedes itself lets a search walk past the entry it is looking for.
  auto const value = *generators::AnyValue();

  RC_ASSERT(!Before(value, value));
}

RC_GTEST_PROP(StoredOrder, ReversesWhenTheValuesAreSwapped, ()) {
  // Asking the same pair both ways round has to give the same answer read
  // backwards. Two entries each placed before the other never settle.
  auto const a = *generators::AnyValue();
  auto const b = *generators::AnyValue();

  auto const forward = a <=> b;
  auto const backward = b <=> a;

  RC_ASSERT(std::is_lt(forward) == std::is_gt(backward));
  RC_ASSERT(std::is_eq(forward) == std::is_eq(backward));
}

RC_GTEST_PROP(StoredOrder, KeepsComingFirstTransitive, ()) {
  auto const a = *generators::AnyValue();
  auto const b = *generators::AnyValue();
  auto const c = *generators::AnyValue();

  if (Before(a, b) && Before(b, c)) RC_ASSERT(Before(a, c));
}

RC_GTEST_PROP(StoredOrder, KeepsSharingAPositionTransitive, ()) {
  // The law a NaN breaks wherever it is placed alongside every number instead
  // of alongside another NaN: the values sharing a position have to form
  // groups, or the order is not one and a sort over it is undefined.
  auto const a = *generators::AnyValue();
  auto const b = *generators::AnyValue();
  auto const c = *generators::AnyValue();

  if (SharesAPosition(a, b) && SharesAPosition(b, c)) RC_ASSERT(SharesAPosition(a, c));
}

RC_GTEST_PROP(StoredOrder, KeepsEveryLawWithinOneType, ()) {
  // Every law above again with all three values of one type. Drawn freely a
  // triple is placed by where its types sit far more often than by what the
  // values hold, so the seating carries laws stated over the values.
  auto const type = *rc::gen::elementOf(std::vector<PropertyValueType>(shapes::kEveryType.begin(),  //
                                                                       shapes::kEveryType.end()));
  auto const of_type = generators::ValueOfType(type, 1);
  auto const a = *of_type;
  auto const b = *of_type;
  auto const c = *of_type;

  RC_ASSERT(!Before(a, a));
  if (Before(a, b)) RC_ASSERT(!Before(b, a));
  if (Before(a, b) && Before(b, c)) RC_ASSERT(Before(a, c));
  if (SharesAPosition(a, b) && SharesAPosition(b, c)) RC_ASSERT(SharesAPosition(a, c));
}

RC_GTEST_PROP(StoredOrder, PlacesAValueWrittenAnotherWayAtOnePosition, ()) {
  // What lets an index find an entry stored by one route when it is looked up
  // by the other. A list of numbers has two spellings and a NaN has many.
  auto const value = *generators::AnyValue();

  RC_ASSERT(SharesAPosition(value, WrittenAnotherWay(value)));
}

RC_GTEST_PROP(StoredOrder, PlacesAValueWrittenAnotherWayAgainstAnyOtherAlike, ()) {
  // The position itself is not enough: the two spellings have to fall the same
  // side of every other value, or a range keeps one and drops the other.
  auto const value = *generators::AnyValue();
  auto const other = *generators::AnyValue();
  auto const respelled = WrittenAnotherWay(value);

  RC_ASSERT(Before(value, other) == Before(respelled, other));
  RC_ASSERT(Before(other, value) == Before(other, respelled));
}

RC_GTEST_PROP(RoundTrip, ReadsBackEveryValueItWasGiven, ()) {
  // A property is encoded on the way in and decoded on the way out, and the
  // value that comes back is the value that went in.
  auto const value = *generators::AnyValue();
  RC_PRE(!HoldsAVectorIndexId(value));

  auto store = memgraph::storage::PropertyStore{};
  auto const id = memgraph::storage::PropertyId::FromUint(1);
  store.SetProperty(id, value);

  RC_ASSERT(store.GetProperty(id) == value);
}

RC_GTEST_PROP(RoundTrip, ReadsBackEveryValueOfASetOfProperties, ()) {
  // Several properties share one buffer, so a value read back on its own says
  // nothing about the same value read back from beside its neighbours.
  constexpr auto kProperties = 5;
  auto properties = std::map<memgraph::storage::PropertyId, PropertyValue>{};
  for (auto n = 0; n < kProperties; ++n) {
    auto const value = *generators::AnyValue();
    RC_PRE(!HoldsAVectorIndexId(value));
    properties.emplace(memgraph::storage::PropertyId::FromUint(static_cast<std::uint32_t>(n) + 1), value);
  }

  auto store = memgraph::storage::PropertyStore{};
  RC_ASSERT(store.InitProperties(properties));

  for (auto const &[id, value] : properties) RC_ASSERT(store.GetProperty(id) == value);
}

RC_GTEST_PROP(RoundTrip, ComparesAStoredValueUndecodedAsItComparesOneDecoded, ()) {
  // A stored value is compared without being decoded, by a reader with a case
  // per type of its own. Two readings of one order part quietly: the answer is
  // wrong only for the pairs the two disagree on, and a lookup finds nothing
  // rather than raising.
  auto const stored = *generators::AnyValue();
  auto const other = *generators::AnyValue();
  RC_PRE(!HoldsAVectorIndexId(stored) && !HoldsAVectorIndexId(other));

  auto store = memgraph::storage::PropertyStore{};
  auto const id = memgraph::storage::PropertyId::FromUint(1);
  store.SetProperty(id, stored);

  RC_ASSERT(store.IsPropertyEqual(id, other) == (store.GetProperty(id) == other));
}

RC_GTEST_PROP(RoundTrip, KeepsTheIdsOfAVectorIndexIdAndDropsItsCoordinates, ()) {
  // The one value the store does not hand back unchanged, stated rather than
  // skipped. A vector index id names where the coordinates are kept, and they
  // are not kept here, so its stored form carries the ids alone.
  auto const value = *generators::ValueOfType(PropertyValueType::VectorIndexId, 0);

  auto store = memgraph::storage::PropertyStore{};
  auto const id = memgraph::storage::PropertyId::FromUint(1);
  store.SetProperty(id, value);
  auto const read_back = store.GetProperty(id);

  RC_ASSERT(read_back.IsVectorIndexId());
  RC_ASSERT(read_back.ValueVectorIndexIds() == value.ValueVectorIndexIds());
  RC_ASSERT(read_back.ValueVectorIndexList().empty());
}

TEST(StoredOrder, RewritesEnoughValuesForTheRespellingLawsToSayAnything) {
  // Both respelling laws are satisfied by a value that was not respelled, and a
  // rewrite that mostly hands its argument back passes them while asking
  // nothing. What it actually rewrites is measured here, per route.
  constexpr auto kDraws = 20'000;
  constexpr auto kLeastShare = 0.02;

  auto held_another_way = 0;
  auto respelled_nans = 0;
  auto rebuilt_containers = 0;

  auto const generator = generators::AnyValue();
  for (auto draw = 0; draw < kDraws; ++draw) {
    auto const value = generator(rc::Random(static_cast<std::uint64_t>(draw)), rc::kNominalSize).value();
    auto const respelled = WrittenAnotherWay(value);

    if (respelled.type() != value.type()) ++held_another_way;
    if (value.IsDouble() && std::isnan(value.ValueDouble())) ++respelled_nans;
    if (value.IsMap() || value.type() == PropertyValueType::List) ++rebuilt_containers;
  }

  auto const held_share = static_cast<double>(held_another_way) / kDraws;
  EXPECT_GT(held_share, kLeastShare) << "only " << held_share * 100
                                     << "% of draws are a list with another representation to be held in";
  EXPECT_GT(respelled_nans, 0) << "no draw was a NaN, so no draw was respelled with other bits";
  EXPECT_GT(rebuilt_containers, 0) << "no draw was a container, so nothing was rebuilt from rebuilt elements";
}

TEST(Stretch, IsMarkedByTheBoundsItHandsBack) {
  // A range covering a whole stretch is written as that stretch's own bounds,
  // whose two ends are of different types by construction. A scan is handed
  // both that shape and a user's range, so the marker has to be recognised.
  constexpr auto kDraws = 4'000;
  auto marked = 0;

  for (auto draw = 0; draw < kDraws; ++draw) {
    auto const value = generators::AnyValue()(rc::Random(static_cast<std::uint64_t>(draw)), rc::kNominalSize).value();
    auto const lower = LowerBoundComparableWith(value);
    auto const upper = UpperBoundComparableWith(value);
    // A null begins the order and the last stretch ends it, so each has one
    // bound and neither is written as a pair.
    if (!lower || !upper) continue;
    ASSERT_TRUE(memgraph::storage::BoundsMarkAWholeStretch(*lower, *upper))
        << "the bounds of the stretch holding " << value << " are not read back as marking it";
    ++marked;
  }

  EXPECT_GT(marked, kDraws / 2) << "too few draws had a stretch with both bounds for this to say much";
}

TEST(Stretch, ReachesTheAntecedentsTheFencingLawsRestOn) {
  // Both fencing laws are conditionals, and a conditional is silent over a run
  // that never meets its antecedent without saying so anywhere. The rates are
  // measured here, drawing pairs the way the properties draw them, and the
  // spread over types with them: a law reached only by strings is a law about
  // strings.
  constexpr auto kPairs = 20'000;
  constexpr auto kLeastShare = 0.02;

  auto answered = 0;
  auto inside = 0;
  auto bounds_with_a_partner_inside = std::map<PropertyValueType, int>{};

  auto const generator = generators::AnyValue();
  for (auto draw = 0; draw < kPairs; ++draw) {
    auto const at = [&](int offset) {
      auto const seed = static_cast<std::uint64_t>(draw) * 2 + static_cast<std::uint64_t>(offset);
      return generator(rc::Random(seed), rc::kNominalSize).value();
    };
    auto const bound = at(0);
    auto const value = at(1);

    if (AComparisonCanHold(bound, value)) ++answered;
    if (InsideTheStretchOf(bound, value)) {
      ++inside;
      ++bounds_with_a_partner_inside[bound.type()];
    }
  }

  auto const answered_share = static_cast<double>(answered) / kPairs;
  auto const inside_share = static_cast<double>(inside) / kPairs;

  EXPECT_GT(answered_share, kLeastShare) << "a comparison answers on " << answered_share * 100
                                         << "% of pairs, too few to establish what a fence holds";
  EXPECT_GT(inside_share, kLeastShare) << "a value lands inside the fence on " << inside_share * 100
                                       << "% of pairs, too few to establish what it must not hold";

  // Every type has to appear as a bound with something inside its own stretch,
  // or that type's fence is never asked about at all.
  for (auto const type : shapes::kEveryType) {
    EXPECT_GT(bounds_with_a_partner_inside[type], 0) << "no pair ever put a value inside the stretch of a " << type;
  }
}

TEST(Stretch, ReachesEachTemporalKindAgainstEachOther) {
  // The four kinds sharing one stored type are where a fence reading the type
  // alone leaks, and the pair that leaks is two unlike kinds. Drawn freely both
  // values are temporal on about one draw in two hundred and fifty, so the law
  // asking about them draws the kinds itself; this is the check that doing so
  // reaches all sixteen pairs rather than the four that agree.
  constexpr auto kDraws = 2'000;
  auto reached = std::map<std::pair<TemporalType, TemporalType>, int>{};

  auto const kinds = rc::gen::pair(rc::gen::elementOf(kTemporalKinds), rc::gen::elementOf(kTemporalKinds));
  for (auto draw = 0; draw < kDraws; ++draw) {
    ++reached[kinds(rc::Random(static_cast<std::uint64_t>(draw)), rc::kNominalSize).value()];
  }

  for (auto const bound_kind : kTemporalKinds) {
    for (auto const value_kind : kTemporalKinds) {
      EXPECT_GT((reached[{bound_kind, value_kind}]), 0)
          << "no draw ever fenced kind " << static_cast<unsigned>(bound_kind) << " against kind "
          << static_cast<unsigned>(value_kind);
    }
  }
}
