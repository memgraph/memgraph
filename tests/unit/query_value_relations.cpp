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

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"

#include "query/relations/agreement.hpp"
#include "query/relations/comparability.hpp"
#include "query/relations/equivalence.hpp"
#include "query/relations/orderability.hpp"

namespace memgraph::query::tests {
namespace {

using memgraph::query::TypedValue;
namespace orderability = memgraph::query::relations::orderability;
namespace equivalence = memgraph::query::relations::equivalence;

/// The types orderability places, one value of each, written in the order the
/// relation is meant to put them. Graph, VirtualGraph and Function are absent
/// because no order is defined over them.
std::vector<TypedValue> OrderedSample() {
  return {
      TypedValue(std::vector<TypedValue>{TypedValue(1)}),  // List
      TypedValue("string"),
      TypedValue(true),
      TypedValue(int64_t{42}),
      TypedValue(),  // Null
  };
}

TEST(Orderability, PlacesEveryPairOfUnlikeTypes) {
  auto const sample = OrderedSample();
  for (auto const &a : sample) {
    for (auto const &b : sample) {
      // Nothing is left unplaced; a weak ordering has no such state to return.
      auto const order = orderability::Compare(a, b);
      EXPECT_TRUE(std::is_lt(order) || std::is_gt(order) || order == std::weak_ordering::equivalent);
    }
  }
}

TEST(Orderability, SampleIsInAscendingOrder) {
  auto const sample = OrderedSample();
  for (size_t i = 1; i < sample.size(); ++i) {
    EXPECT_TRUE(std::is_lt(orderability::Compare(sample[i - 1], sample[i])))
        << "index " << i - 1 << " should sort before " << i;
  }
}

TEST(Orderability, IsAntisymmetric) {
  auto const sample = OrderedSample();
  for (auto const &a : sample) {
    for (auto const &b : sample) {
      auto const forward = orderability::Compare(a, b);
      auto const backward = orderability::Compare(b, a);
      EXPECT_EQ(std::is_lt(forward), std::is_gt(backward));
      EXPECT_EQ(std::is_gt(forward), std::is_lt(backward));
    }
  }
}

TEST(Orderability, IsTransitive) {
  auto const sample = OrderedSample();
  for (auto const &a : sample) {
    for (auto const &b : sample) {
      for (auto const &c : sample) {
        if (std::is_lt(orderability::Compare(a, b)) && std::is_lt(orderability::Compare(b, c))) {
          EXPECT_TRUE(std::is_lt(orderability::Compare(a, c)));
        }
      }
    }
  }
}

TEST(Orderability, PlacesNaNAfterEveryNumber) {
  auto const nan = TypedValue(std::numeric_limits<double>::quiet_NaN());
  for (auto const &number :
       {TypedValue(int64_t{0}), TypedValue(-1.5), TypedValue(std::numeric_limits<double>::infinity())}) {
    EXPECT_TRUE(std::is_gt(orderability::Compare(nan, number)));
    EXPECT_TRUE(std::is_lt(orderability::Compare(number, nan)));
  }
}

TEST(Orderability, PlacesTwoNaNsAlongsideOneAnother) {
  auto const nan = TypedValue(std::numeric_limits<double>::quiet_NaN());
  EXPECT_EQ(orderability::Compare(nan, nan), std::weak_ordering::equivalent);
}

TEST(Orderability, PlacesNullAfterEverything) {
  auto const null = TypedValue();
  for (auto const &value : OrderedSample()) {
    if (value.IsNull()) continue;
    EXPECT_TRUE(std::is_gt(orderability::Compare(null, value)));
  }
  EXPECT_EQ(orderability::Compare(null, null), std::weak_ordering::equivalent);
}

TEST(Orderability, OrdersIntegersAgainstDoublesAsNumbers) {
  // Both share one rank, so the ordering reads their values rather than their
  // types.
  EXPECT_TRUE(std::is_lt(orderability::Compare(TypedValue(int64_t{1}), TypedValue(1.5))));
  EXPECT_TRUE(std::is_gt(orderability::Compare(TypedValue(int64_t{2}), TypedValue(1.5))));
  EXPECT_EQ(orderability::Compare(TypedValue(int64_t{2}), TypedValue(2.0)), std::weak_ordering::equivalent);
}

/// A map of the given entries, built through the interface a query value takes.
TypedValue MapOf(std::initializer_list<std::pair<std::string const, TypedValue>> entries) {
  auto map = std::map<std::string, TypedValue>{};
  for (auto const &[key, value] : entries) map.emplace(key, value);
  return TypedValue(std::move(map));
}

TEST(Orderability, PlacesTheSmallerMapFirstWhateverItsKeysAre) {
  // Size is read before anything inside the map is. The smaller map comes
  // first even when its only key sorts after every key of the larger one, which
  // is the pair that separates this from reading the keys first.
  auto const one = MapOf({{"b", TypedValue(int64_t{1})}});
  auto const two = MapOf({{"a", TypedValue(int64_t{1})}, {"z", TypedValue(int64_t{2})}});

  EXPECT_TRUE(std::is_lt(orderability::Compare(one, two)));
  EXPECT_TRUE(std::is_gt(orderability::Compare(two, one)));
}

TEST(Orderability, OrdersTwoMapsOfOneSizeByEveryKeyBeforeAnyValue) {
  // The keys of both maps are read before either map's values are. The two
  // share their first key and differ under it, and the first map's second key
  // sorts first, so walking a key and its value together would answer from the
  // value and place the two the other way around.
  auto const lhs = MapOf({{"a", TypedValue(int64_t{2})}, {"b", TypedValue(int64_t{1})}});
  auto const rhs = MapOf({{"a", TypedValue(int64_t{1})}, {"c", TypedValue(int64_t{9})}});

  EXPECT_TRUE(std::is_lt(orderability::Compare(lhs, rhs)));
  EXPECT_TRUE(std::is_gt(orderability::Compare(rhs, lhs)));
}

TEST(Orderability, OrdersTwoMapsSharingTheirKeysByTheirValues) {
  auto const lhs = MapOf({{"a", TypedValue(int64_t{1})}, {"b", TypedValue(int64_t{1})}});
  auto const rhs = MapOf({{"a", TypedValue(int64_t{1})}, {"b", TypedValue(int64_t{2})}});

  EXPECT_TRUE(std::is_lt(orderability::Compare(lhs, rhs)));
  EXPECT_TRUE(std::is_gt(orderability::Compare(rhs, lhs)));
}

TEST(Orderability, PlacesTwoAlikeMapsAlongsideOneAnother) {
  auto const lhs = MapOf({{"a", TypedValue(int64_t{1})}, {"b", TypedValue(int64_t{2})}});
  auto const rhs = MapOf({{"b", TypedValue(int64_t{2})}, {"a", TypedValue(int64_t{1})}});

  EXPECT_EQ(orderability::Compare(lhs, rhs), std::weak_ordering::equivalent);
}

TEST(Orderability, SortsAMixedColumnWithoutRaising) {
  auto values = OrderedSample();
  std::ranges::reverse(values);
  // Sorting is the property the relation exists for, and it is undefined
  // behaviour unless the relation is a strict weak ordering.
  std::ranges::sort(values,
                    [](TypedValue const &a, TypedValue const &b) { return std::is_lt(orderability::Compare(a, b)); });
  auto const expected = OrderedSample();
  for (size_t i = 0; i != values.size(); ++i) {
    EXPECT_EQ(orderability::Compare(values[i], expected[i]), std::weak_ordering::equivalent) << "at index " << i;
  }
}

// The boundary between equality and equivalence.
//
// The two relations agree everywhere except over a Null, and three lookup
// structures in the engine match by equivalence while being asked an equality
// question. Whether they may answer is exactly whether the two agree for the
// value in hand.

/// Values carrying no Null anywhere, for which the two relations must agree.
std::vector<TypedValue> Agreeing() {
  auto map = std::map<std::string, TypedValue>{};
  map.emplace("k", TypedValue(int64_t{1}));
  return {
      TypedValue(int64_t{1}),
      TypedValue(1.0),
      TypedValue("a"),
      TypedValue(true),
      TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1}), TypedValue(int64_t{2})}),
      TypedValue(std::move(map)),
  };
}

/// Values carrying a Null somewhere, for which they need not.
std::vector<TypedValue> Disagreeing() {
  auto map = std::map<std::string, TypedValue>{};
  map.emplace("k", TypedValue());
  return {
      TypedValue(),
      TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1}), TypedValue()}),
      TypedValue(std::move(map)),
      TypedValue(std::vector<TypedValue>{TypedValue(std::vector<TypedValue>{TypedValue()})}),
  };
}

TEST(RelationAgreement, HoldsForAValueCarryingNoNull) {
  for (auto const &value : Agreeing()) {
    EXPECT_TRUE(relations::EqualityAgreesWithEquivalence(value));
  }
}

TEST(RelationAgreement, FailsForAValueCarryingANullAnywhere) {
  for (auto const &value : Disagreeing()) {
    EXPECT_FALSE(relations::EqualityAgreesWithEquivalence(value));
  }
}

TEST(RelationAgreement, FailsForAValueCarryingANaNAnywhere) {
  // A NaN is the other value the relations part company over: equality reads it
  // as different from itself, while the order and equivalence hold two of them
  // alike. A structure keyed by the order cannot answer for one either.
  auto const nan = std::numeric_limits<double>::quiet_NaN();
  auto nested = std::map<std::string, TypedValue>{};
  nested.emplace("k", TypedValue(nan));
  std::vector<TypedValue> const carrying{
      TypedValue(nan),
      TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1}), TypedValue(nan)}),
      TypedValue(std::move(nested)),
  };
  for (auto const &value : carrying) {
    EXPECT_FALSE(relations::EqualityAgreesWithEquivalence(value));
  }
}

TEST(RelationAgreement, WhereItHoldsTheTwoRelationsAnswerAlike) {
  // The property the lookup structures rely on: given it, a set keyed by
  // equivalence may answer a question asked by equality.
  auto const values = Agreeing();
  for (auto const &a : values) {
    for (auto const &b : values) {
      ASSERT_TRUE(relations::EqualityAgreesWithEquivalence(a) && relations::EqualityAgreesWithEquivalence(b));
      auto const by_equality = a == b;
      ASSERT_FALSE(by_equality.IsNull()) << "equality left an answer open where the two agree";
      EXPECT_EQ(by_equality.ValueBool(), memgraph::query::relations::equivalence::KeyEqual{}(a, b));
    }
  }
}

TEST(RelationAgreement, WhereItFailsTheTwoRelationsCanDisagree) {
  // The case that makes the guard load-bearing rather than defensive: two lists
  // alike but for a Null are equivalent, while equality cannot tell them apart
  // and answers Null. A set keyed by equivalence would report a match here.
  auto const lhs = TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1}), TypedValue()});
  auto const rhs = TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1}), TypedValue()});

  EXPECT_FALSE(relations::EqualityAgreesWithEquivalence(lhs));
  EXPECT_TRUE(memgraph::query::relations::equivalence::KeyEqual{}(lhs, rhs));
  EXPECT_TRUE((lhs == rhs).IsNull());
}

TEST(RelationAgreement, EquivalentValuesHashAlike) {
  // The invariant pairing Hash with Equivalent, and the reason the two are
  // declared together: a change to one that is not made to the other is silent
  // until a lookup misses.
  auto const pairs = std::vector<std::pair<TypedValue, TypedValue>>{
      {TypedValue(int64_t{1}), TypedValue(int64_t{1})},
      {TypedValue("a"), TypedValue("a")},
      {TypedValue(), TypedValue()},
      {TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1}), TypedValue()}),
       TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1}), TypedValue()})},
  };
  for (auto const &[a, b] : pairs) {
    ASSERT_TRUE(memgraph::query::relations::equivalence::KeyEqual{}(a, b));
    EXPECT_EQ(memgraph::query::relations::equivalence::Hasher{}(a),
              memgraph::query::relations::equivalence::Hasher{}(b));
  }
}

TEST(Equivalence, HoldsTwoNaNsEquivalent) {
  auto const nan = TypedValue(std::numeric_limits<double>::quiet_NaN());
  EXPECT_TRUE(equivalence::Equivalent(nan, nan));
  EXPECT_FALSE(equivalence::Equivalent(nan, TypedValue(1.0)));
}

TEST(Equivalence, HoldsTwoNaNsInsideAContainerEquivalent) {
  auto const nan = std::numeric_limits<double>::quiet_NaN();
  auto const lhs = TypedValue(std::vector<TypedValue>{TypedValue(nan)});
  auto const rhs = TypedValue(std::vector<TypedValue>{TypedValue(nan)});
  EXPECT_TRUE(equivalence::Equivalent(lhs, rhs));
}

TEST(Equivalence, HoldsNaNsOfEitherSignEquivalentAndHashingAlike) {
  // A NaN carries a sign, and dividing or rooting into one can produce either.
  // Holding them equivalent obliges the hash to place them in one bucket, or a
  // set keyed by the relation keeps both.
  auto const positive = TypedValue(std::numeric_limits<double>::quiet_NaN());
  auto const negative = TypedValue(-std::numeric_limits<double>::quiet_NaN());
  EXPECT_TRUE(equivalence::Equivalent(positive, negative));
  EXPECT_EQ(memgraph::query::relations::equivalence::Hasher{}(positive),
            memgraph::query::relations::equivalence::Hasher{}(negative));
}

TEST(Equivalence, GivesADoubleItsOwnBucketUnlessItHoldsAWholeNumber) {
  // A double holding a whole number is equivalent to that integer, so the two
  // have to reach one bucket. Every other double stands for itself, and giving
  // it the bucket of the integer it sits above collapses a column of them onto
  // the few integers they lie between.
  auto const integer = TypedValue(int64_t{2});
  auto const whole = TypedValue(2.0);

  ASSERT_TRUE(equivalence::Equivalent(whole, integer));
  EXPECT_EQ(memgraph::query::relations::equivalence::Hasher{}(whole),
            memgraph::query::relations::equivalence::Hasher{}(integer));

  for (double const fraction : {2.25, 2.5, 2.75}) {
    auto const value = TypedValue(fraction);
    ASSERT_FALSE(equivalence::Equivalent(value, integer));
    EXPECT_NE(memgraph::query::relations::equivalence::Hasher{}(value),
              memgraph::query::relations::equivalence::Hasher{}(integer))
        << fraction << " must not take the bucket of the integer below it";
  }
}

TEST(Equivalence, HoldsTwoAlikePointsCarryingANaNEquivalent) {
  // A point holds its coordinates as doubles, so the rule that holds two NaNs
  // alike has to reach inside one, as it already reaches inside a list.
  auto const nan = std::numeric_limits<double>::quiet_NaN();
  auto const carrying =
      TypedValue(memgraph::storage::Point2d{memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, nan, 1.0});
  auto const alike =
      TypedValue(memgraph::storage::Point2d{memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, nan, 1.0});
  auto const carrying_a_number =
      TypedValue(memgraph::storage::Point2d{memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, 1.0, 1.0});

  EXPECT_TRUE(equivalence::Equivalent(carrying, alike));
  EXPECT_FALSE(equivalence::Equivalent(carrying, carrying_a_number));
  EXPECT_EQ(memgraph::query::relations::equivalence::Hasher{}(carrying),
            memgraph::query::relations::equivalence::Hasher{}(alike))
      << "two equivalent points must reach one bucket";
}

TEST(Equivalence, SeparatesANaNFromANull) {
  auto const nan = TypedValue(std::numeric_limits<double>::quiet_NaN());
  EXPECT_FALSE(equivalence::Equivalent(nan, TypedValue()));
}

// Comparability, which the four ordered comparisons each read. Asserted against
// the relation rather than through the operators, so the two kinds of non-answer
// it gives are told apart: `unordered`, which every comparison reads as false,
// and nothing at all, which every comparison reads as Null.
namespace comparability = memgraph::query::relations::comparability;

TEST(Comparability, LeavesNaNUnorderedRatherThanUnanswered) {
  auto const nan = TypedValue(std::numeric_limits<double>::quiet_NaN());
  auto const order = comparability::Compare(nan, TypedValue(1.0));
  ASSERT_TRUE(order.has_value()) << "a NaN is comparable, it just has no place in the order";
  EXPECT_EQ(*order, std::partial_ordering::unordered);
}

TEST(Comparability, AnswersNothingForANullOperand) {
  EXPECT_FALSE(comparability::Compare(TypedValue(), TypedValue(int64_t{1})).has_value());
  EXPECT_FALSE(comparability::Compare(TypedValue(int64_t{1}), TypedValue()).has_value());
  EXPECT_FALSE(comparability::Compare(TypedValue(), TypedValue()).has_value());
}

TEST(Comparability, AnswersNothingForUnlikeTypes) {
  // Unlike orderability, which places every pair, comparability declines.
  EXPECT_FALSE(comparability::Compare(TypedValue(int64_t{1}), TypedValue("a")).has_value());
  EXPECT_FALSE(comparability::Compare(TypedValue(true), TypedValue(int64_t{1})).has_value());
}

TEST(Comparability, OrdersTheNumbersAgainstOneAnother) {
  auto const order = comparability::Compare(TypedValue(int64_t{1}), TypedValue(1.5));
  ASSERT_TRUE(order.has_value());
  EXPECT_TRUE(std::is_lt(*order));
}

TEST(Comparability, OrdersListsElementByElement) {
  auto const shorter = TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1})});
  auto const longer = TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1}), TypedValue(int64_t{2})});
  auto const order = comparability::Compare(shorter, longer);
  ASSERT_TRUE(order.has_value());
  EXPECT_TRUE(std::is_lt(*order)) << "a list that is the start of another comes first";
}

// The relations, checked over every type rather than over a sample.
//
// A type added to TypedValue::Type already fails to compile until each relation
// places it, because every relation switches exhaustively. What that does not
// reach is whether the placements are consistent with one another, which is
// what the properties below assert. Driving them from Representative puts the
// new type into all of them at once.

using memgraph::storage::Enum;
using memgraph::storage::EnumTypeId;
using memgraph::storage::EnumValueId;
using memgraph::storage::Point2d;
using memgraph::storage::Point3d;
using enum memgraph::storage::CoordinateReferenceSystem;

/// One value of the given type, or nothing where this suite cannot supply one.
///
/// Exhaustive by design: adding a type will not compile until it is placed here,
/// which is the point. Two groups answer nothing, for different reasons, and
/// both are named rather than omitted.
std::optional<TypedValue> Representative(TypedValue::Type type) {
  switch (type) {
    using enum TypedValue::Type;
    case Null:
      return TypedValue();
    case Bool:
      return TypedValue(true);
    case Int:
      return TypedValue(int64_t{42});
    case Double:
      return TypedValue(3.5);
    case String:
      return TypedValue("s");
    case List:
      return TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1})});
    case Map: {
      auto entries = std::map<std::string, TypedValue>{};
      entries.emplace("k", TypedValue(int64_t{1}));
      return TypedValue(std::move(entries));
    }
    case Date:
      return TypedValue(memgraph::utils::Date{std::chrono::microseconds{86'400'000'000}});
    case LocalTime:
      return TypedValue(memgraph::utils::LocalTime{int64_t{3'600'000'000}});
    case LocalDateTime:
      return TypedValue(memgraph::utils::LocalDateTime{int64_t{86'400'000'000}});
    case ZonedDateTime:
      return TypedValue(memgraph::utils::ZonedDateTime{
          std::chrono::sys_time<std::chrono::microseconds>{std::chrono::microseconds{86'400'000'000}},
          memgraph::utils::Timezone{std::chrono::minutes{0}}});
    case Duration:
      return TypedValue(memgraph::utils::Duration{1'000'000});
    case Enum:
      return TypedValue(memgraph::storage::Enum{EnumTypeId{2}, EnumValueId{42}});
    case Point2d:
      return TypedValue(
          memgraph::storage::Point2d{memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, 1.0, 2.0});
    case Point3d:
      return TypedValue(memgraph::storage::Point3d{Cartesian_3d, 1.0, 2.0, 3.0});

    // Building one needs a storage accessor, which this suite deliberately does
    // without so that a relation can be exercised apart from a database. The
    // plan-level suites cover the relations over these.
    case Vertex:
    case Edge:
    case Path:
    case VirtualEdge:
    case VirtualNode:
      return std::nullopt;

    // No relation places these; each raises rather than comparing.
    case Graph:
    case VirtualGraph:
    case Function:
      return std::nullopt;
  }
}

/// Every type, so the properties below run over all of them.
///
/// The exhaustive switch above is what forces a new type to be considered; this
/// list still has to be extended by hand when one is added at the end, which the
/// assertion below catches only for a type added anywhere else.
constexpr std::array kAllTypes{
    TypedValue::Type::Null,          TypedValue::Type::Bool,          TypedValue::Type::Int,
    TypedValue::Type::Double,        TypedValue::Type::String,        TypedValue::Type::List,
    TypedValue::Type::Map,           TypedValue::Type::Vertex,        TypedValue::Type::Edge,
    TypedValue::Type::Path,          TypedValue::Type::Date,          TypedValue::Type::LocalTime,
    TypedValue::Type::LocalDateTime, TypedValue::Type::ZonedDateTime, TypedValue::Type::Duration,
    TypedValue::Type::Graph,         TypedValue::Type::VirtualGraph,  TypedValue::Type::Function,
    TypedValue::Type::Enum,          TypedValue::Type::Point2d,       TypedValue::Type::Point3d,
    TypedValue::Type::VirtualEdge,   TypedValue::Type::VirtualNode};
static_assert(kAllTypes.size() == static_cast<size_t>(TypedValue::Type::VirtualNode) + 1,
              "a type was added; extend kAllTypes so the relation properties cover it");

/// The representatives this suite can build, one per type.
std::vector<TypedValue> Representatives() {
  std::vector<TypedValue> values;
  for (auto const type : kAllTypes) {
    if (auto value = Representative(type)) values.push_back(std::move(*value));
  }
  return values;
}

// The order a value is kept in and the order a query sorts by.
//
// An index is ordered by the first and a sort by the second, and a plan that
// drops a sort because an index already returned its rows in order is sound
// only while the two are one order. Nothing but this holds them together.

/// One stored value of each type storage can hold, in no particular order.
std::vector<memgraph::storage::PropertyValue> StoredValues(memgraph::storage::NameIdMapper &mapper) {
  using memgraph::storage::PropertyValue;
  auto list = PropertyValue::list_t{};
  list.emplace_back(int64_t{1});
  list.emplace_back(int64_t{2});
  return {
      PropertyValue(),
      PropertyValue(false),
      PropertyValue(true),
      PropertyValue(int64_t{-3}),
      PropertyValue(int64_t{7}),
      PropertyValue(2.5),
      PropertyValue(-1.0),
      PropertyValue("a"),
      PropertyValue("zz"),
      PropertyValue(std::move(list)),
      PropertyValue(memgraph::storage::TemporalData{memgraph::storage::TemporalType::Date, 86'400'000'000}),
      PropertyValue(memgraph::storage::TemporalData{memgraph::storage::TemporalType::LocalDateTime, 86'400'000'000}),
      PropertyValue(std::numeric_limits<double>::quiet_NaN()),
      PropertyValue(memgraph::storage::Point2d{
          memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, std::numeric_limits<double>::quiet_NaN(), 1.0}),
      PropertyValue(PropertyValue::map_t{
          {memgraph::storage::PropertyId::FromUint(mapper.NameToId("k")), PropertyValue(int64_t{1})}}),
      PropertyValue(memgraph::storage::Point2d{memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, 1.0, 2.0}),
      PropertyValue(memgraph::storage::Point3d{Cartesian_3d, 1.0, 2.0, 3.0}),
      PropertyValue(memgraph::storage::TemporalData{memgraph::storage::TemporalType::LocalTime, 3'600'000'000}),
      PropertyValue(memgraph::storage::TemporalData{memgraph::storage::TemporalType::Duration, 90'000'000}),
      PropertyValue(memgraph::storage::ZonedTemporalData{memgraph::storage::ZonedTemporalType::ZonedDateTime,
                                                         memgraph::utils::AsSysTime(86'400'000'000),
                                                         memgraph::utils::Timezone(std::chrono::minutes{60})}),
      PropertyValue(memgraph::storage::Enum{memgraph::storage::EnumTypeId{1}, memgraph::storage::EnumValueId{2}}),
      PropertyValue(std::vector<int>{1, 2}),
      PropertyValue(std::vector<double>{1.5, 2.5}),
      PropertyValue(std::vector<std::variant<int, double>>{1, 2.5}),
  };
}

TEST(RelationProperties, ComparabilityAnswersForExactlyTheTypesItSaysItAdmits) {
  // Comparability names the types it places in one switch and orders them in
  // another. A type reaching only one of the two would be refused by the
  // operators while carrying an order, or placed by an order the operators
  // never ask for, and neither switch can see the other.
  for (auto const type : kAllTypes) {
    auto const value = Representative(type);
    if (!value) continue;
    EXPECT_EQ(comparability::ComparePayload(*value, *value).has_value(), comparability::Admits(type))
        << "comparability disagrees with itself about " << type;
  }
}

TEST(RelationProperties, ComparabilityAnswersNullForEveryPairItCannotPlace) {
  // Two values the relation cannot place are incomparable, and comparability
  // says so by answering nothing, which the four operators read as Null. It
  // never raises. A pair that raised for some types and answered Null for
  // others would make a filter's behaviour depend on which types a column
  // happened to hold, and would let a scan fenced to one type pass over a row
  // the filter it stands in for could not even reach.
  for (auto const left : kAllTypes) {
    auto const a = Representative(left);
    if (!a) continue;
    for (auto const right : kAllTypes) {
      auto const b = Representative(right);
      if (!b) continue;
      EXPECT_NO_THROW({
        auto const order = comparability::Compare(*a, *b);
        // Either the relation places the pair, or it declines. There is no
        // third answer and no way out through an exception.
        (void)order;
      }) << "comparability raised for "
         << left << " against " << right;
    }
  }
}

TEST(RelationProperties, TheOrderedOperatorsAnswerNullRatherThanRaising) {
  // What a query sees of the above: a comparison it cannot settle is Null, the
  // same answer it gives for a Null operand.
  auto const point =
      TypedValue(memgraph::storage::Point2d{memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, 1.0, 2.0});
  auto const date = TypedValue(memgraph::utils::Date({2020, 1, 1}));
  auto const enum_value =
      TypedValue(memgraph::storage::Enum{memgraph::storage::EnumTypeId{1}, memgraph::storage::EnumValueId{1}});

  for (auto const &[a, b] : std::vector<std::pair<TypedValue, TypedValue>>{
           {point, date}, {date, point}, {point, point}, {enum_value, enum_value}, {point, enum_value}}) {
    EXPECT_TRUE((a < b).IsNull()) << "an unplaceable pair must answer Null";
    EXPECT_TRUE((a <= b).IsNull());
    EXPECT_TRUE((a > b).IsNull());
    EXPECT_TRUE((a >= b).IsNull());
  }
}

TEST(RelationProperties, EveryTypeCarryingAnOrderIsPlacedByOneOfTheTwoRelations) {
  // Between them the two relations place every type a value can hold, bar the
  // three no order is defined over. A type slipping out of both would be
  // unsortable, which a sort discovers as undefined behaviour rather than as
  // an error.
  for (auto const type : kAllTypes) {
    auto const value = Representative(type);
    if (!value) continue;
    using enum TypedValue::Type;
    if (type == Graph || type == VirtualGraph || type == Function) continue;

    auto const placed_by_comparability = comparability::Admits(type);
    auto const placed_by_orderability = [&] {
      try {
        orderability::Compare(*value, *value);
        return true;
      } catch (QueryRuntimeException const &) {
        return false;
      }
    }();
    EXPECT_TRUE(placed_by_comparability || placed_by_orderability) << "no relation places " << type;
  }
}

TEST(RelationProperties, EachStoredTypeTakesThePlaceTheQueryTypeTakes) {
  // Both layers report a place from one declaration, so the two can no longer
  // be renumbered apart. What still has to be said is which stored type stands
  // for which query type, and the one pairing that is not one to one: a stored
  // temporal takes the place a local date and time holds, whichever of the four
  // it carries, which is what keeps a sort over such a column.
  using memgraph::storage::PropertyValueType;
  using Rank = memgraph::storage::ValueRank;

  struct Correspondence {
    PropertyValueType stored;
    TypedValue::Type queried;
  };

  auto const pairs = std::vector<Correspondence>{
      {PropertyValueType::Null, TypedValue::Type::Null},
      {PropertyValueType::Bool, TypedValue::Type::Bool},
      {PropertyValueType::Int, TypedValue::Type::Int},
      {PropertyValueType::Double, TypedValue::Type::Double},
      {PropertyValueType::String, TypedValue::Type::String},
      {PropertyValueType::List, TypedValue::Type::List},
      {PropertyValueType::IntList, TypedValue::Type::List},
      {PropertyValueType::DoubleList, TypedValue::Type::List},
      {PropertyValueType::NumericList, TypedValue::Type::List},
      {PropertyValueType::Map, TypedValue::Type::Map},
      {PropertyValueType::Enum, TypedValue::Type::Enum},
      {PropertyValueType::Point2d, TypedValue::Type::Point2d},
      {PropertyValueType::Point3d, TypedValue::Type::Point3d},
      {PropertyValueType::ZonedTemporalData, TypedValue::Type::ZonedDateTime},
      {PropertyValueType::TemporalData, TypedValue::Type::LocalDateTime},
      // A stored vector is handed to a query as the list of its coordinates,
      // so it takes the place a list takes.
      {PropertyValueType::VectorIndexId, TypedValue::Type::List},
  };

  for (auto const &[stored, queried] : pairs) {
    EXPECT_EQ(memgraph::storage::SortRank(stored), orderability::SortRank(queried))
        << "a stored value of type " << static_cast<int>(stored) << " and the query value it becomes "
        << "must be given one place";
  }

  // Null is the last place, so nothing a query holds sorts after it.
  for (auto const &[stored, queried] : pairs) {
    EXPECT_LE(memgraph::storage::SortRank(stored), Rank::Null) << "a stored type sorts after a null";
  }
}

TEST(RelationProperties, AStoredTypeCarryingSeveralKindsHasItsRangeNarrowedToOne) {
  // The reason the reading is what it is, asserted rather than described: one
  // stored type carries all four temporal kinds, and the comparison places no
  // pair drawn from two of them. A scan reading the band between two bounds
  // would return what the comparison declines, so the band is cut to one kind.
  using memgraph::storage::PropertyValueType;
  namespace relations = memgraph::query::relations;

  auto const date = TypedValue(memgraph::utils::Date({2020, 1, 1}));
  auto const duration = TypedValue(memgraph::utils::Duration(1));
  EXPECT_FALSE(relations::comparability::Compare(date, duration).has_value())
      << "the comparison must decline a pair of unlike temporal kinds";

  EXPECT_EQ(relations::HowARangeReadsStoredType(PropertyValueType::TemporalData),
            relations::RangeReading::BoundsNarrowedToKind);
  // A zoned date and time is the one temporal kind carried by a stored type of
  // its own, so nothing has to be cut away from its band.
  EXPECT_EQ(relations::HowARangeReadsStoredType(PropertyValueType::ZonedTemporalData),
            relations::RangeReading::BoundsAlone);
}

TEST(RelationProperties, TheTwoReadingsAgreeAboutEveryTypeBothLayersName) {
  // A bound is written as a query value and read as a stored one, so the two
  // readings have to answer alike about the same value or a range would be
  // narrowed at one end of the journey and not the other.
  using memgraph::storage::PropertyValueType;
  namespace relations = memgraph::query::relations;

  struct Correspondence {
    PropertyValueType stored;
    TypedValue::Type queried;
  };

  auto const pairs = std::vector<Correspondence>{
      {PropertyValueType::Null, TypedValue::Type::Null},
      {PropertyValueType::Bool, TypedValue::Type::Bool},
      {PropertyValueType::Int, TypedValue::Type::Int},
      {PropertyValueType::Double, TypedValue::Type::Double},
      {PropertyValueType::String, TypedValue::Type::String},
      {PropertyValueType::List, TypedValue::Type::List},
      {PropertyValueType::IntList, TypedValue::Type::List},
      {PropertyValueType::DoubleList, TypedValue::Type::List},
      {PropertyValueType::NumericList, TypedValue::Type::List},
      {PropertyValueType::Map, TypedValue::Type::Map},
      {PropertyValueType::Enum, TypedValue::Type::Enum},
      {PropertyValueType::Point2d, TypedValue::Type::Point2d},
      {PropertyValueType::Point3d, TypedValue::Type::Point3d},
      {PropertyValueType::ZonedTemporalData, TypedValue::Type::ZonedDateTime},
      {PropertyValueType::TemporalData, TypedValue::Type::Date},
      {PropertyValueType::TemporalData, TypedValue::Type::LocalTime},
      {PropertyValueType::TemporalData, TypedValue::Type::LocalDateTime},
      {PropertyValueType::TemporalData, TypedValue::Type::Duration},
  };

  for (auto const &[stored, queried] : pairs) {
    EXPECT_EQ(relations::HowARangeReadsStoredType(stored), relations::HowARangeReadsQueryType(queried))
        << "a range over stored type " << static_cast<int>(stored) << " and over the query value it holds "
        << "must be read the same way";
  }
}

TEST(RelationProperties, EveryTypeTakesTheRankTheReferenceGivesIt) {
  // Cypher fixes the order values of unlike types sort in, and a query ordering
  // a mixed column answers alike on both engines only where this order is the
  // one taken. The types Memgraph adds are inserted among them: the virtual
  // node and edge beside the node and relationship they stand in for, and an
  // enum between a duration and a point. None is placed after the numbers,
  // which is the one placement the language refuses an added type.
  auto const ascending = std::vector<TypedValue::Type>{
      TypedValue::Type::Map,    TypedValue::Type::Vertex,        TypedValue::Type::VirtualNode,
      TypedValue::Type::Edge,   TypedValue::Type::VirtualEdge,   TypedValue::Type::List,
      TypedValue::Type::Path,   TypedValue::Type::ZonedDateTime, TypedValue::Type::LocalDateTime,
      TypedValue::Type::Date,   TypedValue::Type::LocalTime,     TypedValue::Type::Duration,
      TypedValue::Type::Enum,   TypedValue::Type::Point2d,       TypedValue::Type::Point3d,
      TypedValue::Type::String, TypedValue::Type::Bool,          TypedValue::Type::Int,
      TypedValue::Type::Null,
  };

  for (auto i = size_t{1}; i != ascending.size(); ++i) {
    EXPECT_LT(orderability::SortRank(ascending[i - 1]), orderability::SortRank(ascending[i]))
        << "type " << ascending[i - 1] << " must sort before " << ascending[i];
  }

  // An integer and a double share the one rank two types share, because they
  // are ordered against each other as the numbers they are.
  EXPECT_EQ(orderability::SortRank(TypedValue::Type::Int), orderability::SortRank(TypedValue::Type::Double));
}

TEST(RelationProperties, AStoredVectorIsOrderedAgainstListsAsTheListItIsReadBackAs) {
  // A vector holds its coordinates unboxed and a query is handed a list of
  // them. An index ordering it anywhere but where a list goes would walk such a
  // column in an order no sort produces, and comparing it against a list by
  // their types alone would report the two alike whatever they hold.
  using memgraph::storage::PropertyValue;
  auto const vector = PropertyValue(PropertyValue::VectorIndexIdData{
      .ids = memgraph::utils::small_vector<uint64_t>{}, .vector = memgraph::utils::small_vector<float>{1.0F, 2.0F}});

  auto shorter = PropertyValue::list_t{};
  shorter.emplace_back(int64_t{1});
  auto const before = PropertyValue(std::move(shorter));

  auto longer = PropertyValue::list_t{};
  longer.emplace_back(int64_t{1});
  longer.emplace_back(int64_t{9});
  auto const after = PropertyValue(std::move(longer));

  EXPECT_TRUE(std::is_gt(vector <=> before)) << "a vector holding [1, 2] comes after the list holding [1]";
  EXPECT_TRUE(std::is_lt(vector <=> after)) << "and before the list holding [1, 9]";
  EXPECT_EQ(memgraph::storage::SortRank(memgraph::storage::PropertyValueType::VectorIndexId),
            memgraph::storage::SortRank(memgraph::storage::PropertyValueType::List));
}

TEST(RelationProperties, TheTwoLayersPlaceDoublesAlike) {
  // The order over two doubles is the one primitive both layers need, and the
  // only pair it answers differently from `<=>` is the one holding a NaN. It is
  // asserted here so that a single definition can serve both without either
  // side drifting away from it unseen.
  using memgraph::storage::PropertyValue;
  auto const values = std::vector<double>{-1.5,
                                          0.0,
                                          1.5,
                                          std::numeric_limits<double>::infinity(),
                                          -std::numeric_limits<double>::infinity(),
                                          std::numeric_limits<double>::quiet_NaN(),
                                          -std::numeric_limits<double>::quiet_NaN()};

  for (auto const lhs : values) {
    for (auto const rhs : values) {
      auto const stored = PropertyValue(lhs) <=> PropertyValue(rhs);
      auto const queried = orderability::Compare(TypedValue(lhs), TypedValue(rhs));
      EXPECT_EQ(stored, queried) << lhs << " against " << rhs;
    }
  }
}

TEST(RelationProperties, WhereTheLayersAreSaidToKeepOneOrderTheyDo) {
  // The condition a plan reads before it drops a sort, asserted rather than
  // described: for every pair of types it admits, an index and a sort place the
  // two values alike. The types it does not admit are the ones that would fail,
  // and naming them here is what keeps the condition honest as either order
  // changes.
  memgraph::storage::NameIdMapper mapper;
  auto const stored = StoredValues(mapper);
  std::vector<TypedValue> converted;
  converted.reserve(stored.size());
  for (auto const &value : stored) converted.emplace_back(value, &mapper);

  size_t compared = 0;
  for (size_t i = 0; i != stored.size(); ++i) {
    if (!relations::LayersKeepThisTypeInOneOrder(stored[i].type())) continue;
    for (size_t j = 0; j != stored.size(); ++j) {
      if (!relations::LayersKeepThisTypeInOneOrder(stored[j].type())) continue;
      ++compared;
      EXPECT_EQ(stored[i] <=> stored[j], orderability::Compare(converted[i], converted[j]))
          << "the two layers place " << converted[i].type() << " and " << converted[j].type()
          << " differently, though the condition a plan reads says they agree";
    }
  }
  EXPECT_GT(compared, 0u) << "the corpus reached none of the types the condition admits";
}

TEST(RelationProperties, TheTypesTheConditionRefusesAreTheOnesThatWouldFail) {
  // The other half: a type is refused because the two layers really do part
  // company over it. Were that to stop being true, the condition would be
  // costing a sort for nothing and should be told.
  memgraph::storage::NameIdMapper mapper;
  auto const dates = memgraph::storage::PropertyValue(
      memgraph::storage::TemporalData{memgraph::storage::TemporalType::Date, 86'400'000'000});
  auto const local = memgraph::storage::PropertyValue(
      memgraph::storage::TemporalData{memgraph::storage::TemporalType::LocalDateTime, 86'400'000'000});
  ASSERT_FALSE(relations::LayersKeepThisTypeInOneOrder(dates.type()));

  auto const by_storage = dates <=> local;
  auto const by_query = orderability::Compare(TypedValue(dates, &mapper), TypedValue(local, &mapper));
  EXPECT_NE(by_storage, by_query) << "the two layers now agree about the temporal types, so the condition may admit "
                                     "them and a sort over such a column need no longer be kept";
}

TEST(RelationProperties, TheConditionRefusesAMapBecauseTheLayersOrderItsKeysApart) {
  // A stored map is kept by the identifiers its keys were given, which is the
  // order the keys were first seen in, and a sort reads their names. Naming
  // the two keys in the order that puts them at odds is what makes the two
  // layers answer differently about the same pair.
  using memgraph::storage::PropertyId;
  using memgraph::storage::PropertyValue;
  memgraph::storage::NameIdMapper mapper;
  auto const later_by_name = PropertyId::FromUint(mapper.NameToId("z"));
  auto const earlier_by_name = PropertyId::FromUint(mapper.NameToId("a"));
  ASSERT_LT(later_by_name.AsUint(), earlier_by_name.AsUint()) << "the mapper did not give the keys the ids this needs";

  auto const map_of = [&](int64_t under_a, int64_t under_z) {
    return PropertyValue(
        PropertyValue::map_t{{earlier_by_name, PropertyValue(under_a)}, {later_by_name, PropertyValue(under_z)}});
  };
  // The two differ under both keys and in opposite directions, so whichever
  // key is read first decides, and the layers read a different one first.
  auto const lhs = map_of(0, 1);
  auto const rhs = map_of(1, 0);
  ASSERT_FALSE(relations::LayersKeepThisTypeInOneOrder(lhs.type()));

  EXPECT_NE(lhs <=> rhs, orderability::Compare(TypedValue(lhs, &mapper), TypedValue(rhs, &mapper)))
      << "the two layers now agree about a map, so the condition may admit it";
}

TEST(RelationProperties, TheConditionRefusesAListWithoutTheLayersHavingToDisagree) {
  // The list types are refused for a different reason than a map or a temporal
  // is: a list can hold a value of any type, including the ones the two layers
  // order apart, and what a given column's lists hold is not something a plan
  // can see. So the refusal is not backed by a pair that disagrees, and this
  // records that rather than leaving it to be read as an omission.
  using memgraph::storage::PropertyValueType;
  for (auto const type : {PropertyValueType::List,
                          PropertyValueType::IntList,
                          PropertyValueType::DoubleList,
                          PropertyValueType::NumericList,
                          PropertyValueType::VectorIndexId}) {
    EXPECT_FALSE(relations::LayersKeepThisTypeInOneOrder(type))
        << "a list is refused because of what it may hold, whatever representation carries it";
  }

  // A list of numbers is ordered alike by both layers, which is the case the
  // refusal gives up on.
  memgraph::storage::NameIdMapper mapper;
  auto const lhs = memgraph::storage::PropertyValue(std::vector<int>{1, 2});
  auto const rhs = memgraph::storage::PropertyValue(std::vector<int>{1, 3});
  EXPECT_EQ(lhs <=> rhs, orderability::Compare(TypedValue(lhs, &mapper), TypedValue(rhs, &mapper)));
}

TEST(RelationProperties, EveryRefusedStoredTypeIsRefusedForAStatedReason) {
  // The condition refuses six types and each is refused for one of two
  // reasons, both asserted above. A type joining them without a reason being
  // recorded costs a sort that may not need keeping, which nothing else
  // reports.
  using memgraph::storage::PropertyValueType;
  constexpr std::array kRefused{PropertyValueType::Map,
                                PropertyValueType::TemporalData,
                                PropertyValueType::List,
                                PropertyValueType::IntList,
                                PropertyValueType::DoubleList,
                                PropertyValueType::NumericList,
                                PropertyValueType::VectorIndexId};
  constexpr std::array kAllStoredTypes{PropertyValueType::Null,
                                       PropertyValueType::Bool,
                                       PropertyValueType::Int,
                                       PropertyValueType::Double,
                                       PropertyValueType::String,
                                       PropertyValueType::List,
                                       PropertyValueType::Map,
                                       PropertyValueType::TemporalData,
                                       PropertyValueType::ZonedTemporalData,
                                       PropertyValueType::Enum,
                                       PropertyValueType::Point2d,
                                       PropertyValueType::Point3d,
                                       PropertyValueType::IntList,
                                       PropertyValueType::DoubleList,
                                       PropertyValueType::NumericList,
                                       PropertyValueType::VectorIndexId};

  for (auto const type : kAllStoredTypes) {
    auto const refused = !relations::LayersKeepThisTypeInOneOrder(type);
    auto const recorded = std::ranges::find(kRefused, type) != kRefused.end();
    EXPECT_EQ(refused, recorded) << "stored type " << static_cast<int>(type)
                                 << " changed sides without a reason being recorded for it";
  }
}

TEST(RelationProperties, OrderabilityPlacesEveryPairOfTypes) {
  auto const values = Representatives();
  ASSERT_FALSE(values.empty());
  for (auto const &a : values) {
    for (auto const &b : values) {
      auto const order = orderability::Compare(a, b);
      EXPECT_TRUE(std::is_lt(order) || std::is_gt(order) || order == std::weak_ordering::equivalent)
          << "types " << static_cast<int>(a.type()) << " and " << static_cast<int>(b.type());
    }
  }
}

TEST(RelationProperties, OrderabilityIsAntisymmetricOverEveryType) {
  auto const values = Representatives();
  for (auto const &a : values) {
    for (auto const &b : values) {
      EXPECT_EQ(std::is_lt(orderability::Compare(a, b)), std::is_gt(orderability::Compare(b, a)))
          << "types " << static_cast<int>(a.type()) << " and " << static_cast<int>(b.type());
    }
  }
}

TEST(RelationProperties, OrderabilityIsTransitiveOverEveryType) {
  auto const values = Representatives();
  for (auto const &a : values) {
    for (auto const &b : values) {
      if (!std::is_lt(orderability::Compare(a, b))) continue;
      for (auto const &c : values) {
        if (std::is_lt(orderability::Compare(b, c))) {
          EXPECT_TRUE(std::is_lt(orderability::Compare(a, c)));
        }
      }
    }
  }
}

TEST(RelationProperties, EqualityAndEquivalenceAgreeWhereTheConditionHolds) {
  auto const values = Representatives();
  for (auto const &a : values) {
    if (!relations::EqualityAgreesWithEquivalence(a)) continue;
    for (auto const &b : values) {
      if (!relations::EqualityAgreesWithEquivalence(b)) continue;
      auto const by_equality = a == b;
      ASSERT_FALSE(by_equality.IsNull()) << "equality left an answer open for types " << static_cast<int>(a.type())
                                         << " and " << static_cast<int>(b.type())
                                         << ", where the condition says the two relations agree";
      EXPECT_EQ(by_equality.ValueBool(), memgraph::query::relations::equivalence::KeyEqual{}(a, b));
    }
  }
}

TEST(RelationProperties, EquivalenceHoldsExactlyWhereOrderabilityPlacesTwoValuesAlike) {
  auto values = Representatives();
  ASSERT_FALSE(values.empty());
  // The two values the relations are defined to treat differently from one
  // another, which a representative per type does not reach.
  values.emplace_back(std::numeric_limits<double>::quiet_NaN());
  values.emplace_back(std::vector<TypedValue>{TypedValue(std::numeric_limits<double>::quiet_NaN())});
  values.emplace_back(memgraph::storage::Point2d{
      memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, std::numeric_limits<double>::quiet_NaN(), 1.0});

  for (auto const &a : values) {
    for (auto const &b : values) {
      auto const same_place = std::is_eq(orderability::Compare(a, b));
      EXPECT_EQ(same_place, equivalence::Equivalent(a, b))
          << "orderability and equivalence disagree about " << a.type() << " against " << b.type();
    }
  }
}

TEST(RelationProperties, TwoValuesTheRelationHoldsAlikeHashAlike) {
  // A copy of a value hashing like the value it was copied from says only that
  // the hash reads what the value holds. What a lookup rests on is the pair the
  // relation holds alike without their being built alike: reaching one of them
  // has to reach the other's bucket, or a set keyed by the relation answers
  // that a value it holds is absent.
  namespace equivalence = memgraph::query::relations::equivalence;
  auto const nan = std::numeric_limits<double>::quiet_NaN();

  auto alike = std::vector<std::pair<TypedValue, TypedValue>>{};
  // A whole double is the number the integer beside it is.
  alike.emplace_back(TypedValue(int64_t{2}), TypedValue(2.0));
  // Two NaNs are one value here, though no comparison places either.
  alike.emplace_back(TypedValue(nan), TypedValue(std::sqrt(-1.0)));
  alike.emplace_back(TypedValue(), TypedValue());
  // A container holds of its elements whatever the relation holds of them.
  alike.emplace_back(TypedValue(std::vector<TypedValue>{TypedValue(int64_t{2})}),
                     TypedValue(std::vector<TypedValue>{TypedValue(2.0)}));
  alike.emplace_back(TypedValue(std::vector<TypedValue>{TypedValue(nan)}),
                     TypedValue(std::vector<TypedValue>{TypedValue(std::sqrt(-1.0))}));
  alike.emplace_back(MapOf({{"a", TypedValue(int64_t{2})}}), MapOf({{"a", TypedValue(2.0)}}));
  // A map is read by its entries rather than by the order they were given in.
  alike.emplace_back(MapOf({{"a", TypedValue(int64_t{1})}, {"b", TypedValue(int64_t{2})}}),
                     MapOf({{"b", TypedValue(int64_t{2})}, {"a", TypedValue(int64_t{1})}}));

  for (auto const &[lhs, rhs] : alike) {
    ASSERT_TRUE(equivalence::KeyEqual{}(lhs, rhs))
        << "the pair this checks the hash of must be one the relation holds alike";
    EXPECT_EQ(equivalence::Hasher{}(lhs), equivalence::Hasher{}(rhs))
        << "two values the relation holds alike must reach one bucket";
  }
}

TEST(RelationProperties, EveryTypeHashesConsistentlyWithEquivalence) {
  for (auto const &value : Representatives()) {
    auto const copy = value;
    ASSERT_TRUE(memgraph::query::relations::equivalence::KeyEqual{}(value, copy));
    EXPECT_EQ(memgraph::query::relations::equivalence::Hasher{}(value),
              memgraph::query::relations::equivalence::Hasher{}(copy))
        << "type " << static_cast<int>(value.type());
  }
}

}  // namespace
}  // namespace memgraph::query::tests
