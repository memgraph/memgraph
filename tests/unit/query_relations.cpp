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

// The four relations asked by name.
//
// Every other test reaches them through an operator, which leaves whatever no operator reads
// covered only by whole queries. `Admits` is the clearest case: an index range is emitted or
// withheld on its answer, and no operator reaches it at all.

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <map>
#include <optional>
#include <ranges>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "query/exceptions.hpp"
#include "query/relations/comparability.hpp"
#include "query/relations/equality.hpp"
#include "query/relations/equivalence.hpp"
#include "query/relations/orderability.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/temporal.hpp"

namespace {

using memgraph::query::TypedValue;
using memgraph::storage::Enum;
using memgraph::storage::EnumTypeId;
using memgraph::storage::EnumValueId;
using memgraph::storage::Point2d;
using memgraph::storage::Point3d;
using enum memgraph::storage::CoordinateReferenceSystem;

namespace comparability = memgraph::query::relations::comparability;
namespace equality = memgraph::query::relations::equality;
namespace equivalence = memgraph::query::relations::equivalence;
namespace orderability = memgraph::query::relations::orderability;

using Type = TypedValue::Type;

/// Every enumerator, listed so that adding one to the value fails here too.
constexpr Type kEveryType[] = {
    Type::Null,          Type::Bool,          Type::Int,      Type::Double,      Type::String,       Type::List,
    Type::Map,           Type::Vertex,        Type::Edge,     Type::Path,        Type::Date,         Type::LocalTime,
    Type::LocalDateTime, Type::ZonedDateTime, Type::Duration, Type::Graph,       Type::VirtualGraph, Type::Function,
    Type::Enum,          Type::Point2d,       Type::Point3d,  Type::VirtualEdge, Type::VirtualNode};

/// Two values of one type, the first ordering before the second.
struct OrderedPair {
  TypedValue lesser;
  TypedValue greater;
};

memgraph::utils::ZonedDateTime ZonedAt(int minute) {
  return memgraph::utils::ZonedDateTime{memgraph::utils::ZonedDateTimeParameters{
      {2024, 3, 20}, {10, minute, 0, 0, 0}, memgraph::utils::Timezone{std::chrono::minutes{60}}}};
}

/// A pair of the given type, where one can be built without a graph to hold it.
///
/// Nothing is returned for a type carrying no order of its own, and for the graph types, which
/// need an accessor. The switch names every case so that a type added to the value has to be
/// placed here rather than silently going untested.
std::optional<OrderedPair> PairOf(Type type) {
  switch (type) {
    case Type::Bool:
      return OrderedPair{TypedValue(false), TypedValue(true)};
    case Type::Int:
      return OrderedPair{TypedValue(int64_t{1}), TypedValue(int64_t{2})};
    case Type::Double:
      return OrderedPair{TypedValue(1.5), TypedValue(2.5)};
    case Type::String:
      return OrderedPair{TypedValue("a"), TypedValue("b")};
    case Type::Date:
      return OrderedPair{TypedValue(memgraph::utils::Date({2024, 3, 19})),
                         TypedValue(memgraph::utils::Date({2024, 3, 20}))};
    case Type::LocalTime:
      return OrderedPair{TypedValue(memgraph::utils::LocalTime({10, 56, 2, 7, 100})),
                         TypedValue(memgraph::utils::LocalTime({10, 56, 2, 7, 200}))};
    case Type::LocalDateTime:
      return OrderedPair{TypedValue(memgraph::utils::LocalDateTime({2024, 3, 20}, {10, 56, 2, 7, 100})),
                         TypedValue(memgraph::utils::LocalDateTime({2024, 3, 20}, {10, 56, 2, 7, 200}))};
    case Type::ZonedDateTime:
      return OrderedPair{TypedValue(ZonedAt(10)), TypedValue(ZonedAt(20))};
    case Type::Duration:
      return OrderedPair{TypedValue(memgraph::utils::Duration(1)), TypedValue(memgraph::utils::Duration(2))};
    case Type::Enum:
      return OrderedPair{TypedValue(Enum{EnumTypeId{2}, EnumValueId{1}}),
                         TypedValue(Enum{EnumTypeId{2}, EnumValueId{2}})};
    case Type::Point2d:
      return OrderedPair{TypedValue(Point2d{Cartesian_2d, 1.0, 1.0}), TypedValue(Point2d{Cartesian_2d, 1.0, 2.0})};
    case Type::Point3d:
      return OrderedPair{TypedValue(Point3d{Cartesian_3d, 1.0, 1.0, 1.0}),
                         TypedValue(Point3d{Cartesian_3d, 1.0, 1.0, 2.0})};

    case Type::Null:
    case Type::List:
    case Type::Map:
    case Type::Vertex:
    case Type::Edge:
    case Type::Path:
    case Type::Graph:
    case Type::VirtualGraph:
    case Type::Function:
    case Type::VirtualEdge:
    case Type::VirtualNode:
      return std::nullopt;
  }
}

TypedValue ListOf(std::vector<TypedValue> elements) { return TypedValue(std::move(elements)); }

TypedValue MapOf(std::map<std::string, TypedValue> entries) { return TypedValue(std::move(entries)); }

TypedValue Int(int64_t value) { return TypedValue(value); }

/// One value of the given type, for every type a test with no graph can build.
///
/// Wider than `PairOf`, which needs two values in a known order and so has nothing for a type
/// carrying no order of its own. Placing such a type against a different one asks only where
/// the two types sit, which is a question that has an answer for all of these.
std::optional<TypedValue> AValueOfType(Type type) {
  switch (type) {
    case Type::Null:
      return TypedValue();
    case Type::List:
      return ListOf({Int(1)});
    case Type::Map:
      return MapOf({{"a", Int(1)}});

    case Type::Bool:
    case Type::Int:
    case Type::Double:
    case Type::String:
    case Type::Date:
    case Type::LocalTime:
    case Type::LocalDateTime:
    case Type::ZonedDateTime:
    case Type::Duration:
    case Type::Enum:
    case Type::Point2d:
    case Type::Point3d:
      return PairOf(type)->lesser;

    case Type::Vertex:
    case Type::Edge:
    case Type::Path:
    case Type::Graph:
    case Type::VirtualGraph:
    case Type::Function:
    case Type::VirtualEdge:
    case Type::VirtualNode:
      return std::nullopt;
  }
}

}  // namespace

// Comparability

TEST(Comparability, AdmitsExactlyTheTypesItCanPlace) {
  for (auto const type : kEveryType) {
    auto const pair = PairOf(type);
    if (!pair) continue;
    // A type it admits has to be a type it can answer for, and the reverse. Two switches state
    // this separately, so nothing but a test holds them together.
    EXPECT_EQ(comparability::ValidFor(type), comparability::ComparePayload(pair->lesser, pair->greater).has_value())
        << "type " << static_cast<unsigned>(type);
  }
}

TEST(Comparability, PlacesNoGraphElement) {
  EXPECT_FALSE(comparability::ValidFor(Type::Vertex));
  EXPECT_FALSE(comparability::ValidFor(Type::Edge));
  EXPECT_FALSE(comparability::ValidFor(Type::Path));
  EXPECT_FALSE(comparability::ValidFor(Type::Graph));
  EXPECT_FALSE(comparability::ValidFor(Type::Function));
  EXPECT_FALSE(comparability::ValidFor(Type::VirtualEdge));
  EXPECT_FALSE(comparability::ValidFor(Type::VirtualNode));
  EXPECT_FALSE(comparability::ValidFor(Type::VirtualGraph));
}

TEST(Comparability, PlacesNoContainerAndNoNull) {
  EXPECT_FALSE(comparability::ValidFor(Type::Null));
  EXPECT_FALSE(comparability::ValidFor(Type::List));
  EXPECT_FALSE(comparability::ValidFor(Type::Map));
}

TEST(Comparability, OrdersEveryTypeItAdmits) {
  for (auto const type : kEveryType) {
    if (!comparability::ValidFor(type)) continue;
    auto const pair = PairOf(type);
    ASSERT_TRUE(pair.has_value()) << "an admitted type needs a pair here: " << static_cast<unsigned>(type);
    auto const order = comparability::Compare(pair->lesser, pair->greater);
    ASSERT_TRUE(order.has_value()) << "type " << static_cast<unsigned>(type);
    EXPECT_TRUE(std::is_lt(*order)) << "type " << static_cast<unsigned>(type);
    EXPECT_TRUE(std::is_gt(*comparability::Compare(pair->greater, pair->lesser)));
    EXPECT_TRUE(std::is_eq(*comparability::Compare(pair->lesser, pair->lesser)));
  }
}

TEST(Comparability, AnswersNothingForAPairItCannotPlace) {
  EXPECT_FALSE(comparability::Compare(TypedValue(int64_t{1}), TypedValue("a")).has_value());
  EXPECT_FALSE(comparability::Compare(TypedValue(), TypedValue(int64_t{1})).has_value());
  EXPECT_FALSE(comparability::Compare(TypedValue(int64_t{1}), TypedValue()).has_value());
  EXPECT_FALSE(
      comparability::Compare(TypedValue(Point2d{Cartesian_2d, 1.0, 1.0}), TypedValue(Point2d{Cartesian_2d, 1.0, 2.0}))
          .has_value());
}

TEST(Comparability, PlacesOneIntegerAgainstOneDouble) {
  EXPECT_TRUE(std::is_lt(*comparability::Compare(TypedValue(int64_t{1}), TypedValue(1.5))));
  EXPECT_TRUE(std::is_gt(*comparability::Compare(TypedValue(2.5), TypedValue(int64_t{2}))));
  EXPECT_TRUE(std::is_eq(*comparability::Compare(TypedValue(int64_t{2}), TypedValue(2.0))));
}

TEST(Comparability, PlacesEveryValueOfATypeItAdmitsExceptANaN) {
  // A scan is fenced by a bound value rather than by a type, so the value-level question is the
  // one it has to ask. The two answers agree everywhere except on the one value of an admitted
  // type that has no order.
  for (auto const type : kEveryType) {
    auto const pair = PairOf(type);
    if (!pair) continue;
    EXPECT_EQ(comparability::ValidFor(pair->lesser), comparability::ValidFor(type))
        << "type " << static_cast<unsigned>(type);
  }

  auto const nan = TypedValue(std::nan(""));
  EXPECT_TRUE(comparability::ValidFor(nan.type()));
  EXPECT_FALSE(comparability::ValidFor(nan));
}

TEST(Comparability, PlacesNoValueOfATypeItRefuses) {
  EXPECT_FALSE(comparability::ValidFor(TypedValue()));
  EXPECT_FALSE(comparability::ValidFor(ListOf({Int(1)})));
  EXPECT_FALSE(comparability::ValidFor(MapOf({{"a", Int(1)}})));
}

TEST(Comparability, AnswersFalseForEveryComparisonAgainstANaN) {
  // Which is why a bound holding one keeps no row: a filter reading any of the four drops every
  // row, so a scan standing in for it has to hand back nothing rather than a band.
  auto const nan = TypedValue(std::nan(""));
  auto const number = TypedValue(1.0);
  EXPECT_FALSE((nan < number).ValueBool());
  EXPECT_FALSE((number < nan).ValueBool());
  EXPECT_FALSE((nan <= nan).ValueBool());
  EXPECT_FALSE((number > nan).ValueBool());
  EXPECT_FALSE((number >= nan).ValueBool());
}

TEST(Comparability, LeavesAPairHoldingANaNUnordered) {
  auto const nan = TypedValue(std::nan(""));
  auto const order = comparability::Compare(nan, TypedValue(1.0));
  ASSERT_TRUE(order.has_value());
  EXPECT_EQ(*order, std::partial_ordering::unordered);
  EXPECT_EQ(*comparability::Compare(nan, nan), std::partial_ordering::unordered);
}

// Orderability, and where it has to agree with comparability

TEST(Orderability, AdmitsExactlyTheTypesItPlacesAPairOf) {
  // Which types a sort can order is stated by `Admits`, and `Compare` states it again by refusing
  // the rest. Two switches, so nothing but this holds them together: a type added to one and not
  // the other answers a query wrongly rather than failing to compile.
  for (auto const type : kEveryType) {
    auto const pair = PairOf(type);
    if (!pair) continue;

    auto placed = true;
    try {
      orderability::Compare(pair->lesser, pair->greater);
    } catch (memgraph::query::QueryRuntimeException const &) {
      placed = false;
    }
    EXPECT_EQ(orderability::ValidFor(type), placed) << "type " << static_cast<unsigned>(type);
  }
}

TEST(Orderability, AdmitsNoTypeCarryingNoOrderOfItsOwn) {
  // The types a sort refuses, named rather than counted, so that a type added to the value has to
  // be placed on one side or the other rather than joining this set by default.
  constexpr Type kRefused[] = {Type::Map,
                               Type::Vertex,
                               Type::Edge,
                               Type::VirtualEdge,
                               Type::VirtualNode,
                               Type::Path,
                               Type::Graph,
                               Type::VirtualGraph,
                               Type::Function};
  for (auto const type : kRefused) {
    EXPECT_FALSE(orderability::ValidFor(type)) << "type " << static_cast<unsigned>(type);
  }

  auto refused = 0;
  for (auto const type : kEveryType) {
    if (!orderability::ValidFor(type)) ++refused;
  }
  EXPECT_EQ(refused, std::ssize(kRefused)) << "a type joined or left the refused set without being named here";
}

TEST(Orderability, AnswersForAValueExactlyWhereEveryTypeWithinIsAdmitted) {
  // The value-level question reads through a list, because a list is ordered by what it holds.
  EXPECT_TRUE(orderability::ValidFor(TypedValue(int64_t{1})));
  EXPECT_TRUE(orderability::ValidFor(ListOf({Int(1), Int(2)})));
  EXPECT_FALSE(orderability::ValidFor(MapOf({{"a", Int(1)}})));
  EXPECT_FALSE(orderability::ValidFor(ListOf({MapOf({{"a", Int(1)}})})));

  // And it names the type that has no order, so a refusal can say which.
  EXPECT_EQ(orderability::UnorderedTypeWithin(ListOf({Int(1), MapOf({{"a", Int(1)}})})), Type::Map);
  EXPECT_EQ(orderability::UnorderedTypeWithin(TypedValue(int64_t{1})), std::nullopt);
}

TEST(Orderability, AgreesWithComparabilityWhereverComparabilityAnswers) {
  for (auto const type : kEveryType) {
    if (!comparability::ValidFor(type)) continue;
    auto const pair = PairOf(type);
    ASSERT_TRUE(pair.has_value());

    // Each type's own order has to be one order, however it is reached. Two relations reading it
    // differently would sort a column one way and filter it another.
    for (auto const &[a, b] : {std::pair{&pair->lesser, &pair->greater},
                               std::pair{&pair->greater, &pair->lesser},
                               std::pair{&pair->lesser, &pair->lesser}}) {
      auto const placed = comparability::Compare(*a, *b);
      ASSERT_TRUE(placed.has_value()) << "type " << static_cast<unsigned>(type);
      EXPECT_EQ(*placed, orderability::Compare(*a, *b)) << "type " << static_cast<unsigned>(type);
    }
  }
}

TEST(Orderability, AgreesWithComparabilityOnOneIntegerAgainstOneDouble) {
  auto const one = TypedValue(int64_t{1});
  auto const one_and_a_half = TypedValue(1.5);
  EXPECT_EQ(*comparability::Compare(one, one_and_a_half), orderability::Compare(one, one_and_a_half));
  EXPECT_EQ(*comparability::Compare(one_and_a_half, one), orderability::Compare(one_and_a_half, one));
}

TEST(Orderability, SortsANullAfterEverything) {
  auto const null = TypedValue();
  EXPECT_TRUE(std::is_gt(orderability::Compare(null, TypedValue(int64_t{1}))));
  EXPECT_TRUE(std::is_lt(orderability::Compare(TypedValue(int64_t{1}), null)));
  EXPECT_TRUE(std::is_eq(orderability::Compare(null, null)));
}

TEST(Orderability, PlacesANaNWhereComparabilityStillWillNot) {
  // The two relations part company here on purpose. A sort needs a position for
  // every pair; `<` must answer that it has none, since a NaN is unordered
  // against everything IEEE hands it. Placing a NaN for the sort must not put an
  // order behind the comparison operators.
  auto const nan = TypedValue(std::nan(""));

  // A Double is a type comparability admits, so it answers rather than
  // declining, and what it answers is that it has no order for the pair.
  EXPECT_FALSE(comparability::ValidFor(nan));
  EXPECT_EQ(comparability::Compare(nan, TypedValue(1.0)), std::partial_ordering::unordered);
  EXPECT_EQ(comparability::Compare(nan, nan), std::partial_ordering::unordered);

  EXPECT_TRUE(std::is_eq(orderability::Compare(nan, nan)));
  EXPECT_TRUE(std::is_gt(orderability::Compare(nan, TypedValue(1.0))));
}

TEST(Orderability, PlacesTwoNaNsTogether) {
  // A sort needs an answer for every pair it is handed, and IEEE gives none for
  // a NaN. Two of them share one position, so a sort may treat them as the same
  // value without treating either as the same value as a number.
  auto const nan = TypedValue(std::nan(""));
  EXPECT_TRUE(std::is_eq(orderability::Compare(nan, nan)));
}

TEST(Orderability, PlacesANaNAfterEveryNumber) {
  // Including after an infinity, which is the only number a NaN could plausibly
  // be put before.
  auto const nan = TypedValue(std::nan(""));
  for (auto const number : {std::numeric_limits<double>::lowest(),
                            -1.0,
                            0.0,
                            1.0,
                            std::numeric_limits<double>::max(),
                            std::numeric_limits<double>::infinity()}) {
    EXPECT_TRUE(std::is_gt(orderability::Compare(nan, TypedValue(number)))) << "not after " << number;
    EXPECT_TRUE(std::is_lt(orderability::Compare(TypedValue(number), nan))) << "not after " << number;
  }
}

TEST(Orderability, PlacesANaNAfterAnIntegerToo) {
  // An integer against a double is the one pair of unlike types this relation
  // orders, and it reaches a different arm from the pair of doubles above.
  auto const nan = TypedValue(std::nan(""));
  EXPECT_TRUE(std::is_gt(orderability::Compare(nan, Int(1))));
  EXPECT_TRUE(std::is_lt(orderability::Compare(Int(1), nan)));
  EXPECT_TRUE(std::is_gt(orderability::Compare(nan, Int(std::numeric_limits<int64_t>::max()))));
}

TEST(Orderability, PlacesTwoPointsHoldingANaNTogether) {
  // Two values are equivalent exactly where they share a position under this
  // relation, so a pair equivalence holds alike and this one leaves unplaced
  // would put the two relations at odds, and a sort keyed by one would disagree
  // with a grouping keyed by the other.
  auto const nan = std::nan("");
  auto const flat = TypedValue(Point2d{Cartesian_2d, 1.0, nan});
  auto const respelled = TypedValue(Point2d{Cartesian_2d, 1.0, -nan});
  auto const solid = TypedValue(Point3d{Cartesian_3d, 1.0, 2.0, nan});

  ASSERT_TRUE(equivalence::Equivalent(flat, respelled));
  EXPECT_TRUE(std::is_eq(orderability::Compare(flat, respelled)));
  EXPECT_TRUE(std::is_eq(orderability::Compare(solid, solid)));
}

TEST(Orderability, PlacesAPointHoldingANaNAfterOneThatDoesNot) {
  auto const nan = std::nan("");
  auto const holding = TypedValue(Point2d{Cartesian_2d, 1.0, nan});
  auto const whole = TypedValue(Point2d{Cartesian_2d, 1.0, 2.0});

  EXPECT_TRUE(std::is_gt(orderability::Compare(holding, whole)));
  EXPECT_TRUE(std::is_lt(orderability::Compare(whole, holding)));
}

TEST(Orderability, SortsAColumnHoldingANaN) {
  // What the relation is for, and the reason a position for every pair is not a
  // nicety. A sort reads "no position" as "neither comes first", which makes a
  // NaN interchangeable with every number while no two numbers are with each
  // other. That is not a strict weak ordering, and a standard sort handed one
  // is free to do anything at all rather than merely order the column oddly.
  auto const nan = std::nan("");
  auto column = std::vector<TypedValue>{TypedValue(3.0),
                                        TypedValue(nan),
                                        TypedValue(1.0),
                                        TypedValue(-nan),
                                        TypedValue(2.0),
                                        TypedValue(std::numeric_limits<double>::infinity())};

  std::ranges::sort(column,
                    [](TypedValue const &a, TypedValue const &b) { return std::is_lt(orderability::Compare(a, b)); });

  auto const numbers_in_order =
      std::ranges::is_sorted(column | std::views::take(4), {}, [](auto const &value) { return value.ValueDouble(); });
  EXPECT_TRUE(numbers_in_order) << "the numbers did not come out in order";
  EXPECT_TRUE(std::isnan(column[4].ValueDouble())) << "a NaN did not land after every number";
  EXPECT_TRUE(std::isnan(column[5].ValueDouble())) << "a NaN did not land after every number";
}

TEST(Orderability, PlacesTheTypesComparabilityRefuses) {
  for (auto const type : {Type::Enum, Type::Point2d, Type::Point3d}) {
    auto const pair = PairOf(type);
    ASSERT_TRUE(pair.has_value());
    EXPECT_FALSE(comparability::ValidFor(type));
    EXPECT_TRUE(std::is_lt(orderability::Compare(pair->lesser, pair->greater)));
  }
}

// Where an integer stops fitting in a double

/// The largest integer every larger integer's double no longer tells apart. Above it the
/// doubles thin out, so distinct integers share one.
constexpr int64_t kWidestExactInteger = int64_t{1} << 53;

TEST(Orderability, HoldsTwoLargeIntegersApartAgainstTheDoubleBetweenThem) {
  auto const rounds_to = TypedValue(static_cast<double>(kWidestExactInteger));
  auto const lower = Int(kWidestExactInteger);
  auto const higher = Int(kWidestExactInteger + 1);

  EXPECT_TRUE(std::is_eq(orderability::Compare(lower, rounds_to)));
  EXPECT_TRUE(std::is_gt(orderability::Compare(higher, rounds_to)));
  EXPECT_TRUE(std::is_lt(orderability::Compare(rounds_to, higher)));
}

TEST(Orderability, KeepsSharingAPositionTransitiveOverLargeIntegers) {
  // The law reading the pair through a double breaks: two values sharing a position with a
  // third have to share one with each other, or a sort is handed a comparator that is not a
  // strict weak ordering and its result is decided by the algorithm rather than the order.
  auto const between = TypedValue(static_cast<double>(kWidestExactInteger));
  auto const lower = Int(kWidestExactInteger);
  auto const higher = Int(kWidestExactInteger + 1);

  auto const shares = [](TypedValue const &a, TypedValue const &b) { return std::is_eq(orderability::Compare(a, b)); };

  EXPECT_TRUE(shares(lower, between));
  EXPECT_FALSE(shares(higher, between)) << "two integers one apart share a position with one double";
  EXPECT_FALSE(shares(lower, higher));
}

TEST(Orderability, PlacesAnIntegerAgainstADoubleNoIntegerCanHold) {
  // The exact comparison cannot reach these through a conversion, since turning a double
  // outside the integer range into one is undefined rather than merely inexact.
  auto const widest = Int(std::numeric_limits<int64_t>::max());
  auto const narrowest = Int(std::numeric_limits<int64_t>::min());

  EXPECT_TRUE(std::is_lt(orderability::Compare(widest, TypedValue(1e300))));
  EXPECT_TRUE(std::is_gt(orderability::Compare(narrowest, TypedValue(-1e300))));
  EXPECT_TRUE(std::is_lt(orderability::Compare(widest, TypedValue(std::numeric_limits<double>::infinity()))));
  EXPECT_TRUE(std::is_gt(orderability::Compare(narrowest, TypedValue(-std::numeric_limits<double>::infinity()))));
}

TEST(Orderability, PlacesAnIntegerEitherSideOfTheFractionBesideIt) {
  EXPECT_TRUE(std::is_lt(orderability::Compare(Int(2), TypedValue(2.5))));
  EXPECT_TRUE(std::is_gt(orderability::Compare(Int(3), TypedValue(2.5))));
  EXPECT_TRUE(std::is_lt(orderability::Compare(Int(-3), TypedValue(-2.5))));
  EXPECT_TRUE(std::is_gt(orderability::Compare(Int(-2), TypedValue(-2.5))));
}

TEST(Comparability, HoldsALargeIntegerApartFromTheDoubleItWouldRoundTo) {
  auto const rounds_to = TypedValue(static_cast<double>(kWidestExactInteger));
  auto const placed = comparability::Compare(Int(kWidestExactInteger + 1), rounds_to);
  ASSERT_TRUE(placed.has_value());
  ASSERT_NE(*placed, std::partial_ordering::unordered);
  EXPECT_TRUE(std::is_gt(*placed));
}

TEST(Equality, HoldsALargeIntegerUnequalToTheDoubleItWouldRoundTo) {
  // Equality has to move with the order, or a value a sort holds apart is one a grouping
  // holds together, and the two answers are read off the same column.
  auto const rounds_to = TypedValue(static_cast<double>(kWidestExactInteger));

  EXPECT_FALSE(equality::Equal(Int(kWidestExactInteger + 1), rounds_to).ValueBool());
  EXPECT_FALSE(equality::Equal(rounds_to, Int(kWidestExactInteger + 1)).ValueBool());
  EXPECT_TRUE(equality::Equal(Int(kWidestExactInteger), rounds_to).ValueBool());
  EXPECT_TRUE(equality::Equal(Int(1), TypedValue(1.0)).ValueBool());
}

TEST(Orderability, OrdersUnlikeTypesInTheOrderTheSpecificationFixes) {
  // A map first, then a node, a relationship, a list, a path, a string, a boolean, a number,
  // and a null last. The three holding a piece of the graph are left out here, since building
  // one needs a database.
  std::vector<TypedValue> const ascending{
      MapOf({{"a", Int(1)}}), ListOf({Int(1)}), TypedValue("a"), TypedValue(true), Int(1), TypedValue()};

  for (auto lesser = 0U; lesser != ascending.size(); ++lesser) {
    for (auto greater = lesser + 1; greater != ascending.size(); ++greater) {
      EXPECT_TRUE(std::is_lt(orderability::Compare(ascending[lesser], ascending[greater])))
          << "the value at " << lesser << " did not come before the one at " << greater;
      EXPECT_TRUE(std::is_gt(orderability::Compare(ascending[greater], ascending[lesser])))
          << "the value at " << greater << " did not come after the one at " << lesser;
    }
  }
}

TEST(Orderability, PlacesNoTypeTheSpecificationDoesNotNameAboveANaN) {
  // The specification names a map, a node, a relationship, a list, a path, a string, a boolean,
  // a number and a null, and forbids seating any other type above a NaN. A NaN is the largest
  // number, so the whole gap between the numbers and the null is closed to the rest, and each of
  // them sits below the strings.
  constexpr Type kNamed[] = {Type::Map,
                             Type::Vertex,
                             Type::Edge,
                             Type::List,
                             Type::Path,
                             Type::String,
                             Type::Bool,
                             Type::Int,
                             Type::Double,
                             Type::Null};
  auto const not_a_number = TypedValue(std::numeric_limits<double>::quiet_NaN());

  for (auto const type : kEveryType) {
    if (std::ranges::contains(kNamed, type)) continue;
    auto const value = AValueOfType(type);
    if (!value) continue;
    EXPECT_TRUE(std::is_lt(orderability::Compare(*value, not_a_number)))
        << "a value of type " << static_cast<unsigned>(type) << " was placed above a NaN";
  }
}

TEST(Orderability, PlacesADateBelowAString) {
  // The pair the rule above is most easily broken on, and the one the reference implementation
  // answers this way.
  auto const date = AValueOfType(Type::Date);
  ASSERT_TRUE(date.has_value());
  EXPECT_TRUE(std::is_lt(orderability::Compare(*date, TypedValue("a"))));
  EXPECT_TRUE(std::is_gt(orderability::Compare(TypedValue("a"), *date)));
}

TEST(Orderability, PlacesAnIntegerAndADoubleAlikeAgainstAThirdType) {
  // The two numeric types share one position. Were they to sit apart, a column holding 1 and
  // 1.0 would put them on either side of every string, while a comparison holds them equal.
  auto const text = TypedValue("a");
  EXPECT_EQ(orderability::Compare(Int(1), text), orderability::Compare(TypedValue(1.0), text));
  EXPECT_EQ(orderability::Compare(text, Int(1)), orderability::Compare(text, TypedValue(1.0)));
}

TEST(Orderability, PlacesEveryPairOfUnlikeTypesItCanBuild) {
  // A sort has to be given somewhere to put every pair of rows, so a column holding two types
  // is placed rather than refused. Throwing fails this test as surely as answering unordered.
  for (auto const left : kEveryType) {
    auto const a = AValueOfType(left);
    if (!a) continue;
    for (auto const right : kEveryType) {
      if (left == right) continue;
      auto const b = AValueOfType(right);
      if (!b) continue;

      EXPECT_NE(orderability::Compare(*a, *b), std::partial_ordering::unordered)
          << "type " << static_cast<unsigned>(left) << " against type " << static_cast<unsigned>(right);
    }
  }
}

TEST(Orderability, RefusesTwoValuesOfATypeCarryingNoOrderOfItsOwn) {
  // Placing a pair of unlike types settles nothing about two maps. Where the types are the
  // same, where each sits no longer answers the question, and a map has no order of its own.
  auto const map = MapOf({{"a", Int(1)}});
  EXPECT_THROW(orderability::Compare(map, map), memgraph::query::QueryRuntimeException);
}

TEST(Orderability, SortsAColumnHoldingUnlikeTypes) {
  std::vector<TypedValue> column{
      TypedValue(), Int(2), TypedValue("a"), TypedValue(true), TypedValue(1.5), ListOf({Int(1)})};
  std::ranges::sort(column,
                    [](TypedValue const &a, TypedValue const &b) { return std::is_lt(orderability::Compare(a, b)); });

  std::vector<Type> const settled{Type::List, Type::String, Type::Bool, Type::Double, Type::Int, Type::Null};
  auto const types = column | std::views::transform([](auto const &value) { return value.type(); });
  EXPECT_TRUE(std::ranges::equal(types, settled)) << "the column did not come out in the order the types sit in";
}

TEST(Orderability, OrdersAListByItsElements) {
  auto const shorter = TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1})});
  auto const longer = TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1}), TypedValue(int64_t{2})});
  auto const greater = TypedValue(std::vector<TypedValue>{TypedValue(int64_t{2})});
  EXPECT_TRUE(std::is_lt(orderability::Compare(shorter, longer)));
  EXPECT_TRUE(std::is_lt(orderability::Compare(shorter, greater)));
  EXPECT_TRUE(std::is_eq(orderability::Compare(shorter, shorter)));
}

// The walks each relation delegates a container to, asked directly

TEST(Orderability, CompareOfListsPlacesAPrefixFirst) {
  auto const prefix = ListOf({Int(1)});
  auto const longer = ListOf({Int(1), Int(2)});
  auto const empty = ListOf({});

  EXPECT_TRUE(std::is_lt(orderability::CompareOfLists(prefix.ValueList(), longer.ValueList())));
  EXPECT_TRUE(std::is_gt(orderability::CompareOfLists(longer.ValueList(), prefix.ValueList())));
  EXPECT_TRUE(std::is_lt(orderability::CompareOfLists(empty.ValueList(), prefix.ValueList())));
  EXPECT_TRUE(std::is_eq(orderability::CompareOfLists(empty.ValueList(), empty.ValueList())));
}

TEST(Orderability, CompareOfListsSettlesOnTheFirstElementThatDiffers) {
  // A later element cannot overturn an earlier one, whichever way it would have gone.
  auto const first_lesser = ListOf({Int(1), Int(9)});
  auto const first_greater = ListOf({Int(2), Int(0)});
  EXPECT_TRUE(std::is_lt(orderability::CompareOfLists(first_lesser.ValueList(), first_greater.ValueList())));
}

TEST(Orderability, CompareOfListsReadsTheRelationAgainForEachElement) {
  // Whatever orderability does for a scalar it has to do inside a list, so the element order
  // is asked for rather than restated: a null sorts last, a nested list is walked, and one
  // integer is placed against one double.
  auto const with_null = ListOf({TypedValue()});
  auto const with_number = ListOf({Int(1)});
  EXPECT_TRUE(std::is_gt(orderability::CompareOfLists(with_null.ValueList(), with_number.ValueList())));

  auto const nested_lesser = ListOf({ListOf({Int(1)})});
  auto const nested_greater = ListOf({ListOf({Int(1), Int(0)})});
  EXPECT_TRUE(std::is_lt(orderability::CompareOfLists(nested_lesser.ValueList(), nested_greater.ValueList())));

  auto const one = ListOf({Int(1)});
  auto const one_and_a_half = ListOf({TypedValue(1.5)});
  EXPECT_TRUE(std::is_lt(orderability::CompareOfLists(one.ValueList(), one_and_a_half.ValueList())));
}

TEST(Orderability, CompareOfListsPlacesAnElementComparabilityCannot) {
  // A list is ordered by its elements, so an element with no position would
  // leave the list with none either, and a sort over a column of lists is no
  // better defined than a sort over a column of numbers holding a NaN.
  auto const nan = ListOf({TypedValue(std::nan(""))});
  auto const number = ListOf({TypedValue(1.0)});
  auto const another_nan = ListOf({TypedValue(-std::nan(""))});

  EXPECT_TRUE(std::is_gt(orderability::CompareOfLists(nan.ValueList(), number.ValueList())));
  EXPECT_TRUE(std::is_lt(orderability::CompareOfLists(number.ValueList(), nan.ValueList())));
  EXPECT_TRUE(std::is_eq(orderability::CompareOfLists(nan.ValueList(), another_nan.ValueList())));
}

TEST(Orderability, CompareOfListsRefusesAnElementPairItHasNoOrderFor) {
  // A walk answers with the relation, so it refuses exactly where the relation does: for two
  // elements of one type carrying no order of its own. Two elements of unlike types it places,
  // by where the two types sit, so a list is no harder to sort than what it holds.
  auto const holds_a_map = ListOf({MapOf({{"a", Int(1)}})});
  EXPECT_THROW(orderability::CompareOfLists(holds_a_map.ValueList(), holds_a_map.ValueList()),
               memgraph::query::QueryRuntimeException);

  auto const number = ListOf({Int(1)});
  auto const text = ListOf({TypedValue("a")});
  EXPECT_TRUE(std::is_gt(orderability::CompareOfLists(number.ValueList(), text.ValueList())));
}

TEST(Equivalence, EquivalentOfListsWalksElementByElement) {
  auto const one_two = ListOf({Int(1), Int(2)});
  auto const two_one = ListOf({Int(2), Int(1)});
  auto const shorter = ListOf({Int(1)});

  EXPECT_TRUE(equivalence::EquivalentOfLists(one_two.ValueList(), one_two.ValueList()));
  EXPECT_FALSE(equivalence::EquivalentOfLists(one_two.ValueList(), two_one.ValueList()));
  EXPECT_FALSE(equivalence::EquivalentOfLists(one_two.ValueList(), shorter.ValueList()));
}

TEST(Equivalence, EquivalentOfListsReadsTheRelationAgainForEachElement) {
  // The element answer is equivalence rather than equality, so a null element decides rather
  // than leaving the list undecided, and a nested container is reached the same way.
  auto const with_null = ListOf({TypedValue()});
  EXPECT_TRUE(equivalence::EquivalentOfLists(with_null.ValueList(), with_null.ValueList()));

  auto const with_number = ListOf({Int(1)});
  EXPECT_FALSE(equivalence::EquivalentOfLists(with_null.ValueList(), with_number.ValueList()));

  auto const nested = ListOf({ListOf({TypedValue(), Int(1)})});
  EXPECT_TRUE(equivalence::EquivalentOfLists(nested.ValueList(), nested.ValueList()));
}

TEST(Equivalence, EquivalentOfMapsMatchesByKeyRatherThanByPosition) {
  auto const one_then_two = MapOf({{"a", Int(1)}, {"b", Int(2)}});
  auto const same_pairs = MapOf({{"b", Int(2)}, {"a", Int(1)}});
  auto const swapped_values = MapOf({{"a", Int(2)}, {"b", Int(1)}});

  EXPECT_TRUE(equivalence::EquivalentOfMaps(one_then_two.ValueMap(), same_pairs.ValueMap()));
  EXPECT_FALSE(equivalence::EquivalentOfMaps(one_then_two.ValueMap(), swapped_values.ValueMap()));
}

TEST(Equivalence, EquivalentOfMapsRefusesAKeyTheOtherSideLacks) {
  auto const under_a = MapOf({{"a", Int(1)}});
  auto const under_b = MapOf({{"b", Int(1)}});
  auto const two_keys = MapOf({{"a", Int(1)}, {"b", Int(1)}});

  EXPECT_FALSE(equivalence::EquivalentOfMaps(under_a.ValueMap(), under_b.ValueMap()));
  EXPECT_FALSE(equivalence::EquivalentOfMaps(under_a.ValueMap(), two_keys.ValueMap()));
}

TEST(Equivalence, EquivalentOfMapsReadsTheRelationAgainForEachValue) {
  auto const holding_null = MapOf({{"a", TypedValue()}});
  EXPECT_TRUE(equivalence::EquivalentOfMaps(holding_null.ValueMap(), holding_null.ValueMap()));
  EXPECT_FALSE(equivalence::EquivalentOfMaps(holding_null.ValueMap(), MapOf({{"a", Int(1)}}).ValueMap()));

  auto const nested = MapOf({{"a", ListOf({TypedValue()})}});
  EXPECT_TRUE(equivalence::EquivalentOfMaps(nested.ValueMap(), nested.ValueMap()));
}

TEST(Equivalence, HashesEquivalentMapsAlike) {
  auto const holding_null = MapOf({{"a", TypedValue()}, {"b", Int(1)}});
  auto const same = MapOf({{"b", Int(1)}, {"a", TypedValue()}});
  ASSERT_TRUE(equivalence::Equivalent(holding_null, same));
  EXPECT_EQ(equivalence::Hash(holding_null), equivalence::Hash(same));
}

// Equality, the three-valued relation

TEST(Equality, AnswersNullWhereverANullSits) {
  EXPECT_TRUE(equality::Equal(TypedValue(), TypedValue(int64_t{1})).IsNull());
  EXPECT_TRUE(equality::Equal(TypedValue(int64_t{1}), TypedValue()).IsNull());
  EXPECT_TRUE(equality::Equal(TypedValue(), TypedValue()).IsNull());
}

TEST(Equality, AnswersFalseForUnlikeTypesThatAreNotBothNumbers) {
  EXPECT_FALSE(equality::Equal(TypedValue(int64_t{1}), TypedValue("a")).ValueBool());
  EXPECT_TRUE(equality::Equal(TypedValue(int64_t{1}), TypedValue(1.0)).ValueBool());
}

TEST(Equality, LeavesAContainerHoldingANullUndecided) {
  auto const holding_null = TypedValue(std::vector<TypedValue>{TypedValue()});
  EXPECT_TRUE(equality::Equal(holding_null, holding_null).IsNull());

  // A pair that differs still settles it, so a null beside a difference does not hide it.
  auto const differing = TypedValue(std::vector<TypedValue>{TypedValue(), TypedValue(int64_t{1})});
  auto const other = TypedValue(std::vector<TypedValue>{TypedValue(), TypedValue(int64_t{2})});
  EXPECT_FALSE(equality::Equal(differing, other).ValueBool());
}

TEST(Equality, HoldsANullSeesThroughAContainer) {
  EXPECT_TRUE(equality::HoldsANull(TypedValue()));
  EXPECT_TRUE(equality::HoldsANull(TypedValue(std::vector<TypedValue>{TypedValue()})));
  EXPECT_TRUE(
      equality::HoldsANull(TypedValue(std::vector<TypedValue>{TypedValue(std::vector<TypedValue>{TypedValue()})})));
  EXPECT_FALSE(equality::HoldsANull(TypedValue(int64_t{1})));
  EXPECT_FALSE(equality::HoldsANull(TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1})})));
}

TEST(Equality, EqualsItselfIsFalseForTheValuesEqualityCannotDecide) {
  auto const nan = TypedValue(std::numeric_limits<double>::quiet_NaN());

  // The two values equality does not hold equal to themselves, for its two
  // different reasons: a Null leaves the pair undecided, a NaN answers false.
  EXPECT_FALSE(equality::EqualsItself(TypedValue()));
  EXPECT_FALSE(equality::EqualsItself(nan));
  EXPECT_TRUE(equality::Equal(nan, nan).IsBool());
  EXPECT_FALSE(equality::Equal(nan, nan).ValueBool());

  // A container carrying either is undecidable for the same reason.
  EXPECT_FALSE(equality::EqualsItself(TypedValue(std::vector<TypedValue>{TypedValue()})));
  EXPECT_FALSE(equality::EqualsItself(TypedValue(std::vector<TypedValue>{TypedValue(1.0), nan})));
  EXPECT_FALSE(equality::EqualsItself(TypedValue(std::vector<TypedValue>{TypedValue(std::vector<TypedValue>{nan})})));
  EXPECT_FALSE(equality::EqualsItself(TypedValue(std::map<std::string, TypedValue>{{"a", nan}})));

  // Every other value is equal to itself, which is what lets a container keyed
  // by equivalence answer an equality at all.
  EXPECT_TRUE(equality::EqualsItself(TypedValue(int64_t{1})));
  EXPECT_TRUE(equality::EqualsItself(TypedValue(1.0)));
  EXPECT_TRUE(equality::EqualsItself(TypedValue(std::numeric_limits<double>::infinity())));
  EXPECT_TRUE(equality::EqualsItself(TypedValue("a")));
  EXPECT_TRUE(equality::EqualsItself(TypedValue(std::vector<TypedValue>{TypedValue(1.0), TypedValue(2.0)})));
}

// Equivalence, the two-valued one a hash container is keyed by

TEST(Equivalence, HoldsANullEquivalentToANull) {
  EXPECT_TRUE(equivalence::Equivalent(TypedValue(), TypedValue()));
  EXPECT_FALSE(equivalence::Equivalent(TypedValue(), TypedValue(int64_t{1})));
}

TEST(Equivalence, HoldsAContainerHoldingANullEquivalentToItself) {
  // Equality cannot decide this, and collapsing that to false would leave such a value not
  // equivalent to itself, so a hash container would never find the key again.
  auto const holding_null = TypedValue(std::vector<TypedValue>{TypedValue(), TypedValue(int64_t{1})});
  EXPECT_TRUE(equivalence::Equivalent(holding_null, holding_null));
  EXPECT_TRUE(equality::Equal(holding_null, holding_null).IsNull());
}

TEST(Equivalence, HoldsEveryValueEquivalentToItself) {
  // A hash container keyed by this relation finds a key again only if the key is equivalent to
  // itself, so the property is asked of every type, of a null, and of an empty container.
  for (auto const type : kEveryType) {
    auto const pair = PairOf(type);
    if (!pair) continue;
    EXPECT_TRUE(equivalence::Equivalent(pair->lesser, pair->lesser)) << "type " << static_cast<unsigned>(type);
  }

  for (auto const &value :
       {TypedValue(), ListOf({TypedValue(), Int(1)}), MapOf({{"a", TypedValue()}}), ListOf({}), MapOf({})}) {
    EXPECT_TRUE(equivalence::Equivalent(value, value));
  }
}

TEST(Equivalence, HashesTheTwoInfinitiesApart) {
  // An infinity is whole, so reading it as the integer it equals looks like the
  // right move, and there is no such integer: the conversion is undefined, and
  // on a machine that answers it at all both infinities arrive at the same
  // number. Two values equivalence holds apart would then share a bucket, and a
  // lookup for one would walk the other.
  auto const above = TypedValue(std::numeric_limits<double>::infinity());
  auto const below = TypedValue(-std::numeric_limits<double>::infinity());

  ASSERT_FALSE(equivalence::Equivalent(above, below));
  EXPECT_NE(equivalence::Hash(above), equivalence::Hash(below));
}

TEST(Equivalence, HoldsANaNEquivalentToItself) {
  // The one value the property above does not reach through a pair. Equality answers false for a
  // NaN against itself, and taking that answer would leave each NaN its own group under DISTINCT
  // and a key a hash container could never find again.
  auto const nan = TypedValue(std::nan(""));
  EXPECT_TRUE(equivalence::Equivalent(nan, nan));
  EXPECT_TRUE(equivalence::Equivalent(ListOf({nan}), ListOf({nan})));
  EXPECT_TRUE(equivalence::Equivalent(MapOf({{"a", nan}}), MapOf({{"a", nan}})));
  EXPECT_TRUE(equivalence::Equivalent(ListOf({ListOf({nan})}), ListOf({ListOf({nan})})));
}

TEST(Equivalence, HoldsAPointHoldingANaNEquivalentToItself) {
  // A point holds its coordinates as doubles and compares them together, so one
  // holding a NaN is no more equal to itself than the NaN is. Storage reads a
  // point when it asks whether a value equals itself, and the two layers spell
  // that question separately, so each has to answer it the same way.
  auto const nan = std::nan("");
  auto const flat = TypedValue(Point2d{Cartesian_2d, 1.0, nan});
  auto const solid = TypedValue(Point3d{Cartesian_3d, 1.0, 2.0, nan});

  EXPECT_FALSE(equality::EqualsItself(flat));
  EXPECT_FALSE(equality::EqualsItself(solid));

  EXPECT_TRUE(equivalence::Equivalent(flat, flat));
  EXPECT_TRUE(equivalence::Equivalent(solid, solid));
  EXPECT_TRUE(equivalence::Equivalent(ListOf({flat}), ListOf({flat})));
  EXPECT_EQ(equivalence::Hash(flat), equivalence::Hash(TypedValue(Point2d{Cartesian_2d, 1.0, -nan})));
}

TEST(Equivalence, HoldsAPointHoldingANaNEquivalentToNoOther) {
  auto const nan = std::nan("");
  auto const flat = TypedValue(Point2d{Cartesian_2d, 1.0, nan});

  EXPECT_FALSE(equivalence::Equivalent(flat, TypedValue(Point2d{Cartesian_2d, 1.0, 2.0})));
  EXPECT_FALSE(equivalence::Equivalent(flat, TypedValue(Point2d{Cartesian_2d, 2.0, nan})));
  EXPECT_FALSE(equivalence::Equivalent(flat, TypedValue(Point2d{WGS84_2d, 1.0, nan})));
  EXPECT_FALSE(equivalence::Equivalent(flat, TypedValue(Point3d{Cartesian_3d, 1.0, nan, 3.0})));
}

TEST(Equivalence, HoldsANaNEquivalentToNoOtherNumber) {
  auto const nan = TypedValue(std::nan(""));
  EXPECT_FALSE(equivalence::Equivalent(nan, TypedValue(1.0)));
  EXPECT_FALSE(equivalence::Equivalent(TypedValue(1.0), nan));
  EXPECT_FALSE(equivalence::Equivalent(nan, Int(1)));
  EXPECT_FALSE(equivalence::Equivalent(nan, TypedValue()));
  EXPECT_FALSE(equivalence::Equivalent(ListOf({nan}), ListOf({TypedValue(1.0)})));
  EXPECT_FALSE(equivalence::Equivalent(ListOf({TypedValue(1.0)}), ListOf({nan})));
}

TEST(Equivalence, HashesTwoNaNsAlikeWhateverBitsEachCarries) {
  // A NaN is written in more than one bit pattern, and a hash over the bits tells them apart.
  // Equivalence holds them alike, so the hash has to as well or the lookup never reaches the
  // comparison.
  auto const nan = TypedValue(std::nan(""));
  auto const negated = TypedValue(-std::nan(""));
  auto const computed = TypedValue(std::numeric_limits<double>::quiet_NaN());

  ASSERT_TRUE(equivalence::Equivalent(nan, negated));
  ASSERT_TRUE(equivalence::Equivalent(nan, computed));
  EXPECT_EQ(equivalence::Hash(nan), equivalence::Hash(negated));
  EXPECT_EQ(equivalence::Hash(nan), equivalence::Hash(computed));
  EXPECT_EQ(equivalence::Hash(ListOf({nan})), equivalence::Hash(ListOf({negated})));
}

TEST(Equivalence, LeavesEqualityAnsweringAsItDidOverANaN) {
  // Equivalence holding two NaNs alike is not equality doing so: `=` still answers false, which
  // is what a NaN being equal to nothing means.
  auto const nan = TypedValue(std::nan(""));
  auto const equality_result = equality::Equal(nan, nan);
  ASSERT_EQ(equality_result.type(), TypedValue::Type::Bool);
  EXPECT_FALSE(equality_result.ValueBool());
  EXPECT_FALSE(equality::EqualsItself(nan));
}

TEST(Equivalence, HashesADoubleByWhatItHoldsPastThePoint) {
  // A whole double hashes as the integer it equals, so that a column holding
  // both finds one key. What the double carries past the point has to reach the
  // hash as well: sending every double between two integers to the lower one
  // puts a whole run of distinct keys in one bucket, and the probe that follows
  // walks the run.
  EXPECT_EQ(equivalence::Hash(TypedValue(2.0)), equivalence::Hash(TypedValue(int64_t{2})));

  EXPECT_NE(equivalence::Hash(TypedValue(2.5)), equivalence::Hash(TypedValue(2.0)));
  EXPECT_NE(equivalence::Hash(TypedValue(2.5)), equivalence::Hash(TypedValue(2.25)));
}

TEST(Equivalence, HashesADoubleNoIntegerCanHold) {
  // Reading a double as an integer is only defined while it is in the integer's
  // range. A double outside it, an infinity among them, has to be hashed as the
  // double it is.
  auto const vast = TypedValue(1e300);
  auto const other = TypedValue(-1e300);
  auto const endless = TypedValue(std::numeric_limits<double>::infinity());

  // Asking at all is the test: reading either as an integer is undefined, and a
  // sanitizer stops here.
  EXPECT_EQ(equivalence::Hash(vast), equivalence::Hash(TypedValue(1e300)));
  EXPECT_EQ(equivalence::Hash(endless), equivalence::Hash(TypedValue(std::numeric_limits<double>::infinity())));
  EXPECT_NE(equivalence::Hash(vast), equivalence::Hash(other));
  EXPECT_NE(equivalence::Hash(endless), equivalence::Hash(TypedValue(-std::numeric_limits<double>::infinity())));
}

TEST(Equivalence, HashesEquivalentValuesAlike) {
  auto const holding_null = TypedValue(std::vector<TypedValue>{TypedValue(), TypedValue(int64_t{1})});
  auto const same = TypedValue(std::vector<TypedValue>{TypedValue(), TypedValue(int64_t{1})});
  ASSERT_TRUE(equivalence::Equivalent(holding_null, same));
  EXPECT_EQ(equivalence::Hash(holding_null), equivalence::Hash(same));

  EXPECT_EQ(equivalence::Hash(TypedValue()), equivalence::Hash(TypedValue()));
}
