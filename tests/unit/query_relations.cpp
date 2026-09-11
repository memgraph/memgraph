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

#include <chrono>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

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

}  // namespace

// Comparability

TEST(Comparability, AdmitsExactlyTheTypesItCanPlace) {
  for (auto const type : kEveryType) {
    auto const pair = PairOf(type);
    if (!pair) continue;
    // A type it admits has to be a type it can answer for, and the reverse. Two switches state
    // this separately, so nothing but a test holds them together.
    EXPECT_EQ(comparability::Admits(type), comparability::ComparePayload(pair->lesser, pair->greater).has_value())
        << "type " << static_cast<unsigned>(type);
  }
}

TEST(Comparability, PlacesNoGraphElement) {
  EXPECT_FALSE(comparability::Admits(Type::Vertex));
  EXPECT_FALSE(comparability::Admits(Type::Edge));
  EXPECT_FALSE(comparability::Admits(Type::Path));
  EXPECT_FALSE(comparability::Admits(Type::Graph));
  EXPECT_FALSE(comparability::Admits(Type::Function));
  EXPECT_FALSE(comparability::Admits(Type::VirtualEdge));
  EXPECT_FALSE(comparability::Admits(Type::VirtualNode));
  EXPECT_FALSE(comparability::Admits(Type::VirtualGraph));
}

TEST(Comparability, PlacesNoContainerAndNoNull) {
  EXPECT_FALSE(comparability::Admits(Type::Null));
  EXPECT_FALSE(comparability::Admits(Type::List));
  EXPECT_FALSE(comparability::Admits(Type::Map));
}

TEST(Comparability, OrdersEveryTypeItAdmits) {
  for (auto const type : kEveryType) {
    if (!comparability::Admits(type)) continue;
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

TEST(Comparability, LeavesAPairHoldingANaNUnordered) {
  auto const nan = TypedValue(std::nan(""));
  auto const order = comparability::Compare(nan, TypedValue(1.0));
  ASSERT_TRUE(order.has_value());
  EXPECT_EQ(*order, std::partial_ordering::unordered);
  EXPECT_EQ(*comparability::Compare(nan, nan), std::partial_ordering::unordered);
}

// Orderability, and where it has to agree with comparability

TEST(Orderability, AgreesWithComparabilityWhereverComparabilityAnswers) {
  for (auto const type : kEveryType) {
    if (!comparability::Admits(type)) continue;
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

TEST(Orderability, PlacesTheTypesComparabilityRefuses) {
  for (auto const type : {Type::Enum, Type::Point2d, Type::Point3d}) {
    auto const pair = PairOf(type);
    ASSERT_TRUE(pair.has_value());
    EXPECT_FALSE(comparability::Admits(type));
    EXPECT_TRUE(std::is_lt(orderability::Compare(pair->lesser, pair->greater)));
  }
}

TEST(Orderability, OrdersAListByItsElements) {
  auto const shorter = TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1})});
  auto const longer = TypedValue(std::vector<TypedValue>{TypedValue(int64_t{1}), TypedValue(int64_t{2})});
  auto const greater = TypedValue(std::vector<TypedValue>{TypedValue(int64_t{2})});
  EXPECT_TRUE(std::is_lt(orderability::Compare(shorter, longer)));
  EXPECT_TRUE(std::is_lt(orderability::Compare(shorter, greater)));
  EXPECT_TRUE(std::is_eq(orderability::Compare(shorter, shorter)));
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

TEST(Equivalence, HashesEquivalentValuesAlike) {
  auto const holding_null = TypedValue(std::vector<TypedValue>{TypedValue(), TypedValue(int64_t{1})});
  auto const same = TypedValue(std::vector<TypedValue>{TypedValue(), TypedValue(int64_t{1})});
  ASSERT_TRUE(equivalence::Equivalent(holding_null, same));
  EXPECT_EQ(equivalence::Hash(holding_null), equivalence::Hash(same));

  EXPECT_EQ(equivalence::Hash(TypedValue()), equivalence::Hash(TypedValue()));
}
