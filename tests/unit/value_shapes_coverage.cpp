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

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>

#include "tests/unit/value_shapes.hpp"

using memgraph::storage::PropertyValue;
using memgraph::storage::PropertyValueType;

namespace shapes = memgraph::test::shapes;

namespace {

/// Whether a NaN sits somewhere below the top of a value, which is where a
/// comparison reaches one by a route a top-level NaN does not exercise.
bool HoldsANaNBelowTheTop(PropertyValue const &value, bool at_top = true) {
  switch (value.type()) {
    case PropertyValueType::Double:
      return !at_top && std::isnan(value.ValueDouble());
    case PropertyValueType::List:
      return std::ranges::any_of(value.ValueList(),
                                 [](auto const &element) { return HoldsANaNBelowTheTop(element, false); });
    case PropertyValueType::Map:
      return std::ranges::any_of(value.ValueMap(),
                                 [](auto const &entry) { return HoldsANaNBelowTheTop(entry.second, false); });
    case PropertyValueType::DoubleList:
      return std::ranges::any_of(value.ValueDoubleList(), [](double element) { return std::isnan(element); });
    case PropertyValueType::NumericList:
      return std::ranges::any_of(value.ValueNumericList(), [](auto const &element) {
        auto const *as_double = std::get_if<double>(&element);
        return as_double != nullptr && std::isnan(*as_double);
      });
    case PropertyValueType::VectorIndexId:
      return std::ranges::any_of(value.ValueVectorIndexList(), [](float element) { return std::isnan(element); });
    case PropertyValueType::Point2d:
      return std::isnan(value.ValuePoint2d().x()) || std::isnan(value.ValuePoint2d().y());
    case PropertyValueType::Point3d:
      return std::isnan(value.ValuePoint3d().x()) || std::isnan(value.ValuePoint3d().y()) ||
             std::isnan(value.ValuePoint3d().z());
    default:
      return false;
  }
}

}  // namespace

TEST(ValueShapes, EveryTypeIsListedOnce) {
  // `ShapesOfType` switches without a default, so a type added to the enum
  // stops the build there. Nothing does the same for the list the callers walk,
  // and the enum runs from zero without a gap, so its last value counts it.
  ASSERT_FALSE(shapes::kEveryType.empty());
  for (auto index = std::size_t{0}; index < shapes::kEveryType.size(); ++index) {
    EXPECT_EQ(static_cast<std::uint8_t>(shapes::kEveryType[index]), index)
        << "the list is out of step with the enum at position " << index;
  }
}

TEST(ValueShapes, EveryTypeHasShapesAndEachHoldsItsOwnType) {
  for (auto const type : shapes::kEveryType) {
    auto const group = shapes::ShapesOfType(type);
    EXPECT_FALSE(group.empty()) << "type " << static_cast<unsigned>(type) << " has no shapes";
    for (auto const &value : group) {
      EXPECT_EQ(value.type(), type) << "a shape of type " << static_cast<unsigned>(type) << " holds "
                                    << static_cast<unsigned>(value.type());
    }
  }
}

TEST(ValueShapes, EveryShapeReachesEveryType) {
  auto const every = shapes::EveryShape();
  for (auto const type : shapes::kEveryType) {
    EXPECT_TRUE(std::ranges::any_of(every, [type](auto const &value) { return value.type() == type; }))
        << "no shape of type " << static_cast<unsigned>(type);
  }
}

TEST(ValueShapes, ExcludingATypeLeavesEveryOther) {
  auto const kept = shapes::EveryShapeExcept({PropertyValueType::Null, PropertyValueType::Map});
  EXPECT_TRUE(std::ranges::none_of(kept, [](auto const &value) {
    return value.type() == PropertyValueType::Null || value.type() == PropertyValueType::Map;
  }));
  for (auto const type : shapes::kEveryType) {
    if (type == PropertyValueType::Null || type == PropertyValueType::Map) continue;
    EXPECT_TRUE(std::ranges::any_of(kept, [type](auto const &value) { return value.type() == type; }))
        << "excluding two types dropped type " << static_cast<unsigned>(type);
  }
}

TEST(ValueShapes, TheDoublesReachTheValuesMagnitudeCannotOrder) {
  auto const doubles = shapes::ShapesOfType(PropertyValueType::Double);
  auto const holds = [&](auto predicate) {
    return std::ranges::any_of(doubles, [&](auto const &value) { return predicate(value.ValueDouble()); });
  };

  EXPECT_TRUE(holds([](double d) { return std::isnan(d); })) << "no NaN";
  EXPECT_TRUE(holds([](double d) { return std::isinf(d) && d > 0; })) << "no positive infinity";
  EXPECT_TRUE(holds([](double d) { return std::isinf(d) && d < 0; })) << "no negative infinity";
  EXPECT_TRUE(holds([](double d) { return d == 0.0 && std::signbit(d); })) << "no negative zero";
  EXPECT_TRUE(holds([](double d) { return d == 0.0 && !std::signbit(d); })) << "no positive zero";
}

TEST(ValueShapes, TheIntegersReachWhereAnIntAndADoubleStopAgreeing) {
  // Above two to the fifty-third an integer has no double that equals it, so a
  // comparison between the two answers on a rounded value unless it is written
  // not to.
  auto const integers = shapes::ShapesOfType(PropertyValueType::Int);

  EXPECT_TRUE(std::ranges::any_of(integers, [](auto const &value) {
    auto const held = value.ValueInt();
    return static_cast<std::int64_t>(static_cast<double>(held)) != held;
  })) << "every integer survives a trip through a double";
}

TEST(ValueShapes, ANaNIsReachableBelowTheTopOfAValue) {
  // A comparison reaches a nested NaN through its container's own code, which
  // is a route a top-level NaN leaves untested.
  auto const every = shapes::EveryShape();
  auto const reached = [&](PropertyValueType type) {
    return std::ranges::any_of(every,
                               [&](auto const &value) { return value.type() == type && HoldsANaNBelowTheTop(value); });
  };

  EXPECT_TRUE(reached(PropertyValueType::List)) << "no list holds a NaN";
  EXPECT_TRUE(reached(PropertyValueType::Map)) << "no map holds a NaN";
  EXPECT_TRUE(reached(PropertyValueType::DoubleList)) << "no double list holds a NaN";
  EXPECT_TRUE(reached(PropertyValueType::NumericList)) << "no numeric list holds a NaN";
  EXPECT_TRUE(reached(PropertyValueType::VectorIndexId)) << "no vector holds a NaN coordinate";
  EXPECT_TRUE(reached(PropertyValueType::Point2d)) << "no two-dimensional point holds a NaN coordinate";
  EXPECT_TRUE(reached(PropertyValueType::Point3d)) << "no three-dimensional point holds a NaN coordinate";
}

TEST(ValueShapes, ANaNIsReachableThroughTwoLevelsOfNesting) {
  auto const lists = shapes::ShapesOfType(PropertyValueType::List);
  EXPECT_TRUE(std::ranges::any_of(lists, [](auto const &value) {
    return std::ranges::any_of(value.ValueList(),
                               [](auto const &element) { return HoldsANaNBelowTheTop(element, false); });
  })) << "no list holds a container that holds a NaN";
}

TEST(ValueShapes, TheTemporalsReachEveryKind) {
  // The four kinds share one stored type, so a range over one of them is fenced
  // by where that kind sits within the type rather than by the type itself.
  auto const temporals = shapes::ShapesOfType(PropertyValueType::TemporalData);
  for (auto const kind : {memgraph::storage::TemporalType::Date,
                          memgraph::storage::TemporalType::LocalTime,
                          memgraph::storage::TemporalType::LocalDateTime,
                          memgraph::storage::TemporalType::Duration}) {
    EXPECT_TRUE(
        std::ranges::any_of(temporals, [kind](auto const &value) { return value.ValueTemporalData().type == kind; }))
        << "no temporal of kind " << static_cast<unsigned>(kind);
  }
}

TEST(ValueShapes, ThePointsReachEveryCoordinateReferenceSystem) {
  using memgraph::storage::CoordinateReferenceSystem;

  auto const two_d = shapes::ShapesOfType(PropertyValueType::Point2d);
  for (auto const crs : {CoordinateReferenceSystem::WGS84_2d, CoordinateReferenceSystem::Cartesian_2d}) {
    EXPECT_TRUE(std::ranges::any_of(two_d, [crs](auto const &value) { return value.ValuePoint2d().crs() == crs; }))
        << "no two-dimensional point in system " << static_cast<unsigned>(crs);
  }

  auto const three_d = shapes::ShapesOfType(PropertyValueType::Point3d);
  for (auto const crs : {CoordinateReferenceSystem::WGS84_3d, CoordinateReferenceSystem::Cartesian_3d}) {
    EXPECT_TRUE(std::ranges::any_of(three_d, [crs](auto const &value) { return value.ValuePoint3d().crs() == crs; }))
        << "no three-dimensional point in system " << static_cast<unsigned>(crs);
  }
}

TEST(ValueShapes, TheStringsReachTheLengthsAStoreLaysOutDifferently) {
  auto const strings = shapes::ShapesOfType(PropertyValueType::String);
  auto const longest = std::ranges::max(strings, {}, [](auto const &value) { return value.ValueString().size(); });

  EXPECT_TRUE(std::ranges::any_of(strings, [](auto const &value) { return value.ValueString().empty(); }))
      << "no empty string";
  EXPECT_GT(longest.ValueString().size(), 255U) << "no string long enough to need more than one byte of length";
}

TEST(ValueShapes, TheContainersReachEmpty) {
  auto const empty_of = [](PropertyValueType type, auto size_of) {
    auto const group = shapes::ShapesOfType(type);
    return std::ranges::any_of(group, [&](auto const &value) { return size_of(value) == 0; });
  };

  EXPECT_TRUE(empty_of(PropertyValueType::List, [](auto const &v) { return v.ValueList().size(); })) << "no empty list";
  EXPECT_TRUE(empty_of(PropertyValueType::Map, [](auto const &v) { return v.ValueMap().size(); })) << "no empty map";
}
