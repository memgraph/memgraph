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

#include <algorithm>
#include <cmath>
#include <memory>

#include "query/db_accessor.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/unit/typed_value_shapes.hpp"

using memgraph::query::TypedValue;

namespace shapes = memgraph::test::shapes;

namespace {

bool IsUnshaped(TypedValue::Type type) {
  return std::ranges::find(shapes::kUnshapedTypedValueTypes, type) != shapes::kUnshapedTypedValueTypes.end();
}

class TypedValueShapes : public ::testing::Test {
 protected:
  std::unique_ptr<memgraph::storage::Storage> db_ =
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{});
  std::unique_ptr<memgraph::storage::Storage::Accessor> accessor_ = db_->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba_{accessor_.get()};
};

}  // namespace

TEST_F(TypedValueShapes, EveryTypeIsListedOnce) {
  // `ShapesOfType` switches without a default, so a type added to the enum
  // stops the build there. Nothing does the same for the list the callers walk,
  // and the enum runs from zero without a gap, so its last value counts it.
  ASSERT_FALSE(shapes::kEveryTypedValueType.empty());
  for (auto index = std::size_t{0}; index < shapes::kEveryTypedValueType.size(); ++index) {
    EXPECT_EQ(static_cast<unsigned>(shapes::kEveryTypedValueType[index]), index)
        << "the list is out of step with the enum at position " << index;
  }
}

TEST_F(TypedValueShapes, EveryShapedTypeHasShapesAndEachHoldsItsOwnType) {
  for (auto const type : shapes::kEveryTypedValueType) {
    auto const group = shapes::ShapesOfType(type, &dba_);
    if (IsUnshaped(type)) {
      EXPECT_TRUE(group.empty()) << "type " << static_cast<unsigned>(type) << " is named unshaped but has shapes";
      continue;
    }
    EXPECT_FALSE(group.empty()) << "type " << static_cast<unsigned>(type) << " has no shapes";
    for (auto const &value : group) {
      EXPECT_EQ(value.type(), type) << "a shape of type " << static_cast<unsigned>(type) << " holds "
                                    << static_cast<unsigned>(value.type());
    }
  }
}

TEST_F(TypedValueShapes, TheTypesThatNeedTheGraphAreTheOnlyOnesAnAccessorAdds) {
  for (auto const type : shapes::kEveryTypedValueType) {
    if (IsUnshaped(type)) continue;
    auto const without = shapes::ShapesOfType(type, nullptr);
    auto const with = shapes::ShapesOfType(type, &dba_);

    auto const needs_the_graph =
        std::ranges::find(shapes::kGraphTypedValueTypes, type) != shapes::kGraphTypedValueTypes.end();
    if (needs_the_graph) {
      EXPECT_TRUE(without.empty()) << "type " << static_cast<unsigned>(type) << " made a value with no accessor";
      EXPECT_FALSE(with.empty());
    } else {
      EXPECT_EQ(without.size(), with.size())
          << "type " << static_cast<unsigned>(type) << " reads the accessor it does not need";
    }
  }
}

TEST_F(TypedValueShapes, TheScalarsDrawTheSameValuesTheStoredShapesDo) {
  // Writing the awkward doubles twice lets one copy fall behind the other, so
  // the query side unwraps the stored shapes rather than restating them.
  auto const doubles = shapes::ShapesOfType(TypedValue::Type::Double, &dba_);
  auto const holds = [&](auto predicate) {
    return std::ranges::any_of(doubles, [&](auto const &value) { return predicate(value.ValueDouble()); });
  };

  EXPECT_TRUE(holds([](double d) { return std::isnan(d); })) << "no NaN";
  EXPECT_TRUE(holds([](double d) { return std::isinf(d); })) << "no infinity";
  EXPECT_TRUE(holds([](double d) { return d == 0.0 && std::signbit(d); })) << "no negative zero";
}

TEST_F(TypedValueShapes, ANaNIsReachableBelowTheTopOfAValue) {
  auto const holds_a_nested_nan = [](TypedValue const &value) {
    if (value.type() == TypedValue::Type::List) {
      return std::ranges::any_of(value.ValueList(), [](auto const &element) {
        return element.type() == TypedValue::Type::Double && std::isnan(element.ValueDouble());
      });
    }
    if (value.type() == TypedValue::Type::Map) {
      return std::ranges::any_of(value.ValueMap(), [](auto const &entry) {
        return entry.second.type() == TypedValue::Type::Double && std::isnan(entry.second.ValueDouble());
      });
    }
    return false;
  };

  auto const every = shapes::EveryTypedValueShape(&dba_);
  EXPECT_TRUE(std::ranges::any_of(every, [&](auto const &value) {
    return value.type() == TypedValue::Type::List && holds_a_nested_nan(value);
  })) << "no list holds a NaN";
  EXPECT_TRUE(std::ranges::any_of(every, [&](auto const &value) {
    return value.type() == TypedValue::Type::Map && holds_a_nested_nan(value);
  })) << "no map holds a NaN";
}

TEST_F(TypedValueShapes, EveryShapeReachesEveryShapedType) {
  auto const every = shapes::EveryTypedValueShape(&dba_);
  for (auto const type : shapes::kEveryTypedValueType) {
    if (IsUnshaped(type)) continue;
    EXPECT_TRUE(std::ranges::any_of(every, [type](auto const &value) { return value.type() == type; }))
        << "no shape of type " << static_cast<unsigned>(type);
  }
}
