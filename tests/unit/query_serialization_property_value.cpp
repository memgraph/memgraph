// Copyright 2025 Memgraph Ltd.
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

#include "query/serialization/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/temporal.hpp"

namespace {
void ExpectPropEq(const memgraph::storage::IntermediatePropertyValue &a,
                  const memgraph::storage::IntermediatePropertyValue &b) {
  ASSERT_EQ(a.type(), b.type());
  ASSERT_EQ(a, b);
}

void CheckJsonConversion(const memgraph::storage::IntermediatePropertyValue &property_value) {
  const auto json_string =
      memgraph::query::serialization::SerializeIntermediatePropertyValue(property_value, nullptr).dump();
  const auto json_object = nlohmann::json::parse(json_string);
  ExpectPropEq(property_value,
               memgraph::query::serialization::DeserializeIntermediatePropertyValue(json_object, nullptr));
}

}  // namespace

TEST(IntermediatePropertyValueSerializationTest, Null) {
  CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{});
}

TEST(IntermediatePropertyValueSerializationTest, Bool) {
  CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{true});
  CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{false});
}

TEST(IntermediatePropertyValueSerializationTest, Int) {
  CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{1});
  CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{100});
}

TEST(IntermediatePropertyValueSerializationTest, Double) {
  CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{1.0});
  CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{2.321});
}

TEST(IntermediatePropertyValueSerializationTest, String) {
  CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{"TestString"});
  CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{""});
}

TEST(IntermediatePropertyValueSerializationTest, TemporalData) {
  const auto test_temporal_data_conversion = [](const auto type, const auto microseconds) {
    CheckJsonConversion(
        memgraph::storage::IntermediatePropertyValue{memgraph::storage::TemporalData{type, microseconds}});
  };

  test_temporal_data_conversion(memgraph::storage::TemporalType::Date, 20);
  test_temporal_data_conversion(memgraph::storage::TemporalType::LocalDateTime, -20);
  test_temporal_data_conversion(memgraph::storage::TemporalType::Duration, 10000);
}

TEST(IntermediatePropertyValueSerializationTest, ZonedTemporalData) {
  const auto test_zoned_temporal_data_conversion = [](const auto type, const auto microseconds,
                                                      const memgraph::utils::Timezone &timezone) {
    CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{
        memgraph::storage::ZonedTemporalData{type, microseconds, timezone}});
  };

  const auto sample_duration = memgraph::utils::AsSysTime(20);
  test_zoned_temporal_data_conversion(memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                                      memgraph::utils::Timezone("Europe/Zagreb"));
  test_zoned_temporal_data_conversion(memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                                      memgraph::utils::Timezone(std::chrono::minutes{60}));
}

namespace {

std::vector<memgraph::storage::IntermediatePropertyValue> GetIntermediatePropertyValueListWithBasicTypes() {
  return {memgraph::storage::IntermediatePropertyValue{}, memgraph::storage::IntermediatePropertyValue{true},
          memgraph::storage::IntermediatePropertyValue{"string"}, memgraph::storage::IntermediatePropertyValue{1},
          memgraph::storage::IntermediatePropertyValue{1.0}};
}

memgraph::storage::IntermediatePropertyValue::map_t GetIntermediatePropertyValueMapWithBasicTypes() {
  using memgraph::storage::IntermediatePropertyValue;
  using memgraph::storage::Point2d;
  using memgraph::storage::Point3d;
  using enum memgraph::storage::CoordinateReferenceSystem;
  return {
      {"null", IntermediatePropertyValue{}},
      {"bool", IntermediatePropertyValue{true}},
      {"int", IntermediatePropertyValue{1}},
      {"double", IntermediatePropertyValue{1.0}},
      {"string", IntermediatePropertyValue{"string"}},
      {"point2d_1", IntermediatePropertyValue{Point2d{WGS84_2d, 1.0, 2.0}}},
      {"point2d_2", IntermediatePropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}}},
      {"point3d_1", IntermediatePropertyValue{Point3d{WGS84_3d, 1.0, 2.0, 3.0}}},
      {"point3d_2", IntermediatePropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}}},
  };
}

}  // namespace

TEST(IntermediatePropertyValueSerializationTest, List) {
  memgraph::storage::IntermediatePropertyValue list =
      memgraph::storage::IntermediatePropertyValue{GetIntermediatePropertyValueListWithBasicTypes()};

  {
    SCOPED_TRACE("Basic list");
    CheckJsonConversion(list);
  }

  {
    SCOPED_TRACE("Nested list");
    CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{
        std::vector<memgraph::storage::IntermediatePropertyValue>{list, list}});
  }

  {
    SCOPED_TRACE("List with map");
    list.ValueList().emplace_back(GetIntermediatePropertyValueMapWithBasicTypes());
    CheckJsonConversion(list);
  }
}

TEST(IntermediatePropertyValueSerializationTest, Map) {
  auto map = GetIntermediatePropertyValueMapWithBasicTypes();
  {
    SCOPED_TRACE("Basic map");
    CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{map});
  }

  {
    SCOPED_TRACE("Nested map");
    map.emplace("map", memgraph::storage::IntermediatePropertyValue{map});
    CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{map});
  }

  {
    SCOPED_TRACE("Map with list");
    map.emplace("list", memgraph::storage::IntermediatePropertyValue{GetIntermediatePropertyValueListWithBasicTypes()});
    CheckJsonConversion(memgraph::storage::IntermediatePropertyValue{map});
  }
}
