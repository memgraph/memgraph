// Copyright 2024 Memgraph Ltd.
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
#include "utils/logging.hpp"
#include "utils/temporal.hpp"

namespace {
void ExpectPropEq(const memgraph::storage::PropertyValue &a, const memgraph::storage::PropertyValue &b) {
  ASSERT_EQ(a.type(), b.type());
  ASSERT_EQ(a, b);
}

void CheckJsonConversion(const memgraph::storage::PropertyValue &property_value) {
  const auto json_string = memgraph::query::serialization::SerializePropertyValue(property_value, nullptr).dump();
  const auto json_object = nlohmann::json::parse(json_string);
  ExpectPropEq(property_value, memgraph::query::serialization::DeserializePropertyValue(json_object, nullptr));
}

}  // namespace

TEST(PropertyValueSerializationTest, Null) { CheckJsonConversion(memgraph::storage::PropertyValue{}); }

TEST(PropertyValueSerializationTest, Bool) {
  CheckJsonConversion(memgraph::storage::PropertyValue{true});
  CheckJsonConversion(memgraph::storage::PropertyValue{false});
}

TEST(PropertyValueSerializationTest, Int) {
  CheckJsonConversion(memgraph::storage::PropertyValue{1});
  CheckJsonConversion(memgraph::storage::PropertyValue{100});
}

TEST(PropertyValueSerializationTest, Double) {
  CheckJsonConversion(memgraph::storage::PropertyValue{1.0});
  CheckJsonConversion(memgraph::storage::PropertyValue{2.321});
}

TEST(PropertyValueSerializationTest, String) {
  CheckJsonConversion(memgraph::storage::PropertyValue{"TestString"});
  CheckJsonConversion(memgraph::storage::PropertyValue{""});
}

TEST(PropertyValueSerializationTest, TemporalData) {
  const auto test_temporal_data_conversion = [](const auto type, const auto microseconds) {
    CheckJsonConversion(memgraph::storage::PropertyValue{memgraph::storage::TemporalData{type, microseconds}});
  };

  test_temporal_data_conversion(memgraph::storage::TemporalType::Date, 20);
  test_temporal_data_conversion(memgraph::storage::TemporalType::LocalDateTime, -20);
  test_temporal_data_conversion(memgraph::storage::TemporalType::Duration, 10000);
}

TEST(PropertyValueSerializationTest, ZonedTemporalData) {
  const auto test_zoned_temporal_data_conversion = [](const auto type, const auto microseconds,
                                                      const memgraph::utils::Timezone &timezone) {
    CheckJsonConversion(
        memgraph::storage::PropertyValue{memgraph::storage::ZonedTemporalData{type, microseconds, timezone}});
  };

  const auto sample_duration = memgraph::utils::AsSysTime(20);
  test_zoned_temporal_data_conversion(memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                                      memgraph::utils::Timezone("Europe/Zagreb"));
  test_zoned_temporal_data_conversion(memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                                      memgraph::utils::Timezone(std::chrono::minutes{60}));
}

namespace {

std::vector<memgraph::storage::PropertyValue> GetPropertyValueListWithBasicTypes() {
  return {memgraph::storage::PropertyValue{}, memgraph::storage::PropertyValue{true},
          memgraph::storage::PropertyValue{"string"}, memgraph::storage::PropertyValue{1},
          memgraph::storage::PropertyValue{1.0}};
}

memgraph::storage::PropertyValue::map_t GetPropertyValueMapWithBasicTypes() {
  using memgraph::storage::Point2d;
  using memgraph::storage::Point3d;
  using memgraph::storage::PropertyValue;
  using enum memgraph::storage::CoordinateReferenceSystem;
  return {
      {"null", PropertyValue{}},
      {"bool", PropertyValue{true}},
      {"int", PropertyValue{1}},
      {"double", PropertyValue{1.0}},
      {"string", PropertyValue{"string"}},
      {"point2d_1", PropertyValue{Point2d{WGS84_2d, 1.0, 2.0}}},
      {"point2d_2", PropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}}},
      {"point3d_1", PropertyValue{Point3d{WGS84_3d, 1.0, 2.0, 3.0}}},
      {"point3d_2", PropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}}},
  };
}

}  // namespace

TEST(PropertyValueSerializationTest, List) {
  memgraph::storage::PropertyValue list = memgraph::storage::PropertyValue{GetPropertyValueListWithBasicTypes()};

  {
    SCOPED_TRACE("Basic list");
    CheckJsonConversion(list);
  }

  {
    SCOPED_TRACE("Nested list");
    CheckJsonConversion(memgraph::storage::PropertyValue{std::vector<memgraph::storage::PropertyValue>{list, list}});
  }

  {
    SCOPED_TRACE("List with map");
    list.ValueList().emplace_back(GetPropertyValueMapWithBasicTypes());
    CheckJsonConversion(list);
  }
}

TEST(PropertyValueSerializationTest, Map) {
  auto map = GetPropertyValueMapWithBasicTypes();
  {
    SCOPED_TRACE("Basic map");
    CheckJsonConversion(memgraph::storage::PropertyValue{map});
  }

  {
    SCOPED_TRACE("Nested map");
    map.emplace("map", memgraph::storage::PropertyValue{map});
    CheckJsonConversion(memgraph::storage::PropertyValue{map});
  }

  {
    SCOPED_TRACE("Map with list");
    map.emplace("list", memgraph::storage::PropertyValue{GetPropertyValueListWithBasicTypes()});
    CheckJsonConversion(memgraph::storage::PropertyValue{map});
  }
}
