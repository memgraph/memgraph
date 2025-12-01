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
void ExpectPropEq(const memgraph::storage::ExternalPropertyValue &a,
                  const memgraph::storage::ExternalPropertyValue &b) {
  ASSERT_EQ(a.type(), b.type());
  ASSERT_EQ(a, b);
}

void CheckJsonConversion(const memgraph::storage::ExternalPropertyValue &property_value) {
  const auto json_string =
      memgraph::query::serialization::SerializeExternalPropertyValue(property_value, nullptr).dump();
  const auto json_object = nlohmann::json::parse(json_string);
  ExpectPropEq(property_value, memgraph::query::serialization::DeserializeExternalPropertyValue(json_object, nullptr));
}

}  // namespace

TEST(ExternalPropertyValueSerializationTest, Null) { CheckJsonConversion(memgraph::storage::ExternalPropertyValue{}); }

TEST(ExternalPropertyValueSerializationTest, Bool) {
  CheckJsonConversion(memgraph::storage::ExternalPropertyValue{true});
  CheckJsonConversion(memgraph::storage::ExternalPropertyValue{false});
}

TEST(ExternalPropertyValueSerializationTest, Int) {
  CheckJsonConversion(memgraph::storage::ExternalPropertyValue{1});
  CheckJsonConversion(memgraph::storage::ExternalPropertyValue{100});
}

TEST(ExternalPropertyValueSerializationTest, Double) {
  CheckJsonConversion(memgraph::storage::ExternalPropertyValue{1.0});
  CheckJsonConversion(memgraph::storage::ExternalPropertyValue{2.321});
}

TEST(ExternalPropertyValueSerializationTest, String) {
  CheckJsonConversion(memgraph::storage::ExternalPropertyValue{"TestString"});
  CheckJsonConversion(memgraph::storage::ExternalPropertyValue{""});
}

TEST(ExternalPropertyValueSerializationTest, TemporalData) {
  const auto test_temporal_data_conversion = [](const auto type, const auto microseconds) {
    CheckJsonConversion(memgraph::storage::ExternalPropertyValue{memgraph::storage::TemporalData{type, microseconds}});
  };

  test_temporal_data_conversion(memgraph::storage::TemporalType::Date, 20);
  test_temporal_data_conversion(memgraph::storage::TemporalType::LocalDateTime, -20);
  test_temporal_data_conversion(memgraph::storage::TemporalType::Duration, 10000);
}

TEST(ExternalPropertyValueSerializationTest, ZonedTemporalData) {
  const auto test_zoned_temporal_data_conversion = [](const auto type, const auto microseconds,
                                                      const memgraph::utils::Timezone &timezone) {
    CheckJsonConversion(
        memgraph::storage::ExternalPropertyValue{memgraph::storage::ZonedTemporalData{type, microseconds, timezone}});
  };

  const auto sample_duration = memgraph::utils::AsSysTime(20);
  test_zoned_temporal_data_conversion(memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                                      memgraph::utils::Timezone("Europe/Zagreb"));
  test_zoned_temporal_data_conversion(memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                                      memgraph::utils::Timezone(std::chrono::minutes{60}));
}

namespace {

std::vector<memgraph::storage::ExternalPropertyValue> GetExternalPropertyValueListWithBasicTypes() {
  return {memgraph::storage::ExternalPropertyValue{}, memgraph::storage::ExternalPropertyValue{true},
          memgraph::storage::ExternalPropertyValue{"string"}, memgraph::storage::ExternalPropertyValue{1},
          memgraph::storage::ExternalPropertyValue{1.0}};
}

memgraph::storage::ExternalPropertyValue::map_t GetExternalPropertyValueMapWithBasicTypes() {
  using memgraph::storage::ExternalPropertyValue;
  using memgraph::storage::Point2d;
  using memgraph::storage::Point3d;
  using enum memgraph::storage::CoordinateReferenceSystem;
  return {
      {"null", ExternalPropertyValue{}},
      {"bool", ExternalPropertyValue{true}},
      {"int", ExternalPropertyValue{1}},
      {"double", ExternalPropertyValue{1.0}},
      {"string", ExternalPropertyValue{"string"}},
      {"point2d_1", ExternalPropertyValue{Point2d{WGS84_2d, 1.0, 2.0}}},
      {"point2d_2", ExternalPropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}}},
      {"point3d_1", ExternalPropertyValue{Point3d{WGS84_3d, 1.0, 2.0, 3.0}}},
      {"point3d_2", ExternalPropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}}},
  };
}

}  // namespace

TEST(ExternalPropertyValueSerializationTest, List) {
  memgraph::storage::ExternalPropertyValue list =
      memgraph::storage::ExternalPropertyValue{GetExternalPropertyValueListWithBasicTypes()};

  {
    SCOPED_TRACE("Basic list");
    CheckJsonConversion(list);
  }

  {
    SCOPED_TRACE("Nested list");
    CheckJsonConversion(
        memgraph::storage::ExternalPropertyValue{std::vector<memgraph::storage::ExternalPropertyValue>{list, list}});
  }

  {
    SCOPED_TRACE("List with map");
    list.ValueList().emplace_back(GetExternalPropertyValueMapWithBasicTypes());
    CheckJsonConversion(list);
  }
}

TEST(ExternalPropertyValueSerializationTest, Map) {
  auto map = GetExternalPropertyValueMapWithBasicTypes();
  {
    SCOPED_TRACE("Basic map");
    CheckJsonConversion(memgraph::storage::ExternalPropertyValue{map});
  }

  {
    SCOPED_TRACE("Nested map");
    map.emplace("map", memgraph::storage::ExternalPropertyValue{map});
    CheckJsonConversion(memgraph::storage::ExternalPropertyValue{map});
  }

  {
    SCOPED_TRACE("Map with list");
    map.emplace("list", memgraph::storage::ExternalPropertyValue{GetExternalPropertyValueListWithBasicTypes()});
    CheckJsonConversion(memgraph::storage::ExternalPropertyValue{map});
  }
}
