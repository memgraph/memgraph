// Copyright 2021 Memgraph Ltd.
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

namespace {
void ExpectPropEq(const storage::PropertyValue &a, const storage::PropertyValue &b) {
  ASSERT_EQ(a.type(), b.type());
  ASSERT_EQ(a, b);
}

void CheckJsonConversion(const storage::PropertyValue &property_value) {
  const auto json_string = query::serialization::SerializePropertyValue(property_value).dump();
  const auto json_object = nlohmann::json::parse(json_string);
  ExpectPropEq(property_value, query::serialization::DeserializePropertyValue(json_object));
}

}  // namespace

TEST(PropertyValueSerializationTest, Null) { CheckJsonConversion(storage::PropertyValue{}); }

TEST(PropertyValueSerializationTest, Bool) {
  CheckJsonConversion(storage::PropertyValue{true});
  CheckJsonConversion(storage::PropertyValue{false});
}

TEST(PropertyValueSerializationTest, Int) {
  CheckJsonConversion(storage::PropertyValue{1});
  CheckJsonConversion(storage::PropertyValue{100});
}

TEST(PropertyValueSerializationTest, Double) {
  CheckJsonConversion(storage::PropertyValue{1.0});
  CheckJsonConversion(storage::PropertyValue{2.321});
}

TEST(PropertyValueSerializationTest, String) {
  CheckJsonConversion(storage::PropertyValue{"TestString"});
  CheckJsonConversion(storage::PropertyValue{""});
}

TEST(PropertyValueSerializationTest, TemporalData) {
  const auto test_temporal_data_conversion = [](const auto type, const auto microseconds) {
    CheckJsonConversion(storage::PropertyValue{storage::TemporalData{type, microseconds}});
  };

  test_temporal_data_conversion(storage::TemporalType::Date, 20);
  test_temporal_data_conversion(storage::TemporalType::LocalDateTime, -20);
  test_temporal_data_conversion(storage::TemporalType::Duration, 10000);
}

namespace {

std::vector<storage::PropertyValue> GetPropertyValueListWithBasicTypes() {
  return {storage::PropertyValue{}, storage::PropertyValue{true}, storage::PropertyValue{"string"},
          storage::PropertyValue{1}, storage::PropertyValue{1.0}};
}

std::map<std::string, storage::PropertyValue> GetPropertyValueMapWithBasicTypes() {
  return {{"null", storage::PropertyValue{}},
          {"bool", storage::PropertyValue{true}},
          {"int", storage::PropertyValue{1}},
          {"double", storage::PropertyValue{1.0}},
          {"string", storage::PropertyValue{"string"}}};
}

}  // namespace

TEST(PropertyValueSerializationTest, List) {
  storage::PropertyValue list = storage::PropertyValue{GetPropertyValueListWithBasicTypes()};

  {
    SCOPED_TRACE("Basic list");
    CheckJsonConversion(list);
  }

  {
    SCOPED_TRACE("Nested list");
    CheckJsonConversion(storage::PropertyValue{std::vector<storage::PropertyValue>{list, list}});
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
    CheckJsonConversion(storage::PropertyValue{map});
  }

  {
    SCOPED_TRACE("Nested map");
    map.emplace("map", storage::PropertyValue{map});
    CheckJsonConversion(storage::PropertyValue{map});
  }

  {
    SCOPED_TRACE("Map with list");
    map.emplace("list", storage::PropertyValue{GetPropertyValueListWithBasicTypes()});
    CheckJsonConversion(storage::PropertyValue{map});
  }
}
