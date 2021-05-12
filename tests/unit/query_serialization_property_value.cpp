#include <gtest/gtest.h>

#include "query/serialization/property_value.hpp"
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

  SPDLOG_DEBUG("Basic list");
  CheckJsonConversion(list);

  SPDLOG_DEBUG("Nested list");
  CheckJsonConversion(storage::PropertyValue{std::vector<storage::PropertyValue>{list, list}});

  SPDLOG_DEBUG("List with map");
  list.ValueList().emplace_back(GetPropertyValueMapWithBasicTypes());
  CheckJsonConversion(list);
}

TEST(PropertyValueSerializationTest, Map) {
  auto map = GetPropertyValueMapWithBasicTypes();
  SPDLOG_DEBUG("Basic map");
  CheckJsonConversion(storage::PropertyValue{map});

  SPDLOG_DEBUG("Nested map");
  map.emplace("map", storage::PropertyValue{map});
  CheckJsonConversion(storage::PropertyValue{map});

  SPDLOG_DEBUG("Map with list");
  map.emplace("list", storage::PropertyValue{GetPropertyValueListWithBasicTypes()});
  CheckJsonConversion(storage::PropertyValue{map});
}
