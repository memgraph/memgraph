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

#include <memory_resource>
#include <sstream>

#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"

using namespace memgraph::storage;
using enum CoordinateReferenceSystem;

///!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
///!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
///!!!!!!!!!!!THIS FILE IS TOO LONG!!!!!!!!!!!!
///!!!!!!!!!!!!! TODO: REFACTOR !!!!!!!!!!!!!!!
///!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
///!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

// helpers
template <typename Alloc, typename KeyType>
bool IsOnlyOneType(const PropertyValueImpl<Alloc, KeyType> &pv) {
  auto count = pv.IsNull() + pv.IsBool() + pv.IsInt() + pv.IsDouble() + pv.IsString() + pv.IsList() + pv.IsMap() +
               pv.IsEnum() + pv.IsPoint2d() + pv.IsPoint3d();
  return count == 1;
}

template <typename Alloc, typename KeyType>
struct PropertyTestCase {
  PropertyValueImpl<Alloc, KeyType> value;
  PropertyValue::Type expected_type;
  std::string expected_type_str;
  std::string expected_value_str;
};

template <typename Alloc, typename KeyType>
void RunCommonPropertyValueChecks(const PropertyTestCase<Alloc, KeyType> &tc) {
  const auto &pv = tc.value;
  ASSERT_EQ(pv.type(), tc.expected_type);
  ASSERT_TRUE(IsOnlyOneType(pv));

  using AccessorFn = std::function<void()>;
  const std::unordered_map<PropertyValueType, AccessorFn> accessors = {
      {PropertyValueType::Bool, [&]() { pv.ValueBool(); }},
      {PropertyValueType::Int, [&]() { pv.ValueInt(); }},
      {PropertyValueType::Double, [&]() { pv.ValueDouble(); }},
      {PropertyValueType::String, [&]() { pv.ValueString(); }},
      {PropertyValueType::List, [&]() { pv.ValueList(); }},
      {PropertyValueType::Map, [&]() { pv.ValueMap(); }},
      {PropertyValueType::Enum, [&]() { pv.ValueEnum(); }},
      {PropertyValueType::Point2d, [&]() { pv.ValuePoint2d(); }},
      {PropertyValueType::Point3d, [&]() { pv.ValuePoint3d(); }},
  };

  for (const auto &[type, accessor] : accessors) {
    if (type == tc.expected_type) {
      EXPECT_NO_THROW(accessor());
    } else {
      EXPECT_THROW(accessor(), PropertyValueException);
    }
  }
  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), tc.expected_type_str);
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), tc.expected_value_str);
  }
}

template <typename TPropertyValue, typename MapKey>
std::vector<TPropertyValue> MakeTestPropertyValues(MapKey map_key) {
  std::vector<TPropertyValue> vec{TPropertyValue(true), TPropertyValue(123)};
  typename TPropertyValue::map_t map{{map_key, TPropertyValue(false)}};
  const auto zdt_dur = memgraph::utils::AsSysTime(23);
  Enum enum_val{EnumTypeId{2}, EnumValueId{42}};
  std::vector<TPropertyValue> int_list{TPropertyValue(1), TPropertyValue(2), TPropertyValue(3)};
  std::vector<TPropertyValue> double_list{TPropertyValue(1.0), TPropertyValue(2.0), TPropertyValue(3.0)};
  std::vector<TPropertyValue> numeric_list{TPropertyValue(1), TPropertyValue(2.0), TPropertyValue(3.0)};

  return {
      TPropertyValue(),
      TPropertyValue(true),
      TPropertyValue(123),
      TPropertyValue(123.5),
      TPropertyValue("nandare"),
      TPropertyValue(vec),
      TPropertyValue(map),
      TPropertyValue{TemporalData(TemporalType::Date, 23)},
      TPropertyValue{
          ZonedTemporalData(ZonedTemporalType::ZonedDateTime, zdt_dur, memgraph::utils::Timezone("Etc/UTC"))},
      TPropertyValue{ZonedTemporalData(ZonedTemporalType::ZonedDateTime, zdt_dur,
                                       memgraph::utils::Timezone(std::chrono::minutes{-60}))},
      TPropertyValue{enum_val},
      TPropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}},
      TPropertyValue{Point2d{WGS84_2d, 3.0, 4.0}},
      TPropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}},
      TPropertyValue{Point3d{WGS84_3d, 4.0, 5.0, 6.0}},
      TPropertyValue{IntListTag{}, int_list},
      TPropertyValue{DoubleListTag{}, double_list},
      TPropertyValue{NumericListTag{}, numeric_list},
  };
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Null) {
  // RunCommonPropertyValueChecks({PropertyValue(), PropertyValue::Type::Null, "null", "null"});
  PropertyTestCase tc{PropertyValue(), PropertyValue::Type::Null, "null", "null"};
  RunCommonPropertyValueChecks(tc);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Bool) {
  {
    PropertyValue pvfalse(false);
    EXPECT_EQ(pvfalse.ValueBool(), false);
    PropertyTestCase tc{pvfalse, PropertyValue::Type::Bool, "bool", "false"};
    RunCommonPropertyValueChecks(tc);
    auto &cpvfalse = pvfalse;
    EXPECT_EQ(cpvfalse.ValueBool(), false);
    PropertyTestCase ctc{cpvfalse, PropertyValue::Type::Bool, "bool", "false"};
    RunCommonPropertyValueChecks(ctc);
  }
  {
    PropertyValue pvtrue(true);
    std::stringstream ss;
    ss << pvtrue;
    ASSERT_EQ(ss.str(), "true");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Int) {
  {
    PropertyValue pv(123L);
    PropertyTestCase tc{pv, PropertyValue::Type::Int, "int", "123"};
    RunCommonPropertyValueChecks(tc);

    const auto &cpv = pv;
    PropertyTestCase ctc{cpv, PropertyValue::Type::Int, "int", "123"};
    RunCommonPropertyValueChecks(ctc);
  }
  {
    PropertyValue pvint(123);
    ASSERT_EQ(pvint.type(), PropertyValue::Type::Int);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Double) {
  PropertyValue pv(123.5);
  PropertyTestCase tc{pv, PropertyValue::Type::Double, "double", "123.5"};
  RunCommonPropertyValueChecks(tc);

  const auto &cpv = pv;
  PropertyTestCase ctc{cpv, PropertyValue::Type::Double, "double", "123.5"};
  RunCommonPropertyValueChecks(ctc);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Enum) {
  auto enum_val = Enum{EnumTypeId{2}, EnumValueId{42}};
  PropertyValue pv(enum_val);
  PropertyTestCase tc{pv, PropertyValue::Type::Enum, "enum", "{ type: 2, value: 42 }"};
  RunCommonPropertyValueChecks(tc);

  const auto &cpv = pv;
  PropertyTestCase ctc{cpv, PropertyValue::Type::Enum, "enum", "{ type: 2, value: 42 }"};
  RunCommonPropertyValueChecks(ctc);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, StringCopy) {
  std::string str("nandare");
  PropertyValue pv(str);
  ASSERT_EQ(str, "nandare");
  PropertyTestCase tc{pv, PropertyValue::Type::String, "string", "\"nandare\""};
  RunCommonPropertyValueChecks(tc);

  const auto &cpv = pv;
  ASSERT_EQ(cpv.ValueString(), "nandare");
  PropertyTestCase ctc{cpv, PropertyValue::Type::String, "string", "\"nandare\""};
  RunCommonPropertyValueChecks(ctc);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, StringMove) {
  std::string str = "nandare";
  PropertyValue pv(std::move(str));
  ASSERT_EQ(str, "");
  PropertyTestCase tc{pv, PropertyValue::Type::String, "string", "\"nandare\""};
  RunCommonPropertyValueChecks(tc);
  ASSERT_EQ(pv.ValueString(), "nandare");

  const auto &cpv = pv;
  PropertyTestCase ctc{cpv, PropertyValue::Type::String, "string", "\"nandare\""};
  RunCommonPropertyValueChecks(ctc);
  ASSERT_EQ(cpv.ValueString(), "nandare");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, ListCopy) {
  std::vector<PropertyValue> vec{PropertyValue("nandare"), PropertyValue(123)};
  PropertyValue pv(vec);

  ASSERT_EQ(vec.size(), 2);
  ASSERT_EQ(vec[0].ValueString(), "nandare");
  ASSERT_EQ(vec[1].ValueInt(), 123);
  PropertyTestCase tc{pv, PropertyValue::Type::List, "list", R"(["nandare", 123])"};
  RunCommonPropertyValueChecks(tc);
  {
    const auto &ret = pv.ValueList();
    ASSERT_EQ(ret.size(), 2);
    ASSERT_EQ(ret[0].ValueString(), "nandare");
    ASSERT_EQ(ret[1].ValueInt(), 123);
  }

  const auto &cpv = pv;
  PropertyTestCase ctc{cpv, PropertyValue::Type::List, "list", R"(["nandare", 123])"};
  RunCommonPropertyValueChecks(ctc);
  {
    const auto &ret = cpv.ValueList();
    ASSERT_EQ(ret.size(), 2);
    ASSERT_EQ(ret[0].ValueString(), "nandare");
    ASSERT_EQ(ret[1].ValueInt(), 123);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, ListMove) {
  std::vector<PropertyValue> vec{PropertyValue("nandare"), PropertyValue(123)};
  PropertyValue pv(std::move(vec));

  ASSERT_EQ(vec.size(), 0);
  ASSERT_TRUE(pv.IsList());
  PropertyTestCase tc{pv, PropertyValue::Type::List, "list", R"(["nandare", 123])"};
  RunCommonPropertyValueChecks(tc);
  {
    const auto &ret = pv.ValueList();
    ASSERT_EQ(ret.size(), 2);
    ASSERT_EQ(ret[0].ValueString(), "nandare");
    ASSERT_EQ(ret[1].ValueInt(), 123);
  }

  const auto &cpv = pv;
  PropertyTestCase ctc{cpv, PropertyValue::Type::List, "list", R"(["nandare", 123])"};
  RunCommonPropertyValueChecks(ctc);
  {
    const auto &ret = cpv.ValueList();
    ASSERT_EQ(ret.size(), 2);
    ASSERT_EQ(ret[0].ValueString(), "nandare");
    ASSERT_EQ(ret[1].ValueInt(), 123);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, MapCopy) {
  auto property_id = PropertyId::FromUint(1);
  PropertyValue::map_t map{{property_id, PropertyValue(123)}};
  PropertyValue pv(map);

  ASSERT_EQ(map.size(), 1);
  ASSERT_EQ(map.at(property_id).ValueInt(), 123);
  ASSERT_TRUE(pv.IsMap());
  PropertyTestCase tc{pv, PropertyValue::Type::Map, "map", "{1: 123}"};
  RunCommonPropertyValueChecks(tc);
  {
    const auto &ret = pv.ValueMap();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_EQ(ret.at(property_id).ValueInt(), 123);
  }

  const auto &cpv = pv;
  PropertyTestCase ctc{cpv, PropertyValue::Type::Map, "map", "{1: 123}"};
  RunCommonPropertyValueChecks(ctc);
  {
    const auto &ret = cpv.ValueMap();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_EQ(ret.at(property_id).ValueInt(), 123);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, MapMove) {
  auto property_id = PropertyId::FromUint(1);
  PropertyValue::map_t map{{property_id, PropertyValue(123)}};
  PropertyValue pv(std::move(map));

  ASSERT_EQ(map.size(), 0);
  PropertyTestCase tc{pv, PropertyValue::Type::Map, "map", "{1: 123}"};
  RunCommonPropertyValueChecks(tc);
  {
    const auto &ret = pv.ValueMap();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_EQ(ret.at(property_id).ValueInt(), 123);
  }

  const auto &cpv = pv;
  PropertyTestCase ctc{cpv, PropertyValue::Type::Map, "map", "{1: 123}"};
  RunCommonPropertyValueChecks(ctc);
  {
    const auto &ret = cpv.ValueMap();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_EQ(ret.at(property_id).ValueInt(), 123);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, ExternalMapCopy) {
  ExternalPropertyValue::map_t map{{"nandare", ExternalPropertyValue(123)}};
  ExternalPropertyValue pv(map);

  ASSERT_EQ(map.size(), 1);
  ASSERT_EQ(map.at("nandare").ValueInt(), 123);
  ASSERT_TRUE(pv.IsMap());
  PropertyTestCase tc{pv, ExternalPropertyValue::Type::Map, "map", "{nandare: 123}"};
  RunCommonPropertyValueChecks(tc);
  {
    const auto &ret = pv.ValueMap();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_EQ(ret.at("nandare").ValueInt(), 123);
  }

  const auto &cpv = pv;
  PropertyTestCase ctc{cpv, ExternalPropertyValue::Type::Map, "map", "{nandare: 123}"};
  RunCommonPropertyValueChecks(ctc);
  {
    const auto &ret = cpv.ValueMap();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_EQ(ret.at("nandare").ValueInt(), 123);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, ExternalMapMove) {
  ExternalPropertyValue::map_t map{{"nandare", ExternalPropertyValue(123)}};
  ExternalPropertyValue pv(std::move(map));

  ASSERT_EQ(map.size(), 0);
  ASSERT_TRUE(pv.IsMap());
  PropertyTestCase tc{pv, ExternalPropertyValue::Type::Map, "map", "{nandare: 123}"};
  RunCommonPropertyValueChecks(tc);
  {
    const auto &ret = pv.ValueMap();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_EQ(ret.at("nandare").ValueInt(), 123);
  }

  const auto &cpv = pv;
  PropertyTestCase ctc{cpv, ExternalPropertyValue::Type::Map, "map", "{nandare: 123}"};
  RunCommonPropertyValueChecks(ctc);
  {
    const auto &ret = cpv.ValueMap();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_EQ(ret.at("nandare").ValueInt(), 123);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Point2d) {
  auto point_val = Point2d{CoordinateReferenceSystem::WGS84_2d, 1.0, 2.0};
  PropertyValue pv(point_val);

  ASSERT_TRUE(pv.IsPoint2d());
  PropertyTestCase tc{pv, PropertyValue::Type::Point2d, "point", "point({ x:1, y:2, srid:4326 })"};
  RunCommonPropertyValueChecks(tc);
  ASSERT_EQ(pv.ValuePoint2d(), point_val);

  const auto &cpv = pv;
  PropertyTestCase ctc{cpv, PropertyValue::Type::Point2d, "point", "point({ x:1, y:2, srid:4326 })"};
  RunCommonPropertyValueChecks(ctc);
  ASSERT_EQ(cpv.ValuePoint2d(), point_val);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Point3d) {
  auto point_val = Point3d{CoordinateReferenceSystem::WGS84_3d, 1.0, 2.0, 3.0};
  PropertyValue pv(point_val);

  ASSERT_TRUE(pv.IsPoint3d());
  PropertyTestCase tc{pv, PropertyValue::Type::Point3d, "point", "point({ x:1, y:2, z:3, srid:4979 })"};
  RunCommonPropertyValueChecks(tc);
  ASSERT_EQ(pv.ValuePoint3d(), point_val);

  const auto &cpv = pv;
  PropertyTestCase ctc{cpv, PropertyValue::Type::Point3d, "point", "point({ x:1, y:2, z:3, srid:4979 })"};
  RunCommonPropertyValueChecks(ctc);
  ASSERT_EQ(cpv.ValuePoint3d(), point_val);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, CopyConstructor) {
  auto property_id = PropertyId::FromUint(1);
  auto data = MakeTestPropertyValues<PropertyValue>(property_id);

  for (const auto &item : data) {
    PropertyValue pv(item);
    ASSERT_EQ(pv.type(), item.type());
    switch (item.type()) {
      case PropertyValue::Type::Null:
        ASSERT_TRUE(pv.IsNull());
        break;
      case PropertyValue::Type::Bool:
        ASSERT_EQ(pv.ValueBool(), item.ValueBool());
        break;
      case PropertyValue::Type::Int:
        ASSERT_EQ(pv.ValueInt(), item.ValueInt());
        break;
      case PropertyValue::Type::Double:
        ASSERT_EQ(pv.ValueDouble(), item.ValueDouble());
        break;
      case PropertyValue::Type::String:
        ASSERT_EQ(pv.ValueString(), item.ValueString());
        break;
      case PropertyValue::Type::List:
        ASSERT_EQ(pv.ValueList(), item.ValueList());
        break;
      case PropertyValue::Type::IntList:
        ASSERT_EQ(pv.ValueIntList(), item.ValueIntList());
        break;
      case PropertyValue::Type::DoubleList:
        ASSERT_EQ(pv.ValueDoubleList(), item.ValueDoubleList());
        break;
      case PropertyValue::Type::NumericList:
        ASSERT_EQ(pv.ValueNumericList(), item.ValueNumericList());
        break;
      case PropertyValue::Type::Map:
        ASSERT_EQ(pv.ValueMap(), item.ValueMap());
        break;
      case PropertyValue::Type::TemporalData:
        ASSERT_EQ(pv.ValueTemporalData(), item.ValueTemporalData());
        break;
      case PropertyValue::Type::ZonedTemporalData:
        ASSERT_EQ(pv.ValueZonedTemporalData(), item.ValueZonedTemporalData());
        break;
      case PropertyValue::Type::Enum:
        ASSERT_EQ(pv.ValueEnum(), item.ValueEnum());
        break;
      case PropertyValue::Type::Point2d:
        ASSERT_EQ(pv.ValuePoint2d(), item.ValuePoint2d());
        break;
      case PropertyValue::Type::Point3d:
        ASSERT_EQ(pv.ValuePoint3d(), item.ValuePoint3d());
        break;
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, MoveConstructor) {
  auto property_id = PropertyId::FromUint(1);
  auto data = MakeTestPropertyValues<PropertyValue>(property_id);

  for (auto &item : data) {
    PropertyValue copy(item);
    PropertyValue pv(std::move(item));
    ASSERT_EQ(pv.type(), copy.type());
    switch (copy.type()) {
      case PropertyValue::Type::Null:
        ASSERT_TRUE(pv.IsNull());
        break;
      case PropertyValue::Type::Bool:
        ASSERT_EQ(pv.ValueBool(), copy.ValueBool());
        break;
      case PropertyValue::Type::Int:
        ASSERT_EQ(pv.ValueInt(), copy.ValueInt());
        break;
      case PropertyValue::Type::Double:
        ASSERT_EQ(pv.ValueDouble(), copy.ValueDouble());
        break;
      case PropertyValue::Type::String:
        ASSERT_EQ(pv.ValueString(), copy.ValueString());
        break;
      case PropertyValue::Type::List:
        ASSERT_EQ(pv.ValueList(), copy.ValueList());
        break;
      case PropertyValue::Type::IntList:
        ASSERT_EQ(pv.ValueIntList(), copy.ValueIntList());
        break;
      case PropertyValue::Type::DoubleList:
        ASSERT_EQ(pv.ValueDoubleList(), copy.ValueDoubleList());
        break;
      case PropertyValue::Type::NumericList:
        ASSERT_EQ(pv.ValueNumericList(), copy.ValueNumericList());
        break;
      case PropertyValue::Type::Map:
        ASSERT_EQ(pv.ValueMap(), copy.ValueMap());
        break;
      case PropertyValue::Type::TemporalData:
        ASSERT_EQ(pv.ValueTemporalData(), copy.ValueTemporalData());
        break;
      case PropertyValue::Type::ZonedTemporalData:
        ASSERT_EQ(pv.ValueZonedTemporalData(), copy.ValueZonedTemporalData());
        break;
      case PropertyValue::Type::Enum:
        ASSERT_EQ(pv.ValueEnum(), copy.ValueEnum());
        break;
      case PropertyValue::Type::Point2d:
        ASSERT_EQ(pv.ValuePoint2d(), copy.ValuePoint2d());
        break;
      case PropertyValue::Type::Point3d:
        ASSERT_EQ(pv.ValuePoint3d(), copy.ValuePoint3d());
        break;
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, CopyAssignment) {
  auto property_id = PropertyId::FromUint(1);
  auto data = MakeTestPropertyValues<PropertyValue>(property_id);

  for (const auto &item : data) {
    PropertyValue pv(123);
    pv = item;
    ASSERT_EQ(pv.type(), item.type());
    switch (item.type()) {
      case PropertyValue::Type::Null:
        ASSERT_TRUE(pv.IsNull());
        break;
      case PropertyValue::Type::Bool:
        ASSERT_EQ(pv.ValueBool(), item.ValueBool());
        break;
      case PropertyValue::Type::Int:
        ASSERT_EQ(pv.ValueInt(), item.ValueInt());
        break;
      case PropertyValue::Type::Double:
        ASSERT_EQ(pv.ValueDouble(), item.ValueDouble());
        break;
      case PropertyValue::Type::String:
        ASSERT_EQ(pv.ValueString(), item.ValueString());
        break;
      case PropertyValue::Type::List:
        ASSERT_EQ(pv.ValueList(), item.ValueList());
        break;
      case PropertyValue::Type::IntList:
        ASSERT_EQ(pv.ValueIntList(), item.ValueIntList());
        break;
      case PropertyValue::Type::DoubleList:
        ASSERT_EQ(pv.ValueDoubleList(), item.ValueDoubleList());
        break;
      case PropertyValue::Type::NumericList:
        ASSERT_EQ(pv.ValueNumericList(), item.ValueNumericList());
        break;
      case PropertyValue::Type::Map:
        ASSERT_EQ(pv.ValueMap(), item.ValueMap());
        break;
      case PropertyValue::Type::TemporalData:
        ASSERT_EQ(pv.ValueTemporalData(), item.ValueTemporalData());
        break;
      case PropertyValue::Type::ZonedTemporalData:
        ASSERT_EQ(pv.ValueZonedTemporalData(), item.ValueZonedTemporalData());
        break;
      case PropertyValue::Type::Enum:
        ASSERT_EQ(pv.ValueEnum(), item.ValueEnum());
        break;
      case PropertyValue::Type::Point2d:
        ASSERT_EQ(pv.ValuePoint2d(), item.ValuePoint2d());
        break;
      case PropertyValue::Type::Point3d:
        ASSERT_EQ(pv.ValuePoint3d(), item.ValuePoint3d());
        break;
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, MoveAssignment) {
  auto property_id = PropertyId::FromUint(1);
  auto data = MakeTestPropertyValues<PropertyValue>(property_id);

  for (auto &item : data) {
    PropertyValue copy(item);
    PropertyValue pv(123);
    pv = std::move(item);
    ASSERT_EQ(pv.type(), copy.type());
    switch (copy.type()) {
      case PropertyValue::Type::Null:
        ASSERT_TRUE(pv.IsNull());
        break;
      case PropertyValue::Type::Bool:
        ASSERT_EQ(pv.ValueBool(), copy.ValueBool());
        break;
      case PropertyValue::Type::Int:
        ASSERT_EQ(pv.ValueInt(), copy.ValueInt());
        break;
      case PropertyValue::Type::Double:
        ASSERT_EQ(pv.ValueDouble(), copy.ValueDouble());
        break;
      case PropertyValue::Type::String:
        ASSERT_EQ(pv.ValueString(), copy.ValueString());
        break;
      case PropertyValue::Type::List:
        ASSERT_EQ(pv.ValueList(), copy.ValueList());
        break;
      case PropertyValue::Type::IntList:
        ASSERT_EQ(pv.ValueIntList(), copy.ValueIntList());
        break;
      case PropertyValue::Type::DoubleList:
        ASSERT_EQ(pv.ValueDoubleList(), copy.ValueDoubleList());
        break;
      case PropertyValue::Type::NumericList:
        ASSERT_EQ(pv.ValueNumericList(), copy.ValueNumericList());
        break;
      case PropertyValue::Type::Map:
        ASSERT_EQ(pv.ValueMap(), copy.ValueMap());
        break;
      case PropertyValue::Type::TemporalData:
        ASSERT_EQ(pv.ValueTemporalData(), copy.ValueTemporalData());
        break;
      case PropertyValue::Type::ZonedTemporalData:
        ASSERT_EQ(pv.ValueZonedTemporalData(), copy.ValueZonedTemporalData());
        break;
      case PropertyValue::Type::Enum:
        ASSERT_EQ(pv.ValueEnum(), copy.ValueEnum());
        break;
      case PropertyValue::Type::Point2d:
        ASSERT_EQ(pv.ValuePoint2d(), copy.ValuePoint2d());
        break;
      case PropertyValue::Type::Point3d:
        ASSERT_EQ(pv.ValuePoint3d(), copy.ValuePoint3d());
        break;
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, CopyAssignmentSelf) {
  PropertyValue pv("nandare");
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-assign-overloaded"
  pv = pv;
#pragma clang diagnostic pop
  ASSERT_EQ(pv.type(), PropertyValue::Type::String);
  ASSERT_EQ(pv.ValueString(), "nandare");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, MoveAssignmentSelf) {
  PropertyValue pv("nandare");
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-move"
  pv = std::move(pv);
#pragma clang diagnostic pop
  ASSERT_EQ(pv.type(), PropertyValue::Type::String);
  ASSERT_EQ(pv.ValueString(), "nandare");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Equal) {
  auto property_id = PropertyId::FromUint(1);
  const auto data = MakeTestPropertyValues<PropertyValue>(property_id);

  auto same_type = [](const auto &a, const auto &b) { return a.type() == b.type(); };
  auto same_point2d = [](const auto &a, const auto &b) { return a.ValuePoint2d().crs() == b.ValuePoint2d().crs(); };
  auto same_point3d = [](const auto &a, const auto &b) { return a.ValuePoint3d().crs() == b.ValuePoint3d().crs(); };
  auto same_zoned_temporal = [](const auto &a, const auto &b) {
    return a.ValueZonedTemporalData().timezone == b.ValueZonedTemporalData().timezone;
  };

  for (const auto &a : data) {
    for (const auto &b : data) {
      if (!same_type(a, b)) {
        ASSERT_FALSE(a == b);
        continue;
      }
      if (a.IsPoint2d()) {
        ASSERT_EQ(a == b, same_point2d(a, b));
      } else if (a.IsPoint3d()) {
        ASSERT_EQ(a == b, same_point3d(a, b));
      } else if (a.IsZonedTemporalData()) {
        ASSERT_EQ(a == b, same_zoned_temporal(a, b));
      } else {
        ASSERT_TRUE(a == b);
      }
    }
  }
}

TEST(PropertyValue, EqualMap) {
  auto a = PropertyValue(PropertyValue::map_t());
  auto b = PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(5)}});
  auto c = PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(10)}});

  ASSERT_EQ(a, a);
  ASSERT_EQ(b, b);
  ASSERT_EQ(c, c);

  ASSERT_NE(a, b);
  ASSERT_NE(a, c);
  ASSERT_NE(b, a);
  ASSERT_NE(b, c);
  ASSERT_NE(c, a);
  ASSERT_NE(c, b);
}

TEST(PropertyValue, ExternalEqualMap) {
  auto a = ExternalPropertyValue(ExternalPropertyValue::map_t());
  auto b = ExternalPropertyValue(ExternalPropertyValue::map_t{{"id", ExternalPropertyValue(5)}});
  auto c = ExternalPropertyValue(ExternalPropertyValue::map_t{{"id", ExternalPropertyValue(10)}});

  ASSERT_EQ(a, a);
  ASSERT_EQ(b, b);
  ASSERT_EQ(c, c);

  ASSERT_NE(a, b);
  ASSERT_NE(a, c);
  ASSERT_NE(b, a);
  ASSERT_NE(b, c);
  ASSERT_NE(c, a);
  ASSERT_NE(c, b);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Less) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123)};
  auto map = PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(false)}};
  auto enum_val = Enum{EnumTypeId{2}, EnumValueId{42}};
  std::vector<PropertyValue> data{
      PropertyValue(),
      PropertyValue(true),
      PropertyValue(123),
      PropertyValue(123.5),
      PropertyValue("nandare"),
      PropertyValue(vec),
      PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(false)}}),
      PropertyValue{enum_val},
      PropertyValue{Point2d{WGS84_2d, 3.0, 4.0}},
      PropertyValue{Point3d{WGS84_3d, 4.0, 5.0, 6.0}},
  };
  for (size_t i = 0; i < data.size(); ++i) {
    for (size_t j = 0; j < data.size(); ++j) {
      auto item1 = data[i];
      auto item2 = data[j];
      if (i < j) {
        ASSERT_TRUE(item1 < item2);
      } else {
        ASSERT_FALSE(item1 < item2);
      }
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, ExternalLess) {
  std::vector<ExternalPropertyValue> vec{ExternalPropertyValue(true), ExternalPropertyValue(123)};
  auto map = ExternalPropertyValue::map_t{{"id", ExternalPropertyValue(false)}};
  auto enum_val = Enum{EnumTypeId{2}, EnumValueId{42}};
  std::vector<ExternalPropertyValue> data{
      ExternalPropertyValue(),
      ExternalPropertyValue(true),
      ExternalPropertyValue(123),
      ExternalPropertyValue(123.5),
      ExternalPropertyValue("nandare"),
      ExternalPropertyValue(vec),
      ExternalPropertyValue(ExternalPropertyValue::map_t{{"id", ExternalPropertyValue(false)}}),
      ExternalPropertyValue{enum_val},
      ExternalPropertyValue{Point2d{WGS84_2d, 3.0, 4.0}},
      ExternalPropertyValue{Point3d{WGS84_3d, 4.0, 5.0, 6.0}},
  };
  for (size_t i = 0; i < data.size(); ++i) {
    for (size_t j = 0; j < data.size(); ++j) {
      auto item1 = data[i];
      auto item2 = data[j];
      if (i < j) {
        ASSERT_TRUE(item1 < item2);
      } else {
        ASSERT_FALSE(item1 < item2);
      }
    }
  }
}

TEST(PropertyValue, NumeralTypesComparison) {
  auto v_int = PropertyValue(2);
  auto v_double = PropertyValue(2.0);
  ASSERT_TRUE(v_int.IsInt());
  ASSERT_TRUE(v_double.IsDouble());
  ASSERT_TRUE(v_int == v_double);
  ASSERT_FALSE(v_int < v_double);
  ASSERT_FALSE(v_double < v_int);
}

TEST(PropertyValue, NestedNumeralTypesComparison) {
  auto v1 = PropertyValue(std::vector<PropertyValue>{PropertyValue(1)});
  auto v2 = PropertyValue(std::vector<PropertyValue>{PropertyValue(1.5)});
  auto v3 = PropertyValue(std::vector<PropertyValue>{PropertyValue(2)});

  auto v1alt = PropertyValue(std::vector<PropertyValue>{PropertyValue(1.0)});
  auto v3alt = PropertyValue(std::vector<PropertyValue>{PropertyValue(2.0)});

  ASSERT_TRUE(v1 == v1alt);
  ASSERT_TRUE(v3 == v3alt);

  ASSERT_FALSE(v1 == v2);
  ASSERT_FALSE(v2 == v1);
  ASSERT_FALSE(v2 == v3);
  ASSERT_FALSE(v3 == v2);
  ASSERT_FALSE(v1 == v3);
  ASSERT_FALSE(v3 == v1);

  ASSERT_TRUE(v1 < v2);
  ASSERT_TRUE(v2 < v3);
  ASSERT_TRUE(v1 < v3);
  ASSERT_FALSE(v2 < v1);
  ASSERT_FALSE(v3 < v2);
  ASSERT_FALSE(v3 < v1);

  ASSERT_TRUE(v1alt < v2);
  ASSERT_TRUE(v2 < v3alt);
  ASSERT_TRUE(v1alt < v3alt);
  ASSERT_FALSE(v2 < v1alt);
  ASSERT_FALSE(v3alt < v2);
  ASSERT_FALSE(v3 < v1alt);
}

TEST(PMRPropertyValue, GivenNullAllocatorFailsIfTriesToAllocate) {
  auto const nmr = std::pmr::null_memory_resource();
  using sut_t = memgraph::storage::pmr::PropertyValue;

  auto test_number = sut_t{42, std::pmr::new_delete_resource()};

  auto test_list = sut_t::list_t{{test_number}, std::pmr::new_delete_resource()};
  EXPECT_THROW(sut_t(test_list, nmr), std::bad_alloc);

  auto test_map = sut_t::map_t{{std::pair(PropertyId::FromInt(1), test_number)}, std::pmr::new_delete_resource()};
  EXPECT_THROW(sut_t(test_map, nmr), std::bad_alloc);

  {
    auto sut = sut_t{std::pmr::polymorphic_allocator<>(nmr)};
    auto list_cpy = sut_t(test_list, std::pmr::new_delete_resource());
    EXPECT_THROW((sut = list_cpy), std::bad_alloc);
    EXPECT_THROW((sut = std::move(list_cpy)), std::bad_alloc);
  }

  {
    auto sut = sut_t{std::pmr::polymorphic_allocator<>(nmr)};
    auto map_cpy = sut_t(test_map, std::pmr::new_delete_resource());
    EXPECT_THROW((sut = map_cpy), std::bad_alloc);
    EXPECT_THROW((sut = std::move(map_cpy)), std::bad_alloc);
  }
}

TEST(PMRPropertyValue, InteropWithPropertyValue) {
  using sut_t = memgraph::storage::pmr::PropertyValue;

  auto const raw_test_string = "long long long long long string";
  auto const raw_test_int = 42;

  auto const test_string = sut_t{raw_test_string};
  auto const test_number = sut_t{raw_test_int};
  auto const test_list = sut_t{sut_t::list_t{test_number, test_number}};
  auto const property_id = PropertyId::FromUint(1);
  auto const test_map = sut_t{sut_t::map_t{std::pair{property_id, test_number}}};

  {
    /// String -> pmr to regular
    auto const as_pv = memgraph::storage::PropertyValue(test_string);
    ASSERT_EQ(as_pv.ValueString(), raw_test_string);

    /// String -> regular to pmr
    auto const as_pmr_pv = sut_t{as_pv};
    ASSERT_EQ(as_pmr_pv.ValueString(), raw_test_string);
  }

  {
    /// List -> pmr to regular
    auto const as_pv = memgraph::storage::PropertyValue(test_list);
    ASSERT_EQ(as_pv.ValueList().size(), 2);
    ASSERT_EQ(as_pv.ValueList()[0].ValueInt(), raw_test_int);
    ASSERT_EQ(as_pv.ValueList()[1].ValueInt(), raw_test_int);

    /// String -> regular to pmr
    auto const as_pmr_pv = sut_t{as_pv};
    ASSERT_EQ(as_pmr_pv.ValueList().size(), 2);
    ASSERT_EQ(as_pmr_pv.ValueList()[0].ValueInt(), raw_test_int);
    ASSERT_EQ(as_pmr_pv.ValueList()[1].ValueInt(), raw_test_int);
  }

  {
    /// Map -> pmr to regular
    auto const as_pv = memgraph::storage::PropertyValue(test_map);
    ASSERT_EQ(as_pv.ValueMap().size(), 1);
    ASSERT_TRUE(as_pv.ValueMap().contains(property_id));
    ASSERT_EQ(as_pv.ValueMap().at(property_id).ValueInt(), raw_test_int);

    /// Map -> regular to pmr
    auto const as_pmr_pv = sut_t{as_pv};
    ASSERT_EQ(as_pmr_pv.ValueMap().size(), 1);
    ASSERT_TRUE(as_pmr_pv.ValueMap().contains(property_id));
    ASSERT_EQ(as_pmr_pv.ValueMap().at(property_id).ValueInt(), raw_test_int);
  }
}

TEST(PropertyValue, PropertyValueToExternalPropertyValue) {
  memgraph::storage::NameIdMapper name_id_mapper;
  auto property_id = PropertyId::FromUint(name_id_mapper.NameToId("id"));
  auto data = MakeTestPropertyValues<ExternalPropertyValue>("id");

  for (const auto &val : data) {
    auto pv = ToPropertyValue(val, &name_id_mapper);
    ASSERT_EQ(pv.type(), val.type());
    switch (pv.type()) {
      case PropertyValue::Type::Null:
        ASSERT_TRUE(pv.IsNull());
        break;
      case PropertyValue::Type::Bool:
        ASSERT_EQ(pv.ValueBool(), val.ValueBool());
        break;
      case PropertyValue::Type::Int:
        ASSERT_EQ(pv.ValueInt(), val.ValueInt());
        break;
      case PropertyValue::Type::Double:
        ASSERT_EQ(pv.ValueDouble(), val.ValueDouble());
        break;
      case PropertyValue::Type::String:
        ASSERT_EQ(pv.ValueString(), val.ValueString());
        break;
      case PropertyValue::Type::List:
        ASSERT_EQ(pv.ValueList().size(), 2);
        ASSERT_EQ(pv.ValueList()[0].ValueBool(), true);
        ASSERT_EQ(pv.ValueList()[1].ValueInt(), 123);
        break;
      case PropertyValue::Type::IntList:
        ASSERT_EQ(pv.ValueIntList(), val.ValueIntList());
        break;
      case PropertyValue::Type::DoubleList:
        ASSERT_EQ(pv.ValueDoubleList(), val.ValueDoubleList());
        break;
      case PropertyValue::Type::NumericList:
        ASSERT_EQ(pv.ValueNumericList(), val.ValueNumericList());
        break;
      case PropertyValue::Type::Map:
        ASSERT_EQ(pv.ValueMap().size(), 1);
        ASSERT_EQ(pv.ValueMap().at(property_id).ValueBool(), false);
        break;
      case PropertyValue::Type::TemporalData:
        ASSERT_EQ(pv.ValueTemporalData(), val.ValueTemporalData());
        break;
      case PropertyValue::Type::ZonedTemporalData:
        ASSERT_EQ(pv.ValueZonedTemporalData(), val.ValueZonedTemporalData());
        break;
      case PropertyValue::Type::Enum:
        ASSERT_EQ(pv.ValueEnum(), val.ValueEnum());
        break;
      case PropertyValue::Type::Point2d:
        ASSERT_EQ(pv.ValuePoint2d(), val.ValuePoint2d());
        break;
      case PropertyValue::Type::Point3d:
        ASSERT_EQ(pv.ValuePoint3d(), val.ValuePoint3d());
        break;
    }
  }
}

TEST(PropertyValue, ExternalPropertyValueToPropertyValue) {
  memgraph::storage::NameIdMapper name_id_mapper;
  auto property_id = PropertyId::FromUint(name_id_mapper.NameToId("id"));
  auto data = MakeTestPropertyValues<PropertyValue>(property_id);

  for (const auto &val : data) {
    auto pv = ToExternalPropertyValue(val, &name_id_mapper);
    ASSERT_EQ(pv.type(), val.type());
    switch (pv.type()) {
      case PropertyValue::Type::Null:
        ASSERT_TRUE(pv.IsNull());
        break;
      case PropertyValue::Type::Bool:
        ASSERT_EQ(pv.ValueBool(), val.ValueBool());
        break;
      case PropertyValue::Type::Int:
        ASSERT_EQ(pv.ValueInt(), val.ValueInt());
        break;
      case PropertyValue::Type::Double:
        ASSERT_EQ(pv.ValueDouble(), val.ValueDouble());
        break;
      case PropertyValue::Type::String:
        ASSERT_EQ(pv.ValueString(), val.ValueString());
        break;
      case PropertyValue::Type::List:
        ASSERT_EQ(pv.ValueList().size(), 2);
        ASSERT_EQ(pv.ValueList()[0].ValueBool(), true);
        ASSERT_EQ(pv.ValueList()[1].ValueInt(), 123);
        break;
      case PropertyValue::Type::IntList:
        ASSERT_EQ(pv.ValueIntList(), val.ValueIntList());
        break;
      case PropertyValue::Type::DoubleList:
        ASSERT_EQ(pv.ValueDoubleList(), val.ValueDoubleList());
        break;
      case PropertyValue::Type::NumericList:
        ASSERT_EQ(pv.ValueNumericList(), val.ValueNumericList());
        break;
      case PropertyValue::Type::Map:
        ASSERT_EQ(pv.ValueMap().size(), 1);
        ASSERT_EQ(pv.ValueMap().at("id").ValueBool(), false);
        break;
      case PropertyValue::Type::TemporalData:
        ASSERT_EQ(pv.ValueTemporalData(), val.ValueTemporalData());
        break;
      case PropertyValue::Type::ZonedTemporalData:
        ASSERT_EQ(pv.ValueZonedTemporalData(), val.ValueZonedTemporalData());
        break;
      case PropertyValue::Type::Enum:
        ASSERT_EQ(pv.ValueEnum(), val.ValueEnum());
        break;
      case PropertyValue::Type::Point2d:
        ASSERT_EQ(pv.ValuePoint2d(), val.ValuePoint2d());
        break;
      case PropertyValue::Type::Point3d:
        ASSERT_EQ(pv.ValuePoint3d(), val.ValuePoint3d());
        break;
    }
  }
}
