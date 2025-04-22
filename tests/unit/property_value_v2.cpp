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
bool IsOnlyOneType(PropertyValue const &pv) {
  auto count = pv.IsNull() + pv.IsBool() + pv.IsInt() + pv.IsDouble() + pv.IsString() + pv.IsList() + pv.IsMap() +
               pv.IsEnum() + pv.IsPoint2d() + pv.IsPoint3d();
  return count == 1;
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Null) {
  PropertyValue pv;

  ASSERT_EQ(pv.type(), PropertyValue::Type::Null);

  ASSERT_TRUE(pv.IsNull());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint3d(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "null");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "null");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Bool) {
  PropertyValue pv(false);

  ASSERT_EQ(pv.type(), PropertyValue::Type::Bool);

  ASSERT_TRUE(pv.IsBool());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_EQ(pv.ValueBool(), false);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint3d(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_EQ(cpv.ValueBool(), false);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint3d(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "bool");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "false");
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
  PropertyValue pv(123L);

  ASSERT_EQ(pv.type(), PropertyValue::Type::Int);

  ASSERT_TRUE(pv.IsInt());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_EQ(pv.ValueInt(), 123L);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint3d(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_EQ(cpv.ValueInt(), 123L);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint3d(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "int");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "123");
  }

  {
    PropertyValue pvint(123);
    ASSERT_EQ(pvint.type(), PropertyValue::Type::Int);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Double) {
  PropertyValue pv(123.5);

  ASSERT_EQ(pv.type(), PropertyValue::Type::Double);

  ASSERT_TRUE(pv.IsDouble());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_EQ(pv.ValueDouble(), 123.5);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint3d(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_EQ(cpv.ValueDouble(), 123.5);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint3d(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "double");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "123.5");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Enum) {
  auto enum_val = Enum{EnumTypeId{2}, EnumValueId{42}};
  PropertyValue pv(enum_val);

  ASSERT_EQ(pv.type(), PropertyValue::Type::Enum);

  ASSERT_TRUE(pv.IsEnum());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_EQ(pv.ValueEnum(), enum_val);
  ASSERT_THROW(pv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint3d(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_EQ(cpv.ValueEnum(), enum_val);
  ASSERT_THROW(cpv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint3d(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "enum");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "{ type: 2, value: 42 }");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, StringCopy) {
  std::string str("nandare");
  PropertyValue pv(str);

  ASSERT_EQ(str, "nandare");

  ASSERT_EQ(pv.type(), PropertyValue::Type::String);

  ASSERT_TRUE(pv.IsString());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_EQ(pv.ValueString(), "nandare");
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint3d(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_EQ(cpv.ValueString(), "nandare");
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint3d(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "string");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), R"("nandare")");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, StringMove) {
  std::string str("nandare");
  PropertyValue pv(std::move(str));

  ASSERT_EQ(str, "");

  ASSERT_EQ(pv.type(), PropertyValue::Type::String);

  ASSERT_TRUE(pv.IsString());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_EQ(pv.ValueString(), "nandare");
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint3d(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_EQ(cpv.ValueString(), "nandare");
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint3d(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "string");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), R"("nandare")");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, ListCopy) {
  std::vector<PropertyValue> vec{PropertyValue("nandare"), PropertyValue(123)};
  PropertyValue pv(vec);

  ASSERT_EQ(vec.size(), 2);
  ASSERT_EQ(vec[0].ValueString(), "nandare");
  ASSERT_EQ(vec[1].ValueInt(), 123);

  ASSERT_EQ(pv.type(), PropertyValue::Type::List);

  ASSERT_TRUE(pv.IsList());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  {
    const auto &ret = pv.ValueList();
    ASSERT_EQ(ret.size(), 2);
    ASSERT_EQ(ret[0].ValueString(), "nandare");
    ASSERT_EQ(ret[1].ValueInt(), 123);
  }
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint3d(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  {
    const auto &ret = cpv.ValueList();
    ASSERT_EQ(ret.size(), 2);
    ASSERT_EQ(ret[0].ValueString(), "nandare");
    ASSERT_EQ(ret[1].ValueInt(), 123);
  }
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint3d(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "list");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), R"(["nandare", 123])");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, ListMove) {
  std::vector<PropertyValue> vec{PropertyValue("nandare"), PropertyValue(123)};
  PropertyValue pv(std::move(vec));

  ASSERT_EQ(vec.size(), 0);

  ASSERT_EQ(pv.type(), PropertyValue::Type::List);

  ASSERT_TRUE(pv.IsList());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  {
    const auto &ret = pv.ValueList();
    ASSERT_EQ(ret.size(), 2);
    ASSERT_EQ(ret[0].ValueString(), "nandare");
    ASSERT_EQ(ret[1].ValueInt(), 123);
  }
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint3d(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  {
    const auto &ret = cpv.ValueList();
    ASSERT_EQ(ret.size(), 2);
    ASSERT_EQ(ret[0].ValueString(), "nandare");
    ASSERT_EQ(ret[1].ValueInt(), 123);
  }
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint3d(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "list");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), R"(["nandare", 123])");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, MapCopy) {
  StringToPropertyValueMap map{{"nandare", PropertyValue(123)}};
  PropertyValue pv(map);

  ASSERT_EQ(map.size(), 1);
  ASSERT_EQ(map.at("nandare").ValueInt(), 123);

  ASSERT_EQ(pv.type(), PropertyValue::Type::Map);

  ASSERT_TRUE(pv.IsMap());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  {
    const auto &ret = pv.ValueMap();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_EQ(ret.at("nandare").ValueInt(), 123);
  }
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint3d(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  {
    const auto &ret = cpv.ValueMap();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_EQ(ret.at("nandare").ValueInt(), 123);
  }
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint3d(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "map");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "{nandare: 123}");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, MapMove) {
  PropertyValue::map_t map{{"nandare", PropertyValue(123)}};
  PropertyValue pv(std::move(map));

  ASSERT_EQ(map.size(), 0);

  ASSERT_EQ(pv.type(), PropertyValue::Type::Map);

  ASSERT_TRUE(pv.IsMap());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  {
    const auto &ret = pv.ValueMap();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_EQ(ret.at("nandare").ValueInt(), 123);
  }
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint3d(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  {
    const auto &ret = cpv.ValueMap();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_EQ(ret.at("nandare").ValueInt(), 123);
  }
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint2d(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint3d(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "map");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "{nandare: 123}");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Point2d) {
  auto point_val = Point2d{CoordinateReferenceSystem::WGS84_2d, 1.0, 2.0};
  PropertyValue pv(point_val);

  ASSERT_EQ(pv.type(), PropertyValue::Type::Point2d);

  ASSERT_TRUE(pv.IsPoint2d());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);
  ASSERT_EQ(pv.ValuePoint2d(), point_val);
  ASSERT_THROW(pv.ValuePoint3d(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);
  ASSERT_EQ(cpv.ValuePoint2d(), point_val);
  ASSERT_THROW(cpv.ValuePoint3d(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "point");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "point({ x:1, y:2, srid:4326 })");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Point3d) {
  auto point_val = Point3d{CoordinateReferenceSystem::WGS84_3d, 1.0, 2.0, 3.0};
  PropertyValue pv(point_val);

  ASSERT_EQ(pv.type(), PropertyValue::Type::Point3d);

  ASSERT_TRUE(pv.IsPoint3d());
  ASSERT_TRUE(IsOnlyOneType(pv));

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(pv.ValuePoint2d(), PropertyValueException);
  ASSERT_EQ(pv.ValuePoint3d(), point_val);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);
  ASSERT_THROW(cpv.ValuePoint2d(), PropertyValueException);
  ASSERT_EQ(cpv.ValuePoint3d(), point_val);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "point");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "point({ x:1, y:2, z:3, srid:4979 })");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, CopyConstructor) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123)};
  PropertyValue::map_t map{{"nandare", PropertyValue(false)}};
  const auto zdt_dur = memgraph::utils::AsSysTime(23);
  std::vector<PropertyValue> data{
      PropertyValue(),
      PropertyValue(true),
      PropertyValue(123),
      PropertyValue(123.5),
      PropertyValue("nandare"),
      PropertyValue(vec),
      PropertyValue(map),
      PropertyValue(TemporalData(TemporalType::Date, 23)),
      PropertyValue(ZonedTemporalData(ZonedTemporalType::ZonedDateTime, zdt_dur, memgraph::utils::Timezone("Etc/UTC"))),
      PropertyValue(ZonedTemporalData(ZonedTemporalType::ZonedDateTime, zdt_dur,
                                      memgraph::utils::Timezone(std::chrono::minutes{-60}))),
      PropertyValue{Enum{EnumTypeId{2}, EnumValueId{42}}},
      PropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}},
      PropertyValue{Point2d{WGS84_2d, 3.0, 4.0}},
      PropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}},
      PropertyValue{Point3d{WGS84_3d, 4.0, 5.0, 6.0}},
  };

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
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123)};
  PropertyValue::map_t map{{"nandare", PropertyValue(false)}};
  const auto zdt_dur = memgraph::utils::AsSysTime(23);
  std::vector<PropertyValue> data{
      PropertyValue(),
      PropertyValue(true),
      PropertyValue(123),
      PropertyValue(123.5),
      PropertyValue("nandare"),
      PropertyValue(vec),
      PropertyValue(map),
      PropertyValue(TemporalData(TemporalType::Date, 23)),
      PropertyValue(ZonedTemporalData(ZonedTemporalType::ZonedDateTime, zdt_dur, memgraph::utils::Timezone("Etc/UTC"))),
      PropertyValue(ZonedTemporalData(ZonedTemporalType::ZonedDateTime, zdt_dur,
                                      memgraph::utils::Timezone(std::chrono::minutes{-60}))),
      PropertyValue{Enum{EnumTypeId{2}, EnumValueId{42}}},
      PropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}},
      PropertyValue{Point2d{WGS84_2d, 3.0, 4.0}},
      PropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}},
      PropertyValue{Point3d{WGS84_3d, 4.0, 5.0, 6.0}},
  };

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
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123)};
  PropertyValue::map_t map{{"nandare", PropertyValue(false)}};
  const auto zdt_dur = memgraph::utils::AsSysTime(23);
  std::vector<PropertyValue> data{
      PropertyValue(),
      PropertyValue(true),
      PropertyValue(123),
      PropertyValue(123.5),
      PropertyValue("nandare"),
      PropertyValue(vec),
      PropertyValue(map),
      PropertyValue(TemporalData(TemporalType::Date, 23)),
      PropertyValue(ZonedTemporalData(ZonedTemporalType::ZonedDateTime, zdt_dur, memgraph::utils::Timezone("Etc/UTC"))),
      PropertyValue(ZonedTemporalData(ZonedTemporalType::ZonedDateTime, zdt_dur,
                                      memgraph::utils::Timezone(std::chrono::minutes{-60}))),
      PropertyValue{Enum{EnumTypeId{2}, EnumValueId{42}}},
      PropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}},
      PropertyValue{Point2d{WGS84_2d, 3.0, 4.0}},
      PropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}},
      PropertyValue{Point3d{WGS84_3d, 4.0, 5.0, 6.0}},
  };

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
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123)};
  PropertyValue::map_t map{{"nandare", PropertyValue(false)}};
  const auto zdt_dur = memgraph::utils::AsSysTime(23);
  std::vector<PropertyValue> data{
      PropertyValue(),
      PropertyValue(true),
      PropertyValue(123),
      PropertyValue(123.5),
      PropertyValue("nandare"),
      PropertyValue(vec),
      PropertyValue(map),
      PropertyValue(TemporalData(TemporalType::Date, 23)),
      PropertyValue(ZonedTemporalData(ZonedTemporalType::ZonedDateTime, zdt_dur, memgraph::utils::Timezone("Etc/UTC"))),
      PropertyValue(ZonedTemporalData(ZonedTemporalType::ZonedDateTime, zdt_dur,
                                      memgraph::utils::Timezone(std::chrono::minutes{-60}))),
      PropertyValue{Enum{EnumTypeId{2}, EnumValueId{42}}},
      PropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}},
      PropertyValue{Point2d{WGS84_2d, 3.0, 4.0}},
      PropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}},
      PropertyValue{Point3d{WGS84_3d, 4.0, 5.0, 6.0}},
  };

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
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123)};
  auto map = PropertyValue::map_t{{"nandare", PropertyValue(false)}};
  auto enum_val = Enum{EnumTypeId{2}, EnumValueId{42}};
  std::vector<PropertyValue> data{
      PropertyValue(),
      PropertyValue(true),
      PropertyValue(123),
      PropertyValue(123.5),
      PropertyValue("nandare"),
      PropertyValue(vec),
      PropertyValue(map),
      PropertyValue{enum_val},
      PropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}},
      PropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}},
  };
  for (const auto &item1 : data) {
    for (const auto &item2 : data) {
      if (item1.type() == item2.type()) {
        ASSERT_TRUE(item1 == item2);
      } else {
        ASSERT_FALSE(item1 == item2);
      }
    }
  }
}

TEST(PropertyValue, EqualMap) {
  auto a = PropertyValue(PropertyValue::map_t());
  auto b = PropertyValue(PropertyValue::map_t{{"id", PropertyValue(5)}});
  auto c = PropertyValue(PropertyValue::map_t{{"id", PropertyValue(10)}});

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
  auto map = PropertyValue::map_t{{"nandare", PropertyValue(false)}};
  auto enum_val = Enum{EnumTypeId{2}, EnumValueId{42}};
  std::vector<PropertyValue> data{
      PropertyValue(),
      PropertyValue(true),
      PropertyValue(123),
      PropertyValue(123.5),
      PropertyValue("nandare"),
      PropertyValue(vec),
      PropertyValue(map),
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

  auto test_string = sut_t::string_t{"long long long long long string", std::pmr::new_delete_resource()};
  EXPECT_THROW(sut_t(test_string, nmr), std::bad_alloc);

  auto test_list = sut_t::list_t{{test_number}, std::pmr::new_delete_resource()};
  EXPECT_THROW(sut_t(test_list, nmr), std::bad_alloc);

  auto test_map = sut_t::map_t{{std::pair(test_string, test_number)}, std::pmr::new_delete_resource()};
  EXPECT_THROW(sut_t(test_map, nmr), std::bad_alloc);

  {
    auto sut = sut_t{std::pmr::polymorphic_allocator<>(nmr)};
    auto string_cpy = sut_t(test_string, std::pmr::new_delete_resource());
    EXPECT_THROW((sut = string_cpy), std::bad_alloc);
    EXPECT_THROW((sut = std::move(string_cpy)), std::bad_alloc);
  }

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
  auto const test_map = sut_t{sut_t::map_t{std::pair{raw_test_string, test_number}}};

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
    ASSERT_TRUE(as_pv.ValueMap().contains(raw_test_string));
    ASSERT_EQ(as_pv.ValueMap().at(raw_test_string).ValueInt(), raw_test_int);

    /// Map -> regular to pmr
    auto const as_pmr_pv = sut_t{as_pv};
    ASSERT_EQ(as_pmr_pv.ValueMap().size(), 1);
    ASSERT_TRUE(as_pmr_pv.ValueMap().contains(raw_test_string));
    ASSERT_EQ(as_pmr_pv.ValueMap().at(raw_test_string).ValueInt(), raw_test_int);
  }
}
