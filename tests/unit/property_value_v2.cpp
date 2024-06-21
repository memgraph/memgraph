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

#include <sstream>

#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"

using namespace memgraph::storage;

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Null) {
  PropertyValue pv;

  ASSERT_EQ(pv.type(), PropertyValue::Type::Null);

  ASSERT_TRUE(pv.IsNull());
  ASSERT_FALSE(pv.IsBool());
  ASSERT_FALSE(pv.IsInt());
  ASSERT_FALSE(pv.IsDouble());
  ASSERT_FALSE(pv.IsString());
  ASSERT_FALSE(pv.IsList());
  ASSERT_FALSE(pv.IsMap());
  ASSERT_FALSE(pv.IsEnum());

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);

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

  ASSERT_FALSE(pv.IsNull());
  ASSERT_TRUE(pv.IsBool());
  ASSERT_FALSE(pv.IsInt());
  ASSERT_FALSE(pv.IsDouble());
  ASSERT_FALSE(pv.IsString());
  ASSERT_FALSE(pv.IsList());
  ASSERT_FALSE(pv.IsMap());
  ASSERT_FALSE(pv.IsEnum());

  ASSERT_EQ(pv.ValueBool(), false);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_EQ(cpv.ValueBool(), false);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);

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

  ASSERT_FALSE(pv.IsNull());
  ASSERT_FALSE(pv.IsBool());
  ASSERT_TRUE(pv.IsInt());
  ASSERT_FALSE(pv.IsDouble());
  ASSERT_FALSE(pv.IsString());
  ASSERT_FALSE(pv.IsList());
  ASSERT_FALSE(pv.IsMap());
  ASSERT_FALSE(pv.IsEnum());

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_EQ(pv.ValueInt(), 123L);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_EQ(cpv.ValueInt(), 123L);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);

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

  ASSERT_FALSE(pv.IsNull());
  ASSERT_FALSE(pv.IsBool());
  ASSERT_FALSE(pv.IsInt());
  ASSERT_TRUE(pv.IsDouble());
  ASSERT_FALSE(pv.IsString());
  ASSERT_FALSE(pv.IsList());
  ASSERT_FALSE(pv.IsMap());
  ASSERT_FALSE(pv.IsEnum());

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_EQ(pv.ValueDouble(), 123.5);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_EQ(cpv.ValueDouble(), 123.5);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);

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

  ASSERT_FALSE(pv.IsNull());
  ASSERT_FALSE(pv.IsBool());
  ASSERT_FALSE(pv.IsInt());
  ASSERT_FALSE(pv.IsDouble());
  ASSERT_FALSE(pv.IsString());
  ASSERT_FALSE(pv.IsList());
  ASSERT_FALSE(pv.IsMap());
  ASSERT_TRUE(pv.IsEnum());

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(pv.ValueString(), PropertyValueException);
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_EQ(pv.ValueEnum(), enum_val);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_THROW(cpv.ValueString(), PropertyValueException);
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_EQ(cpv.ValueEnum(), enum_val);

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

  ASSERT_FALSE(pv.IsNull());
  ASSERT_FALSE(pv.IsBool());
  ASSERT_FALSE(pv.IsInt());
  ASSERT_FALSE(pv.IsDouble());
  ASSERT_TRUE(pv.IsString());
  ASSERT_FALSE(pv.IsList());
  ASSERT_FALSE(pv.IsMap());
  ASSERT_FALSE(pv.IsEnum());

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_EQ(pv.ValueString(), "nandare");
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_EQ(cpv.ValueString(), "nandare");
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "string");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "nandare");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, StringMove) {
  std::string str("nandare");
  PropertyValue pv(std::move(str));

  ASSERT_EQ(str, "");

  ASSERT_EQ(pv.type(), PropertyValue::Type::String);

  ASSERT_FALSE(pv.IsNull());
  ASSERT_FALSE(pv.IsBool());
  ASSERT_FALSE(pv.IsInt());
  ASSERT_FALSE(pv.IsDouble());
  ASSERT_TRUE(pv.IsString());
  ASSERT_FALSE(pv.IsList());
  ASSERT_FALSE(pv.IsMap());
  ASSERT_FALSE(pv.IsEnum());

  ASSERT_THROW(pv.ValueBool(), PropertyValueException);
  ASSERT_THROW(pv.ValueInt(), PropertyValueException);
  ASSERT_THROW(pv.ValueDouble(), PropertyValueException);
  ASSERT_EQ(pv.ValueString(), "nandare");
  ASSERT_THROW(pv.ValueList(), PropertyValueException);
  ASSERT_THROW(pv.ValueMap(), PropertyValueException);
  ASSERT_THROW(pv.ValueEnum(), PropertyValueException);

  const auto &cpv = pv;

  ASSERT_THROW(cpv.ValueBool(), PropertyValueException);
  ASSERT_THROW(cpv.ValueInt(), PropertyValueException);
  ASSERT_THROW(cpv.ValueDouble(), PropertyValueException);
  ASSERT_EQ(cpv.ValueString(), "nandare");
  ASSERT_THROW(cpv.ValueList(), PropertyValueException);
  ASSERT_THROW(cpv.ValueMap(), PropertyValueException);
  ASSERT_THROW(cpv.ValueEnum(), PropertyValueException);

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "string");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "nandare");
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

  ASSERT_FALSE(pv.IsNull());
  ASSERT_FALSE(pv.IsBool());
  ASSERT_FALSE(pv.IsInt());
  ASSERT_FALSE(pv.IsDouble());
  ASSERT_FALSE(pv.IsString());
  ASSERT_TRUE(pv.IsList());
  ASSERT_FALSE(pv.IsMap());
  ASSERT_FALSE(pv.IsEnum());

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

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "list");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "[nandare, 123]");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, ListMove) {
  std::vector<PropertyValue> vec{PropertyValue("nandare"), PropertyValue(123)};
  PropertyValue pv(std::move(vec));

  ASSERT_EQ(vec.size(), 0);

  ASSERT_EQ(pv.type(), PropertyValue::Type::List);

  ASSERT_FALSE(pv.IsNull());
  ASSERT_FALSE(pv.IsBool());
  ASSERT_FALSE(pv.IsInt());
  ASSERT_FALSE(pv.IsDouble());
  ASSERT_FALSE(pv.IsString());
  ASSERT_TRUE(pv.IsList());
  ASSERT_FALSE(pv.IsMap());
  ASSERT_FALSE(pv.IsEnum());

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

  {
    std::stringstream ss;
    ss << pv.type();
    ASSERT_EQ(ss.str(), "list");
  }
  {
    std::stringstream ss;
    ss << pv;
    ASSERT_EQ(ss.str(), "[nandare, 123]");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, MapCopy) {
  std::map<std::string, PropertyValue> map{{"nandare", PropertyValue(123)}};
  PropertyValue pv(map);

  ASSERT_EQ(map.size(), 1);
  ASSERT_EQ(map.at("nandare").ValueInt(), 123);

  ASSERT_EQ(pv.type(), PropertyValue::Type::Map);

  ASSERT_FALSE(pv.IsNull());
  ASSERT_FALSE(pv.IsBool());
  ASSERT_FALSE(pv.IsInt());
  ASSERT_FALSE(pv.IsDouble());
  ASSERT_FALSE(pv.IsString());
  ASSERT_FALSE(pv.IsList());
  ASSERT_TRUE(pv.IsMap());
  ASSERT_FALSE(pv.IsEnum());

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
  std::map<std::string, PropertyValue> map{{"nandare", PropertyValue(123)}};
  PropertyValue pv(std::move(map));

  ASSERT_EQ(map.size(), 0);

  ASSERT_EQ(pv.type(), PropertyValue::Type::Map);

  ASSERT_FALSE(pv.IsNull());
  ASSERT_FALSE(pv.IsBool());
  ASSERT_FALSE(pv.IsInt());
  ASSERT_FALSE(pv.IsDouble());
  ASSERT_FALSE(pv.IsString());
  ASSERT_FALSE(pv.IsList());
  ASSERT_TRUE(pv.IsMap());
  ASSERT_FALSE(pv.IsEnum());

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
TEST(PropertyValue, CopyConstructor) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123)};
  std::map<std::string, PropertyValue> map{{"nandare", PropertyValue(false)}};
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
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, MoveConstructor) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123)};
  std::map<std::string, PropertyValue> map{{"nandare", PropertyValue(false)}};
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
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, CopyAssignment) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123)};
  std::map<std::string, PropertyValue> map{{"nandare", PropertyValue(false)}};
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
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, MoveAssignment) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123)};
  std::map<std::string, PropertyValue> map{{"nandare", PropertyValue(false)}};
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
  auto map = std::map<std::string, PropertyValue>{{"nandare", PropertyValue(false)}};
  auto enum_val = Enum{EnumTypeId{2}, EnumValueId{42}};
  std::vector<PropertyValue> data{
      PropertyValue(),          PropertyValue(true), PropertyValue(123), PropertyValue(123.5),
      PropertyValue("nandare"), PropertyValue(vec),  PropertyValue(map), PropertyValue{enum_val},
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

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(PropertyValue, Less) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123)};
  auto map = std::map<std::string, PropertyValue>{{"nandare", PropertyValue(false)}};
  auto enum_val = Enum{EnumTypeId{2}, EnumValueId{42}};
  std::vector<PropertyValue> data{
      PropertyValue(),          PropertyValue(true), PropertyValue(123), PropertyValue(123.5),
      PropertyValue("nandare"), PropertyValue(vec),  PropertyValue(map), PropertyValue{enum_val},
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
