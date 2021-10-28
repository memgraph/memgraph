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

#include "storage/v2/property_value.hpp"
#include "storage/v2/replication/slk.hpp"

#include "slk_common.hpp"
#include "storage/v2/temporal.hpp"

TEST(SlkAdvanced, PropertyValueList) {
  std::vector<storage::PropertyValue> original{
      storage::PropertyValue("hello world!"),
      storage::PropertyValue(5),
      storage::PropertyValue(1.123423),
      storage::PropertyValue(true),
      storage::PropertyValue(),
      storage::PropertyValue(storage::TemporalData(storage::TemporalType::Date, 23))};
  ASSERT_EQ(original[0].type(), storage::PropertyValue::Type::String);
  ASSERT_EQ(original[1].type(), storage::PropertyValue::Type::Int);
  ASSERT_EQ(original[2].type(), storage::PropertyValue::Type::Double);
  ASSERT_EQ(original[3].type(), storage::PropertyValue::Type::Bool);
  ASSERT_EQ(original[4].type(), storage::PropertyValue::Type::Null);
  ASSERT_EQ(original[5].type(), storage::PropertyValue::Type::TemporalData);

  slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  slk::Save(original, builder);

  std::vector<storage::PropertyValue> decoded;
  auto reader = loopback.GetReader();
  slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}

TEST(SlkAdvanced, PropertyValueMap) {
  std::map<std::string, storage::PropertyValue> original{
      {"hello", storage::PropertyValue("world")},
      {"number", storage::PropertyValue(5)},
      {"real", storage::PropertyValue(1.123423)},
      {"truth", storage::PropertyValue(true)},
      {"nothing", storage::PropertyValue()},
      {"date", storage::PropertyValue(storage::TemporalData(storage::TemporalType::Date, 23))}};
  ASSERT_EQ(original["hello"].type(), storage::PropertyValue::Type::String);
  ASSERT_EQ(original["number"].type(), storage::PropertyValue::Type::Int);
  ASSERT_EQ(original["real"].type(), storage::PropertyValue::Type::Double);
  ASSERT_EQ(original["truth"].type(), storage::PropertyValue::Type::Bool);
  ASSERT_EQ(original["nothing"].type(), storage::PropertyValue::Type::Null);
  ASSERT_EQ(original["date"].type(), storage::PropertyValue::Type::TemporalData);

  slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  slk::Save(original, builder);

  std::map<std::string, storage::PropertyValue> decoded;
  auto reader = loopback.GetReader();
  slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}

TEST(SlkAdvanced, PropertyValueComplex) {
  std::vector<storage::PropertyValue> vec_v{
      storage::PropertyValue("hello world!"),
      storage::PropertyValue(5),
      storage::PropertyValue(1.123423),
      storage::PropertyValue(true),
      storage::PropertyValue(),
      storage::PropertyValue(storage::TemporalData(storage::TemporalType::Date, 23))};
  ASSERT_EQ(vec_v[0].type(), storage::PropertyValue::Type::String);
  ASSERT_EQ(vec_v[1].type(), storage::PropertyValue::Type::Int);
  ASSERT_EQ(vec_v[2].type(), storage::PropertyValue::Type::Double);
  ASSERT_EQ(vec_v[3].type(), storage::PropertyValue::Type::Bool);
  ASSERT_EQ(vec_v[4].type(), storage::PropertyValue::Type::Null);
  ASSERT_EQ(vec_v[5].type(), storage::PropertyValue::Type::TemporalData);

  std::map<std::string, storage::PropertyValue> map_v{
      {"hello", storage::PropertyValue("world")},
      {"number", storage::PropertyValue(5)},
      {"real", storage::PropertyValue(1.123423)},
      {"truth", storage::PropertyValue(true)},
      {"nothing", storage::PropertyValue()},
      {"date", storage::PropertyValue(storage::TemporalData(storage::TemporalType::Date, 23))}};
  ASSERT_EQ(map_v["hello"].type(), storage::PropertyValue::Type::String);
  ASSERT_EQ(map_v["number"].type(), storage::PropertyValue::Type::Int);
  ASSERT_EQ(map_v["real"].type(), storage::PropertyValue::Type::Double);
  ASSERT_EQ(map_v["truth"].type(), storage::PropertyValue::Type::Bool);
  ASSERT_EQ(map_v["nothing"].type(), storage::PropertyValue::Type::Null);
  ASSERT_EQ(map_v["date"].type(), storage::PropertyValue::Type::TemporalData);

  storage::PropertyValue original(
      std::vector<storage::PropertyValue>{storage::PropertyValue(vec_v), storage::PropertyValue(map_v)});
  ASSERT_EQ(original.type(), storage::PropertyValue::Type::List);

  slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  slk::Save(original, builder);

  storage::PropertyValue decoded;
  auto reader = loopback.GetReader();
  slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}
