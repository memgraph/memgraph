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

#include "coordination/coordinator_slk.hpp"
#include "io/network/endpoint.hpp"
#include "replication_coordination_glue/mode.hpp"
#include "slk_common.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/replication/slk.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/temporal.hpp"
#include "utils/uuid.hpp"

import memgraph.coordination.coordinator_communication_config;

using memgraph::io::network::Endpoint;

TEST(SlkAdvanced, PropertyValueList) {
  const auto sample_duration = memgraph::utils::AsSysTime(23);
  std::vector<memgraph::storage::ExternalPropertyValue> original{
      memgraph::storage::ExternalPropertyValue("hello world!"),
      memgraph::storage::ExternalPropertyValue(5),
      memgraph::storage::ExternalPropertyValue(1.123423),
      memgraph::storage::ExternalPropertyValue(true),
      memgraph::storage::ExternalPropertyValue(),
      memgraph::storage::ExternalPropertyValue(
          memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23)),
      memgraph::storage::ExternalPropertyValue(
          memgraph::storage::ZonedTemporalData(memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                                               memgraph::utils::Timezone(std::chrono::minutes{60})))};
  ASSERT_EQ(original[0].type(), memgraph::storage::ExternalPropertyValue::Type::String);
  ASSERT_EQ(original[1].type(), memgraph::storage::ExternalPropertyValue::Type::Int);
  ASSERT_EQ(original[2].type(), memgraph::storage::ExternalPropertyValue::Type::Double);
  ASSERT_EQ(original[3].type(), memgraph::storage::ExternalPropertyValue::Type::Bool);
  ASSERT_EQ(original[4].type(), memgraph::storage::ExternalPropertyValue::Type::Null);
  ASSERT_EQ(original[5].type(), memgraph::storage::ExternalPropertyValue::Type::TemporalData);
  ASSERT_EQ(original[6].type(), memgraph::storage::ExternalPropertyValue::Type::ZonedTemporalData);

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);

  std::vector<memgraph::storage::ExternalPropertyValue> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}

TEST(SlkAdvanced, PropertyValueMap) {
  const auto sample_duration = memgraph::utils::AsSysTime(23);
  memgraph::storage::ExternalPropertyValue::map_t original{
      {"hello", memgraph::storage::ExternalPropertyValue("world")},
      {"number", memgraph::storage::ExternalPropertyValue(5)},
      {"real", memgraph::storage::ExternalPropertyValue(1.123423)},
      {"truth", memgraph::storage::ExternalPropertyValue(true)},
      {"nothing", memgraph::storage::ExternalPropertyValue()},
      {"date", memgraph::storage::ExternalPropertyValue(
                   memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23))},
      {"zoned_temporal", memgraph::storage::ExternalPropertyValue(memgraph::storage::ZonedTemporalData(
                             memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                             memgraph::utils::Timezone("Europe/Zagreb")))}};
  ASSERT_EQ(original["hello"].type(), memgraph::storage::ExternalPropertyValue::Type::String);
  ASSERT_EQ(original["number"].type(), memgraph::storage::ExternalPropertyValue::Type::Int);
  ASSERT_EQ(original["real"].type(), memgraph::storage::ExternalPropertyValue::Type::Double);
  ASSERT_EQ(original["truth"].type(), memgraph::storage::ExternalPropertyValue::Type::Bool);
  ASSERT_EQ(original["nothing"].type(), memgraph::storage::ExternalPropertyValue::Type::Null);
  ASSERT_EQ(original["date"].type(), memgraph::storage::ExternalPropertyValue::Type::TemporalData);
  ASSERT_EQ(original["zoned_temporal"].type(), memgraph::storage::ExternalPropertyValue::Type::ZonedTemporalData);

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);

  memgraph::storage::ExternalPropertyValue::map_t decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}

TEST(SlkAdvanced, PropertyValueComplex) {
  const auto sample_duration = memgraph::utils::AsSysTime(23);
  std::vector<memgraph::storage::ExternalPropertyValue> vec_v{
      memgraph::storage::ExternalPropertyValue("hello world!"),
      memgraph::storage::ExternalPropertyValue(5),
      memgraph::storage::ExternalPropertyValue(1.123423),
      memgraph::storage::ExternalPropertyValue(true),
      memgraph::storage::ExternalPropertyValue(),
      memgraph::storage::ExternalPropertyValue(
          memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23)),
      memgraph::storage::ExternalPropertyValue(
          memgraph::storage::ZonedTemporalData(memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                                               memgraph::utils::Timezone("Europe/Zagreb")))};
  ASSERT_EQ(vec_v[0].type(), memgraph::storage::ExternalPropertyValue::Type::String);
  ASSERT_EQ(vec_v[1].type(), memgraph::storage::ExternalPropertyValue::Type::Int);
  ASSERT_EQ(vec_v[2].type(), memgraph::storage::ExternalPropertyValue::Type::Double);
  ASSERT_EQ(vec_v[3].type(), memgraph::storage::ExternalPropertyValue::Type::Bool);
  ASSERT_EQ(vec_v[4].type(), memgraph::storage::ExternalPropertyValue::Type::Null);
  ASSERT_EQ(vec_v[5].type(), memgraph::storage::ExternalPropertyValue::Type::TemporalData);
  ASSERT_EQ(vec_v[6].type(), memgraph::storage::ExternalPropertyValue::Type::ZonedTemporalData);

  memgraph::storage::ExternalPropertyValue::map_t map_v{
      {"hello", memgraph::storage::ExternalPropertyValue("world")},
      {"number", memgraph::storage::ExternalPropertyValue(5)},
      {"real", memgraph::storage::ExternalPropertyValue(1.123423)},
      {"truth", memgraph::storage::ExternalPropertyValue(true)},
      {"nothing", memgraph::storage::ExternalPropertyValue()},
      {"date", memgraph::storage::ExternalPropertyValue(
                   memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23))},
      {"zoned_temporal", memgraph::storage::ExternalPropertyValue(memgraph::storage::ZonedTemporalData(
                             memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                             memgraph::utils::Timezone("Europe/Zagreb")))}};
  ASSERT_EQ(map_v["hello"].type(), memgraph::storage::ExternalPropertyValue::Type::String);
  ASSERT_EQ(map_v["number"].type(), memgraph::storage::ExternalPropertyValue::Type::Int);
  ASSERT_EQ(map_v["real"].type(), memgraph::storage::ExternalPropertyValue::Type::Double);
  ASSERT_EQ(map_v["truth"].type(), memgraph::storage::ExternalPropertyValue::Type::Bool);
  ASSERT_EQ(map_v["nothing"].type(), memgraph::storage::ExternalPropertyValue::Type::Null);
  ASSERT_EQ(map_v["date"].type(), memgraph::storage::ExternalPropertyValue::Type::TemporalData);
  ASSERT_EQ(map_v["zoned_temporal"].type(), memgraph::storage::ExternalPropertyValue::Type::ZonedTemporalData);

  memgraph::storage::ExternalPropertyValue original(std::vector<memgraph::storage::ExternalPropertyValue>{
      memgraph::storage::ExternalPropertyValue(vec_v), memgraph::storage::ExternalPropertyValue(map_v)});
  ASSERT_EQ(original.type(), memgraph::storage::ExternalPropertyValue::Type::List);

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);

  memgraph::storage::ExternalPropertyValue decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}

TEST(SlkAdvanced, ReplicationClientConfigs) {
  using ReplicationClientInfo = memgraph::coordination::ReplicationClientInfo;
  using ReplicationClientInfoVec = std::vector<ReplicationClientInfo>;
  using ReplicationMode = memgraph::replication_coordination_glue::ReplicationMode;

  ReplicationClientInfoVec original{ReplicationClientInfo{.instance_name = "replica1",
                                                          .replication_mode = ReplicationMode::SYNC,
                                                          .replication_server = Endpoint{"127.0.0.1", 10000}},
                                    ReplicationClientInfo{.instance_name = "replica2",
                                                          .replication_mode = ReplicationMode::ASYNC,
                                                          .replication_server = Endpoint{"127.0.0.1", 10010}},
                                    ReplicationClientInfo{.instance_name = "replica3",
                                                          .replication_mode = ReplicationMode::ASYNC,
                                                          .replication_server = Endpoint{"127.0.0.1", 10011}}};

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);

  ReplicationClientInfoVec decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}

TEST(SlkAdvanced, ExternalPropertyValueIntList) {
  memgraph::storage::ExternalPropertyValue::int_list_t original{1, 2, 3, 4, 5};
  memgraph::storage::ExternalPropertyValue original_value(original);

  memgraph::slk::Loopback loopback;
  auto *builder = loopback.GetBuilder();
  memgraph::slk::Save(original_value, builder);

  memgraph::storage::ExternalPropertyValue decoded_value;
  auto *reader = loopback.GetReader();
  memgraph::slk::Load(&decoded_value, reader);

  ASSERT_EQ(decoded_value.type(), memgraph::storage::ExternalPropertyValue::Type::IntList);
  const auto &decoded_list = decoded_value.ValueIntList();
  ASSERT_EQ(original.size(), decoded_list.size());
  for (size_t i = 0; i < original.size(); ++i) {
    ASSERT_EQ(original[i], decoded_list[i]);
  }
}

TEST(SlkAdvanced, ExternalPropertyValueNumericList) {
  memgraph::storage::ExternalPropertyValue::numeric_list_t original{42, 3.14, 100, 2.718};
  memgraph::storage::ExternalPropertyValue original_value(original);

  memgraph::slk::Loopback loopback;
  auto *builder = loopback.GetBuilder();
  memgraph::slk::Save(original_value, builder);

  memgraph::storage::ExternalPropertyValue decoded_value;
  auto *reader = loopback.GetReader();
  memgraph::slk::Load(&decoded_value, reader);

  ASSERT_EQ(decoded_value.type(), memgraph::storage::ExternalPropertyValue::Type::NumericList);
  const auto &decoded_list = decoded_value.ValueNumericList();
  ASSERT_EQ(original.size(), decoded_list.size());

  ASSERT_TRUE(std::holds_alternative<int>(decoded_list[0]));
  ASSERT_EQ(std::get<int>(decoded_list[0]), 42);

  ASSERT_TRUE(std::holds_alternative<double>(decoded_list[1]));
  ASSERT_DOUBLE_EQ(std::get<double>(decoded_list[1]), 3.14);

  ASSERT_TRUE(std::holds_alternative<int>(decoded_list[2]));
  ASSERT_EQ(std::get<int>(decoded_list[2]), 100);

  ASSERT_TRUE(std::holds_alternative<double>(decoded_list[3]));
  ASSERT_DOUBLE_EQ(std::get<double>(decoded_list[3]), 2.718);
}

TEST(SlkAdvanced, ExternalPropertyValueDoubleList) {
  memgraph::storage::ExternalPropertyValue::double_list_t original{1.1, 2.2, 3.3, 4.4, 5.5};
  memgraph::storage::ExternalPropertyValue original_value(original);

  memgraph::slk::Loopback loopback;
  auto *builder = loopback.GetBuilder();
  memgraph::slk::Save(original_value, builder);

  memgraph::storage::ExternalPropertyValue decoded_value;
  auto *reader = loopback.GetReader();
  memgraph::slk::Load(&decoded_value, reader);

  ASSERT_EQ(decoded_value.type(), memgraph::storage::ExternalPropertyValue::Type::DoubleList);
  const auto &decoded_list = decoded_value.ValueDoubleList();
  ASSERT_EQ(original.size(), decoded_list.size());
  for (size_t i = 0; i < original.size(); ++i) {
    ASSERT_DOUBLE_EQ(original[i], decoded_list[i]);
  }
}

TEST(SlkAdvanced, ExternalPropertyValueList) {
  memgraph::storage::ExternalPropertyValue::list_t original;
  original.emplace_back("hello");
  original.emplace_back(42);
  original.emplace_back(3.14);
  original.emplace_back(true);

  memgraph::storage::ExternalPropertyValue original_value(original);

  memgraph::slk::Loopback loopback;
  auto *builder = loopback.GetBuilder();
  memgraph::slk::Save(original_value, builder);

  memgraph::storage::ExternalPropertyValue decoded_value;
  auto *reader = loopback.GetReader();
  memgraph::slk::Load(&decoded_value, reader);

  ASSERT_EQ(decoded_value.type(), memgraph::storage::ExternalPropertyValue::Type::List);
  const auto &decoded_list = decoded_value.ValueList();
  ASSERT_EQ(original.size(), decoded_list.size());

  ASSERT_EQ(decoded_list[0].type(), memgraph::storage::ExternalPropertyValue::Type::String);
  ASSERT_EQ(decoded_list[0].ValueString(), "hello");

  ASSERT_EQ(decoded_list[1].type(), memgraph::storage::ExternalPropertyValue::Type::Int);
  ASSERT_EQ(decoded_list[1].ValueInt(), 42);

  ASSERT_EQ(decoded_list[2].type(), memgraph::storage::ExternalPropertyValue::Type::Double);
  ASSERT_DOUBLE_EQ(decoded_list[2].ValueDouble(), 3.14);

  ASSERT_EQ(decoded_list[3].type(), memgraph::storage::ExternalPropertyValue::Type::Bool);
  ASSERT_EQ(decoded_list[3].ValueBool(), true);
}
