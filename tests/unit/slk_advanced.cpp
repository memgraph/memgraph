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

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_slk.hpp"
#include "io/network/endpoint.hpp"
#include "replication/config.hpp"
#include "replication_coordination_glue/mode.hpp"
#include "slk_common.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/replication/slk.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/temporal.hpp"

using memgraph::io::network::Endpoint;

TEST(SlkAdvanced, PropertyValueList) {
  const auto sample_duration = memgraph::utils::AsSysTime(23);
  std::vector<memgraph::storage::PropertyValue> original{
      memgraph::storage::PropertyValue("hello world!"),
      memgraph::storage::PropertyValue(5),
      memgraph::storage::PropertyValue(1.123423),
      memgraph::storage::PropertyValue(true),
      memgraph::storage::PropertyValue(),
      memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23)),
      memgraph::storage::PropertyValue(
          memgraph::storage::ZonedTemporalData(memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                                               memgraph::utils::Timezone(std::chrono::minutes{60})))};
  ASSERT_EQ(original[0].type(), memgraph::storage::PropertyValue::Type::String);
  ASSERT_EQ(original[1].type(), memgraph::storage::PropertyValue::Type::Int);
  ASSERT_EQ(original[2].type(), memgraph::storage::PropertyValue::Type::Double);
  ASSERT_EQ(original[3].type(), memgraph::storage::PropertyValue::Type::Bool);
  ASSERT_EQ(original[4].type(), memgraph::storage::PropertyValue::Type::Null);
  ASSERT_EQ(original[5].type(), memgraph::storage::PropertyValue::Type::TemporalData);
  ASSERT_EQ(original[6].type(), memgraph::storage::PropertyValue::Type::ZonedTemporalData);

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);

  std::vector<memgraph::storage::PropertyValue> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}

TEST(SlkAdvanced, PropertyValueMap) {
  const auto sample_duration = memgraph::utils::AsSysTime(23);
  std::map<std::string, memgraph::storage::PropertyValue> original{
      {"hello", memgraph::storage::PropertyValue("world")},
      {"number", memgraph::storage::PropertyValue(5)},
      {"real", memgraph::storage::PropertyValue(1.123423)},
      {"truth", memgraph::storage::PropertyValue(true)},
      {"nothing", memgraph::storage::PropertyValue()},
      {"date",
       memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23))},
      {"zoned_temporal", memgraph::storage::PropertyValue(memgraph::storage::ZonedTemporalData(
                             memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                             memgraph::utils::Timezone("Europe/Zagreb")))}};
  ASSERT_EQ(original["hello"].type(), memgraph::storage::PropertyValue::Type::String);
  ASSERT_EQ(original["number"].type(), memgraph::storage::PropertyValue::Type::Int);
  ASSERT_EQ(original["real"].type(), memgraph::storage::PropertyValue::Type::Double);
  ASSERT_EQ(original["truth"].type(), memgraph::storage::PropertyValue::Type::Bool);
  ASSERT_EQ(original["nothing"].type(), memgraph::storage::PropertyValue::Type::Null);
  ASSERT_EQ(original["date"].type(), memgraph::storage::PropertyValue::Type::TemporalData);
  ASSERT_EQ(original["zoned_temporal"].type(), memgraph::storage::PropertyValue::Type::ZonedTemporalData);

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);

  std::map<std::string, memgraph::storage::PropertyValue> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}

TEST(SlkAdvanced, PropertyValueComplex) {
  const auto sample_duration = memgraph::utils::AsSysTime(23);
  std::vector<memgraph::storage::PropertyValue> vec_v{
      memgraph::storage::PropertyValue("hello world!"),
      memgraph::storage::PropertyValue(5),
      memgraph::storage::PropertyValue(1.123423),
      memgraph::storage::PropertyValue(true),
      memgraph::storage::PropertyValue(),
      memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23)),
      memgraph::storage::PropertyValue(
          memgraph::storage::ZonedTemporalData(memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                                               memgraph::utils::Timezone("Europe/Zagreb")))};
  ASSERT_EQ(vec_v[0].type(), memgraph::storage::PropertyValue::Type::String);
  ASSERT_EQ(vec_v[1].type(), memgraph::storage::PropertyValue::Type::Int);
  ASSERT_EQ(vec_v[2].type(), memgraph::storage::PropertyValue::Type::Double);
  ASSERT_EQ(vec_v[3].type(), memgraph::storage::PropertyValue::Type::Bool);
  ASSERT_EQ(vec_v[4].type(), memgraph::storage::PropertyValue::Type::Null);
  ASSERT_EQ(vec_v[5].type(), memgraph::storage::PropertyValue::Type::TemporalData);
  ASSERT_EQ(vec_v[6].type(), memgraph::storage::PropertyValue::Type::ZonedTemporalData);

  std::map<std::string, memgraph::storage::PropertyValue> map_v{
      {"hello", memgraph::storage::PropertyValue("world")},
      {"number", memgraph::storage::PropertyValue(5)},
      {"real", memgraph::storage::PropertyValue(1.123423)},
      {"truth", memgraph::storage::PropertyValue(true)},
      {"nothing", memgraph::storage::PropertyValue()},
      {"date",
       memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23))},
      {"zoned_temporal", memgraph::storage::PropertyValue(memgraph::storage::ZonedTemporalData(
                             memgraph::storage::ZonedTemporalType::ZonedDateTime, sample_duration,
                             memgraph::utils::Timezone("Europe/Zagreb")))}};
  ASSERT_EQ(map_v["hello"].type(), memgraph::storage::PropertyValue::Type::String);
  ASSERT_EQ(map_v["number"].type(), memgraph::storage::PropertyValue::Type::Int);
  ASSERT_EQ(map_v["real"].type(), memgraph::storage::PropertyValue::Type::Double);
  ASSERT_EQ(map_v["truth"].type(), memgraph::storage::PropertyValue::Type::Bool);
  ASSERT_EQ(map_v["nothing"].type(), memgraph::storage::PropertyValue::Type::Null);
  ASSERT_EQ(map_v["date"].type(), memgraph::storage::PropertyValue::Type::TemporalData);
  ASSERT_EQ(map_v["zoned_temporal"].type(), memgraph::storage::PropertyValue::Type::ZonedTemporalData);

  memgraph::storage::PropertyValue original(std::vector<memgraph::storage::PropertyValue>{
      memgraph::storage::PropertyValue(vec_v), memgraph::storage::PropertyValue(map_v)});
  ASSERT_EQ(original.type(), memgraph::storage::PropertyValue::Type::List);

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);

  memgraph::storage::PropertyValue decoded;
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
