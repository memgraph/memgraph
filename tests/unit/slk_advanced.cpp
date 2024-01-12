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

#include "replication/config.hpp"
#include "replication/coordinator_slk.hpp"
#include "replication/mode.hpp"
#include "slk_common.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/replication/slk.hpp"
#include "storage/v2/temporal.hpp"

TEST(SlkAdvanced, PropertyValueList) {
  std::vector<memgraph::storage::PropertyValue> original{
      memgraph::storage::PropertyValue("hello world!"),
      memgraph::storage::PropertyValue(5),
      memgraph::storage::PropertyValue(1.123423),
      memgraph::storage::PropertyValue(true),
      memgraph::storage::PropertyValue(),
      memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23))};
  ASSERT_EQ(original[0].type(), memgraph::storage::PropertyValue::Type::String);
  ASSERT_EQ(original[1].type(), memgraph::storage::PropertyValue::Type::Int);
  ASSERT_EQ(original[2].type(), memgraph::storage::PropertyValue::Type::Double);
  ASSERT_EQ(original[3].type(), memgraph::storage::PropertyValue::Type::Bool);
  ASSERT_EQ(original[4].type(), memgraph::storage::PropertyValue::Type::Null);
  ASSERT_EQ(original[5].type(), memgraph::storage::PropertyValue::Type::TemporalData);

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);

  std::vector<memgraph::storage::PropertyValue> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}

TEST(SlkAdvanced, PropertyValueMap) {
  std::map<std::string, memgraph::storage::PropertyValue> original{
      {"hello", memgraph::storage::PropertyValue("world")},
      {"number", memgraph::storage::PropertyValue(5)},
      {"real", memgraph::storage::PropertyValue(1.123423)},
      {"truth", memgraph::storage::PropertyValue(true)},
      {"nothing", memgraph::storage::PropertyValue()},
      {"date",
       memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23))}};
  ASSERT_EQ(original["hello"].type(), memgraph::storage::PropertyValue::Type::String);
  ASSERT_EQ(original["number"].type(), memgraph::storage::PropertyValue::Type::Int);
  ASSERT_EQ(original["real"].type(), memgraph::storage::PropertyValue::Type::Double);
  ASSERT_EQ(original["truth"].type(), memgraph::storage::PropertyValue::Type::Bool);
  ASSERT_EQ(original["nothing"].type(), memgraph::storage::PropertyValue::Type::Null);
  ASSERT_EQ(original["date"].type(), memgraph::storage::PropertyValue::Type::TemporalData);

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);

  std::map<std::string, memgraph::storage::PropertyValue> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}

TEST(SlkAdvanced, PropertyValueComplex) {
  std::vector<memgraph::storage::PropertyValue> vec_v{
      memgraph::storage::PropertyValue("hello world!"),
      memgraph::storage::PropertyValue(5),
      memgraph::storage::PropertyValue(1.123423),
      memgraph::storage::PropertyValue(true),
      memgraph::storage::PropertyValue(),
      memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23))};
  ASSERT_EQ(vec_v[0].type(), memgraph::storage::PropertyValue::Type::String);
  ASSERT_EQ(vec_v[1].type(), memgraph::storage::PropertyValue::Type::Int);
  ASSERT_EQ(vec_v[2].type(), memgraph::storage::PropertyValue::Type::Double);
  ASSERT_EQ(vec_v[3].type(), memgraph::storage::PropertyValue::Type::Bool);
  ASSERT_EQ(vec_v[4].type(), memgraph::storage::PropertyValue::Type::Null);
  ASSERT_EQ(vec_v[5].type(), memgraph::storage::PropertyValue::Type::TemporalData);

  std::map<std::string, memgraph::storage::PropertyValue> map_v{
      {"hello", memgraph::storage::PropertyValue("world")},
      {"number", memgraph::storage::PropertyValue(5)},
      {"real", memgraph::storage::PropertyValue(1.123423)},
      {"truth", memgraph::storage::PropertyValue(true)},
      {"nothing", memgraph::storage::PropertyValue()},
      {"date",
       memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23))}};
  ASSERT_EQ(map_v["hello"].type(), memgraph::storage::PropertyValue::Type::String);
  ASSERT_EQ(map_v["number"].type(), memgraph::storage::PropertyValue::Type::Int);
  ASSERT_EQ(map_v["real"].type(), memgraph::storage::PropertyValue::Type::Double);
  ASSERT_EQ(map_v["truth"].type(), memgraph::storage::PropertyValue::Type::Bool);
  ASSERT_EQ(map_v["nothing"].type(), memgraph::storage::PropertyValue::Type::Null);
  ASSERT_EQ(map_v["date"].type(), memgraph::storage::PropertyValue::Type::TemporalData);

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
  using ReplicationClientConfig = memgraph::replication::ReplicationClientConfig;
  using ReplicationClientConfigs = std::vector<ReplicationClientConfig>;
  using ReplicationMode = memgraph::replication::ReplicationMode;
  using SSL = ReplicationClientConfig::SSL;

  ReplicationClientConfigs original{
      ReplicationClientConfig{.name = "replica1",
                              .mode = ReplicationMode::SYNC,
                              .ip_address = "127.0.0.1",
                              .port = 10000,
                              .replica_check_frequency = std::chrono::seconds(1),
                              .ssl = SSL{.key_file = "test_file", .cert_file = "cert_file"}},
      ReplicationClientConfig{.name = "replica2",
                              .mode = ReplicationMode::ASYNC,
                              .ip_address = "127.0.1.1",
                              .port = 10010,
                              .replica_check_frequency = std::chrono::seconds(5),
                              .ssl = SSL{.key_file = "test_file_1", .cert_file = "cert_file_1"}},
      ReplicationClientConfig{.name = "replica3",
                              .mode = ReplicationMode::ASYNC,
                              .ip_address = "127.1.1.1",
                              .port = 1110,
                              .replica_check_frequency = std::chrono::seconds(2),
                              .ssl = SSL{.key_file = "test_file_11", .cert_file = "cert_file_00"}}};

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);

  ReplicationClientConfigs decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}
