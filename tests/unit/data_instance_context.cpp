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

#include "coordination/data_instance_context.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include "utils/uuid.hpp"

#ifdef MG_ENTERPRISE

using memgraph::coordination::DataInstanceConfig;
using memgraph::coordination::DataInstanceContext;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::replication_coordination_glue::ReplicationRole;
using memgraph::utils::UUID;

class DataInstanceContextTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!std::filesystem::exists(test_folder_)) {
      std::filesystem::create_directories(test_folder_);
    }
  }

  void TearDown() override {
    if (!std::filesystem::exists(test_folder_)) return;
    std::filesystem::remove_all(test_folder_);
  }

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "MG_tests_unit_data_instance_context"};
};

TEST_F(DataInstanceContextTest, ToJsonSerialization) {
  DataInstanceConfig config{.instance_name = "instance3",
                            .mgt_server = {"127.0.0.1", 10112},
                            .bolt_server = {"127.0.0.1", 7687},
                            .replication_client_info = {.instance_name = "instance_name",
                                                        .replication_mode = ReplicationMode::ASYNC,
                                                        .replication_server = {"127.0.0.1", 10001}},
                            .instance_health_check_frequency_sec = std::chrono::seconds{1},
                            .instance_down_timeout_sec = std::chrono::seconds{5},
                            .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                            .ssl = std::nullopt};

  nlohmann::json j;
  memgraph::coordination::to_json(j, config);

  EXPECT_EQ(j["instance_name"], "instance3");
  EXPECT_EQ(j["instance_health_check_frequency_sec"], 1);
  EXPECT_EQ(j["instance_down_timeout_sec"], 5);
  EXPECT_EQ(j["instance_get_uuid_frequency_sec"], 10);
}

TEST_F(DataInstanceContextTest, FromJsonDeserialization) {
  DataInstanceConfig config{.instance_name = "instance3",
                            .mgt_server = {"127.0.0.1", 10112},
                            .bolt_server = {"127.0.0.1", 7687},
                            .replication_client_info = {.instance_name = "instance_name",
                                                        .replication_mode = ReplicationMode::ASYNC,
                                                        .replication_server = {"127.0.0.1", 10001}},
                            .instance_health_check_frequency_sec = std::chrono::seconds{1},
                            .instance_down_timeout_sec = std::chrono::seconds{5},
                            .instance_get_uuid_frequency_sec = std::chrono::seconds{10}};

  nlohmann::json j;
  memgraph::coordination::to_json(j, config);

  DataInstanceConfig deserialized_config;
  memgraph::coordination::from_json(j, deserialized_config);

  EXPECT_EQ(config, deserialized_config);
}

TEST_F(DataInstanceContextTest, DataInstanceContextToJsonSerialization) {
  DataInstanceContext instance_state{
      DataInstanceConfig{.instance_name = "instance3",
                         .mgt_server = {"127.0.0.1", 10112},
                         .bolt_server = {"127.0.0.1", 7687},
                         .replication_client_info = {.instance_name = "instance_name",
                                                     .replication_mode = ReplicationMode::ASYNC,
                                                     .replication_server = {"127.0.0.1", 10001}},
                         .instance_health_check_frequency_sec = std::chrono::seconds{1},
                         .instance_down_timeout_sec = std::chrono::seconds{5},
                         .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                         .ssl = std::nullopt},
      ReplicationRole::MAIN, UUID()};

  nlohmann::json j;
  memgraph::coordination::to_json(j, instance_state);

  EXPECT_EQ(j["config"]["instance_name"], "instance3");
  EXPECT_EQ(j["config"]["instance_health_check_frequency_sec"], 1);
  EXPECT_EQ(j["config"]["instance_down_timeout_sec"], 5);
  EXPECT_EQ(j["config"]["instance_get_uuid_frequency_sec"], 10);
  EXPECT_EQ(j["status"], "main");
}

TEST_F(DataInstanceContextTest, DataInstanceContextFromJsonDeserialization) {
  DataInstanceContext expected_instance{
      DataInstanceConfig{.instance_name = "instance3",
                         .mgt_server = {"127.0.0.1", 10112},
                         .bolt_server = {"127.0.0.1", 7687},
                         .replication_client_info = {.instance_name = "instance_name",
                                                     .replication_mode = ReplicationMode::ASYNC,
                                                     .replication_server = {"127.0.0.1", 10001}},
                         .instance_health_check_frequency_sec = std::chrono::seconds{1},
                         .instance_down_timeout_sec = std::chrono::seconds{5},
                         .instance_get_uuid_frequency_sec = std::chrono::seconds{10},
                         .ssl = std::nullopt},
      ReplicationRole::MAIN, UUID()};

  nlohmann::json j;
  memgraph::coordination::to_json(j, expected_instance);

  DataInstanceContext deserialized_instance;
  memgraph::coordination::from_json(j, deserialized_instance);

  EXPECT_EQ(expected_instance, deserialized_instance);
}

#endif
