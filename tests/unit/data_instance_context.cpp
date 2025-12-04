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

#include "replication_coordination_glue/mode.hpp"
#include "replication_coordination_glue/role.hpp"

#include <gtest/gtest.h>
#include <filesystem>
#include <nlohmann/json.hpp>
#include "utils/uuid.hpp"

import memgraph.coordination.coordinator_communication_config;
import memgraph.coordination.data_instance_context;

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

TEST_F(DataInstanceContextTest, Marshalling) {
  DataInstanceConfig config{.instance_name = "instance3",
                            .mgt_server = {"127.0.0.1", 10112},
                            .bolt_server = {"127.0.0.1", 7687},
                            .replication_client_info = {.instance_name = "instance_name",
                                                        .replication_mode = ReplicationMode::ASYNC,
                                                        .replication_server = {"127.0.0.1", 10001}}};

  nlohmann::json j;
  memgraph::coordination::to_json(j, config);

  DataInstanceConfig deserialized_config;
  memgraph::coordination::from_json(j, deserialized_config);

  EXPECT_EQ(config, deserialized_config);
}

#endif
