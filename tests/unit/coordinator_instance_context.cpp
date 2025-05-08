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

#include "gtest/gtest.h"

#include "coordination/coordinator_instance_context.hpp"

#include <filesystem>
#include <nlohmann/json.hpp>

using memgraph::coordination::CoordinatorInstanceContext;

class CoordinatorInstanceContextTest : public ::testing::Test {
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

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() /
                                     "MG_tests_unit_coordinator_instance_context"};
};

TEST_F(CoordinatorInstanceContextTest, MarshallingNonEmptyContext) {
  std::vector<CoordinatorInstanceContext> servers;
  servers.emplace_back(CoordinatorInstanceContext{.id = 1, .bolt_server = "localhost:9011"});
  servers.emplace_back(CoordinatorInstanceContext{.id = 2, .bolt_server = "localhost:9012"});

  auto const serialized_servers = nlohmann::json(servers).dump();
  auto const parsed_servers = nlohmann::json::parse(serialized_servers);

  ASSERT_EQ(servers, parsed_servers.get<std::vector<CoordinatorInstanceContext>>());
}
