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

#include "gtest/gtest.h"

#include "coordination/coordinator_instance_context.hpp"

using memgraph::coordination::CoordinatorInstanceContext;
using memgraph::coordination::DeserializeRaftContext;
using memgraph::coordination::SerializeRaftContext;

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

TEST_F(CoordinatorInstanceContextTest, MarshallingEmptyContext) {
  std::map<uint32_t, CoordinatorInstanceContext> servers;
  ASSERT_EQ(SerializeRaftContext(servers), "");
  ASSERT_TRUE(DeserializeRaftContext("").empty());
}

TEST_F(CoordinatorInstanceContextTest, MarshallingNonEmptyContext) {
  std::map<uint32_t, CoordinatorInstanceContext> servers;
  servers.emplace(1,
                  CoordinatorInstanceContext{.bolt_server = "localhost:9011", .management_server = "localhost:10111"});
  servers.emplace(2,
                  CoordinatorInstanceContext{.bolt_server = "localhost:9012", .management_server = "localhost:10112"});

  ASSERT_EQ(servers, DeserializeRaftContext(SerializeRaftContext(servers)));
}
