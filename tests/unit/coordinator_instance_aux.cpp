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

#include "coordination/coordinator_instance_aux.hpp"
#include <gtest/gtest.h>
#include <filesystem>

#include <nlohmann/json.hpp>

#ifdef MG_ENTERPRISE

using memgraph::coordination::CoordinatorInstanceAux;

class CoordinatorInstanceAuxTest : public ::testing::Test {
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

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "MG_tests_unit_coordinator_instance_aux"};
};

TEST_F(CoordinatorInstanceAuxTest, ToJsonSerialization) {
  CoordinatorInstanceAux instance{42, "coordinator.example.com", "management.example.com"};
  nlohmann::json json;
  memgraph::coordination::to_json(json, instance);

  EXPECT_EQ(json["id"], 42);
  EXPECT_EQ(json["coordinator_server"], "coordinator.example.com");
  EXPECT_EQ(json["management_server"], "management.example.com");
}

TEST_F(CoordinatorInstanceAuxTest, FromJsonDeserialization) {
  nlohmann::json json = {
      {"id", 42}, {"coordinator_server", "coordinator.example.com"}, {"management_server", "management.example.com"}};

  CoordinatorInstanceAux instance;
  memgraph::coordination::from_json(json, instance);

  EXPECT_EQ(instance.id, 42);
  EXPECT_EQ(instance.coordinator_server, "coordinator.example.com");
  EXPECT_EQ(instance.management_server, "management.example.com");
}

TEST_F(CoordinatorInstanceAuxTest, EqualityOperator) {
  CoordinatorInstanceAux instance1{42, "coordinator.example.com", "management.example.com"};
  CoordinatorInstanceAux instance2{42, "coordinator.example.com", "management.example.com"};
  CoordinatorInstanceAux instance3{43, "another-coordinator.com", "another-management.com"};

  EXPECT_EQ(instance1, instance2);
  EXPECT_NE(instance1, instance3);
}

#endif
