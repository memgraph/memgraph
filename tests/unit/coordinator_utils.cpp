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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "coordination/coordinator_instance.hpp"
#include "dbms/constants.hpp"
#include "replication_coordination_glue/common.hpp"
#include "utils/functional.hpp"

TEST(CoordinationUtils, FailoverFirstInstanceNewest) {
  // Choose any if everything is same

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};

  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> instances_info{
      {default_db_uuid,
       {{"instance_1", 51}, {"instance_2", 51}, {"instance_3", 51}

       }}};

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_1" || *maybe_instance_name == "instance_2" ||
              *maybe_instance_name == "instance_3");
}

TEST(CoordinationUtils, FailoverSecondInstanceNewest) {
  // Choose any if everything is same

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};

  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> instances_info{
      {default_db_uuid,
       {{"instance_1", 51}, {"instance_2", 53}, {"instance_3", 51}

       }}};

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_1" || *maybe_instance_name == "instance_2" ||
              *maybe_instance_name == "instance_3");
}

TEST(CoordinationUtils, FailoverLastInstanceNewest) {
  // Prioritize one with the largest last durable timestamp on

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};

  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> instances_info{
      {default_db_uuid,
       {{"instance_1", 51}, {"instance_2", 57}, {"instance_3", 59}

       }}};

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_3");
}

TEST(CoordinationUtils, MTFailover1) {
  // Instance 2 newest for all DBs

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  auto const db_a = std::string{memgraph::utils::UUID()};
  auto const db_b = std::string{memgraph::utils::UUID()};

  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> instances_info{
      {default_db_uuid, {{"instance_1", 11}, {"instance_2", 15}, {"instance_3", 12}}},
      {db_a, {{"instance_1", 51}, {"instance_2", 57}, {"instance_3", 56}}},
      {db_b, {{"instance_1", 30}, {"instance_2", 33}, {"instance_3", 31}}},

  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_2");
}

TEST(CoordinationUtils, MTFailover2) {
  // Instance 3 newest for 2 DBs

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  auto const db_a = std::string{memgraph::utils::UUID()};
  auto const db_b = std::string{memgraph::utils::UUID()};

  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> instances_info{
      {default_db_uuid, {{"instance_1", 11}, {"instance_2", 15}, {"instance_3", 0}}},
      {db_a, {{"instance_1", 51}, {"instance_2", 57}, {"instance_3", 58}}},
      {db_b, {{"instance_1", 30}, {"instance_2", 33}, {"instance_3", 36}}},

  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_3");
}

// TODO: (andi) Fix
// TEST(CoordinationUtils, MTFailover3) {
//   // Instance 2 best for default db, best for db_a
//   // Instance 1 best for default db
//   // Instance 3 best for db_b
//
//   auto const default_db_uuid = std::string{memgraph::utils::UUID()};
//   auto const db_a = std::string{memgraph::utils::UUID()};
//   auto const db_b = std::string{memgraph::utils::UUID()};
//
//   std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> instances_info{
//       {default_db_uuid, {{"instance_1", 15}, {"instance_2", 15}, {"instance_3", 0}}},
//       {db_a, {{"instance_1", 51}, {"instance_2", 57}, {"instance_3", 55}}},
//       {db_b, {{"instance_1", 30}, {"instance_2", 33}, {"instance_3", 34}}},
//
//   };
//
//   auto const maybe_instance_name =
//       memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
//   ASSERT_TRUE(maybe_instance_name.has_value());
//   // NOLINTNEXTLINE
//   ASSERT_TRUE(*maybe_instance_name == "instance_2");
// }

TEST(CoordinationUtils, FailoverNoInstancesAvailable) {
  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> instances_info{{default_db_uuid, {}}};

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_FALSE(maybe_instance_name.has_value());
}

TEST(CoordinationUtils, FailoverSomeInstancesMissingTimestamps) {
  // missing timestamps
  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> instances_info{
      {default_db_uuid, {{"instance_1", 0}, {"instance_2", 15}, {"instance_3", 0}}}};

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_EQ(*maybe_instance_name, "instance_2");
}

TEST(CoordinationUtils, FailoverSingleInstanceOnly) {
  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> instances_info{
      {default_db_uuid, {{"instance_1", 100}}}};

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_EQ(*maybe_instance_name, "instance_1");
}
