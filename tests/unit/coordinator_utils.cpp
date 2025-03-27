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

TEST(CoordinationUtils, MTFailover3) {
  // Instance 2 best for default db, best for db_a
  // Instance 1 best for default db
  // Instance 3 best for db_b

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  auto const db_a = std::string{memgraph::utils::UUID()};
  auto const db_b = std::string{memgraph::utils::UUID()};

  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> const instances_info{
      {default_db_uuid, {{"instance_1", 15}, {"instance_2", 15}, {"instance_3", 0}}},
      {db_a, {{"instance_1", 51}, {"instance_2", 57}, {"instance_3", 55}}},
      {db_b, {{"instance_1", 30}, {"instance_2", 33}, {"instance_3", 34}}},

  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_2");
}

TEST(CoordinationUtils, MTFailover4) {
  // Instance 1 best for all 3 DBs
  // Instance 2 and 3 for default_db_uuid and db_a

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  auto const db_a = std::string{memgraph::utils::UUID()};
  auto const db_b = std::string{memgraph::utils::UUID()};

  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> const instances_info{
      {default_db_uuid, {{"instance_1", 15}, {"instance_2", 15}, {"instance_3", 15}}},
      {db_a, {{"instance_1", 51}, {"instance_2", 51}, {"instance_3", 51}}},
      {db_b, {{"instance_1", 30}, {"instance_2", 29}, {"instance_3", 28}}},

  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_1");
}

TEST(CoordinationUtils, MTFailover5) {
  // The decision needs to be done by summing timestamps
  // All 3 instances are up to date on db_b
  // Instance 1 is the best on default_db_uuid and has +5 over instance_2
  // Instance 2 is the best on db_a and has +6 over instance 1 -> therefore the winner

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  auto const db_a = std::string{memgraph::utils::UUID()};
  auto const db_b = std::string{memgraph::utils::UUID()};

  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> const instances_info{
      {default_db_uuid, {{"instance_1", 15}, {"instance_2", 10}, {"instance_3", 8}}},
      {db_a, {{"instance_1", 19}, {"instance_2", 25}, {"instance_3", 8}}},
      {db_b, {{"instance_1", 30}, {"instance_2", 30}, {"instance_3", 30}}},

  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_2");
}

TEST(CoordinationUtils, MTFailover6) {
  // The decision needs to be done by summing timestamps
  // Instance 1 is the best on default_db_uuid and has +6 over instance_1
  // Instance 2 is the best on db_a and has +5 over instance 1
  // --> Instance 1 is the winner

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  auto const db_a = std::string{memgraph::utils::UUID()};

  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> const instances_info{
      {default_db_uuid, {{"instance_1", 16}, {"instance_2", 10}}}, {db_a, {{"instance_1", 19}, {"instance_2", 24}}}};

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_1");
}

TEST(CoordinationUtils, MTFailover7) {
  // Instances are equally good on the same number of instances and they have the same total sum of timestamps.
  // Take the 1st one as winner

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  auto const db_a = std::string{memgraph::utils::UUID()};

  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> const instances_info{
      {default_db_uuid, {{"instance_1", 16}, {"instance_2", 10}}}, {db_a, {{"instance_1", 10}, {"instance_2", 16}}}};

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_1");
}

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
