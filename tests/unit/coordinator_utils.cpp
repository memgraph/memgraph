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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "coordination/coordinator_instance.hpp"
#include "dbms/constants.hpp"
#include "replication_coordination_glue/common.hpp"
#include "utils/functional.hpp"

using memgraph::coordination::CoordinatorInstanceInitConfig;

class CoordinationUtils : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "MG_tests_unit_coordination"};

  int const bolt_port{8688};
  int const coordinator_port{20111};
  uint32_t const coordinator_id{11};
};

// Networking is used in this test, be careful with ports used.
TEST_F(CoordinationUtils, MemgraphDbHistorySimple) {
  // Choose any if everything is same
  // X = dead
  // Main      : A(24)  B(36)  C(48) D(50) E(51) X
  // replica  1: A(24)  B(36)  C(48) D(50) E(51)
  // replica  2: A(24)  B(36)  C(48) D(50) E(51)
  // replica  3: A(24)  B(36)  C(48) D(50) E(51)
  std::vector<std::pair<std::string, memgraph::replication_coordination_glue::DatabaseHistories>>
      instance_database_histories;

  std::vector<std::pair<memgraph::utils::UUID, uint64_t>> histories;
  histories.emplace_back(memgraph::utils::UUID{}, 24);
  histories.emplace_back(memgraph::utils::UUID{}, 36);
  histories.emplace_back(memgraph::utils::UUID{}, 48);
  histories.emplace_back(memgraph::utils::UUID{}, 50);
  histories.emplace_back(memgraph::utils::UUID{}, 51);

  memgraph::utils::UUID db_uuid;
  std::string default_name = std::string(memgraph::dbms::kDefaultDB);

  auto db_histories = memgraph::utils::fmap(histories, [](const std::pair<memgraph::utils::UUID, uint64_t> &pair) {
    return std::make_pair(std::string(pair.first), pair.second);
  });

  memgraph::replication_coordination_glue::DatabaseHistory history{
      .db_uuid = db_uuid, .history = db_histories, .name = default_name};

  memgraph::replication_coordination_glue::DatabaseHistories instance_1_db_histories_{history};
  instance_database_histories.emplace_back("instance_1", instance_1_db_histories_);

  memgraph::replication_coordination_glue::DatabaseHistories instance_2_db_histories_{history};
  instance_database_histories.emplace_back("instance_2", instance_2_db_histories_);

  memgraph::replication_coordination_glue::DatabaseHistories instance_3_db_histories_{history};
  instance_database_histories.emplace_back("instance_3", instance_3_db_histories_);

  CoordinatorInstanceInitConfig const init_config1{coordinator_id, coordinator_port, bolt_port,
                                                   test_folder_ / "high_availability" / "coordinator"};
  memgraph::coordination::CoordinatorInstance instance{init_config1};

  auto [instance_name, latest_epoch, latest_commit_timestamp] =
      instance.ChooseMostUpToDateInstance(instance_database_histories);
  ASSERT_TRUE(instance_name == "instance_1" || instance_name == "instance_2" || instance_name == "instance_3");
  ASSERT_TRUE(latest_epoch == db_histories.back().first);
  ASSERT_TRUE(latest_commit_timestamp == db_histories.back().second);
}

TEST_F(CoordinationUtils, MemgraphDbHistoryLastEpochDifferent) {
  // Prioritize one with the biggest last commit timestamp on last epoch
  // X = dead
  // Main      : A(24)  B(36)  C(48) D(50) E(59) X
  // replica  1: A(24)  B(12)  C(15) D(17) E(51)
  // replica  2: A(24)  B(12)  C(15) D(17) E(57)
  // replica  3: A(24)  B(12)  C(15) D(17) E(59)
  std::vector<std::pair<std::string, memgraph::replication_coordination_glue::DatabaseHistories>>
      instance_database_histories;

  std::vector<std::pair<memgraph::utils::UUID, uint64_t>> histories;
  histories.emplace_back(memgraph::utils::UUID{}, 24);
  histories.emplace_back(memgraph::utils::UUID{}, 36);
  histories.emplace_back(memgraph::utils::UUID{}, 48);
  histories.emplace_back(memgraph::utils::UUID{}, 50);
  histories.emplace_back(memgraph::utils::UUID{}, 59);

  memgraph::utils::UUID db_uuid;
  std::string default_name = std::string(memgraph::dbms::kDefaultDB);

  auto db_histories = memgraph::utils::fmap(histories, [](const std::pair<memgraph::utils::UUID, uint64_t> &pair) {
    return std::make_pair(std::string(pair.first), pair.second);
  });

  db_histories.back().second = 51;
  memgraph::replication_coordination_glue::DatabaseHistory history1{
      .db_uuid = db_uuid, .history = db_histories, .name = default_name};

  memgraph::replication_coordination_glue::DatabaseHistories instance_1_db_histories_{history1};
  instance_database_histories.emplace_back("instance_1", instance_1_db_histories_);

  db_histories.back().second = 57;
  memgraph::replication_coordination_glue::DatabaseHistory history2{
      .db_uuid = db_uuid, .history = db_histories, .name = default_name};
  memgraph::replication_coordination_glue::DatabaseHistories instance_2_db_histories_{history2};
  instance_database_histories.emplace_back("instance_2", instance_2_db_histories_);

  db_histories.back().second = 59;
  memgraph::replication_coordination_glue::DatabaseHistory history3{
      .db_uuid = db_uuid, .history = db_histories, .name = default_name};
  memgraph::replication_coordination_glue::DatabaseHistories instance_3_db_histories_{history3};
  instance_database_histories.emplace_back("instance_3", instance_3_db_histories_);

  CoordinatorInstanceInitConfig const init_config1{coordinator_id, coordinator_port, bolt_port,
                                                   test_folder_ / "high_availability" / "coordinator"};
  memgraph::coordination::CoordinatorInstance instance{init_config1};
  auto [instance_name, latest_epoch, latest_commit_timestamp] =
      instance.ChooseMostUpToDateInstance(instance_database_histories);

  ASSERT_TRUE(instance_name == "instance_3");
  ASSERT_TRUE(latest_epoch == db_histories.back().first);
  ASSERT_TRUE(latest_commit_timestamp == db_histories.back().second);
}

TEST_F(CoordinationUtils, MemgraphDbHistoryOneInstanceAheadFewEpochs) {
  // Prioritize one biggest commit timestamp
  // X = dead
  // Main      : A(24)  B(36)  C(48) D(50) E(51)   X    X     X  X
  // replica  1: A(24)  B(36)  C(48) D(50) E(51) F(60) G(65)  X  up
  // replica  2: A(24)  B(36)  C(48) D(50) E(51)  X     X     X  up
  // replica  3: A(24)  B(36)  C(48) D(50) E(51)  X     X     X  up
  std::vector<std::pair<std::string, memgraph::replication_coordination_glue::DatabaseHistories>>
      instance_database_histories;

  std::vector<std::pair<memgraph::utils::UUID, uint64_t>> histories;
  histories.emplace_back(memgraph::utils::UUID{}, 24);
  histories.emplace_back(memgraph::utils::UUID{}, 36);
  histories.emplace_back(memgraph::utils::UUID{}, 48);
  histories.emplace_back(memgraph::utils::UUID{}, 50);
  histories.emplace_back(memgraph::utils::UUID{}, 51);

  memgraph::utils::UUID db_uuid;
  std::string default_name = std::string(memgraph::dbms::kDefaultDB);

  auto db_histories = memgraph::utils::fmap(histories, [](const std::pair<memgraph::utils::UUID, uint64_t> &pair) {
    return std::make_pair(std::string(pair.first), pair.second);
  });

  memgraph::replication_coordination_glue::DatabaseHistory history{
      .db_uuid = db_uuid, .history = db_histories, .name = default_name};

  memgraph::replication_coordination_glue::DatabaseHistories instance_1_db_histories_{history};
  instance_database_histories.emplace_back("instance_1", instance_1_db_histories_);

  memgraph::replication_coordination_glue::DatabaseHistories instance_2_db_histories_{history};
  instance_database_histories.emplace_back("instance_2", instance_2_db_histories_);

  histories.emplace_back(memgraph::utils::UUID{}, 60);
  histories.emplace_back(memgraph::utils::UUID{}, 65);
  auto db_histories_longest =
      memgraph::utils::fmap(histories, [](const std::pair<memgraph::utils::UUID, uint64_t> &pair) {
        return std::make_pair(std::string(pair.first), pair.second);
      });

  memgraph::replication_coordination_glue::DatabaseHistory history_longest{
      .db_uuid = db_uuid, .history = db_histories_longest, .name = default_name};

  memgraph::replication_coordination_glue::DatabaseHistories instance_3_db_histories_{history_longest};
  instance_database_histories.emplace_back("instance_3", instance_3_db_histories_);

  CoordinatorInstanceInitConfig const init_config1{coordinator_id, coordinator_port, bolt_port,
                                                   test_folder_ / "high_availability" / "coordinator"};
  memgraph::coordination::CoordinatorInstance instance{init_config1};
  auto [instance_name, latest_epoch, latest_commit_timestamp] =
      instance.ChooseMostUpToDateInstance(instance_database_histories);

  ASSERT_TRUE(instance_name == "instance_3");
  ASSERT_TRUE(latest_epoch == db_histories_longest.back().first);
  ASSERT_TRUE(latest_commit_timestamp == db_histories_longest.back().second);
}

TEST_F(CoordinationUtils, MemgraphDbHistoryInstancesHistoryDiverged) {
  // When history diverged, also prioritize one with biggest last commit timestamp
  // Main      : A(1)  B(2)   C(3)    X
  // replica  1: A(1)  B(2)   C(3)    X     X up
  // replica  2: A(1)  B(2)    X     D(5)   X up
  // replica  3: A(1)  B(2)    X     D(4)   X up
  std::vector<std::pair<std::string, memgraph::replication_coordination_glue::DatabaseHistories>>
      instance_database_histories;

  std::vector<std::pair<memgraph::utils::UUID, uint64_t>> histories;
  histories.emplace_back(memgraph::utils::UUID{}, 1);
  histories.emplace_back(memgraph::utils::UUID{}, 2);
  histories.emplace_back(memgraph::utils::UUID{}, 3);

  memgraph::utils::UUID db_uuid;
  std::string default_name = std::string(memgraph::dbms::kDefaultDB);

  auto db_histories = memgraph::utils::fmap(histories, [](const std::pair<memgraph::utils::UUID, uint64_t> &pair) {
    return std::make_pair(std::string(pair.first), pair.second);
  });

  memgraph::replication_coordination_glue::DatabaseHistory history{
      .db_uuid = db_uuid, .history = db_histories, .name = default_name};

  memgraph::replication_coordination_glue::DatabaseHistories instance_1_db_histories_{history};
  instance_database_histories.emplace_back("instance_1", instance_1_db_histories_);

  db_histories.pop_back();

  auto oldest_commit_timestamp{5};
  auto newest_different_epoch = memgraph::utils::UUID{};
  histories.emplace_back(newest_different_epoch, oldest_commit_timestamp);
  auto db_histories_different =
      memgraph::utils::fmap(histories, [](const std::pair<memgraph::utils::UUID, uint64_t> &pair) {
        return std::make_pair(std::string(pair.first), pair.second);
      });

  memgraph::replication_coordination_glue::DatabaseHistory history_3{
      .db_uuid = db_uuid, .history = db_histories_different, .name = default_name};

  memgraph::replication_coordination_glue::DatabaseHistories instance_3_db_histories_{history_3};
  instance_database_histories.emplace_back("instance_3", instance_3_db_histories_);

  db_histories_different.back().second = 4;
  memgraph::replication_coordination_glue::DatabaseHistory history_2{
      .db_uuid = db_uuid, .history = db_histories_different, .name = default_name};

  memgraph::replication_coordination_glue::DatabaseHistories instance_2_db_histories_{history_2};
  instance_database_histories.emplace_back("instance_2", instance_2_db_histories_);

  CoordinatorInstanceInitConfig const init_config1{coordinator_id, coordinator_port, bolt_port,
                                                   test_folder_ / "high_availability" / "coordinator"};
  memgraph::coordination::CoordinatorInstance instance{init_config1};
  auto [instance_name, latest_epoch, latest_commit_timestamp] =
      instance.ChooseMostUpToDateInstance(instance_database_histories);

  ASSERT_TRUE(instance_name == "instance_3");
  ASSERT_TRUE(latest_epoch == std::string(newest_different_epoch));
  ASSERT_TRUE(latest_commit_timestamp == oldest_commit_timestamp);
}
