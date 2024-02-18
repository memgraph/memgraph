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
#include "coordination/utils.hpp"
#include "replication_coordination_glue/common.hpp"
#include "utils/functional.hpp"

class CoordinationUtils : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "MG_tests_unit_coordination"};
};

TEST_F(CoordinationUtils, MemgraphDbHistorySimple) {
  // Choose any if everything is same
  // X = dead
  // Main      : A(24)  B(12)  C(15) D(17) E(1) X
  // replica  1: A(24)  B(12)  C(15) D(17) E(1)
  // replica  2: A(24)  B(12)  C(15) D(17) E(1)
  // replica  3: A(24)  B(12)  C(15) D(17) E(1)
  std::vector<std::pair<std::string, memgraph::replication_coordination_glue::DatabaseHistories>>
      instance_database_histories;
  std::optional<std::string> latest_epoch;
  std::optional<uint64_t> latest_commit_timestamp;

  std::vector<std::pair<memgraph::utils::UUID, uint64_t>> histories;
  histories.emplace_back(memgraph::utils::UUID{}, 24);
  histories.emplace_back(memgraph::utils::UUID{}, 12);
  histories.emplace_back(memgraph::utils::UUID{}, 15);
  histories.emplace_back(memgraph::utils::UUID{}, 17);
  histories.emplace_back(memgraph::utils::UUID{}, 1);

  memgraph::utils::UUID db_uuid;
  std::string default_name = std::string(memgraph::dbms::kDefaultDB);

  auto db_histories = memgraph::utils::fmap(
      [](const std::pair<memgraph::utils::UUID, uint64_t> &pair) {
        return std::make_pair(std::string(pair.first), pair.second);
      },
      histories);

  memgraph::replication_coordination_glue::DatabaseHistory history{
      .db_uuid = db_uuid, .history = db_histories, .name = default_name};

  memgraph::replication_coordination_glue::DatabaseHistories instance_1_db_histories_{history};
  instance_database_histories.emplace_back("instance_1", instance_1_db_histories_);

  memgraph::replication_coordination_glue::DatabaseHistories instance_2_db_histories_{history};
  instance_database_histories.emplace_back("instance_2", instance_2_db_histories_);

  memgraph::replication_coordination_glue::DatabaseHistories instance_3_db_histories_{history};
  instance_database_histories.emplace_back("instance_3", instance_3_db_histories_);

  auto instance_name = memgraph::coordination::ChooseMostUpToDateInstance(instance_database_histories, latest_epoch,
                                                                          latest_commit_timestamp);
  ASSERT_TRUE(instance_name == "instance_1" || instance_name == "instance_2" || instance_name == "instance_3");
  ASSERT_TRUE(*latest_epoch == db_histories.back().first);
  ASSERT_TRUE(*latest_commit_timestamp == db_histories.back().second);
}

TEST_F(CoordinationUtils, MemgraphDbHistoryLastEpochDifferent) {
  // Prioritize one with the biggest last commit timestamp on last epoch
  // X = dead
  // Main      : A(24)  B(12)  C(15) D(17) E(1) X
  // replica  1: A(24)  B(12)  C(15) D(17) E(10)
  // replica  2: A(24)  B(12)  C(15) D(17) E(15)
  // replica  3: A(24)  B(12)  C(15) D(17) E(20)
  std::vector<std::pair<std::string, memgraph::replication_coordination_glue::DatabaseHistories>>
      instance_database_histories;
  std::optional<std::string> latest_epoch;
  std::optional<uint64_t> latest_commit_timestamp;

  std::vector<std::pair<memgraph::utils::UUID, uint64_t>> histories;
  histories.emplace_back(memgraph::utils::UUID{}, 24);
  histories.emplace_back(memgraph::utils::UUID{}, 12);
  histories.emplace_back(memgraph::utils::UUID{}, 15);
  histories.emplace_back(memgraph::utils::UUID{}, 17);
  histories.emplace_back(memgraph::utils::UUID{}, 1);

  memgraph::utils::UUID db_uuid;
  std::string default_name = std::string(memgraph::dbms::kDefaultDB);

  auto db_histories = memgraph::utils::fmap(
      [](const std::pair<memgraph::utils::UUID, uint64_t> &pair) {
        return std::make_pair(std::string(pair.first), pair.second);
      },
      histories);

  memgraph::replication_coordination_glue::DatabaseHistory history1{
      .db_uuid = db_uuid, .history = db_histories, .name = default_name};

  memgraph::replication_coordination_glue::DatabaseHistories instance_1_db_histories_{history1};
  instance_database_histories.emplace_back("instance_1", instance_1_db_histories_);

  db_histories.back().second = 10;
  memgraph::replication_coordination_glue::DatabaseHistory history2{
      .db_uuid = db_uuid, .history = db_histories, .name = default_name};
  memgraph::replication_coordination_glue::DatabaseHistories instance_2_db_histories_{history2};
  instance_database_histories.emplace_back("instance_2", instance_2_db_histories_);

  db_histories.back().second = 15;
  memgraph::replication_coordination_glue::DatabaseHistory history3{
      .db_uuid = db_uuid, .history = db_histories, .name = default_name};
  memgraph::replication_coordination_glue::DatabaseHistories instance_3_db_histories_{history3};
  instance_database_histories.emplace_back("instance_3", instance_3_db_histories_);

  auto instance_name = memgraph::coordination::ChooseMostUpToDateInstance(instance_database_histories, latest_epoch,
                                                                          latest_commit_timestamp);
  ASSERT_TRUE(instance_name == "instance_3");
  ASSERT_TRUE(*latest_epoch == db_histories.back().first);
  ASSERT_TRUE(*latest_commit_timestamp == db_histories.back().second);
}

TEST_F(CoordinationUtils, MemgraphDbHistoryOneInstanceAheadFewEpochs) {
  // Prioritize one with most epochs if same history
  // X = dead
  // Main      : A(24)  B(12)  C(15) D(17) E(1)   X    X     X  X
  // replica  1: A(24)  B(12)  C(15) D(17) E(1) F(17) G(20)  X  up
  // replica  2: A(24)  B(12)  C(15) D(17) E(1)  X     X     X  up
  // replica  3: A(24)  B(12)  C(15) D(17) E(1)  X     X     X  up
  std::vector<std::pair<std::string, memgraph::replication_coordination_glue::DatabaseHistories>>
      instance_database_histories;
  std::optional<std::string> latest_epoch;
  std::optional<uint64_t> latest_commit_timestamp;

  std::vector<std::pair<memgraph::utils::UUID, uint64_t>> histories;
  histories.emplace_back(memgraph::utils::UUID{}, 24);
  histories.emplace_back(memgraph::utils::UUID{}, 12);
  histories.emplace_back(memgraph::utils::UUID{}, 15);
  histories.emplace_back(memgraph::utils::UUID{}, 17);
  histories.emplace_back(memgraph::utils::UUID{}, 1);

  memgraph::utils::UUID db_uuid;
  std::string default_name = std::string(memgraph::dbms::kDefaultDB);

  auto db_histories = memgraph::utils::fmap(
      [](const std::pair<memgraph::utils::UUID, uint64_t> &pair) {
        return std::make_pair(std::string(pair.first), pair.second);
      },
      histories);

  memgraph::replication_coordination_glue::DatabaseHistory history{
      .db_uuid = db_uuid, .history = db_histories, .name = default_name};

  memgraph::replication_coordination_glue::DatabaseHistories instance_1_db_histories_{history};
  instance_database_histories.emplace_back("instance_1", instance_1_db_histories_);

  memgraph::replication_coordination_glue::DatabaseHistories instance_2_db_histories_{history};
  instance_database_histories.emplace_back("instance_2", instance_2_db_histories_);

  histories.emplace_back(memgraph::utils::UUID{}, 17);
  histories.emplace_back(memgraph::utils::UUID{}, 20);
  auto db_histories_longest = memgraph::utils::fmap(
      [](const std::pair<memgraph::utils::UUID, uint64_t> &pair) {
        return std::make_pair(std::string(pair.first), pair.second);
      },
      histories);

  memgraph::replication_coordination_glue::DatabaseHistory history_longest{
      .db_uuid = db_uuid, .history = db_histories_longest, .name = default_name};

  memgraph::replication_coordination_glue::DatabaseHistories instance_3_db_histories_{history_longest};
  instance_database_histories.emplace_back("instance_3", instance_3_db_histories_);

  auto instance_name = memgraph::coordination::ChooseMostUpToDateInstance(instance_database_histories, latest_epoch,
                                                                          latest_commit_timestamp);
  ASSERT_TRUE(instance_name == "instance_3");
  ASSERT_TRUE(*latest_epoch == db_histories_longest.back().first);
  ASSERT_TRUE(*latest_commit_timestamp == db_histories_longest.back().second);
}

TEST_F(CoordinationUtils, MemgraphDbHistoryInstancesChooseAnyDifferentEpochs) {
  // What to do in this case -> chose any in this case?
  // Main      : A B   C    X
  // replica  1: A B   C    X   X up
  // replica  2: A B   X    D   X up
  // replica  3: A B   X    D   X up
  ASSERT_TRUE(true);
}

TEST_F(CoordinationUtils, MemgraphDbHistoryMostEpochs) {
  // Prioritize one with most epochs?
  // Main      : A B   C    X X X X
  // replica  1: A B   C    X X X X up
  // replica  2: A B   X    D E F X up
  // replica  3: A B   X    D E F G up
  ASSERT_TRUE(true);
}

TEST_F(CoordinationUtils, MemgraphDbHistoryBiggestLastCommitTimestampsDifferentEpochs) {
  // Prioritize one with biggest latest commit timestamp on last epoch if same number of epochs ?
  // Main      : A(0) B(15) C(17)   X     X    X  X
  // replica  1: A(0) B(15) C(17)   D(0)  X    X     up
  // replica  2: A(0) B(15) C(17)   X    E(1)  X     up
  // replica  3: A(0) B(15) C(17)   X    X     F(15) up
  ASSERT_TRUE(true);
}
