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

#include <chrono>
#include <random>
#include <stop_token>
#include <string>
#include <thread>

#include <gtest/gtest.h>
#include "gmock/gmock.h"
#include "spdlog/spdlog.h"

#include "disk_test_utils.hpp"
#include "interpreter_faker.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter_context.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"

constexpr int NUM_INTERPRETERS = 4, INSERTIONS = 4000;

/*
Tests rely on the fact that interpreters are sequentially added to running_interpreters to get transaction_id of its
corresponding interpreter.
*/
template <typename StorageType>
class TransactionQueueMultipleTest : public ::testing::Test {
 protected:
  const std::string testSuite = "transactin_queue_multiple";
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() /
                                       "MG_tests_unit_transaction_queue_multiple_intr"};

  memgraph::storage::Config config{
      [&]() {
        memgraph::storage::Config config{};
        config.durability.storage_directory = data_directory;
        config.disk.main_storage_directory = config.durability.storage_directory / "disk";
        if constexpr (std::is_same_v<StorageType, memgraph::storage::DiskStorage>) {
          config.disk = disk_test_utils::GenerateOnDiskConfig(testSuite).disk;
          config.force_on_disk = true;
        }
        return config;
      }()  // iile
  };

  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config, repl_state};
  memgraph::dbms::DatabaseAccess db{
      [&]() {
        auto db_acc_opt = db_gk.access();
        MG_ASSERT(db_acc_opt, "Failed to access db");
        auto &db_acc = *db_acc_opt;
        MG_ASSERT(db_acc->GetStorageMode() == (std::is_same_v<StorageType, memgraph::storage::DiskStorage>
                                                   ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                                   : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL),
                  "Wrong storage mode!");
        return db_acc;
      }()  // iile
  };

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{}, nullptr, &repl_state, system_state};
  InterpreterFaker main_interpreter{&interpreter_context, db};
  std::vector<InterpreterFaker *> running_interpreters;

  TransactionQueueMultipleTest() {
    for (int i = 0; i < NUM_INTERPRETERS; ++i) {
      InterpreterFaker *faker = new InterpreterFaker(&interpreter_context, db);
      running_interpreters.push_back(faker);
    }
  }

  ~TransactionQueueMultipleTest() override {
    for (int i = 0; i < NUM_INTERPRETERS; ++i) {
      delete running_interpreters[i];
    }
    disk_test_utils::RemoveRocksDbDirs(testSuite);
    std::filesystem::remove_all(data_directory);
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(TransactionQueueMultipleTest, StorageTypes);

// Tests whether admin can see transaction of superadmin
TYPED_TEST(TransactionQueueMultipleTest, TerminateTransaction) {
  std::vector<std::atomic<bool>> started(NUM_INTERPRETERS);
  for (int i = 0; i < NUM_INTERPRETERS; i++) {
    started[i].store(false, std::memory_order_release);
  }
  auto thread_func = [this, &started](int thread_index) {
    try {
      this->running_interpreters[thread_index]->Interpret("BEGIN");
      started[thread_index].store(true, std::memory_order_release);
      // add try-catch block
      for (int j = 0; j < INSERTIONS; ++j) {
        this->running_interpreters[thread_index]->Interpret("CREATE (:Person {prop: " + std::to_string(thread_index) +
                                                            "})");
      }
    } catch (memgraph::query::HintedAbortError &e) {
    }
  };

  {
    std::vector<std::jthread> running_threads;
    running_threads.reserve(NUM_INTERPRETERS);
    for (int i = 0; i < NUM_INTERPRETERS; ++i) {
      running_threads.emplace_back(thread_func, i);
    }

    while (
        !std::all_of(started.begin(), started.end(), [](const auto &v) { return v.load(std::memory_order_acquire); })) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    auto show_stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
    ASSERT_EQ(show_stream.GetResults().size(), NUM_INTERPRETERS + 1);
    // Choose random transaction to kill
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> distr(0, NUM_INTERPRETERS - 1);
    int index_to_terminate = distr(gen);
    // Kill random transaction
    std::string run_trans_id =
        std::to_string(this->running_interpreters[index_to_terminate]->interpreter.GetTransactionId().value());
    std::string esc_run_trans_id = "'" + run_trans_id + "'";
    auto terminate_stream = this->main_interpreter.Interpret("TERMINATE TRANSACTIONS " + esc_run_trans_id);
    // check result of killing
    ASSERT_EQ(terminate_stream.GetResults().size(), 1U);
    EXPECT_EQ(terminate_stream.GetResults()[0][0].ValueString(), run_trans_id);
    ASSERT_TRUE(terminate_stream.GetResults()[0][1].ValueBool());  // that the transaction is actually killed
    // test here show transactions
    auto show_stream_after_kill = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
    ASSERT_EQ(show_stream_after_kill.GetResults().size(), NUM_INTERPRETERS);
    // wait to finish for threads
    for (int i = 0; i < NUM_INTERPRETERS; ++i) {
      running_threads[i].join();
    }
    // test the state of the database
    for (int i = 0; i < NUM_INTERPRETERS; ++i) {
      if (i != index_to_terminate) {
        this->running_interpreters[i]->Interpret("COMMIT");
      }
      std::string fetch_query = "MATCH (n:Person) WHERE n.prop=" + std::to_string(i) + " RETURN n";
      auto results_stream = this->main_interpreter.Interpret(fetch_query);
      if (i == index_to_terminate) {
        ASSERT_EQ(results_stream.GetResults().size(), 0);
      } else {
        ASSERT_EQ(results_stream.GetResults().size(), INSERTIONS);
      }
    }
    this->main_interpreter.Interpret("MATCH (n) DETACH DELETE n");
  }
}
