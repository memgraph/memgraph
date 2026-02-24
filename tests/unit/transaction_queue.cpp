// Copyright 2026 Memgraph Ltd.
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
#include <stop_token>
#include <string>
#include <thread>

#include <gtest/gtest.h>
#include "gmock/gmock.h"

#include "disk_test_utils.hpp"
#include "interpreter_faker.hpp"
#include "query/context.hpp"
#include "query/interpreter_context.hpp"
#include "storage/v2/inmemory/storage.hpp"

/*
Tests rely on the fact that interpreters are sequentially added to runninng_interpreters to get transaction_id of its
corresponding interpreter/.
*/
template <typename StorageType>
class TransactionQueueSimpleTest : public ::testing::Test {
 protected:
  const std::string testSuite = "transactin_queue";
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_tests_unit_transaction_queue_intr"};

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

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
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
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          nullptr,
                                                          repl_state,
                                                          system_state
#ifdef MG_ENTERPRISE
                                                          ,
                                                          std::nullopt,
                                                          nullptr
#endif
  };
  InterpreterFaker running_interpreter{&interpreter_context, db}, main_interpreter{&interpreter_context, db};

  void TearDown() override {
    disk_test_utils::RemoveRocksDbDirs(testSuite);
    std::filesystem::remove_all(data_directory);
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_SUITE(TransactionQueueSimpleTest, StorageTypes);

TYPED_TEST(TransactionQueueSimpleTest, TwoInterpretersInterleaving) {
  std::atomic<bool> started{false};
  std::jthread running_thread = std::jthread(
      [this, &started](std::stop_token st, int thread_index) {
        this->running_interpreter.Interpret("BEGIN");
        started.store(true, std::memory_order_release);
      },
      0);

  {
    while (!started.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    this->main_interpreter.Interpret("CREATE (:Person {prop: 1})");
    auto show_stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
    ASSERT_EQ(show_stream.GetResults().size(), 2U);
    // superadmin executing the transaction
    EXPECT_EQ(show_stream.GetResults()[0][0].ValueString(), "");
    ASSERT_TRUE(show_stream.GetResults()[0][1].IsString());
    EXPECT_EQ(show_stream.GetResults()[0][2].ValueList().at(0).ValueString(), "SHOW TRANSACTIONS");
    EXPECT_EQ(show_stream.GetResults()[0][3].ValueString(), "running");
    // Also anonymous user executing
    EXPECT_EQ(show_stream.GetResults()[1][0].ValueString(), "");
    ASSERT_TRUE(show_stream.GetResults()[1][1].IsString());
    EXPECT_EQ(show_stream.GetResults()[1][3].ValueString(), "running");
    // Kill the other transaction
    std::string run_trans_id = show_stream.GetResults()[1][1].ValueString();
    std::string esc_run_trans_id = "'" + run_trans_id + "'";
    auto terminate_stream = this->main_interpreter.Interpret("TERMINATE TRANSACTIONS " + esc_run_trans_id);
    // check result of killing
    ASSERT_EQ(terminate_stream.GetResults().size(), 1U);
    EXPECT_EQ(terminate_stream.GetResults()[0][0].ValueString(), run_trans_id);
    ASSERT_TRUE(terminate_stream.GetResults()[0][1].ValueBool());  // that the transaction is actually killed
    // After TERMINATE, the transaction should show as "terminating"
    auto show_stream_after_killing = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
    // The terminated transaction is now visible with "terminating" status
    // It shows 2 results: the SHOW TRANSACTIONS itself + the terminated one
    ASSERT_EQ(show_stream_after_killing.GetResults().size(), 2U);
    // Find the terminated transaction by its transaction_id
    for (const auto &row : show_stream_after_killing.GetResults()) {
      if (row[1].ValueString() == run_trans_id) {
        EXPECT_EQ(row[3].ValueString(), "terminating");
      } else {
        EXPECT_EQ(row[3].ValueString(), "running");
      }
    }
    // finish thread
    running_thread.request_stop();
    running_thread.join();
    // After the thread finishes, abort the terminated interpreter so it cleans up
    this->running_interpreter.Abort();
    // After abort completes, the terminated transaction should no longer show
    auto show_stream_final = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
    ASSERT_EQ(show_stream_final.GetResults().size(), 1U);
    // test the state of the database
    auto results_stream = this->main_interpreter.Interpret("MATCH (n) RETURN n");
    ASSERT_EQ(results_stream.GetResults().size(), 1U);  // from the main interpreter
    this->main_interpreter.Interpret("MATCH (n) DETACH DELETE n");
  }
}

TYPED_TEST(TransactionQueueSimpleTest, ShowTransactionStatusCommitting) {
  // Start a transaction on the running interpreter
  this->running_interpreter.Interpret("BEGIN");
  this->running_interpreter.Interpret("CREATE (:Person {prop: 1})");

  // Manually set the status to STARTED_COMMITTING to simulate a committing transaction
  this->running_interpreter.interpreter.transaction_status_.store(
      memgraph::query::TransactionStatus::STARTED_COMMITTING, std::memory_order_release);

  auto show_stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(show_stream.GetResults().size(), 2U);

  // Find the committing transaction
  bool found_committing = false;
  for (const auto &row : show_stream.GetResults()) {
    if (row[3].ValueString() == "committing") {
      found_committing = true;
      ASSERT_TRUE(row[1].IsString());  // transaction_id is present
    }
  }
  EXPECT_TRUE(found_committing);

  // Restore to IDLE so the test can clean up without hitting assertion failures
  this->running_interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::IDLE,
                                                                  std::memory_order_release);
}

TYPED_TEST(TransactionQueueSimpleTest, ShowTransactionStatusAborting) {
  // Start a transaction on the running interpreter
  this->running_interpreter.Interpret("BEGIN");
  this->running_interpreter.Interpret("CREATE (:Person {prop: 1})");

  // Manually set the status to STARTED_ROLLBACK to simulate an aborting transaction
  this->running_interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::STARTED_ROLLBACK,
                                                                  std::memory_order_release);

  auto show_stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(show_stream.GetResults().size(), 2U);

  // Find the aborting transaction
  bool found_aborting = false;
  for (const auto &row : show_stream.GetResults()) {
    if (row[3].ValueString() == "aborting") {
      found_aborting = true;
      ASSERT_TRUE(row[1].IsString());  // transaction_id is present
    }
  }
  EXPECT_TRUE(found_aborting);

  // Restore to IDLE so the test can clean up without hitting assertion failures
  this->running_interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::IDLE,
                                                                  std::memory_order_release);
}
