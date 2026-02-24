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

  // Set metadata so we can verify it's readable during STARTED_COMMITTING
  memgraph::storage::ExternalPropertyValue::map_t md;
  md.emplace("key", memgraph::storage::ExternalPropertyValue("val"));
  this->running_interpreter.interpreter.metadata_ = md;

  // Manually set the status to STARTED_COMMITTING to simulate a committing transaction
  this->running_interpreter.interpreter.transaction_status_.store(
      memgraph::query::TransactionStatus::STARTED_COMMITTING, std::memory_order_release);

  auto show_stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(show_stream.GetResults().size(), 2U);

  std::string expected_tx_id = std::to_string(this->running_interpreter.interpreter.GetTransactionId().value());

  // Find the committing transaction and verify all fields
  bool found_committing = false;
  for (const auto &row : show_stream.GetResults()) {
    if (row[3].ValueString() == "committing") {
      found_committing = true;
      EXPECT_EQ(row[1].ValueString(), expected_tx_id);
      // Verify metadata is readable under CAS protection
      ASSERT_TRUE(row[4].IsMap());
      auto metadata_map = row[4].ValueMap();
      ASSERT_EQ(metadata_map.count("key"), 1);
      EXPECT_EQ(metadata_map.at("key").ValueString(), "val");
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

  // Set metadata so we can verify it's readable during STARTED_ROLLBACK
  memgraph::storage::ExternalPropertyValue::map_t md;
  md.emplace("abort_key", memgraph::storage::ExternalPropertyValue("abort_val"));
  this->running_interpreter.interpreter.metadata_ = md;

  // Manually set the status to STARTED_ROLLBACK to simulate an aborting transaction
  this->running_interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::STARTED_ROLLBACK,
                                                                  std::memory_order_release);

  auto show_stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(show_stream.GetResults().size(), 2U);

  std::string expected_tx_id = std::to_string(this->running_interpreter.interpreter.GetTransactionId().value());

  // Find the aborting transaction and verify all fields
  bool found_aborting = false;
  for (const auto &row : show_stream.GetResults()) {
    if (row[3].ValueString() == "aborting") {
      found_aborting = true;
      EXPECT_EQ(row[1].ValueString(), expected_tx_id);
      // Verify metadata is readable under CAS protection
      ASSERT_TRUE(row[4].IsMap());
      auto metadata_map = row[4].ValueMap();
      ASSERT_EQ(metadata_map.count("abort_key"), 1);
      EXPECT_EQ(metadata_map.at("abort_key").ValueString(), "abort_val");
    }
  }
  EXPECT_TRUE(found_aborting);

  // Restore to IDLE so the test can clean up without hitting assertion failures
  this->running_interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::IDLE,
                                                                  std::memory_order_release);
}

TYPED_TEST(TransactionQueueSimpleTest, ShowTransactionStatusTerminated) {
  // Start a transaction on the running interpreter
  this->running_interpreter.Interpret("BEGIN");
  this->running_interpreter.Interpret("CREATE (:Person {prop: 1})");

  std::string expected_tx_id = std::to_string(this->running_interpreter.interpreter.GetTransactionId().value());

  // Manually set the status to TERMINATED to simulate a killed-but-not-yet-aborted transaction
  this->running_interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::TERMINATED,
                                                                  std::memory_order_release);

  auto show_stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(show_stream.GetResults().size(), 2U);

  // Find the terminated transaction
  bool found_terminating = false;
  for (const auto &row : show_stream.GetResults()) {
    if (row[3].ValueString() == "terminating") {
      found_terminating = true;
      EXPECT_EQ(row[1].ValueString(), expected_tx_id);
    }
  }
  EXPECT_TRUE(found_terminating);

  // Restore to IDLE so the test can clean up
  this->running_interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::IDLE,
                                                                  std::memory_order_release);
}

TYPED_TEST(TransactionQueueSimpleTest, TerminateCommittingTransactionNotFound) {
  // Start a transaction and simulate it being in the middle of committing
  this->running_interpreter.Interpret("BEGIN");
  this->running_interpreter.Interpret("CREATE (:Person {prop: 1})");

  std::string tx_id = std::to_string(this->running_interpreter.interpreter.GetTransactionId().value());

  // Set status to STARTED_COMMITTING — TERMINATE should not be able to kill it
  // (TerminateTransactions only CAS's ACTIVE → VERIFYING)
  this->running_interpreter.interpreter.transaction_status_.store(
      memgraph::query::TransactionStatus::STARTED_COMMITTING, std::memory_order_release);

  auto terminate_stream = this->main_interpreter.Interpret("TERMINATE TRANSACTIONS '" + tx_id + "'");
  ASSERT_EQ(terminate_stream.GetResults().size(), 1U);
  EXPECT_EQ(terminate_stream.GetResults()[0][0].ValueString(), tx_id);
  // The transaction should NOT be killed — it's already committing
  EXPECT_FALSE(terminate_stream.GetResults()[0][1].ValueBool());

  // Restore to IDLE
  this->running_interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::IDLE,
                                                                  std::memory_order_release);
}

TYPED_TEST(TransactionQueueSimpleTest, ShowTransactionsAfterCommit) {
  // Verify that a transaction disappears from SHOW TRANSACTIONS after COMMIT
  this->running_interpreter.Interpret("BEGIN");
  this->running_interpreter.Interpret("CREATE (:Person {prop: 1})");

  // Before commit: both interpreters should be visible
  auto show_before = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(show_before.GetResults().size(), 2U);

  // Commit the running interpreter's transaction
  this->running_interpreter.Interpret("COMMIT");

  // After commit: only the main interpreter should be visible
  auto show_after = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(show_after.GetResults().size(), 1U);
  EXPECT_EQ(show_after.GetResults()[0][3].ValueString(), "running");

  // Verify data persisted
  auto results_stream = this->main_interpreter.Interpret("MATCH (n:Person) RETURN n");
  ASSERT_EQ(results_stream.GetResults().size(), 1U);
  this->main_interpreter.Interpret("MATCH (n) DETACH DELETE n");
}

TYPED_TEST(TransactionQueueSimpleTest, ShowTransactionsAfterAbort) {
  // Verify that a transaction disappears from SHOW TRANSACTIONS after ROLLBACK
  this->running_interpreter.Interpret("BEGIN");
  this->running_interpreter.Interpret("CREATE (:Person {prop: 1})");

  // Before abort: both interpreters should be visible
  auto show_before = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(show_before.GetResults().size(), 2U);

  // Abort the running interpreter's transaction
  this->running_interpreter.Interpret("ROLLBACK");

  // After abort: only the main interpreter should be visible
  auto show_after = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(show_after.GetResults().size(), 1U);
  EXPECT_EQ(show_after.GetResults()[0][3].ValueString(), "running");

  // Verify data was rolled back
  auto results_stream = this->main_interpreter.Interpret("MATCH (n:Person) RETURN n");
  ASSERT_EQ(results_stream.GetResults().size(), 0U);
}

TYPED_TEST(TransactionQueueSimpleTest, ShowTransactionsSelfOnly) {
  // A single interpreter with no other transactions — should see only itself
  auto show_stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(show_stream.GetResults().size(), 1U);

  auto &row = show_stream.GetResults()[0];
  EXPECT_EQ(row[0].ValueString(), "");  // username (anonymous)
  ASSERT_TRUE(row[1].IsString());       // transaction_id
  EXPECT_EQ(row[2].ValueList().at(0).ValueString(), "SHOW TRANSACTIONS");
  EXPECT_EQ(row[3].ValueString(), "running");
  ASSERT_TRUE(row[4].IsMap());  // metadata column exists (empty map)
}

TYPED_TEST(TransactionQueueSimpleTest, StatusColumnInHeader) {
  // Verify the SHOW TRANSACTIONS header includes the status column
  auto [stream, qid] = this->main_interpreter.Prepare("SHOW TRANSACTIONS");
  auto header = stream.GetHeader();
  ASSERT_EQ(header.size(), 5U);
  EXPECT_EQ(header[0], "username");
  EXPECT_EQ(header[1], "transaction_id");
  EXPECT_EQ(header[2], "query");
  EXPECT_EQ(header[3], "status");
  EXPECT_EQ(header[4], "metadata");
}

TYPED_TEST(TransactionQueueSimpleTest, ShowRunningTransactionsFilter) {
  // SHOW RUNNING TRANSACTIONS should show only running transactions (same as unfiltered here)
  auto show_stream = this->main_interpreter.Interpret("SHOW RUNNING TRANSACTIONS");
  ASSERT_EQ(show_stream.GetResults().size(), 1U);
  EXPECT_EQ(show_stream.GetResults()[0][3].ValueString(), "running");
}

TYPED_TEST(TransactionQueueSimpleTest, ShowFilteredTransactionsExcludesNonMatching) {
  // Start a transaction in another interpreter
  std::atomic<bool> started{false};
  std::jthread running_thread = std::jthread(
      [this, &started](std::stop_token st, int) {
        this->running_interpreter.Interpret("BEGIN");
        started.store(true, std::memory_order_release);
      },
      0);

  while (!started.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  // Both are running — SHOW RUNNING TRANSACTIONS should see both
  auto show_running = this->main_interpreter.Interpret("SHOW RUNNING TRANSACTIONS");
  ASSERT_EQ(show_running.GetResults().size(), 2U);

  // SHOW COMMITTING TRANSACTIONS should see none (nothing is committing)
  auto show_committing = this->main_interpreter.Interpret("SHOW COMMITTING TRANSACTIONS");
  ASSERT_EQ(show_committing.GetResults().size(), 0U);

  // SHOW TERMINATING TRANSACTIONS should also see none
  auto show_terminating = this->main_interpreter.Interpret("SHOW TERMINATING TRANSACTIONS");
  ASSERT_EQ(show_terminating.GetResults().size(), 0U);

  // SHOW RUNNING, COMMITTING TRANSACTIONS should still see both running ones
  auto show_multi = this->main_interpreter.Interpret("SHOW RUNNING, COMMITTING TRANSACTIONS");
  ASSERT_EQ(show_multi.GetResults().size(), 2U);

  running_thread.request_stop();
  running_thread.join();
  this->running_interpreter.Abort();
}

TYPED_TEST(TransactionQueueSimpleTest, ShowFilteredTransactionsWithTerminated) {
  // Start a transaction on the running interpreter
  this->running_interpreter.Interpret("BEGIN");
  this->running_interpreter.Interpret("CREATE (:Person {prop: 1})");

  // Manually set the status to TERMINATED to simulate a terminated transaction
  this->running_interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::TERMINATED,
                                                                  std::memory_order_release);

  // SHOW TERMINATING TRANSACTIONS should see only the terminated one
  auto show_terminating = this->main_interpreter.Interpret("SHOW TERMINATING TRANSACTIONS");
  ASSERT_EQ(show_terminating.GetResults().size(), 1U);
  EXPECT_EQ(show_terminating.GetResults()[0][3].ValueString(), "terminating");

  // SHOW RUNNING TRANSACTIONS should see only the main interpreter (running SHOW itself)
  auto show_running = this->main_interpreter.Interpret("SHOW RUNNING TRANSACTIONS");
  ASSERT_EQ(show_running.GetResults().size(), 1U);
  EXPECT_EQ(show_running.GetResults()[0][3].ValueString(), "running");

  // SHOW RUNNING, TERMINATING TRANSACTIONS should see both
  auto show_multi = this->main_interpreter.Interpret("SHOW RUNNING, TERMINATING TRANSACTIONS");
  ASSERT_EQ(show_multi.GetResults().size(), 2U);

  // SHOW COMMITTING TRANSACTIONS should see none of the above
  auto show_committing = this->main_interpreter.Interpret("SHOW COMMITTING TRANSACTIONS");
  ASSERT_EQ(show_committing.GetResults().size(), 0U);

  // Restore to IDLE so the test can clean up
  this->running_interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::IDLE,
                                                                  std::memory_order_release);
}
