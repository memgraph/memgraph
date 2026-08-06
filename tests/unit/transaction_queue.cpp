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

#include <algorithm>
#include <chrono>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include "gmock/gmock.h"

#include "disk_test_utils.hpp"
#include "interpreter_faker.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter_context.hpp"
#include "storage/v2/disk/storage.hpp"
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
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
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
    // After TERMINATE, the transaction should eventually disappear or stay in some other state.
    // However, for this test, we just want to make sure we don't crash and the status is NOT "terminating" anymore.
    auto show_stream_after_killing = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
    // It should only show the SHOW TRANSACTIONS itself, because the terminated one is hidden.
    ASSERT_EQ(show_stream_after_killing.GetResults().size(), 1U);
    for (const auto &row : show_stream_after_killing.GetResults()) {
      EXPECT_NE(row[3].ValueString(), "terminating");
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

// ShowTransactionStatusTerminated removed because TERMINATED status is no longer visible

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

TYPED_TEST(TransactionQueueSimpleTest, StrictIdParsing) {
  // A transaction id must parse in full. Previously a trailing-garbage id parsed as its
  // numeric prefix and silently terminated a different transaction, while an unparseable
  // id reported a kill attempt on a bogus UINT64_MAX id.
  this->running_interpreter.Interpret("BEGIN");
  std::string const tx_id = std::to_string(this->running_interpreter.interpreter.GetTransactionId().value());

  EXPECT_THROW(this->main_interpreter.Interpret("TERMINATE TRANSACTIONS '" + tx_id + "abc'"),
               memgraph::query::QueryRuntimeException);
  EXPECT_THROW(this->main_interpreter.Interpret("TERMINATE TRANSACTIONS 'ALL'"),
               memgraph::query::QueryRuntimeException);
  EXPECT_THROW(this->main_interpreter.Interpret("TERMINATE TRANSACTIONS ''"), memgraph::query::QueryRuntimeException);
  // Non-string literals are rejected rather than degrading to a bogus id. A real id can't be
  // used unquoted here: ids start at 1<<63, which overflows an integer literal.
  EXPECT_THROW(this->main_interpreter.Interpret("TERMINATE TRANSACTIONS 123"), memgraph::query::QueryRuntimeException);
  // Ids are unsigned, so a sign is never part of a valid id — whether it leads or is embedded.
  EXPECT_THROW(this->main_interpreter.Interpret("TERMINATE TRANSACTIONS '-" + tx_id + "'"),
               memgraph::query::QueryRuntimeException);
  EXPECT_THROW(this->main_interpreter.Interpret("TERMINATE TRANSACTIONS '+" + tx_id + "'"),
               memgraph::query::QueryRuntimeException);
  EXPECT_THROW(this->main_interpreter.Interpret("TERMINATE TRANSACTIONS '" + tx_id + "+1'"),
               memgraph::query::QueryRuntimeException);
  // 2^64, the first value that no longer fits an id, is rejected instead of wrapping.
  EXPECT_THROW(this->main_interpreter.Interpret("TERMINATE TRANSACTIONS '18446744073709551616'"),
               memgraph::query::QueryRuntimeException);

  // The victim survived every rejected attempt.
  auto show_stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(show_stream.GetResults().size(), 2U);

  // Leading zeros are digits, so the id still parses in full and names the same transaction.
  auto terminate_stream = this->main_interpreter.Interpret("TERMINATE TRANSACTIONS '00" + tx_id + "'");
  ASSERT_EQ(terminate_stream.GetResults().size(), 1U);
  EXPECT_EQ(terminate_stream.GetResults()[0][0].ValueString(), tx_id);
  EXPECT_TRUE(terminate_stream.GetResults()[0][1].ValueBool());

  this->running_interpreter.Abort();
}

TYPED_TEST(TransactionQueueSimpleTest, WildcardTerminatesOther) {
  this->running_interpreter.Interpret("BEGIN");
  this->running_interpreter.Interpret("CREATE (:Person {prop: 1})");
  std::string const victim_id = std::to_string(this->running_interpreter.interpreter.GetTransactionId().value());

  auto terminate_stream = this->main_interpreter.Interpret("TERMINATE TRANSACTIONS \"*\"");
  ASSERT_EQ(terminate_stream.GetResults().size(), 1U);
  EXPECT_EQ(terminate_stream.GetResults()[0][0].ValueString(), victim_id);
  EXPECT_TRUE(terminate_stream.GetResults()[0][1].ValueBool());
  EXPECT_EQ(this->running_interpreter.interpreter.transaction_status_.load(std::memory_order_acquire),
            memgraph::query::TransactionStatus::TERMINATED);

  this->running_interpreter.Abort();
}

TYPED_TEST(TransactionQueueSimpleTest, WildcardExcludesSelf) {
  // Nothing else is running, so the sweep finds nothing. Crucially the statement itself
  // succeeds: had it terminated its own transaction, its commit would have thrown and the
  // caller would never learn what it killed.
  auto terminate_stream = this->main_interpreter.Interpret("TERMINATE TRANSACTIONS \"*\"");
  EXPECT_EQ(terminate_stream.GetResults().size(), 0U);

  // The issuing interpreter is still usable.
  auto show_stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  EXPECT_EQ(show_stream.GetResults().size(), 1U);
}

TYPED_TEST(TransactionQueueSimpleTest, WildcardRowsSortedAscending) {
  auto victim_2 = std::make_unique<InterpreterFaker>(&this->interpreter_context, this->db);
  auto victim_3 = std::make_unique<InterpreterFaker>(&this->interpreter_context, this->db);

  this->running_interpreter.Interpret("BEGIN");
  victim_2->Interpret("BEGIN");
  victim_3->Interpret("BEGIN");

  auto terminate_stream = this->main_interpreter.Interpret("TERMINATE TRANSACTIONS \"*\"");
  ASSERT_EQ(terminate_stream.GetResults().size(), 3U);

  std::vector<uint64_t> reported_ids;
  for (const auto &row : terminate_stream.GetResults()) {
    EXPECT_TRUE(row[1].ValueBool());
    reported_ids.push_back(std::stoull(row[0].ValueString()));
  }
  EXPECT_TRUE(std::ranges::is_sorted(reported_ids));

  this->running_interpreter.Abort();
  victim_2->Abort();
  victim_3->Abort();
}

TYPED_TEST(TransactionQueueSimpleTest, WildcardSkipsCommitting) {
  this->running_interpreter.Interpret("BEGIN");
  this->running_interpreter.Interpret("CREATE (:Person {prop: 1})");

  // Only an ACTIVE transaction can be pinned for termination, so a committing one is skipped.
  this->running_interpreter.interpreter.transaction_status_.store(
      memgraph::query::TransactionStatus::STARTED_COMMITTING, std::memory_order_release);

  auto terminate_stream = this->main_interpreter.Interpret("TERMINATE TRANSACTIONS \"*\"");
  EXPECT_EQ(terminate_stream.GetResults().size(), 0U);

  this->running_interpreter.interpreter.transaction_status_.store(memgraph::query::TransactionStatus::IDLE,
                                                                  std::memory_order_release);
}

TYPED_TEST(TransactionQueueSimpleTest, WildcardRejectsMixedList) {
  this->running_interpreter.Interpret("BEGIN");
  std::string const victim_id = std::to_string(this->running_interpreter.interpreter.GetTransactionId().value());

  // Mixing is rejected in either order, and a repeated wildcard is still a list.
  EXPECT_THROW(this->main_interpreter.Interpret("TERMINATE TRANSACTIONS \"*\", '" + victim_id + "'"),
               memgraph::query::QueryRuntimeException);
  EXPECT_THROW(this->main_interpreter.Interpret("TERMINATE TRANSACTIONS '" + victim_id + "', \"*\""),
               memgraph::query::QueryRuntimeException);
  EXPECT_THROW(this->main_interpreter.Interpret("TERMINATE TRANSACTIONS \"*\", \"*\""),
               memgraph::query::QueryRuntimeException);

  // The victim survived every rejected attempt.
  EXPECT_EQ(this->running_interpreter.interpreter.transaction_status_.load(std::memory_order_acquire),
            memgraph::query::TransactionStatus::ACTIVE);

  this->running_interpreter.Abort();
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
  ASSERT_EQ(header.size(), 8U);
  EXPECT_EQ(header[0], "username");
  EXPECT_EQ(header[1], "transaction_id");
  EXPECT_EQ(header[2], "query");
  EXPECT_EQ(header[3], "status");
  EXPECT_EQ(header[4], "metadata");
  EXPECT_EQ(header[5], "start_time");
  EXPECT_EQ(header[6], "elapsed_ms");
  EXPECT_EQ(header[7], "database");
}

TYPED_TEST(TransactionQueueSimpleTest, DatabaseColumnPresentWithCorrectArity) {
  // Pins the "database" column: it must exist at index 7, and the row must stay 8 wide --
  // that's what actually breaks on a revert. This fixture never sets config.salient.name, so
  // this->db->name() is the empty string; the EXPECT_EQ below therefore only proves the column
  // carries the fixture's (empty) name, not that ShowTransactions reports a real, non-empty
  // per-session database name. That value-level check lives in
  // tests/e2e/transaction_queue/test_transaction_queue.py::test_show_transactions_database_column.
  //
  // The column is the last one appended by ShowTransactions, sourced from CurrentDB::name().
  auto show_stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
  ASSERT_EQ(show_stream.GetResults().size(), 1U);

  auto &row = show_stream.GetResults()[0];
  ASSERT_EQ(row.size(), 8U);
  EXPECT_EQ(row[2].ValueList().at(0).ValueString(), "SHOW TRANSACTIONS");
  ASSERT_TRUE(row[7].IsString());
  EXPECT_EQ(row[7].ValueString(), this->db->name());
}

TYPED_TEST(TransactionQueueSimpleTest, ElapsedMsAdvances) {
  this->running_interpreter.Interpret("BEGIN");
  const auto run_tx_id = this->running_interpreter.interpreter.GetTransactionId();
  ASSERT_TRUE(run_tx_id.has_value());
  const auto run_tx_id_str = std::to_string(*run_tx_id);

  auto get_running_elapsed = [&]() -> int64_t {
    auto stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
    for (const auto &row : stream.GetResults()) {
      if (row[1].ValueString() == run_tx_id_str) {
        EXPECT_TRUE(row[5].IsZonedDateTime());
        EXPECT_TRUE(row[6].IsInt());
        return row[6].ValueInt();
      }
    }
    ADD_FAILURE() << "running transaction not visible in SHOW TRANSACTIONS";
    return -1;
  };

  auto const d1 = get_running_elapsed();
  EXPECT_GE(d1, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  auto const d2 = get_running_elapsed();
  EXPECT_GT(d2, d1);

  this->running_interpreter.Interpret("ROLLBACK");
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

  // SHOW TERMINATING TRANSACTIONS should also see none (actually it's now removed from grammar)
  // but if it was in the grammar it would see none.
  // Since it's removed from grammar, this query might fail or just not execute.
  // The user says they removed the ability to see a TERMINATING transaction (and to filter by it).

  // SHOW RUNNING, COMMITTING TRANSACTIONS should still see both running ones
  auto show_multi = this->main_interpreter.Interpret("SHOW RUNNING, COMMITTING TRANSACTIONS");
  ASSERT_EQ(show_multi.GetResults().size(), 2U);

  running_thread.request_stop();
  running_thread.join();
  this->running_interpreter.Abort();
}

// ShowFilteredTransactionsWithTerminated removed

// Regression test for the db_acc_mutex_ leaf-lock invariant in query::CurrentDB (interpreter.hpp):
// SetCurrentDB/ResetDB/ReleaseDbIfMarked must swap the outgoing DatabaseAccess out from under
// db_acc_mutex_ and destroy it only after releasing that lock -- never while the lock is held. If that
// invariant regressed (e.g. back to `db_acc_ = std::move(new_db);` under the lock), the outgoing
// Accessor's dtor -- which takes the (unrelated, per-tenant) GKInternals::mutex_ -- would run nested
// under db_acc_mutex_, and GetActiveUsersInfo takes db_acc_mutex_ (via foreign_db_view()) while holding
// the interpreters SpinLock, so a slow-to-release GKInternals::mutex_ elsewhere would stall that spinlock
// across the whole session table.
//
// What this test does NOT cover: it cannot reproduce that exact cross-tenant stall. Every Gatekeeper API
// that holds GKInternals::mutex_ for a non-trivial duration (finish_suspend(), try_delete(),
// try_exclusively()) requires sole ownership (count_==1) as a precondition, which a live CurrentDB
// accessor on the same Database structurally rules out -- and GKInternals itself is a private
// implementation detail of Gatekeeper<T>, not reachable from a test for direct instrumentation. So this
// is a structural/API-level regression test, not a reproduction of the originally-hypothesized deadlock.
// What it DOES cover: heavy concurrent SetCurrentDB/ResetDB/ReleaseDbIfMarked churn on the "owning" thread
// against concurrent foreign_db_view() reads from a second thread (a) completes within a bounded time
// (no hang), and (b) never observes a torn/inconsistent {name, marked_for_deletion} pair -- which is what
// a broken swap (e.g. destroying db_acc_ mid-read) would produce.
TYPED_TEST(TransactionQueueSimpleTest, CurrentDBChurnVsForeignDbViewNoTornReads) {
  memgraph::query::CurrentDB current_db{std::move(this->db_gk.access().value())};
  auto const expected_name = this->db->name();

  constexpr int kIters = 20000;
  std::atomic<int> churn_count{0};

  std::jthread churn_thread([&] {
    for (int i = 0; i < kIters; ++i) {
      auto acc = this->db_gk.access();
      ASSERT_TRUE(acc.has_value());
      current_db.SetCurrentDB(std::move(*acc), i % 2 == 0);
      current_db.ReleaseDbIfMarked();  // never marked in this test; exercises the third writer's lock path
      current_db.ResetDB();
      churn_count.fetch_add(1, std::memory_order_relaxed);
    }
  });

  std::atomic<int> read_count{0};
  std::jthread reader_thread([&](std::stop_token st) {
    while (!st.stop_requested()) {
      auto view = current_db.foreign_db_view();
      // Either no db is currently held (ResetDB() won the race for that instant) or it's exactly this
      // fixture's db -- never a partial/garbage string, which is what a torn read across the swap would
      // produce.
      EXPECT_TRUE(view.name.empty() || view.name == expected_name) << "torn read: '" << view.name << "'";
      EXPECT_FALSE(view.marked_for_deletion);
      read_count.fetch_add(1, std::memory_order_relaxed);
    }
  });

  // Bounded by construction: churn_thread runs a fixed kIters loop (no wait/backoff), so this join cannot
  // hang unless the leaf-lock invariant is broken and something blocks db_acc_mutex_ indefinitely.
  churn_thread.join();
  reader_thread.request_stop();
  reader_thread.join();

  EXPECT_EQ(churn_count.load(), kIters);
  EXPECT_GT(read_count.load(), 0);
  current_db.ResetDB();
}
