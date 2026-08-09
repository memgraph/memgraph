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

#include "dbms/constants.hpp"
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
  ASSERT_EQ(header.size(), 7U);
  EXPECT_EQ(header[0], "username");
  EXPECT_EQ(header[1], "transaction_id");
  EXPECT_EQ(header[2], "query");
  EXPECT_EQ(header[3], "status");
  EXPECT_EQ(header[4], "metadata");
  EXPECT_EQ(header[5], "start_time");
  EXPECT_EQ(header[6], "elapsed_ms");
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

// TERMINATE SESSIONS authorization.
//
// The privilege checker is injected instead of driven through a real auth backend on purpose. In production it is a
// one-line forward to QueryUserOrRole::IsAuthorized(TRANSACTION_MANAGEMENT, db_name) (see HandleTransactionQueueQuery
// in src/query/interpreter.cpp), and what these tests pin is not what that backend decides -- it is *which database
// name reaches it*. Calling the checker directly makes that argument observable and keeps the tests independent of
// auth's own logic, which has its own coverage.

TYPED_TEST(TransactionQueueSimpleTest, PassesTheTargetSessionsDatabaseToThePrivilegeChecker) {
  // The fixture's Config leaves salient.name empty, which a real tenant never is; without a name the target would
  // short-circuit into the "holds no database" refusal and never reach the checker. Named the way
  // DbmsHandler::Rename does it.
  this->db->storage()->config_.salient.name = "tenant_a";

  auto &target = this->running_interpreter.interpreter;
  auto &caller = this->main_interpreter.interpreter;
  target.SetUser(this->running_interpreter.auth_checker.GenQueryUser("bob", {}));
  target.SetSessionInfo("target-session-uuid", "bob", "ts");
  caller.SetUser(this->main_interpreter.auth_checker.GenQueryUser("admin", {}));

  std::vector<std::string> checked_db_names;
  auto checker = [&checked_db_names](memgraph::query::QueryUserOrRole *, std::string const &db_name) {
    checked_db_names.push_back(db_name);
    return true;
  };

  auto result = this->interpreter_context.interpreters.WithLock([&](auto &interpreters) {
    return this->interpreter_context.TerminateSessions(
        interpreters, {"target-session-uuid"}, caller.user_or_role_.get(), checker, "caller-session-uuid");
  });

  // The whole point of the fix: authorization is scoped to the target's tenant, and to nothing else. Asserted
  // against the literal rather than this->db->name(), which would only re-read the name written above and so would
  // still hold if the callee echoed back whatever name it was handed.
  ASSERT_EQ(checked_db_names.size(), 1U);
  EXPECT_EQ(checked_db_names[0], "tenant_a");
  // The outcome too: a checker that grants must actually kill, or "right name, refused anyway" would pass here.
  ASSERT_EQ(result.rows.size(), 1U);
  EXPECT_EQ(result.rows[0][0].ValueString(), "target-session-uuid");
  EXPECT_TRUE(result.rows[0][1].ValueBool());
  EXPECT_THAT(result.to_close, ::testing::ElementsAre("target-session-uuid"));
}

TYPED_TEST(TransactionQueueSimpleTest, RefusesWhenTheCheckerDeniesTheTargetsDatabase) {
  this->db->storage()->config_.salient.name = "tenant_a";  // see PassesTheTargetSessionsDatabaseToThePrivilegeChecker

  auto &target = this->running_interpreter.interpreter;
  auto &caller = this->main_interpreter.interpreter;
  target.SetUser(this->running_interpreter.auth_checker.GenQueryUser("bob", {}));
  target.SetSessionInfo("target-session-uuid", "bob", "ts");
  caller.SetUser(this->main_interpreter.auth_checker.GenQueryUser("admin", {}));

  // Authorized on some other tenant only -- i.e. exactly the admin that used to get through.
  auto checker = [](memgraph::query::QueryUserOrRole *, std::string const &db_name) {
    return db_name == "some_other_tenant";
  };

  auto result = this->interpreter_context.interpreters.WithLock([&](auto &interpreters) {
    return this->interpreter_context.TerminateSessions(
        interpreters, {"target-session-uuid"}, caller.user_or_role_.get(), checker, "caller-session-uuid");
  });

  ASSERT_EQ(result.rows.size(), 1U);
  EXPECT_EQ(result.rows[0][0].ValueString(), "target-session-uuid");
  EXPECT_FALSE(result.rows[0][1].ValueBool());
  EXPECT_TRUE(result.to_close.empty());
}

TYPED_TEST(TransactionQueueSimpleTest, AllowsWhenTheCheckerGrantsTheTargetsDatabase) {
  this->db->storage()->config_.salient.name = "tenant_a";  // see PassesTheTargetSessionsDatabaseToThePrivilegeChecker

  auto &target = this->running_interpreter.interpreter;
  auto &caller = this->main_interpreter.interpreter;
  target.SetUser(this->running_interpreter.auth_checker.GenQueryUser("bob", {}));
  target.SetSessionInfo("target-session-uuid", "bob", "ts");
  caller.SetUser(this->main_interpreter.auth_checker.GenQueryUser("admin", {}));

  // Literal, not this->db->name(): the grant must be pinned to the name this test wrote, not to whatever name the
  // callee happens to pass back.
  auto checker = [](memgraph::query::QueryUserOrRole *, std::string const &db_name) { return db_name == "tenant_a"; };

  auto result = this->interpreter_context.interpreters.WithLock([&](auto &interpreters) {
    return this->interpreter_context.TerminateSessions(
        interpreters, {"target-session-uuid"}, caller.user_or_role_.get(), checker, "caller-session-uuid");
  });

  ASSERT_EQ(result.rows.size(), 1U);
  EXPECT_EQ(result.rows[0][0].ValueString(), "target-session-uuid");
  EXPECT_TRUE(result.rows[0][1].ValueBool());
  EXPECT_THAT(result.to_close, ::testing::ElementsAre("target-session-uuid"));
}

TYPED_TEST(TransactionQueueSimpleTest, FallsBackToTheDefaultDbForATargetHoldingNoDatabase) {
  // Named first, so the fallback below can only come from the ResetDB and not from a fixture that never had a name.
  this->db->storage()->config_.salient.name = "tenant_a";

  auto &target = this->running_interpreter.interpreter;
  auto &caller = this->main_interpreter.interpreter;
  target.SetUser(this->running_interpreter.auth_checker.GenQueryUser("bob", {}));
  target.SetSessionInfo("target-session-uuid", "bob", "ts");
  caller.SetUser(this->main_interpreter.auth_checker.GenQueryUser("admin", {}));
  target.current_db_.ResetDB();

  std::vector<std::string> checked_db_names;
  auto checker = [&checked_db_names](memgraph::query::QueryUserOrRole *, std::string const &db_name) {
    checked_db_names.push_back(db_name);
    return true;
  };

  auto result = this->interpreter_context.interpreters.WithLock([&](auto &interpreters) {
    return this->interpreter_context.TerminateSessions(
        interpreters, {"target-session-uuid"}, caller.user_or_role_.get(), checker, "caller-session-uuid");
  });

  // No tenant means no database-scoped privilege could be evaluated against the target's own database, so the
  // check falls back to dbms::kDefaultDB rather than skipping the checker outright.
  ASSERT_EQ(checked_db_names.size(), 1U);
  EXPECT_EQ(checked_db_names[0], memgraph::dbms::kDefaultDB);
  ASSERT_EQ(result.rows.size(), 1U);
  EXPECT_TRUE(result.rows[0][1].ValueBool());
  EXPECT_THAT(result.to_close, ::testing::ElementsAre("target-session-uuid"));
}

TYPED_TEST(TransactionQueueSimpleTest, RefusesATargetHoldingNoDatabaseWhenTheCheckerDeniesTheDefaultDb) {
  // Negative control for FallsBackToTheDefaultDbForATargetHoldingNoDatabase: the fallback must still be a real
  // privilege check, not an unconditional grant, so a checker that denies dbms::kDefaultDB must refuse.
  this->db->storage()->config_.salient.name = "tenant_a";

  auto &target = this->running_interpreter.interpreter;
  auto &caller = this->main_interpreter.interpreter;
  target.SetUser(this->running_interpreter.auth_checker.GenQueryUser("bob", {}));
  target.SetSessionInfo("target-session-uuid", "bob", "ts");
  caller.SetUser(this->main_interpreter.auth_checker.GenQueryUser("admin", {}));
  target.current_db_.ResetDB();

  // Authorized on some other tenant only -- the fallback target (dbms::kDefaultDB) is not among them.
  auto checker = [](memgraph::query::QueryUserOrRole *, std::string const &db_name) {
    return db_name == "some_other_tenant";
  };

  auto result = this->interpreter_context.interpreters.WithLock([&](auto &interpreters) {
    return this->interpreter_context.TerminateSessions(
        interpreters, {"target-session-uuid"}, caller.user_or_role_.get(), checker, "caller-session-uuid");
  });

  ASSERT_EQ(result.rows.size(), 1U);
  EXPECT_FALSE(result.rows[0][1].ValueBool());
  EXPECT_TRUE(result.to_close.empty());
}

TYPED_TEST(TransactionQueueSimpleTest, SameUserIsStillAllowedWithoutAnyPrivilege) {
  // Named, so the target does hold a tenant and the deny below is a real deny -- the kill must succeed anyway.
  this->db->storage()->config_.salient.name = "tenant_a";

  auto &target = this->running_interpreter.interpreter;
  auto &caller = this->main_interpreter.interpreter;
  target.SetUser(this->running_interpreter.auth_checker.GenQueryUser("admin", {}));
  target.SetSessionInfo("target-session-uuid", "admin", "ts");
  caller.SetUser(this->main_interpreter.auth_checker.GenQueryUser("admin", {}));

  bool checker_called = false;
  auto checker = [&checker_called](memgraph::query::QueryUserOrRole *, std::string const &) {
    checker_called = true;
    return false;
  };

  auto result = this->interpreter_context.interpreters.WithLock([&](auto &interpreters) {
    return this->interpreter_context.TerminateSessions(
        interpreters, {"target-session-uuid"}, caller.user_or_role_.get(), checker, "caller-session-uuid");
  });

  // A user may always terminate their own other connections; the privilege check is never reached.
  ASSERT_EQ(result.rows.size(), 1U);
  EXPECT_TRUE(result.rows[0][1].ValueBool());
  EXPECT_THAT(result.to_close, ::testing::ElementsAre("target-session-uuid"));
  EXPECT_FALSE(checker_called);
}

// This is the authorization half of the use-after-free fix: a foreign reader that observes a torn
// `user_or_role_` can conclude "same user" and skip both the empty-database refusal and the privilege
// checker entirely -- an authorization bypass, not merely a memory-safety bug. The published snapshot
// (foreign_user_view_/foreign_session_view_, see their declaration in interpreter.hpp) is what makes the
// identity TerminateSessions compares stable against that torn read.
//
// Honesty note: on an unfixed tree the failure this test looks for is probabilistic -- a torn read has to
// land during the narrow window between the writer thread's SetSessionInfo/SetUser/ResetUser calls, so this
// test can pass by luck even without the fix. TSan against TerminateSessionsRacingIdentityChurnIsDataRaceFree
// below is the deterministic instrument for the same race.
TYPED_TEST(TransactionQueueSimpleTest, TerminateSessionsCannotBeTrickedIntoSkippingThePrivilegeCheck) {
  this->db->storage()->config_.salient.name = "tenant_a";  // see PassesTheTargetSessionsDatabaseToThePrivilegeChecker

  auto &target = this->running_interpreter.interpreter;
  auto &caller = this->main_interpreter.interpreter;
  // "admin" is a username the writer thread below never sets on the target ("bob", or logged-off in between),
  // so SameUser must be false on every single iteration -- there is no legitimate path to a kill here.
  caller.SetUser(this->main_interpreter.auth_checker.GenQueryUser("admin", {}));
  // Publish the target's session identity once, from this thread, before the writer starts. Without this,
  // foreign_session_view_ is still null for however many reader iterations run before the writer's own first
  // SetSessionInfo call lands; TerminateSessions then takes the not-found branch and never reaches the
  // checker, so checker_call_count below would measure that startup gap instead of the invariant. The
  // writer's own repeated SetSessionInfo calls simply republish the same uuid -- that is the churn under
  // test, and it never makes the target unfindable.
  target.SetSessionInfo("target-session-uuid", "bob", "ts");

  int checker_call_count = 0;
  auto checker = [&checker_call_count](memgraph::query::QueryUserOrRole *, std::string const &) {
    ++checker_call_count;
    return false;  // always refuse; a kill can only happen here by skipping this call entirely
  };

  constexpr int kIterations = 2000;
  std::atomic<int> writer_iterations{0};
  bool saw_unauthorized_kill = false;
  bool saw_nonempty_to_close = false;

  {
    // jthread: joins unconditionally on scope exit -- including through a failed EXPECT below -- so a bad
    // run can never leave a detached thread still churning the target's identity.
    std::jthread writer([this, &target, &writer_iterations] {
      for (int i = 0; i < kIterations; ++i) {
        target.SetSessionInfo("target-session-uuid", "bob", "ts");
        target.SetUser(this->running_interpreter.auth_checker.GenQueryUser("bob", {}));
        target.ResetUser();
        writer_iterations.fetch_add(1, std::memory_order_relaxed);
      }
    });

    for (int i = 0; i < kIterations; ++i) {
      // Not EXPECT/ASSERT here on purpose -- this runs 2000 times per test. Accumulate into plain flags and
      // assert once after the loop.
      auto result = this->interpreter_context.interpreters.WithLock([&](auto &interpreters) {
        return this->interpreter_context.TerminateSessions(
            interpreters, {"target-session-uuid"}, caller.user_or_role_.get(), checker, "caller-session-uuid");
      });
      if (!result.rows.empty() && result.rows[0][1].ValueBool()) saw_unauthorized_kill = true;
      if (!result.to_close.empty()) saw_nonempty_to_close = true;
    }
  }

  // The loop actually ran the full count, so the assertions below cannot be passing vacuously.
  EXPECT_EQ(writer_iterations.load(std::memory_order_relaxed), kIterations);
  // The checker was consulted on every single iteration: no branch (including the empty-database refusal --
  // the target here always holds "tenant_a") returned a decision that bypassed it.
  EXPECT_EQ(checker_call_count, kIterations);
  // The invariant this test exists to pin: caller != target on every iteration and the checker always
  // refuses, so nothing may ever be killed and nothing may ever be handed back for the caller to close. A
  // single killed==true or non-empty to_close means a torn read of the target's identity was mistaken for
  // "same user" and both the privilege check and the empty-database refusal were skipped -- the bypass.
  EXPECT_FALSE(saw_unauthorized_kill);
  EXPECT_FALSE(saw_nonempty_to_close);
}

// Under a normal (non-TSan) build this test asserts almost nothing about outcomes -- its value is that it is
// the workload TSan is run against. It exercises TerminateSessions concurrently with a writer thread churning
// the target's identity (SetSessionInfo/SetUser/ResetUser) the same way
// TerminateSessionsCannotBeTrickedIntoSkippingThePrivilegeCheck above does. Without the published-snapshot
// fix, TSan reports a data race on Interpreter::user_or_role_ / Interpreter::session_info_ here.
//
// InterpreterContext::ShowTransactionsUsingDBName was considered for this test as a second foreign-read path,
// but its implementation (src/query/interpreter_context.cpp) only reads current_db_ and
// transaction_status_/GetTransactionId, guarded by TryAcquireForVerification's CAS -- it never touches
// user_or_role_ or session_info_ at all, so it would not exercise the race this test is for. Dropped rather
// than wired in for its own sake; TerminateSessions alone already reads both racy fields.
TYPED_TEST(TransactionQueueSimpleTest, TerminateSessionsRacingIdentityChurnIsDataRaceFree) {
  this->db->storage()->config_.salient.name = "tenant_a";  // see PassesTheTargetSessionsDatabaseToThePrivilegeChecker

  auto &target = this->running_interpreter.interpreter;
  auto &caller = this->main_interpreter.interpreter;
  caller.SetUser(this->main_interpreter.auth_checker.GenQueryUser("admin", {}));
  // Publish once before the writer starts, same reasoning as the test above: otherwise the target is
  // unfindable until the writer's first SetSessionInfo call, and TerminateSessions exercises far less of
  // itself (the not-found early-exit) instead of the racy identity-comparison path this test is for.
  target.SetSessionInfo("target-session-uuid", "bob", "ts");

  auto checker = [](memgraph::query::QueryUserOrRole *, std::string const &) { return false; };

  constexpr int kIterations = 2000;
  std::atomic<int> writer_iterations{0};
  int reader_iterations = 0;

  {
    // jthread: joins unconditionally on scope exit, same reasoning as the test above.
    std::jthread writer([this, &target, &writer_iterations] {
      for (int i = 0; i < kIterations; ++i) {
        target.SetSessionInfo("target-session-uuid", "bob", "ts");
        target.SetUser(this->running_interpreter.auth_checker.GenQueryUser("bob", {}));
        target.ResetUser();
        writer_iterations.fetch_add(1, std::memory_order_relaxed);
      }
    });

    for (int i = 0; i < kIterations; ++i) {
      this->interpreter_context.interpreters.WithLock([&](auto &interpreters) {
        return this->interpreter_context.TerminateSessions(
            interpreters, {"target-session-uuid"}, caller.user_or_role_.get(), checker, "caller-session-uuid");
      });
      ++reader_iterations;
    }
  }

  // The only claim here: both loops ran to completion and the process is still standing. The outcome's
  // correctness is not this test's job (see the test above) -- reaching this line without TSan aborting the
  // binary is the actual assertion.
  EXPECT_EQ(writer_iterations.load(std::memory_order_relaxed), kIterations);
  EXPECT_EQ(reader_iterations, kIterations);
}
