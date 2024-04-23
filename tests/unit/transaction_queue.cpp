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
#include <stop_token>
#include <string>
#include <thread>

#include <gtest/gtest.h>
#include "gmock/gmock.h"

#include "disk_test_utils.hpp"
#include "interpreter_faker.hpp"
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

  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateRootPath(config), false};
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
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          &repl_state,
                                                          system_state
#ifdef MG_ENTERPRISE
                                                          ,
                                                          std::nullopt
#endif
  };
  InterpreterFaker running_interpreter{&interpreter_context, db}, main_interpreter{&interpreter_context, db};

  void TearDown() override {
    disk_test_utils::RemoveRocksDbDirs(testSuite);
    std::filesystem::remove_all(data_directory);
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(TransactionQueueSimpleTest, StorageTypes);

TYPED_TEST(TransactionQueueSimpleTest, TwoInterpretersInterleaving) {
  bool started = false;
  std::jthread running_thread = std::jthread(
      [this, &started](std::stop_token st, int thread_index) {
        this->running_interpreter.Interpret("BEGIN");
        started = true;
      },
      0);

  {
    while (!started) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    this->main_interpreter.Interpret("CREATE (:Person {prop: 1})");
    auto show_stream = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
    ASSERT_EQ(show_stream.GetResults().size(), 2U);
    // superadmin executing the transaction
    EXPECT_EQ(show_stream.GetResults()[0][0].ValueString(), "");
    ASSERT_TRUE(show_stream.GetResults()[0][1].IsString());
    EXPECT_EQ(show_stream.GetResults()[0][2].ValueList().at(0).ValueString(), "SHOW TRANSACTIONS");
    // Also anonymous user executing
    EXPECT_EQ(show_stream.GetResults()[1][0].ValueString(), "");
    ASSERT_TRUE(show_stream.GetResults()[1][1].IsString());
    // Kill the other transaction
    std::string run_trans_id = show_stream.GetResults()[1][1].ValueString();
    std::string esc_run_trans_id = "'" + run_trans_id + "'";
    auto terminate_stream = this->main_interpreter.Interpret("TERMINATE TRANSACTIONS " + esc_run_trans_id);
    // check result of killing
    ASSERT_EQ(terminate_stream.GetResults().size(), 1U);
    EXPECT_EQ(terminate_stream.GetResults()[0][0].ValueString(), run_trans_id);
    ASSERT_TRUE(terminate_stream.GetResults()[0][1].ValueBool());  // that the transaction is actually killed
    // check the number of transactions now
    auto show_stream_after_killing = this->main_interpreter.Interpret("SHOW TRANSACTIONS");
    ASSERT_EQ(show_stream_after_killing.GetResults().size(), 1U);
    // test the state of the database
    auto results_stream = this->main_interpreter.Interpret("MATCH (n) RETURN n");
    ASSERT_EQ(results_stream.GetResults().size(), 1U);  // from the main interpreter
    this->main_interpreter.Interpret("MATCH (n) DETACH DELETE n");
    // finish thread
    running_thread.request_stop();
  }
}
