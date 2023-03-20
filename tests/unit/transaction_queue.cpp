// Copyright 2023 Memgraph Ltd.
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

#include "interpreter_faker.hpp"

/*
Tests rely on the fact that interpreters are sequentially added to runninng_interpreters to get transaction_id of its
corresponding interpreter/.
*/
class TransactionQueueSimpleTest : public ::testing::Test {
 protected:
  memgraph::storage::Storage db_;
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_tests_unit_transaction_queue_intr"};
  memgraph::query::InterpreterContext interpreter_context{&db_, {}, data_directory};
  InterpreterFaker running_interpreter{&interpreter_context}, main_interpreter{&interpreter_context};
};

// This test relies on explicit transations + explicit thread killing for testing if show transactions work as expected
TEST_F(TransactionQueueSimpleTest, ShowTransactions) {
  std::jthread running_thread = std::jthread(
      [this](int thread_index) {
        running_interpreter.Interpret("BEGIN");
        for (int i = 0; i < 5; i++) {
          running_interpreter.Interpret("CREATE (:Person {prop: " + std::to_string(thread_index) + "})");
          std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        running_interpreter.Interpret("ROLLBACK");
      },
      0);

  {
    main_interpreter.Interpret("CREATE (:Person {prop: 1})");
    auto show_stream = main_interpreter.Interpret("SHOW TRANSACTIONS");
    ASSERT_EQ(show_stream.GetResults().size(), 2U);
    // admin executing the transaction
    EXPECT_EQ(show_stream.GetResults()[0][0].ValueString(), "");
    ASSERT_TRUE(show_stream.GetResults()[0][1].IsString());
    EXPECT_EQ(show_stream.GetResults()[0][2].ValueList().at(0).ValueString(), "SHOW TRANSACTIONS");
    // Also admin executing
    EXPECT_EQ(show_stream.GetResults()[1][0].ValueString(), "");
    ASSERT_TRUE(show_stream.GetResults()[1][1].IsString());
    // test the state of the database
    auto results_stream = main_interpreter.Interpret("MATCH (n) RETURN n");
    ASSERT_EQ(results_stream.GetResults().size(), 1U);
    main_interpreter.Interpret("MATCH (n) DETACH DELETE n");
  }
}

TEST_F(TransactionQueueSimpleTest, TerminateTransaction) {
  std::jthread running_thread = std::jthread(
      [this](std::stop_token st, int thread_index) {
        running_interpreter.Interpret("BEGIN");
        while (!st.stop_requested()) {
          running_interpreter.Interpret("CREATE (:Person {prop: " + std::to_string(thread_index) + "})");
          std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
      },
      0);

  {
    main_interpreter.Interpret("CREATE (:Person {prop: 1})");
    auto show_stream = main_interpreter.Interpret("SHOW TRANSACTIONS");
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
    auto terminate_stream = main_interpreter.Interpret("TERMINATE TRANSACTIONS " + esc_run_trans_id);
    // check result of killing
    ASSERT_EQ(terminate_stream.GetResults().size(), 1U);
    EXPECT_EQ(terminate_stream.GetResults()[0][0].ValueString(), run_trans_id);
    ASSERT_TRUE(terminate_stream.GetResults()[0][1].ValueBool());  // that the transaction is actually killed
    // check the number of transactions now
    auto show_stream_after_killing = main_interpreter.Interpret("SHOW TRANSACTIONS");
    ASSERT_EQ(show_stream_after_killing.GetResults().size(), 1U);
    // test the state of the database
    auto results_stream = main_interpreter.Interpret("MATCH (n) RETURN n");
    ASSERT_EQ(results_stream.GetResults().size(), 1U);  // from the main interpreter
    main_interpreter.Interpret("MATCH (n) DETACH DELETE n");
    // finish thread
    running_thread.request_stop();
  }
}
