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

#include "interpreter_faker.hpp"

#include <gtest/gtest.h>
#include "gmock/gmock.h"

class TransactionQueueSimpleTest : public ::testing::Test {
 protected:
  memgraph::storage::Storage db_;
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_tests_unit_transaction_queue_intr"};
  memgraph::query::InterpreterContext interpreter_context{&db_, {}, data_directory};
  //    InterpreterFaker running_interpreter{&db_, {}, data_directory_running}
  InterpreterFaker main_interpreter{&db_, &interpreter_context};
};

TEST_F(TransactionQueueSimpleTest, OneInterpreterOneTransaction) {
  {
    auto stream = main_interpreter.Interpret("SHOW TRANSACTIONS");
    ASSERT_EQ(stream.GetHeader().size(), 3U);
    EXPECT_EQ(stream.GetHeader()[0], "username");
    EXPECT_EQ(stream.GetHeader()[1], "transaction_id");
    EXPECT_EQ(stream.GetHeader()[2], "query");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    EXPECT_EQ(stream.GetResults()[0][0].ValueString(), "");
    EXPECT_EQ(stream.GetResults()[0][2].ValueList().at(0).ValueString(), "SHOW TRANSACTIONS");
  }
}
