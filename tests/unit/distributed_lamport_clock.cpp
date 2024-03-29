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
#include "distributed/lamport_clock.hpp"

#include <gtest/gtest.h>

using namespace memgraph::distributed;

// Define a test fixture for the LamportClock class
class LamportClockTest : public testing::Test {
 public:
  LamportClock<struct TestTag> clock1;
  LamportClock<struct TestTag> clock2;

  void SetUp() override {
    // Setup code, if any
  }

  void TearDown() override {
    // Tear down code, if any
  }
};

TEST_F(LamportClockTest, GetTimestampInternal) {
  auto ts1_1 = clock1.get_timestamp(internal);
  auto ts1_2 = clock1.get_timestamp(internal);
  ASSERT_GT(ts1_2, ts1_1);  // internal always increments
}

TEST_F(LamportClockTest, GetTimestampSend) {
  auto ts1_1 = clock1.get_timestamp(internal);
  auto ts1_2 = clock1.get_timestamp(send);
  ASSERT_GT(ts1_2, ts1_1);  // send always increments
}

TEST_F(LamportClockTest, GetTimestampReceive) {
  auto ts1_1 = clock1.get_timestamp(send);
  auto ts2_1 = clock2.get_timestamp(internal);
  auto ts2_2 = clock2.get_timestamp(receive, ts1_1);
  ASSERT_GE(ts2_1, ts1_1);  // receive is at least send +1
  ASSERT_GE(ts2_2, ts2_1);  // receive is at least internal +1
}
