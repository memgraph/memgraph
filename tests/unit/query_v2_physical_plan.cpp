// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <atomic>
#include <chrono>
#include <thread>

#include "gtest/gtest.h"

#include "query/v2/physical/physical.hpp"
#include "utils/thread_pool.hpp"

namespace memgraph::query::v2::tests {

class PhysicalPlanTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    thread_pool_.Shutdown();
  }

  physical::MultiframePool multiframe_pool_{16, 100};
  utils::ThreadPool thread_pool_{16};
};

TEST_F(PhysicalPlanTest, MultiframePool) {
  std::atomic<int> got_access;
  for (int i = 0; i < 1000000; ++i) {
    thread_pool_.AddTask([&]() {
      while (true) {
        auto token = multiframe_pool_.GetAccess();
        if (token) {
          ASSERT_TRUE(token->id >= 0 && token->id < 16);
          got_access.fetch_add(1);
          multiframe_pool_.ReturnAccess(token->id);
          break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    });
  }

  while (thread_pool_.UnfinishedTasksNum() != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(got_access.load(), 1000000);
}

}  // namespace memgraph::query::v2::tests
