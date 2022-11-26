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

#include <gtest/gtest.h>

#include "query/v2/physical/multiframe.hpp"
#include "utils/logging.hpp"
#include "utils/thread_pool.hpp"
#include "utils/timer.hpp"

namespace memgraph::query::v2::tests {

class MultiframePoolFixture : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    thread_pool_.Shutdown();
  }

  physical::multiframe::MPMCMultiframeFCFSPool multiframe_pool_{16, 100};
  utils::ThreadPool thread_pool_{16};
};

TEST_F(MultiframePoolFixture, ConcurrentMultiframePoolAccess) {
  std::atomic<int> readers_got_access_cnt;
  std::atomic<int> writers_got_access_cnt;
  utils::Timer timer;

  for (int i = 0; i < 1000000; ++i) {
    // Add readers
    thread_pool_.AddTask([&]() {
      while (true) {
        auto token = multiframe_pool_.GetFull();
        if (token) {
          ASSERT_TRUE(token->id >= 0 && token->id < 16);
          readers_got_access_cnt.fetch_add(1);
          multiframe_pool_.ReturnEmpty(token->id);
          break;
        }
      }
    });
    // Add writers
    thread_pool_.AddTask([&]() {
      while (true) {
        auto token = multiframe_pool_.GetEmpty();
        if (token) {
          ASSERT_TRUE(token->id >= 0 && token->id < 16);
          writers_got_access_cnt.fetch_add(1);
          multiframe_pool_.ReturnFull(token->id);
          break;
        }
      }
    });
  }
  SPDLOG_TRACE("All readers and writters scheduled");

  while (thread_pool_.UnfinishedTasksNum() != 0) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  ASSERT_EQ(readers_got_access_cnt.load(), 1000000);
  ASSERT_EQ(writers_got_access_cnt.load(), 1000000);
}

}  // namespace memgraph::query::v2::tests
