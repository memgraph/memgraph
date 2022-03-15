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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include <utils/thread_pool.hpp>

using namespace std::chrono_literals;

TEST(ThreadPool, Basic) {
  constexpr size_t adder_count = 500000;
  constexpr std::array<size_t, 5> pool_sizes{1, 2, 4, 8, 100};

  for (const auto pool_size : pool_sizes) {
    memgraph::utils::ThreadPool pool{pool_size};

    std::atomic<int> count{0};
    for (size_t i = 0; i < adder_count; ++i) {
      pool.AddTask([&] { count.fetch_add(1); });
    }

    while (pool.UnfinishedTasksNum() != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_EQ(count.load(), adder_count);
  }
}
