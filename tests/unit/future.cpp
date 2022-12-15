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

#include <string>
#include <thread>

#include "gtest/gtest.h"

#include "io/future.hpp"

using namespace memgraph::io;

void Fill(Promise<std::string> promise_1) { promise_1.Fill("success"); }

void Wait(Future<std::string> future_1, Promise<std::string> promise_2) {
  std::string result_1 = std::move(future_1).Wait();
  EXPECT_TRUE(result_1 == "success");
  promise_2.Fill("it worked");
}

TEST(Future, BasicLifecycle) {
  std::atomic_bool waiting = false;
  std::atomic_bool filled = false;

  std::function<bool()> wait_notifier = [&] {
    waiting.store(true, std::memory_order_seq_cst);
    return false;
  };

  std::function<bool()> fill_notifier = [&] {
    filled.store(true, std::memory_order_seq_cst);
    return false;
  };

  auto [future_1, promise_1] = FuturePromisePairWithNotifications<std::string>(wait_notifier, fill_notifier);
  auto [future_2, promise_2] = FuturePromisePair<std::string>();

  std::jthread t1(Wait, std::move(future_1), std::move(promise_2));

  // spin in a loop until the promise signals
  // that it is waiting
  while (!waiting.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  std::jthread t2(Fill, std::move(promise_1));

  t1.join();
  t2.join();

  EXPECT_TRUE(filled.load(std::memory_order_acquire));

  std::string result_2 = std::move(future_2).Wait();
  EXPECT_TRUE(result_2 == "it worked");
}
