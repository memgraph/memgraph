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

#include "io/v3/future.hpp"
#include "utils/logging.hpp"

void Fill(MgPromise<std::string> promise_1) { promise_1.Fill("success"); }

void Wait(MgFuture<std::string> future_1, MgPromise<std::string> promise_2) {
  std::string result_1 = future_1.Wait();
  MG_ASSERT(result_1 == "success");
  promise_2.Fill("it worked");
}

int main() {
  std::atomic_bool filled = false;

  std::function<void()> notifier = [&] { filled.store(true, std::memory_order_seq_cst); };

  auto [future_1, promise_1] = FuturePromisePairWithNotifier<std::string>(notifier);
  auto [future_2, promise_2] = FuturePromisePair<std::string>();

  std::jthread t1(Wait, std::move(future_1), std::move(promise_2));

  // spin in a loop until the promise signals
  // that it is waiting
  while (!filled.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  std::jthread t2(Fill, std::move(promise_1));

  t1.join();
  t2.join();

  std::string result_2 = future_2.Wait();
  MG_ASSERT(result_2 == "it worked");

  return 0;
}
