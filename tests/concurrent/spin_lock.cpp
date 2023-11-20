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

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include "utils/spin_lock.hpp"

int x = 0;
memgraph::utils::SpinLock lock;

void test_lock() {
  using namespace std::literals;

  {
    std::unique_lock<memgraph::utils::SpinLock> guard(lock);
    x++;

    std::this_thread::sleep_for(25ms);

    MG_ASSERT(x < 2,
              "x always has to be less than 2 (other "
              "threads shouldn't be able to change the x simultaneously");
    x--;
  }
}

int main() {
  static constexpr int N = 16;
  std::vector<std::thread> threads;

  threads.reserve(N);
  for (int i = 0; i < N; ++i) threads.emplace_back(test_lock);

  for (auto &thread : threads) {
    thread.join();
  }

  return 0;
}
