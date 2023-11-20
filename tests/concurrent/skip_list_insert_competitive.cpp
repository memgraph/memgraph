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
#include <thread>
#include <vector>

#include "utils/skip_list.hpp"

const int kNumThreads = 8;
const uint64_t kMaxNum = 10000000;

int main() {
  memgraph::utils::SkipList<uint64_t> list;

  std::atomic<uint64_t> success{0};

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&list, &success] {
      for (uint64_t num = 0; num < kMaxNum; ++num) {
        auto acc = list.access();
        if (acc.insert(num).second) {
          success.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
  }
  MG_ASSERT(success == kMaxNum);

  MG_ASSERT(list.size() == kMaxNum);
  for (uint64_t i = 0; i < kMaxNum; ++i) {
    auto acc = list.access();
    auto it = acc.find(i);
    MG_ASSERT(it != acc.end());
    MG_ASSERT(*it == i);
  }

  return 0;
}
