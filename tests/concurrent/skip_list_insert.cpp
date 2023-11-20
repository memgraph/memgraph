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

#include <thread>
#include <vector>

#include "utils/skip_list.hpp"

const int kNumThreads = 8;
const uint64_t kMaxNum = 10000000;

int main() {
  memgraph::utils::SkipList<uint64_t> list;

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&list, i] {
      for (uint64_t num = i * kMaxNum; num < (i + 1) * kMaxNum; ++num) {
        auto acc = list.access();
        MG_ASSERT(acc.insert(num).second);
      }
    });
  }
  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
  }

  MG_ASSERT(list.size() == kMaxNum * kNumThreads);
  for (uint64_t i = 0; i < kMaxNum * kNumThreads; ++i) {
    auto acc = list.access();
    auto it = acc.find(i);
    MG_ASSERT(it != acc.end());
    MG_ASSERT(*it == i);
  }

  return 0;
}
