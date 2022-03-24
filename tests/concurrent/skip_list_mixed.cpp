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
#include <random>
#include <thread>
#include <vector>

#include "utils/skip_list.hpp"

// kNumThreadsRemove should be smaller than kNumThreadsInsert because there
// should be some leftover items in the list for the find threads.
const int kNumThreadsInsert = 5;
const int kNumThreadsRemove = 2;
const int kNumThreadsFind = 3;

const uint64_t kMaxNum = 10000000;

int main() {
  memgraph::utils::SkipList<uint64_t> list;

  std::atomic<bool> run{true}, modify_done{false};

  std::vector<std::thread> threads_modify, threads_find;

  for (int i = 0; i < kNumThreadsInsert; ++i) {
    threads_modify.push_back(std::thread([&list, i] {
      for (uint64_t num = i * kMaxNum; num < (i + 1) * kMaxNum; ++num) {
        auto acc = list.access();
        MG_ASSERT(acc.insert(num).second);
      }
    }));
  }
  for (int i = 0; i < kNumThreadsRemove; ++i) {
    threads_modify.push_back(std::thread([&list, i] {
      for (uint64_t num = i * kMaxNum; num < (i + 1) * kMaxNum; ++num) {
        auto acc = list.access();
        while (!acc.remove(num))
          ;
      }
    }));
  }

  for (int i = 0; i < kNumThreadsFind; ++i) {
    threads_find.push_back(std::thread([&list, &run, &modify_done, i] {
      std::mt19937 gen(3137 + i);
      std::uniform_int_distribution<uint64_t> dist(0, kNumThreadsInsert * kMaxNum - 1);
      while (run.load(std::memory_order_relaxed)) {
        auto acc = list.access();
        auto num = dist(gen);
        auto it = acc.find(num);
        if (modify_done.load(std::memory_order_relaxed) && num >= kNumThreadsRemove * kMaxNum) {
          MG_ASSERT(it != acc.end());
          MG_ASSERT(*it == num);
        }
      }
    }));
  }

  for (int i = 0; i < threads_modify.size(); ++i) {
    threads_modify[i].join();
  }

  modify_done.store(true, std::memory_order_relaxed);
  std::this_thread::sleep_for(std::chrono::seconds(10));
  run.store(false, std::memory_order_relaxed);

  for (int i = 0; i < threads_find.size(); ++i) {
    threads_find[i].join();
  }

  MG_ASSERT(list.size() == (kNumThreadsInsert - kNumThreadsRemove) * kMaxNum);
  for (uint64_t i = kMaxNum * kNumThreadsRemove; i < kMaxNum * kNumThreadsInsert; ++i) {
    auto acc = list.access();
    auto it = acc.find(i);
    MG_ASSERT(it != acc.end());
    MG_ASSERT(*it == i);
  }

  return 0;
}
