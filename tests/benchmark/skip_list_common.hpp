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

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

DEFINE_int32(num_threads, 8, "Number of concurrent threads");
DEFINE_int32(duration, 10, "Duration of test (in seconds)");

struct Stats {
  uint64_t total{0};
  uint64_t succ[4] = {0, 0, 0, 0};
};

const int OP_INSERT = 0;
const int OP_CONTAINS = 1;
const int OP_REMOVE = 2;
const int OP_FIND = 3;

inline void RunConcurrentTest(std::function<void(std::atomic<bool> *, Stats *)> test_func) {
  std::atomic<bool> run{true};

  std::unique_ptr<Stats[]> stats(new Stats[FLAGS_num_threads]);

  std::vector<std::thread> threads;
  threads.reserve(FLAGS_num_threads);
  for (int i = 0; i < FLAGS_num_threads; ++i) {
    threads.emplace_back(test_func, &run, &stats.get()[i]);
  }

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_duration));

  run.store(false, std::memory_order_relaxed);
  for (int i = 0; i < FLAGS_num_threads; ++i) {
    Stats *tstats = &stats.get()[i];
    threads[i].join();
    std::cout << "Thread " << i << " stats:" << std::endl;
    std::cout << "    Operations: " << tstats->total << std::endl;
    std::cout << "    Successful insert: " << tstats->succ[0] << std::endl;
    std::cout << "    Successful contains: " << tstats->succ[1] << std::endl;
    std::cout << "    Successful remove: " << tstats->succ[2] << std::endl;
    std::cout << "    Successful find: " << tstats->succ[3] << std::endl;
  }

  std::cout << std::endl;
  uint64_t agg[4] = {0, 0, 0, 0};
  for (int i = 0; i < 4; ++i) {
    for (int j = 0; j < FLAGS_num_threads; ++j) {
      agg[i] += stats.get()[j].succ[i];
    }
  }
  std::cout << "Successful insert: " << agg[0] << " (" << agg[0] / FLAGS_duration << " calls/s)" << std::endl;
  std::cout << "Successful contains: " << agg[1] << " (" << agg[1] / FLAGS_duration << " calls/s)" << std::endl;
  std::cout << "Successful remove: " << agg[2] << " (" << agg[2] / FLAGS_duration << " calls/s)" << std::endl;
  std::cout << "Successful find: " << agg[3] << " (" << agg[3] / FLAGS_duration << " calls/s)" << std::endl;

  std::cout << std::endl;
  uint64_t tot = 0, tops = 0;
  for (uint64_t val : agg) {
    tot += val;
  }
  for (int i = 0; i < FLAGS_num_threads; ++i) {
    tops += stats.get()[i].total;
  }
  std::cout << "Total successful: " << tot << " (" << tot / FLAGS_duration << " calls/s)" << std::endl;
  std::cout << "Total ops: " << tops << " (" << tops / FLAGS_duration << " calls/s)" << std::endl;
}
