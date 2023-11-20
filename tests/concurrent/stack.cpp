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
#include <cstring>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include "utils/stack.hpp"
#include "utils/timer.hpp"

const int kNumThreads = 4;

DEFINE_int32(max_value, 100000000, "Maximum value that should be inserted");

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::utils::Stack<uint64_t, 8190> stack;

  std::vector<std::thread> threads;
  memgraph::utils::Timer timer;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&stack, i] {
      for (uint64_t item = i; item < FLAGS_max_value; item += kNumThreads) {
        stack.Push(item);
      }
    });
  }

  std::atomic<bool> run{true};
  std::thread verify([&stack, &run] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    std::vector<bool> found;
    found.resize(FLAGS_max_value);
    std::optional<uint64_t> item;
    while (run || (item = stack.Pop())) {
      if (item) {
        MG_ASSERT(*item < FLAGS_max_value);
        found[*item] = true;
      }
    }
    MG_ASSERT(!stack.Pop());
    for (uint64_t i = 0; i < FLAGS_max_value; ++i) {
      MG_ASSERT(found[i]);
    }
  });

  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
  }

  auto elapsed = timer.Elapsed().count();

  run.store(false);
  verify.join();

  std::cout << "Duration: " << elapsed << std::endl;
  std::cout << "Throughput: " << FLAGS_max_value / elapsed << std::endl;

  return 0;
}
