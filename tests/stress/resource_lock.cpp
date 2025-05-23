// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <chrono>
#include <random>
#include "spdlog/spdlog.h"
#include "utils/logging.hpp"

#include "utils/resource_lock.hpp"

DEFINE_int32(loop_count, 1000, "The number tries to lock the resource");
DEFINE_int32(min_timeout_ms, 10, "The minimum timeout to lock the resource");
DEFINE_int32(max_timeout_ms, 10000, "The maximum timeout to lock the resource");

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  spdlog::info("Starting resource lock try_lock stress test");

  // Take a unique lock and loop through trying to lock
  memgraph::utils::ResourceLock resource_lock;
  auto locked = std::unique_lock{resource_lock};

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(FLAGS_min_timeout_ms, FLAGS_max_timeout_ms);  // ms

  std::atomic_int counter{0};

  std::jthread monitoring_thread([&]() {
    // Monitor the progress
    int prev_count = counter;
    while (counter < FLAGS_loop_count) {
      std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_max_timeout_ms * 2));
      MG_ASSERT(prev_count < counter, "Counter should be increasing");
      prev_count = counter;
    }
  });

  for (counter = 0; counter < FLAGS_loop_count; ++counter) {
    if (counter % 20 == 0) {
      spdlog::info("Iteration: {}", counter);
    }
    // Get random period
    auto period = std::chrono::milliseconds(distr(gen));
    const auto start = std::chrono::steady_clock::now();
    // Try to lock the resource
    auto lock = resource_lock.try_lock_for(period);
    MG_ASSERT(!lock, "Lock should not be acquired");
    const auto duration = std::chrono::steady_clock::now() - start;
    MG_ASSERT(duration >= period && duration < period + std::chrono::seconds{1},
              "Lock should fail after the period. Expected: {}ms, got: {}ms", period.count(), duration.count());
  }

  spdlog::info("Stress test done");

  return 0;
}
