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

/**
 * Demo 2: Scheduler Timing with condition_variable
 *
 * Purpose: Compare different scheduler wait strategies for non-determinism analysis
 *
 * Tests:
 * 1. Polling with short interval (100ms) - like current thread pool monitoring
 * 2. Polling with long interval (1000ms) - proposed improvement
 * 3. Event-driven with signal - optimal for determinism
 *
 * Usage:
 *   ./scheduler_timing short     # 100ms polling (5 iterations)
 *   ./scheduler_timing long      # 1000ms polling (5 iterations)
 *   ./scheduler_timing event     # Event-driven wait
 *   ./scheduler_timing all       # Run all tests (default)
 *
 * Run under live-recorder:
 *   ~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/live-record -o sched_test.undo ./scheduler_timing short
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <mutex>
#include <thread>

// Test configuration
constexpr int SHORT_INTERVAL_MS = 100;
constexpr int LONG_INTERVAL_MS = 1000;
constexpr int ITERATIONS = 5;

std::mutex mtx;
std::condition_variable cv;
std::atomic<bool> should_stop{false};

// Simulates current scheduler behavior with short polling
void test_short_polling() {
  std::cout << "=== Short Polling (" << SHORT_INTERVAL_MS << "ms, " << ITERATIONS << " iterations) ===" << std::endl;

  int wakeups = 0;
  auto start = std::chrono::steady_clock::now();

  for (int i = 0; i < ITERATIONS; ++i) {
    std::unique_lock<std::mutex> lock(mtx);

    auto wait_until = std::chrono::steady_clock::now() + std::chrono::milliseconds(SHORT_INTERVAL_MS);

    cv.wait_until(lock, wait_until);
    ++wakeups;

    // Simulate some work
    auto now = std::chrono::steady_clock::now();
    [[maybe_unused]] volatile auto elapsed = now - start;
  }

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

  std::cout << "  Duration: " << elapsed.count() << "ms" << std::endl;
  std::cout << "  Wakeups: " << wakeups << std::endl;
  std::cout << "  Events per second (approx): " << (1000.0 / SHORT_INTERVAL_MS) << std::endl;
}

// Simulates improved scheduler with longer polling
void test_long_polling() {
  std::cout << "\n=== Long Polling (" << LONG_INTERVAL_MS << "ms, " << ITERATIONS << " iterations) ===" << std::endl;

  int wakeups = 0;
  auto start = std::chrono::steady_clock::now();

  for (int i = 0; i < ITERATIONS; ++i) {
    std::unique_lock<std::mutex> lock(mtx);

    auto wait_until = std::chrono::steady_clock::now() + std::chrono::milliseconds(LONG_INTERVAL_MS);

    cv.wait_until(lock, wait_until);
    ++wakeups;

    auto now = std::chrono::steady_clock::now();
    [[maybe_unused]] volatile auto elapsed = now - start;
  }

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

  std::cout << "  Duration: " << elapsed.count() << "ms" << std::endl;
  std::cout << "  Wakeups: " << wakeups << std::endl;
  std::cout << "  Events per second (approx): " << (1000.0 / LONG_INTERVAL_MS) << std::endl;
}

// Simulates event-driven scheduler - only wakes on actual events
void test_event_driven() {
  std::cout << "\n=== Event-Driven (signal-based) ===" << std::endl;

  std::atomic<int> wakeups{0};
  should_stop = false;

  // Worker thread that waits for events
  std::thread worker([&]() {
    while (!should_stop) {
      std::unique_lock<std::mutex> lock(mtx);
      cv.wait(lock, [] { return should_stop.load(); });
      if (!should_stop) {
        ++wakeups;
      }
    }
  });

  auto start = std::chrono::steady_clock::now();

  // Simulate 5 events over ~500ms
  for (int i = 0; i < ITERATIONS; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    {
      std::lock_guard<std::mutex> lock(mtx);
      ++wakeups;  // Count event
    }
    cv.notify_one();
  }

  // Stop the worker
  should_stop = true;
  cv.notify_one();
  worker.join();

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

  std::cout << "  Duration: " << elapsed.count() << "ms" << std::endl;
  std::cout << "  Actual events: " << ITERATIONS << std::endl;
  std::cout << "  No polling overhead when idle" << std::endl;
}

// Test comparing steady_clock vs system_clock in wait_until
void test_clock_in_wait() {
  std::cout << "\n=== Clock Type in wait_until ===" << std::endl;

  // With steady_clock
  {
    std::cout << "Using steady_clock:" << std::endl;
    std::unique_lock<std::mutex> lock(mtx);

    for (int i = 0; i < 3; ++i) {
      auto wait_until = std::chrono::steady_clock::now() + std::chrono::milliseconds(10);
      cv.wait_until(lock, wait_until);
    }
    std::cout << "  3 wait_until calls completed" << std::endl;
  }

  // With system_clock
  {
    std::cout << "Using system_clock:" << std::endl;
    std::unique_lock<std::mutex> lock(mtx);

    for (int i = 0; i < 3; ++i) {
      auto wait_until = std::chrono::system_clock::now() + std::chrono::milliseconds(10);
      cv.wait_until(lock, wait_until);
    }
    std::cout << "  3 wait_until calls completed" << std::endl;
  }
}

int main(int argc, char *argv[]) {
  const char *mode = (argc > 1) ? argv[1] : "all";

  std::cout << "Scheduler Timing Demo for Non-Determinism Analysis" << std::endl;
  std::cout << "====================================================" << std::endl;
  std::cout << "Mode: " << mode << std::endl;

  if (strcmp(mode, "short") == 0) {
    test_short_polling();
  } else if (strcmp(mode, "long") == 0) {
    test_long_polling();
  } else if (strcmp(mode, "event") == 0) {
    test_event_driven();
  } else if (strcmp(mode, "clock") == 0) {
    test_clock_in_wait();
  } else {
    test_short_polling();
    test_long_polling();
    test_event_driven();
    test_clock_in_wait();
  }

  std::cout << "\n=== Summary ===" << std::endl;
  std::cout << "For minimal non-determinism:" << std::endl;
  std::cout << "  1. Use longer polling intervals when possible" << std::endl;
  std::cout << "  2. Use event-driven design where applicable" << std::endl;
  std::cout << "  3. Use steady_clock for wait_until" << std::endl;

  return 0;
}
