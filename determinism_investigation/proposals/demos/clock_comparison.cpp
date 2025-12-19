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
 * Demo 1: Clock Comparison
 *
 * Purpose: Compare steady_clock vs system_clock behavior for non-determinism analysis
 *
 * Run under live-recorder:
 *   ~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/live-record -o clock_test.undo ./clock_comparison
 *
 * Analyze with udb:
 *   ~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/udb clock_test.undo
 *   (udb) info event-stats
 *
 * Or with strace:
 *   strace -c ./clock_comparison
 *   strace -e clock_gettime ./clock_comparison
 *
 * Expected observations:
 * - steady_clock uses CLOCK_MONOTONIC (reliable for intervals)
 * - system_clock uses CLOCK_REALTIME (can be adjusted)
 * - Both should use vDSO on Linux (no actual syscall, fast)
 * - steady_clock is preferred for schedulers/timeouts
 */

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

// Number of iterations for timing tests
constexpr int ITERATIONS = 10000;

// Test steady_clock (CLOCK_MONOTONIC)
void test_steady_clock() {
  std::cout << "=== Testing steady_clock (CLOCK_MONOTONIC) ===" << std::endl;

  auto start = std::chrono::steady_clock::now();

  std::vector<std::chrono::steady_clock::time_point> timestamps;
  timestamps.reserve(ITERATIONS);

  for (int i = 0; i < ITERATIONS; ++i) {
    timestamps.push_back(std::chrono::steady_clock::now());
  }

  auto end = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  std::cout << "  Iterations: " << ITERATIONS << std::endl;
  std::cout << "  Total time: " << elapsed.count() << " us" << std::endl;
  std::cout << "  Avg per call: " << (elapsed.count() / static_cast<double>(ITERATIONS)) << " us" << std::endl;

  // Check monotonicity
  bool monotonic = true;
  for (size_t i = 1; i < timestamps.size(); ++i) {
    if (timestamps[i] < timestamps[i - 1]) {
      monotonic = false;
      std::cout << "  WARNING: Non-monotonic at index " << i << std::endl;
      break;
    }
  }
  std::cout << "  Monotonic: " << (monotonic ? "YES" : "NO") << std::endl;
}

// Test system_clock (CLOCK_REALTIME)
void test_system_clock() {
  std::cout << "\n=== Testing system_clock (CLOCK_REALTIME) ===" << std::endl;

  auto start = std::chrono::system_clock::now();

  std::vector<std::chrono::system_clock::time_point> timestamps;
  timestamps.reserve(ITERATIONS);

  for (int i = 0; i < ITERATIONS; ++i) {
    timestamps.push_back(std::chrono::system_clock::now());
  }

  auto end = std::chrono::system_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  std::cout << "  Iterations: " << ITERATIONS << std::endl;
  std::cout << "  Total time: " << elapsed.count() << " us" << std::endl;
  std::cout << "  Avg per call: " << (elapsed.count() / static_cast<double>(ITERATIONS)) << " us" << std::endl;

  // Check if time went backwards (can happen with NTP adjustments)
  bool backwards = false;
  for (size_t i = 1; i < timestamps.size(); ++i) {
    if (timestamps[i] < timestamps[i - 1]) {
      backwards = true;
      std::cout << "  WARNING: Time went backwards at index " << i << std::endl;
      break;
    }
  }
  std::cout << "  Time went backwards: " << (backwards ? "YES" : "NO") << std::endl;

  // Show current time as time_t (calendar time conversion)
  auto now = std::chrono::system_clock::now();
  std::time_t now_t = std::chrono::system_clock::to_time_t(now);
  std::cout << "  Can convert to calendar time: YES" << std::endl;
  std::cout << "  Current time: " << std::ctime(&now_t);
}

// Test condition_variable wait behavior with different clocks
void test_wait_behavior() {
  std::cout << "\n=== Testing wait_until behavior ===" << std::endl;

  // steady_clock based wait
  {
    auto start = std::chrono::steady_clock::now();
    auto wait_until = start + std::chrono::milliseconds(10);

    // Simulate what scheduler does: check time, wait, repeat
    int checks = 0;
    while (std::chrono::steady_clock::now() < wait_until) {
      ++checks;
      std::this_thread::yield();
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
    std::cout << "  steady_clock wait (10ms): " << elapsed.count() << " us, checks: " << checks << std::endl;
  }

  // system_clock based wait
  {
    auto start = std::chrono::system_clock::now();
    auto wait_until = start + std::chrono::milliseconds(10);

    int checks = 0;
    while (std::chrono::system_clock::now() < wait_until) {
      ++checks;
      std::this_thread::yield();
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - start);
    std::cout << "  system_clock wait (10ms): " << elapsed.count() << " us, checks: " << checks << std::endl;
  }
}

// Test high_resolution_clock (implementation-defined)
void test_high_resolution_clock() {
  std::cout << "\n=== Testing high_resolution_clock ===" << std::endl;

  // Check which clock it aliases
  std::cout << "  Is steady: " << std::chrono::high_resolution_clock::is_steady << std::endl;

  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < ITERATIONS; ++i) {
    [[maybe_unused]] auto now = std::chrono::high_resolution_clock::now();
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  std::cout << "  Iterations: " << ITERATIONS << std::endl;
  std::cout << "  Total time: " << elapsed.count() << " us" << std::endl;
}

// Demonstrate the difference: epoch and representation
void show_clock_properties() {
  std::cout << "\n=== Clock Properties ===" << std::endl;

  // steady_clock
  std::cout << "steady_clock:" << std::endl;
  std::cout << "  is_steady: " << std::chrono::steady_clock::is_steady << std::endl;
  std::cout << "  period: " << std::chrono::steady_clock::period::num << "/" << std::chrono::steady_clock::period::den
            << " seconds" << std::endl;

  // system_clock
  std::cout << "system_clock:" << std::endl;
  std::cout << "  is_steady: " << std::chrono::system_clock::is_steady << std::endl;
  std::cout << "  period: " << std::chrono::system_clock::period::num << "/" << std::chrono::system_clock::period::den
            << " seconds" << std::endl;

  // high_resolution_clock
  std::cout << "high_resolution_clock:" << std::endl;
  std::cout << "  is_steady: " << std::chrono::high_resolution_clock::is_steady << std::endl;
  std::cout << "  period: " << std::chrono::high_resolution_clock::period::num << "/"
            << std::chrono::high_resolution_clock::period::den << " seconds" << std::endl;
}

int main() {
  std::cout << "Clock Comparison Demo for Non-Determinism Analysis" << std::endl;
  std::cout << "==================================================" << std::endl;

  show_clock_properties();
  test_steady_clock();
  test_system_clock();
  test_high_resolution_clock();
  test_wait_behavior();

  std::cout << "\n=== Summary ===" << std::endl;
  std::cout << "For scheduler timing (intervals, timeouts):" << std::endl;
  std::cout << "  - Use steady_clock: monotonic, never adjusted" << std::endl;
  std::cout << "  - Avoid system_clock: can jump forward/backward with NTP" << std::endl;
  std::cout << "\nFor wall-clock time (logging, calendar):" << std::endl;
  std::cout << "  - Use system_clock: convertible to time_t" << std::endl;
  std::cout << "  - steady_clock cannot represent calendar time" << std::endl;

  return 0;
}
