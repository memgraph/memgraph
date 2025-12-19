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
 * Demo 3: high_resolution_clock Investigation
 *
 * Purpose: Understand what high_resolution_clock actually is on Linux
 * and how it behaves under live-recorder.
 *
 * Key questions:
 * 1. Is high_resolution_clock an alias for steady_clock or system_clock?
 * 2. Does it generate WALLCLOCK_TIME events?
 * 3. Is it suitable for scheduler use?
 */

#include <chrono>
#include <iostream>
#include <type_traits>

constexpr int ITERATIONS = 10000;

void analyze_clock_types() {
  std::cout << "=== Clock Type Analysis ===" << std::endl;

  // Check if high_resolution_clock is the same type as others
  std::cout << "\nType identity checks:" << std::endl;

  constexpr bool is_steady = std::is_same_v<std::chrono::high_resolution_clock, std::chrono::steady_clock>;

  constexpr bool is_system = std::is_same_v<std::chrono::high_resolution_clock, std::chrono::system_clock>;

  std::cout << "  high_resolution_clock == steady_clock: " << (is_steady ? "YES" : "NO") << std::endl;
  std::cout << "  high_resolution_clock == system_clock: " << (is_system ? "YES" : "NO") << std::endl;

  if (!is_steady && !is_system) {
    std::cout << "  high_resolution_clock is a DISTINCT type" << std::endl;
  }

  // Properties
  std::cout << "\nClock properties:" << std::endl;
  std::cout << "  steady_clock::is_steady: " << std::chrono::steady_clock::is_steady << std::endl;
  std::cout << "  system_clock::is_steady: " << std::chrono::system_clock::is_steady << std::endl;
  std::cout << "  high_resolution_clock::is_steady: " << std::chrono::high_resolution_clock::is_steady << std::endl;

  // Resolution
  std::cout << "\nClock resolution (period):" << std::endl;
  std::cout << "  steady_clock: " << std::chrono::steady_clock::period::num << "/"
            << std::chrono::steady_clock::period::den << " sec" << std::endl;
  std::cout << "  system_clock: " << std::chrono::system_clock::period::num << "/"
            << std::chrono::system_clock::period::den << " sec" << std::endl;
  std::cout << "  high_resolution_clock: " << std::chrono::high_resolution_clock::period::num << "/"
            << std::chrono::high_resolution_clock::period::den << " sec" << std::endl;
}

void test_high_res_clock() {
  std::cout << "\n=== high_resolution_clock Event Test (" << ITERATIONS << " calls) ===" << std::endl;

  [[maybe_unused]] volatile int64_t sum = 0;
  for (int i = 0; i < ITERATIONS; ++i) {
    auto now = std::chrono::high_resolution_clock::now();
    sum += now.time_since_epoch().count();
  }

  std::cout << "  Completed " << ITERATIONS << " high_resolution_clock::now() calls" << std::endl;
}

int main() {
  std::cout << "high_resolution_clock Investigation" << std::endl;
  std::cout << "====================================" << std::endl;

  analyze_clock_types();
  test_high_res_clock();

  std::cout << "\n=== Conclusion ===" << std::endl;

  constexpr bool is_steady = std::is_same_v<std::chrono::high_resolution_clock, std::chrono::steady_clock>;

  if (is_steady) {
    std::cout << "high_resolution_clock IS steady_clock on this platform." << std::endl;
    std::cout << "It will NOT generate WALLCLOCK_TIME events." << std::endl;
    std::cout << "Safe to use for scheduling." << std::endl;
  } else {
    std::cout << "high_resolution_clock is NOT steady_clock on this platform." << std::endl;
    std::cout << "It MAY generate WALLCLOCK_TIME events." << std::endl;
    std::cout << "Prefer steady_clock for scheduling." << std::endl;
  }

  return 0;
}
