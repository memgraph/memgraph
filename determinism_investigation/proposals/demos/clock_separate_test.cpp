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
 * Clock Separate Test
 *
 * Tests steady_clock and system_clock separately to measure
 * their individual event contributions under live-recorder.
 *
 * Usage:
 *   ./clock_separate_test steady    # Test only steady_clock
 *   ./clock_separate_test system    # Test only system_clock
 *   ./clock_separate_test both      # Test both (default)
 */

#include <chrono>
#include <cstring>
#include <iostream>

constexpr int ITERATIONS = 10000;

void test_steady_only() {
  std::cout << "Testing steady_clock only (" << ITERATIONS << " iterations)" << std::endl;

  [[maybe_unused]] volatile int64_t sum = 0;
  for (int i = 0; i < ITERATIONS; ++i) {
    auto now = std::chrono::steady_clock::now();
    sum += now.time_since_epoch().count();
  }
}

void test_system_only() {
  std::cout << "Testing system_clock only (" << ITERATIONS << " iterations)" << std::endl;

  [[maybe_unused]] volatile int64_t sum = 0;
  for (int i = 0; i < ITERATIONS; ++i) {
    auto now = std::chrono::system_clock::now();
    sum += now.time_since_epoch().count();
  }
}

int main(int argc, char *argv[]) {
  const char *mode = (argc > 1) ? argv[1] : "both";

  std::cout << "Clock Separate Test - Mode: " << mode << std::endl;
  std::cout << "========================================" << std::endl;

  if (strcmp(mode, "steady") == 0) {
    test_steady_only();
  } else if (strcmp(mode, "system") == 0) {
    test_system_only();
  } else {
    test_steady_only();
    test_system_only();
  }

  return 0;
}
