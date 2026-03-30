// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <thread>

#include "utils/system_info.hpp"

namespace {
unsigned CountProcessorsInProcCpuinfo() {
  std::ifstream cpuinfo("/proc/cpuinfo");
  unsigned count = 0;
  std::string line;
  while (std::getline(cpuinfo, line)) {
    if (line.starts_with("processor")) {
      ++count;
    }
  }
  return count;
}
}  // namespace

TEST(SafeHardwareConcurrency, AlwaysReturnsNonZero) {
  auto safe_default = memgraph::utils::GetSafeHardwareConcurrency();
  auto safe_custom = memgraph::utils::GetSafeHardwareConcurrency(4);

  EXPECT_GT(safe_default, 0U);
  EXPECT_GT(safe_custom, 0U);

  auto hw_threads = std::thread::hardware_concurrency();
  if (hw_threads > 0) {
    EXPECT_EQ(safe_default, hw_threads);
    EXPECT_EQ(safe_custom, hw_threads);
  } else {
    // hardware_concurrency() returned 0, so the function falls back to
    // /proc/cpuinfo. If that also fails, the numeric fallback is used.
    auto proc_count = CountProcessorsInProcCpuinfo();
    if (proc_count > 0) {
      EXPECT_EQ(safe_default, proc_count);
      EXPECT_EQ(safe_custom, proc_count);
    } else {
      EXPECT_EQ(safe_default, 2U);
      EXPECT_EQ(safe_custom, 4U);
    }
  }
}

TEST(SafeHardwareConcurrency, ZeroFallbackStillReturnsNonZero) {
  EXPECT_GE(memgraph::utils::GetSafeHardwareConcurrency(0), 1U);
}
