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

#include "utils/system_info.hpp"

using memgraph::utils::CpuQuotaToCores;
using memgraph::utils::ParseCgroupV2CpuMax;

// --- cgroup v2 (cpu.max) --------------------------------------------------

TEST(UsableCoreCount, V2ExactCores) {
  // 200000 / 100000 = 2 cores exactly.
  const auto cores = ParseCgroupV2CpuMax("200000 100000");
  ASSERT_TRUE(cores.has_value());
  EXPECT_EQ(*cores, 2U);
}

TEST(UsableCoreCount, V2CeilsFraction) {
  // 150000 / 100000 = 1.5 -> ceil -> 2 cores.
  const auto cores = ParseCgroupV2CpuMax("150000 100000");
  ASSERT_TRUE(cores.has_value());
  EXPECT_EQ(*cores, 2U);
}

TEST(UsableCoreCount, V2MaxIsUnlimited) {
  // "max" means no quota -> nullopt (caller falls back to host cores).
  EXPECT_FALSE(ParseCgroupV2CpuMax("max 100000").has_value());
}

TEST(UsableCoreCount, V2TrailingWhitespaceTolerated) {
  const auto cores = ParseCgroupV2CpuMax("200000 100000\n");
  ASSERT_TRUE(cores.has_value());
  EXPECT_EQ(*cores, 2U);
}

TEST(UsableCoreCount, V2Garbage) { EXPECT_FALSE(ParseCgroupV2CpuMax("not-a-number").has_value()); }

// --- cgroup v1 (cpu.cfs_quota_us / cpu.cfs_period_us) ---------------------

TEST(UsableCoreCount, V1UnlimitedQuota) {
  // Quota of -1 means unlimited -> nullopt.
  EXPECT_FALSE(CpuQuotaToCores(-1, 100000).has_value());
}

TEST(UsableCoreCount, V1ExactCores) {
  const auto cores = CpuQuotaToCores(200000, 100000);
  ASSERT_TRUE(cores.has_value());
  EXPECT_EQ(*cores, 2U);
}

TEST(UsableCoreCount, V1CeilsFraction) {
  // 250000 / 100000 = 2.5 -> ceil -> 3 cores.
  const auto cores = CpuQuotaToCores(250000, 100000);
  ASSERT_TRUE(cores.has_value());
  EXPECT_EQ(*cores, 3U);
}

TEST(UsableCoreCount, V1InvalidPeriod) { EXPECT_FALSE(CpuQuotaToCores(200000, 0).has_value()); }

TEST(UsableCoreCount, V1ZeroQuota) { EXPECT_FALSE(CpuQuotaToCores(0, 100000).has_value()); }
