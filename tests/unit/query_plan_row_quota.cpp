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

#include <cstdint>
#include <memory>
#include <vector>

#include "query/plan/row_quota.hpp"
#include "utils/shared_quota.hpp"

using memgraph::query::plan::RowQuota;
using memgraph::utils::QuotaCoordinator;
using memgraph::utils::SharedQuota;
using PlanQuotas = std::vector<SharedQuota *>;

namespace {
uint64_t Drain(RowQuota &quota) {
  uint64_t taken = 0;
  while (quota.Decrement() > 0) ++taken;
  return taken;
}
}  // namespace

TEST(RowQuotaTest, SerialQuotaCountsEachExecution) {
  RowQuota quota;
  EXPECT_FALSE(quota.IsArmed());
  quota.Arm(2);
  EXPECT_EQ(Drain(quota), 2);
  quota.Release();
  EXPECT_FALSE(quota.IsArmed());
  quota.Arm(3);
  EXPECT_EQ(Drain(quota), 3);
}

// Only the armed copy is in the branch's plan list, so that a waiting branch can free it.
TEST(RowQuotaTest, ArmedCopyJoinsTheBranchPlanList) {
  auto list = std::make_shared<PlanQuotas>();
  RowQuota quota(SharedQuota(std::make_shared<QuotaCoordinator>()), list, 2);
  EXPECT_TRUE(list->empty());
  quota.Arm(5);
  EXPECT_EQ(list->size(), 1);
  quota.Release();
  EXPECT_TRUE(list->empty());
}

// The branches of a parallel operator draw one count per execution; the operator re-arms the coordinator in between.
TEST(RowQuotaTest, BranchesShareOneCountPerExecution) {
  auto coord = std::make_shared<QuotaCoordinator>();
  RowQuota first(SharedQuota(coord), std::make_shared<PlanQuotas>(), 2);
  RowQuota second(SharedQuota(coord), std::make_shared<PlanQuotas>(), 2);
  for (const uint64_t count : {3U, 5U}) {
    first.Arm(count);
    second.Arm(count);
    EXPECT_EQ(Drain(first) + Drain(second), count);
    first.Release();
    second.Release();
    coord->Rearm();
  }
}
