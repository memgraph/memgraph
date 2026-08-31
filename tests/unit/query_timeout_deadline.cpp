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

#include <atomic>
#include <chrono>

#include "query/context.hpp"
#include "query/exceptions.hpp"

using memgraph::query::AbortReason;
using memgraph::query::StoppingContext;
using memgraph::query::TransactionStatus;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

// --- StoppingContext::MustAbort() — the deadline decision -----------------
// These pin the exact line this PR rewrote (`deadline && now() >= *deadline`) and its precedence
// against the other abort reasons. Fully deterministic: the deadline is set well into the past or
// future, so no timing window is involved.

TEST(MustAbort, NoDeadlineNeverTimesOut) {
  StoppingContext ctx{};  // deadline == nullopt
  EXPECT_EQ(ctx.MustAbort(), AbortReason::NO_ABORT);
}

TEST(MustAbort, FutureDeadlineDoesNotAbort) {
  StoppingContext ctx{.deadline = Clock::now() + 1h};
  EXPECT_EQ(ctx.MustAbort(), AbortReason::NO_ABORT);
}

TEST(MustAbort, PastDeadlineTimesOut) {
  StoppingContext ctx{.deadline = Clock::now() - 1s};
  EXPECT_EQ(ctx.MustAbort(), AbortReason::TIMEOUT);
}

TEST(MustAbort, TerminatedTakesPrecedenceOverTimeout) {
  std::atomic<TransactionStatus> status{TransactionStatus::TERMINATED};
  StoppingContext ctx{.transaction_status = &status, .deadline = Clock::now() - 1s};
  EXPECT_EQ(ctx.MustAbort(), AbortReason::TERMINATED);
}

TEST(MustAbort, ShutdownTakesPrecedenceOverTimeout) {
  std::atomic<bool> shutting_down{true};
  StoppingContext ctx{.is_shutting_down = &shutting_down, .deadline = Clock::now() - 1s};
  EXPECT_EQ(ctx.MustAbort(), AbortReason::SHUTDOWN);
}
