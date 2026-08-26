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
#include <cmath>

#include "query/context.hpp"
#include "query/exceptions.hpp"

using memgraph::query::AbortReason;
using memgraph::query::DeadlineFromTimeout;
using memgraph::query::StoppingContext;
using memgraph::query::TransactionStatus;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

// The deadline is `now() + timeout`, where `now()` is sampled inside the call. We can't observe that
// exact sample, so every non-saturating case is bracketed: sampling the Clock immediately before and
// after the call bounds the internal `now()`, hence bounds the returned deadline. This is exact, not
// timing-dependent, so it cannot flake.

TEST(DeadlineFromTimeout, NonPositiveNeverExpires) {
  // Non-positive means "no timeout" at the producers; the helper fail-closes to time_point::max()
  // (never expire) rather than an immediate deadline.
  EXPECT_EQ(DeadlineFromTimeout(0s), Clock::time_point::max());
  EXPECT_EQ(DeadlineFromTimeout(std::chrono::duration<double>{-1.0}), Clock::time_point::max());
}

TEST(DeadlineFromTimeout, NaNNeverExpires) {
  // NaN fails every comparison, so without an explicit guard it would reach the duration_cast (UB).
  EXPECT_EQ(DeadlineFromTimeout(std::chrono::duration<double>{std::nan("")}), Clock::time_point::max());
}

TEST(DeadlineFromTimeout, PositiveTimeout) {
  auto const before = Clock::now();
  auto const deadline = DeadlineFromTimeout(2s);
  auto const after = Clock::now();
  EXPECT_GE(deadline, before + 2s);
  EXPECT_LE(deadline, after + 2s);
}

TEST(DeadlineFromTimeout, SubSecondTimeoutKeepsFraction) {
  // 0.5s must survive the double -> steady_clock::duration conversion (regression against dropping
  // the fractional part).
  auto const timeout = std::chrono::duration<double>{0.5};
  auto const before = Clock::now();
  auto const deadline = DeadlineFromTimeout(timeout);
  auto const after = Clock::now();
  EXPECT_GE(deadline, before + 500ms);
  EXPECT_LE(deadline, after + 500ms);
}

TEST(DeadlineFromTimeout, SaturatesAtMaxRepresentable) {
  // A timeout equal to the largest representable duration saturates (the guard uses `>=`).
  auto const max_representable = std::chrono::duration<double>{Clock::duration::max()};
  EXPECT_EQ(DeadlineFromTimeout(max_representable), Clock::time_point::max());
}

TEST(DeadlineFromTimeout, SaturatesAboveMaxRepresentable) {
  // Far beyond the representable range (~292 years): must clamp, not overflow the seconds->ticks cast.
  auto const timeout = std::chrono::duration<double>{1e30};
  EXPECT_EQ(DeadlineFromTimeout(timeout), Clock::time_point::max());
}

TEST(DeadlineFromTimeout, NearMaxNeverWrapsIntoThePast) {
  // Just below max_representable: this passes the first guard but `now() + ticks` could overflow the
  // time_point. The addition guard must then clamp to time_point::max() rather than let signed
  // overflow wrap the deadline into the past (which would abort every query instantly). Whether the
  // machine's steady_clock epoch puts us in the finite band or the clamped band is platform
  // dependent, so we assert only the portable invariant: the deadline is never before now.
  auto const max_representable = std::chrono::duration<double>{Clock::duration::max()};
  auto const almost_max = std::chrono::duration<double>{std::nextafter(max_representable.count(), 0.0)};
  auto const before = Clock::now();
  auto const deadline = DeadlineFromTimeout(almost_max);
  EXPECT_GE(deadline, before);
}

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
