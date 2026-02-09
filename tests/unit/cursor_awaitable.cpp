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
//
// Test matrix (yield × function shape):
//
//   Yield scenario            | 1. Single coroutine | 2. Chain all yield | 3. Chain some yield | 4. Chain + regular
//   --------------------------|---------------------|--------------------|--------------------|--------------------
//   1. Yield before called   | YES (FirstOpportunity) | Phase 2           | Phase 2            | Phase 2
//   2. Yield never triggered | YES (NoYield, ReturnsFalse) | Phase 2   | Phase 2            | Phase 2
//   3. Yield while running   | YES (YieldsAndResume, RandomYield) | Phase 2 | Phase 2        | Phase 2
//
// All pull coroutines return PullAwaitable; chains co_await child PullAwaitables.

#include <atomic>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "query/context.hpp"
#include "query/plan/cursor_awaitable.hpp"
#include "utils/counter.hpp"

using memgraph::query::ExecutionContext;
using memgraph::query::plan::PullAwaitable;
using memgraph::query::plan::PullRunResult;
using memgraph::query::plan::RunPullToCompletion;
using memgraph::query::plan::YieldPointAwaitable;
using memgraph::utils::ResettableCounter;

namespace {

/// Test-only: request yield via stopping context after N calls. Call before each YieldPointAwaitable.
/// When *yield_after_count hits 0, sets *ctx.stopping_context.yield_requested = true so the next
/// YieldPointAwaitable (production code) will suspend. Requires ctx.stopping_context.yield_requested set by test.
inline void RequestYieldAfterN(ExecutionContext &ctx, int *yield_after_count) {
  if (yield_after_count && *yield_after_count > 0 && (--*yield_after_count == 0) &&
      ctx.stopping_context.yield_requested) {
    ctx.stopping_context.yield_requested->store(true);
  }
}

// Single coroutine: one yield point, then returns true. yield_after_count: when non-null, request yield when it hits 0.
PullAwaitable CoroutineThatYieldsOnce(ExecutionContext &ctx, ResettableCounter &counter,
                                      int *yield_after_count = nullptr) {
  RequestYieldAfterN(ctx, yield_after_count);
  co_await YieldPointAwaitable(ctx, counter);
  co_return true;
}

// Single coroutine: no yield (throttle never fires).
PullAwaitable CoroutineNoYield(ExecutionContext &ctx, ResettableCounter &counter) {
  co_await YieldPointAwaitable(ctx, counter);
  co_return true;
}

// Single coroutine: returns false (no row).
PullAwaitable CoroutineReturnsFalse(ExecutionContext &ctx, ResettableCounter &counter) {
  co_await YieldPointAwaitable(ctx, counter);
  co_return false;
}

// Single coroutine: multiple yield points; yield can happen at any of them.
PullAwaitable CoroutineMultipleYieldPoints(ExecutionContext &ctx, ResettableCounter &counter,
                                           int *yield_after_count = nullptr) {
  RequestYieldAfterN(ctx, yield_after_count);
  co_await YieldPointAwaitable(ctx, counter);
  RequestYieldAfterN(ctx, yield_after_count);
  co_await YieldPointAwaitable(ctx, counter);
  RequestYieldAfterN(ctx, yield_after_count);
  co_await YieldPointAwaitable(ctx, counter);
  RequestYieldAfterN(ctx, yield_after_count);
  co_await YieldPointAwaitable(ctx, counter);
  RequestYieldAfterN(ctx, yield_after_count);
  co_await YieldPointAwaitable(ctx, counter);
  co_return true;
}

// --- Phase 2: coroutines that return PullAwaitable (can be co_await'ed in a chain) ---

PullAwaitable ChainChild(ExecutionContext &ctx, ResettableCounter &counter, int *yield_after_count = nullptr) {
  RequestYieldAfterN(ctx, yield_after_count);
  co_await YieldPointAwaitable(ctx, counter);
  co_return true;
}

PullAwaitable ChainRoot(ExecutionContext &ctx, ResettableCounter &counter, int *yield_after_count = nullptr) {
  RequestYieldAfterN(ctx, yield_after_count);
  co_await YieldPointAwaitable(ctx, counter);
  bool b = co_await ChainChild(ctx, counter, yield_after_count);
  co_return b;
}

// Child has no yield check; only root has yield check.
PullAwaitable ChainChildNoYield(ExecutionContext &ctx, ResettableCounter &) { co_return true; }

PullAwaitable ChainRootSomeYield(ExecutionContext &ctx, ResettableCounter &counter, int *yield_after_count = nullptr) {
  RequestYieldAfterN(ctx, yield_after_count);
  co_await YieldPointAwaitable(ctx, counter);
  bool b = co_await ChainChildNoYield(ctx, counter);
  co_return b;
}

// Regular function (non-coroutine) that returns PullAwaitable; used in chain.
PullAwaitable RegularFunctionReturnsTrue() { return PullAwaitable(true); }

PullAwaitable ChainWithRegularLeaf(ExecutionContext &ctx, ResettableCounter &counter,
                                   int *yield_after_count = nullptr) {
  RequestYieldAfterN(ctx, yield_after_count);
  co_await YieldPointAwaitable(ctx, counter);
  bool b = co_await RegularFunctionReturnsTrue();
  co_return b;
}

// Same as ChainRoot but co_awaits an lvalue (variable) to test operator co_await() &.
PullAwaitable ChainRootAwaitLvalue(ExecutionContext &ctx, ResettableCounter &counter,
                                   int *yield_after_count = nullptr) {
  RequestYieldAfterN(ctx, yield_after_count);
  co_await YieldPointAwaitable(ctx, counter);
  PullAwaitable child = ChainChild(ctx, counter, yield_after_count);
  bool has_row = co_await child;  // lvalue co_await
  co_return has_row;
}

// Throws at the very first execution (before any co_await completes).
PullAwaitable ThrowFromFirst(ExecutionContext &, ResettableCounter &) { throw std::runtime_error("throw from first"); }

// Throws at the end, just before co_return (last step).
PullAwaitable ThrowFromLast(ExecutionContext &ctx, ResettableCounter &counter) {
  co_await YieldPointAwaitable(ctx, counter);
  throw std::runtime_error("throw from last");
}

// Chain: child throws (middle of the chain). Root awaits child; child throws.
PullAwaitable ChainChildThatThrows(ExecutionContext &ctx, ResettableCounter &counter) {
  co_await YieldPointAwaitable(ctx, counter);
  throw std::runtime_error("throw from middle");
}

PullAwaitable ChainRootAwaitsChildThatThrows(ExecutionContext &ctx, ResettableCounter &counter) {
  co_await YieldPointAwaitable(ctx, counter);
  (void)co_await ChainChildThatThrows(ctx, counter);
  co_return true;
}

}  // namespace

// --- Baseline: immediate (non-coroutine) PullAwaitable ---

TEST(CursorAwaitable, RunPullToCompletionImmediateBool) {
  PullAwaitable done_false(false);
  PullAwaitable done_true(true);

  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;

  auto r1 = RunPullToCompletion(done_false, ctx);
  EXPECT_EQ(r1.status, PullRunResult::Status::Done);

  auto r2 = RunPullToCompletion(done_true, ctx);
  EXPECT_EQ(r2.status, PullRunResult::Status::HasRow);
}

// --- Exception propagation: initial caller (test) must catch exceptions from coroutines ---

TEST(CursorAwaitable, ExceptionPropagates_ThrowFromFirst) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  ResettableCounter counter(100);

  std::string message;
  EXPECT_THROW(
      {
        try {
          PullAwaitable awaitable = ThrowFromFirst(ctx, counter);
          RunPullToCompletion(awaitable, ctx);
        } catch (const std::runtime_error &e) {
          message = e.what();
          throw;
        }
      },
      std::runtime_error);
  EXPECT_EQ(message, "throw from first");
}

TEST(CursorAwaitable, ExceptionPropagates_ThrowFromMiddle) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  ResettableCounter counter(100);

  std::string message;
  EXPECT_THROW(
      {
        try {
          PullAwaitable awaitable = ChainRootAwaitsChildThatThrows(ctx, counter);
          RunPullToCompletion(awaitable, ctx);
        } catch (const std::runtime_error &e) {
          message = e.what();
          throw;
        }
      },
      std::runtime_error);
  EXPECT_EQ(message, "throw from middle");
}

TEST(CursorAwaitable, ExceptionPropagates_ThrowFromLast) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  ResettableCounter counter(100);

  std::string message;
  EXPECT_THROW(
      {
        try {
          PullAwaitable awaitable = ThrowFromLast(ctx, counter);
          RunPullToCompletion(awaitable, ctx);
        } catch (const std::runtime_error &e) {
          message = e.what();
          throw;
        }
      },
      std::runtime_error);
  EXPECT_EQ(message, "throw from last");
}

// --- 1. Single coroutine: yield triggered before / at first opportunity ---

TEST(CursorAwaitable, SingleCoroutine_YieldAtFirstOpportunity) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  std::atomic<bool> yield_requested_flag{false};
  ctx.stopping_context.yield_requested = &yield_requested_flag;
  int yield_after = 1;  // yield on first yield point
  ResettableCounter counter(1);

  PullAwaitable awaitable = CoroutineThatYieldsOnce(ctx, counter, &yield_after);

  auto r1 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r1.status, PullRunResult::Status::Yielded);
  EXPECT_TRUE(stored);

  yield_requested_flag.store(false);
  stored.resume();

  auto r2 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r2.status, PullRunResult::Status::HasRow);
}

// --- 1. Single coroutine: yield never triggered ---

TEST(CursorAwaitable, SingleCoroutine_YieldNeverTriggered_HasRow) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  ResettableCounter counter(100);  // throttle never fires at our single yield point

  PullAwaitable awaitable = CoroutineNoYield(ctx, counter);

  auto r = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r.status, PullRunResult::Status::HasRow);
}

TEST(CursorAwaitable, SingleCoroutine_YieldNeverTriggered_ReturnsFalse) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  ResettableCounter counter(100);

  PullAwaitable awaitable = CoroutineReturnsFalse(ctx, counter);

  auto r = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r.status, PullRunResult::Status::Done);
}

// --- 1. Single coroutine: yield (deterministically) while running ---

TEST(CursorAwaitable, SingleCoroutine_YieldWhileRunning_ThenResume) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  std::atomic<bool> yield_requested_flag{false};
  ctx.stopping_context.yield_requested = &yield_requested_flag;
  int yield_after = 1;
  ResettableCounter counter(1);

  PullAwaitable awaitable = CoroutineThatYieldsOnce(ctx, counter, &yield_after);

  auto r1 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r1.status, PullRunResult::Status::Yielded);
  yield_requested_flag.store(false);
  stored.resume();
  auto r2 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r2.status, PullRunResult::Status::HasRow);
}

// Yield on 3rd yield point (of 5), then resume to completion.
TEST(CursorAwaitable, SingleCoroutine_YieldRandomlyWhileRunning) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  std::atomic<bool> yield_requested_flag{false};
  ctx.stopping_context.yield_requested = &yield_requested_flag;
  int yield_after = 3;           // yield on 3rd hit
  ResettableCounter counter(1);  // every yield point is checked

  PullAwaitable awaitable = CoroutineMultipleYieldPoints(ctx, counter, &yield_after);

  auto r1 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r1.status, PullRunResult::Status::Yielded);
  EXPECT_TRUE(stored);

  yield_requested_flag.store(false);
  stored.resume();

  auto r2 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r2.status, PullRunResult::Status::HasRow);
}

// ========== Phase 2: Chain of coroutines (all have yield checks) ==========

TEST(CursorAwaitable, ChainAllYield_YieldNeverTriggered) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  ResettableCounter counter(100);

  PullAwaitable awaitable = ChainRoot(ctx, counter);

  auto r = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r.status, PullRunResult::Status::HasRow);
}

TEST(CursorAwaitable, ChainAllYield_YieldAtFirstOpportunity) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  std::atomic<bool> yield_requested_flag{false};
  ctx.stopping_context.yield_requested = &yield_requested_flag;
  int yield_after = 1;
  ResettableCounter counter(1);

  PullAwaitable awaitable = ChainRoot(ctx, counter, &yield_after);

  auto r1 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r1.status, PullRunResult::Status::Yielded);
  EXPECT_TRUE(stored);

  yield_requested_flag.store(false);
  stored.resume();
  auto r2 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r2.status, PullRunResult::Status::HasRow);
}

TEST(CursorAwaitable, ChainAllYield_YieldWhileRunning) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  std::atomic<bool> yield_requested_flag{false};
  ctx.stopping_context.yield_requested = &yield_requested_flag;
  int yield_after = 2;  // yield on 2nd hit (root then child)
  ResettableCounter counter(1);

  PullAwaitable awaitable = ChainRoot(ctx, counter, &yield_after);

  auto r1 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r1.status, PullRunResult::Status::Yielded);
  yield_requested_flag.store(false);
  stored.resume();
  auto r2 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r2.status, PullRunResult::Status::HasRow);
}

// Chain with lvalue co_await (tests operator co_await() &); rvalue is tested by all other chain tests.
TEST(CursorAwaitable, ChainAllYield_LvalueCoAwait) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  ResettableCounter counter(100);

  PullAwaitable awaitable = ChainRootAwaitLvalue(ctx, counter);

  auto result = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(result.status, PullRunResult::Status::HasRow);
}

// ========== Phase 2: Chain with only some yield checks ==========

TEST(CursorAwaitable, ChainSomeYield_YieldNeverTriggered) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  ResettableCounter counter(100);

  PullAwaitable awaitable = ChainRootSomeYield(ctx, counter);

  auto r = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r.status, PullRunResult::Status::HasRow);
}

TEST(CursorAwaitable, ChainSomeYield_YieldAtFirstOpportunity) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  std::atomic<bool> yield_requested_flag{false};
  ctx.stopping_context.yield_requested = &yield_requested_flag;
  int yield_after = 1;
  ResettableCounter counter(1);

  PullAwaitable awaitable = ChainRootSomeYield(ctx, counter, &yield_after);

  auto r1 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r1.status, PullRunResult::Status::Yielded);
  yield_requested_flag.store(false);
  stored.resume();
  auto r2 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r2.status, PullRunResult::Status::HasRow);
}

TEST(CursorAwaitable, ChainSomeYield_YieldWhileRunning) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  std::atomic<bool> yield_requested_flag{false};
  ctx.stopping_context.yield_requested = &yield_requested_flag;
  int yield_after = 1;
  ResettableCounter counter(1);

  PullAwaitable awaitable = ChainRootSomeYield(ctx, counter, &yield_after);

  auto r1 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r1.status, PullRunResult::Status::Yielded);
  yield_requested_flag.store(false);
  stored.resume();
  auto r2 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r2.status, PullRunResult::Status::HasRow);
}

// ========== Phase 2: Chain of coroutines + regular function ==========

TEST(CursorAwaitable, ChainWithRegular_YieldNeverTriggered) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  ResettableCounter counter(100);

  PullAwaitable awaitable = ChainWithRegularLeaf(ctx, counter);

  auto r = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r.status, PullRunResult::Status::HasRow);
}

TEST(CursorAwaitable, ChainWithRegular_YieldAtFirstOpportunity) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  std::atomic<bool> yield_requested_flag{false};
  ctx.stopping_context.yield_requested = &yield_requested_flag;
  int yield_after = 1;
  ResettableCounter counter(1);

  PullAwaitable awaitable = ChainWithRegularLeaf(ctx, counter, &yield_after);

  auto r1 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r1.status, PullRunResult::Status::Yielded);
  yield_requested_flag.store(false);
  stored.resume();
  auto r2 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r2.status, PullRunResult::Status::HasRow);
}

TEST(CursorAwaitable, ChainWithRegular_YieldWhileRunning) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  std::atomic<bool> yield_requested_flag{false};
  ctx.stopping_context.yield_requested = &yield_requested_flag;
  int yield_after = 1;
  ResettableCounter counter(1);

  PullAwaitable awaitable = ChainWithRegularLeaf(ctx, counter, &yield_after);

  auto r1 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r1.status, PullRunResult::Status::Yielded);
  yield_requested_flag.store(false);
  stored.resume();
  auto r2 = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r2.status, PullRunResult::Status::HasRow);
}

// Verify that a parent can catch an exception from a child co_await.
TEST(CursorAwaitable, ExceptionCaughtByParent) {
  ExecutionContext ctx{};
  std::coroutine_handle<> stored;
  ctx.suspended_task_handle_ptr = &stored;
  ResettableCounter counter(100);

  // We need a helper similar to the one described above
  auto awaitable = [](ExecutionContext &c, ResettableCounter &cnt) -> PullAwaitable {
    try {
      co_await ChainChildThatThrows(c, cnt);
    } catch (...) {
      co_return false;
    }
    co_return true;
  }(ctx, counter);

  auto r = RunPullToCompletion(awaitable, ctx);
  EXPECT_EQ(r.status, PullRunResult::Status::Done);  // Returns false, so Status::Done
}

// Explicitly test rvalue move-into-awaiter
TEST(CursorAwaitable, RValueCoAwaitOwnership) {
  ExecutionContext ctx{};
  ResettableCounter counter(100);

  auto root = [](ExecutionContext &c, ResettableCounter &cnt) -> PullAwaitable {
    // This triggers operator co_await() &&
    bool res = co_await ChainChild(c, cnt);
    co_return res;
  }(ctx, counter);

  auto r = RunPullToCompletion(root, ctx);
  EXPECT_EQ(r.status, PullRunResult::Status::HasRow);
}
