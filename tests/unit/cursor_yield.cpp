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

// COOPERATIVE-YIELD UNIT TEST (Phase 2 gate).
//
// Exercises YieldPointAwaitable, ResumePullStep (yield path), and PullDriverScope
// using a MINIMAL FAKE coroutine chain built on top of the real seam types —
// no DB, no storage, no interpreter.
//
// DESIGN:
//   A "leaf" coroutine (LeafPull) is a PullAwaitable-returning function that:
//     1. co_awaits YieldPointAwaitable — may yield or abort depending on the
//        stopping_context flags.
//     2. co_yield true — produces a row.
//     Repeats kLeafRows times, then co_return false (exhausted).
//
//   The driver loop is a helper (DriveChain) that:
//     a. Creates a PullAwaitable from the leaf coroutine.
//     b. Wraps it in a ResumeAwaitable.
//     c. Calls ResumePullStep in a loop, counting HasRow / Yielded / Done.
//     d. Returns {rows, yields}.
//
//   Tests set yield_requested and/or transaction_status on a default-constructed
//   ExecutionContext, wrap the driver region in PullDriverScope(Enabled), and
//   assert exact counts.
//
// NOTE: the fake chain uses a default-constructed ExecutionContext (the yield path only touches
// stopping_context.yield_requested/transaction_status + suspended_task_handle_ptr) and a Frame{0} the
// leaf never reads. The PullAwaitable returned by each fake coroutine must outlive the drive loop (its
// ResumeAwaitable is non-owning).

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <stdexcept>

#include "gtest/gtest.h"

#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/cursor_awaitable.hpp"
#include "query/plan/cursor_awaitable_core.hpp"
#include "utils/counter.hpp"

namespace memgraph::query::plan {
namespace {

// ─────────────────────────────────────────────────────────────────────────────
// Fake coroutine: produces kRowCount rows, checking for yield/abort before
// each row via YieldPointAwaitable.
//
// `throttle_period`: the ResettableCounter period passed to YieldPointAwaitable.
//   Period=1 means every call to CheckAbortOrYield fires.
//   Period=N means every Nth call fires.
//
// NOTE: Frame& is required by the PullAwaitable coroutine signature even though
// the fake body never reads from it.
// ─────────────────────────────────────────────────────────────────────────────
PullAwaitable LeafPull(Frame & /*f*/, ExecutionContext &ctx, int row_count, std::size_t throttle_period) {
  utils::ResettableCounter counter{throttle_period};
  for (int i = 0; i < row_count; ++i) {
    // May yield (control returns to driver without producing a row) or abort.
    co_await YieldPointAwaitable(ctx, counter);
    co_yield true;  // row produced
  }
  co_return false;  // exhausted
}

// ─────────────────────────────────────────────────────────────────────────────
// Fake PARENT coroutine: drives a child generator via `co_await child.Resume()`
// (the same symmetric-transfer hop a converted cursor uses for `co_await
// PullChild`), emitting one row per child row. The child holds the
// YieldPointAwaitable, so a yield originates DEEP (below this parent's co_await)
// — exercising the leaf-handle stash + resume-back-up-through-the-parent path
// that the single-coroutine tests do NOT reach. The child PullAwaitable must
// outlive the parent (the parent holds it by reference).
// ─────────────────────────────────────────────────────────────────────────────
PullAwaitable ParentPull(Frame & /*f*/, ExecutionContext & /*ctx*/, PullAwaitable &child) {
  while (true) {
    const bool has = co_await child.Resume();  // symmetric transfer into the child (leaf) frame
    if (!has) co_return false;                 // child exhausted -> parent exhausted
    co_yield true;                             // one parent row per child row
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Driver-loop result.
// ─────────────────────────────────────────────────────────────────────────────
struct DriveResult {
  int rows{0};
  int yields{0};
};

// ─────────────────────────────────────────────────────────────────────────────
// DriveChain: runs the leaf coroutine to completion (or until an exception)
// under an Enabled PullDriverScope, returning exact row and yield counts.
//
// `yield_arm_each_step`: if non-null, this callback is invoked BEFORE each
//   ResumePullStep call so the test can arm/disarm yield_requested per step.
// ─────────────────────────────────────────────────────────────────────────────
DriveResult DriveChain(PullAwaitable &pa, ExecutionContext &ctx, std::function<void(int /*step*/)> before_step = {}) {
  // Install the driver scope so YieldPointAwaitable::await_suspend has somewhere to write.
  PullDriverScope scope{ctx, YieldMode::Enabled};

  auto ra = pa.Resume();
  DriveResult result;
  int step = 0;

  while (!ra.Done()) {
    if (before_step) before_step(step);
    PullRunResult r = ResumePullStep(ra, ctx);
    ++step;
    if (r.status == PullRunResult::Status::HasRow) {
      ++result.rows;
    } else if (r.status == PullRunResult::Status::Yielded) {
      ++result.yields;
    } else {
      // Done — exit.
      break;
    }
  }

  return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 1 — No-yield baseline.
//
// yield_requested is null (not set).  All kRows rows are produced with zero
// Yielded steps.  Then Done.
// ─────────────────────────────────────────────────────────────────────────────
TEST(CursorYield, NoYieldBaseline) {
  constexpr int kRows = 5;
  ExecutionContext ctx;  // all pointers null, stopping_context.yield_requested=nullptr
  Frame frame{0};

  auto pa = LeafPull(frame, ctx, kRows, /*throttle_period=*/1);
  ASSERT_FALSE(pa.Done());  // not vacuously empty

  auto result = DriveChain(pa, ctx);

  EXPECT_EQ(result.rows, kRows);
  EXPECT_EQ(result.yields, 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 2 — Single yield round-trip.
//
// We arm yield_requested before step 0 only.  That step should return Yielded
// (the leaf suspended before producing its row).  We then disarm and re-drive:
// the stashed frame resumes, the row is emitted, and the rest run through
// without yielding.
//
// Expected: rows==kRows, yields==1.
// ─────────────────────────────────────────────────────────────────────────────
TEST(CursorYield, SingleYieldRoundTrip) {
  constexpr int kRows = 3;
  ExecutionContext ctx;
  Frame frame{0};
  std::atomic<bool> yield_flag{false};
  ctx.stopping_context.yield_requested = &yield_flag;

  auto pa = LeafPull(frame, ctx, kRows, /*throttle_period=*/1);
  ASSERT_FALSE(pa.Done());

  int expected_yields = 1;

  auto result = DriveChain(pa, ctx, [&](int step) {
    // Arm only on step 0 (the very first ResumePullStep call).
    yield_flag.store(step == 0, std::memory_order_release);
  });

  EXPECT_EQ(result.rows, kRows);
  EXPECT_EQ(result.yields, expected_yields);
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 3 — Multiple yields interleaved with rows.
//
// With throttle_period=1 and yield_requested armed for every even-numbered
// driver step (0, 2, 4, ...) — which are always the steps where the leaf's
// YieldPointAwaitable fires — and disarmed for every odd step (which are the
// resume-after-yield steps, where the leaf runs from await_resume to co_yield
// without another check firing):
//
//   step 0: even, armed  → leaf co_await fires → Yield           (yields=1)
//   step 1: odd, disarm  → resume stashed → await_resume(ok) → co_yield true → Row (rows=1)
//   step 2: even, armed  → leaf co_await fires → Yield           (yields=2)
//   step 3: odd, disarm  → resume stashed → Row                  (rows=2)
//   step 4: even, armed  → leaf co_await fires → Yield           (yields=3)
//   step 5: odd, disarm  → resume stashed → Row                  (rows=3)
//   step 6: loop back, leaf co_return false → Done
//
// Each row is preceded by exactly one yield.
// Expected: rows==kRows, yields==kRows.
// ─────────────────────────────────────────────────────────────────────────────
TEST(CursorYield, MultipleYieldsInterleavedWithRows) {
  constexpr int kRows = 3;
  ExecutionContext ctx;
  Frame frame{0};
  std::atomic<bool> yield_flag{false};
  ctx.stopping_context.yield_requested = &yield_flag;

  auto pa = LeafPull(frame, ctx, kRows, /*throttle_period=*/1);
  ASSERT_FALSE(pa.Done());

  // Even steps = fresh co_await fire → arm yield.
  // Odd steps = resume-after-yield → disarm (the leaf runs to co_yield, no re-check).
  auto result = DriveChain(pa, ctx, [&](int step) { yield_flag.store((step % 2) == 0, std::memory_order_release); });

  EXPECT_EQ(result.rows, kRows);
  EXPECT_EQ(result.yields, kRows) << "One yield per row when yield is armed on every co_await step";
  EXPECT_FALSE(result.rows == 0) << "Must not be vacuously empty";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 4 — Abort.
//
// yield_requested is null but transaction_status is set to TERMINATED.
// CheckAbortOrYield returns Abort → await_ready returns true with abort_reason
// stored → await_resume throws HintedAbortError.  ResumePullStep rethrows it.
//
// We expect EXPECT_THROW on the DriveChain helper (which calls ResumePullStep).
// The coroutine stores the exception via unhandled_exception, so RethrowIfException
// surfaces it at the driver.
//
// Expected: EXPECT_THROW(HintedAbortError) on the first step, zero rows.
// ─────────────────────────────────────────────────────────────────────────────
TEST(CursorYield, AbortThrows) {
  constexpr int kRows = 5;
  ExecutionContext ctx;
  Frame frame{0};

  // Arm abort.
  std::atomic<TransactionStatus> tx_status{TransactionStatus::TERMINATED};
  ctx.stopping_context.transaction_status = &tx_status;
  // yield_requested stays null → no yield, only abort path.

  auto pa = LeafPull(frame, ctx, kRows, /*throttle_period=*/1);
  ASSERT_FALSE(pa.Done());

  // Install driver scope manually so we can call ResumePullStep ourselves.
  PullDriverScope scope{ctx, YieldMode::Enabled};
  auto ra = pa.Resume();

  EXPECT_THROW(ResumePullStep(ra, ctx), HintedAbortError);
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 5 — Nested suppression.
//
// We have an outer Enabled scope (simulated by creating one) and an inner
// Suppressed scope that brackets the sub-chain's drive loop.  Even though
// yield_requested is armed throughout, the suppressed region must produce ALL
// rows with ZERO Yielded.  After the suppressed scope exits, the outer scope
// sees the original yield_requested restored (and could yield again).
//
// Implementation: we do NOT use the DriveChain helper here so we can control
// the two scopes explicitly.
// ─────────────────────────────────────────────────────────────────────────────
TEST(CursorYield, NestedSuppression) {
  constexpr int kRows = 4;
  ExecutionContext ctx;
  Frame frame{0};
  std::atomic<bool> yield_flag{true};  // armed throughout
  ctx.stopping_context.yield_requested = &yield_flag;

  auto pa = LeafPull(frame, ctx, kRows, /*throttle_period=*/1);
  ASSERT_FALSE(pa.Done());

  int sub_rows = 0;
  int sub_yields = 0;

  {
    // Inner suppressed scope: yield_requested set to nullptr for this region.
    PullDriverScope suppressed{ctx, YieldMode::Suppressed};

    // Verify suppression: ctx.stopping_context.yield_requested must be null here.
    EXPECT_EQ(ctx.stopping_context.yield_requested, nullptr) << "Suppressed scope must set yield_requested to null";

    auto ra = pa.Resume();
    while (!ra.Done()) {
      PullRunResult r = ResumePullStep(ra, ctx);
      if (r.status == PullRunResult::Status::HasRow) {
        ++sub_rows;
      } else if (r.status == PullRunResult::Status::Yielded) {
        ++sub_yields;
      } else {
        break;
      }
    }
  }  // suppressed scope exits here; both pointers restored.

  // After the scope, yield_requested must be restored to the original flag.
  EXPECT_EQ(ctx.stopping_context.yield_requested, &yield_flag)
      << "Suppressed scope must restore yield_requested on exit";

  // The sub-chain ran to completion with zero yields.
  EXPECT_EQ(sub_rows, kRows);
  EXPECT_EQ(sub_yields, 0) << "Suppressed scope must never Yield";

  // Also verify that suspended_task_handle_ptr is restored to null after scope exit.
  EXPECT_EQ(ctx.suspended_task_handle_ptr, nullptr)
      << "Suppressed scope must restore suspended_task_handle_ptr on exit";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 6 — Throttle: yield only on counter-firing iterations.
//
// Use throttle_period=3: CheckAbortOrYield returns non-Continue only every 3rd
// call.  With yield armed, the coroutine yields on steps 3, 6, 9, … (1-indexed
// counting from the counter's perspective, i.e. the 1st, 4th, 7th absolute
// call to CheckAbortOrYield from the beginning of the coroutine's lifetime).
//
// The leaf coroutine calls CheckAbortOrYield once per row (at the co_await
// before co_yield).  With period=3, the 3rd row's check fires, the 6th row's
// check fires, etc.
//
// kRows=6, period=3: yields on rows 3 and 6 (1-indexed) → 2 yields expected.
// After each Yielded step the driver re-arms yield before the next step so the
// stashed frame can resume; then we disarm so the re-run of the same
// YieldPointAwaitable call returns Continue (the counter won't fire again for
// 3 more rows since it already reset).
//
// NOTE: This relies on ResettableCounter surviving across yield/resume cycles —
// it lives in the leaf coroutine frame (local variable), which is preserved
// across co_await suspensions.  The counter state is intact.
//
// Strategy: arm yield_requested permanently (always true). The throttle controls
// whether the check actually fires. When the check fires (every kPeriod rows)
// Yield is returned. When the check does NOT fire, Continue is returned regardless
// of yield_requested. This gives us exactly floor(kRows / kPeriod) yields.
// ─────────────────────────────────────────────────────────────────────────────
TEST(CursorYield, ThrottleYieldsOnlyOnFiring) {
  constexpr int kRows = 6;
  constexpr std::size_t kPeriod = 3;
  // With period=3 and kRows=6, the counter fires on the 3rd and 6th co_await calls.
  constexpr int kExpectedYields = static_cast<int>(kRows / kPeriod);  // 2

  ExecutionContext ctx;
  Frame frame{0};
  std::atomic<bool> yield_flag{true};  // always armed; throttle controls firing
  ctx.stopping_context.yield_requested = &yield_flag;

  auto pa = LeafPull(frame, ctx, kRows, kPeriod);
  ASSERT_FALSE(pa.Done());

  // yield_flag is always true; the throttle (kPeriod=3) gates the actual check.
  // Yields occur only on every 3rd co_await call — i.e. after rows 2 and 5 (0-indexed).
  auto result = DriveChain(pa, ctx);

  EXPECT_EQ(result.rows, kRows);
  EXPECT_EQ(result.yields, kExpectedYields)
      << "Expected exactly kRows/kPeriod yields (throttle-controlled, flag always armed)";
  EXPECT_FALSE(result.rows == 0 && result.yields == 0) << "Test must not be vacuously empty";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 7 — PullDriverScope pointer identity: Enabled mode does NOT null out
// yield_requested, Suppressed mode does.  Verify pointer restoration in both.
// ─────────────────────────────────────────────────────────────────────────────
TEST(CursorYield, ScopePointerRestoration) {
  ExecutionContext ctx;
  std::atomic<bool> yield_flag{false};
  std::coroutine_handle<> fake_outer_slot{};

  // Set up a "prior outer" state.
  ctx.stopping_context.yield_requested = &yield_flag;
  ctx.suspended_task_handle_ptr = &fake_outer_slot;

  {
    // Enabled scope: handle-channel overwrites, yield_requested preserved.
    PullDriverScope enabled{ctx, YieldMode::Enabled};
    EXPECT_NE(ctx.suspended_task_handle_ptr, &fake_outer_slot) << "Enabled scope must install its own slot";
    EXPECT_EQ(ctx.stopping_context.yield_requested, &yield_flag) << "Enabled scope must NOT touch yield_requested";
  }
  EXPECT_EQ(ctx.suspended_task_handle_ptr, &fake_outer_slot) << "Enabled scope must restore handle ptr";
  EXPECT_EQ(ctx.stopping_context.yield_requested, &yield_flag) << "Enabled scope must restore yield_requested";

  {
    // Suppressed scope: both pointers overwritten.
    PullDriverScope suppressed{ctx, YieldMode::Suppressed};
    EXPECT_NE(ctx.suspended_task_handle_ptr, &fake_outer_slot) << "Suppressed scope must install its own slot";
    EXPECT_EQ(ctx.stopping_context.yield_requested, nullptr) << "Suppressed scope must null yield_requested";
  }
  EXPECT_EQ(ctx.suspended_task_handle_ptr, &fake_outer_slot) << "Suppressed scope must restore handle ptr";
  EXPECT_EQ(ctx.stopping_context.yield_requested, &yield_flag) << "Suppressed scope must restore yield_requested";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 8 — DEEP-CHAIN yield round-trip (the architecturally-critical case).
//
// parent -> child(leaf) via `co_await child.Resume()`. The yield fires at the
// LEAF, BELOW the parent's co_await. The driver must:
//   • see the yield unwind all the way back to ResumePullStep (not stop at the
//     parent's co_await),
//   • stash the LEAF handle (not the parent/root handle),
//   • on the next step resume the LEAF, which co_yields and symmetric-transfers
//     back UP through the parent, which then co_yields the row to the driver.
// If the driver stashed the root instead of the leaf, resuming would be wrong
// and rows would be lost/duplicated — so this test distinguishes correct
// leaf-stash from a root-stash bug.
//
// Arm yield on step 0 only: expect exactly 1 Yielded then all kRows rows.
// ─────────────────────────────────────────────────────────────────────────────
TEST(CursorYield, DeepChainYieldRoundTrip) {
  constexpr int kRows = 3;
  ExecutionContext ctx;
  Frame frame{0};
  std::atomic<bool> yield_flag{false};
  ctx.stopping_context.yield_requested = &yield_flag;

  auto child = LeafPull(frame, ctx, kRows, /*throttle_period=*/1);
  auto parent = ParentPull(frame, ctx, child);  // child must outlive parent
  ASSERT_FALSE(parent.Done());

  auto result = DriveChain(parent, ctx, [&](int step) {
    yield_flag.store(step == 0, std::memory_order_release);  // deep yield on the first step only
  });

  EXPECT_EQ(result.rows, kRows) << "all rows must survive a deep yield/resume round-trip";
  EXPECT_EQ(result.yields, 1) << "exactly one deep yield (leaf handle stashed + resumed through parent)";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 9 — DEEP-CHAIN yield on EVERY row (stress the leaf-stash path repeatedly).
// Arm on even steps (the co_await-firing steps), disarm on odd (resume steps);
// one deep yield precedes every row, through the parent hop. rows==yields==kRows.
// ─────────────────────────────────────────────────────────────────────────────
TEST(CursorYield, DeepChainYieldEveryRow) {
  constexpr int kRows = 4;
  ExecutionContext ctx;
  Frame frame{0};
  std::atomic<bool> yield_flag{false};
  ctx.stopping_context.yield_requested = &yield_flag;

  auto child = LeafPull(frame, ctx, kRows, /*throttle_period=*/1);
  auto parent = ParentPull(frame, ctx, child);
  ASSERT_FALSE(parent.Done());

  auto result =
      DriveChain(parent, ctx, [&](int step) { yield_flag.store((step % 2) == 0, std::memory_order_release); });

  EXPECT_EQ(result.rows, kRows);
  EXPECT_EQ(result.yields, kRows) << "one deep yield per row, each resumed correctly through the parent";
  EXPECT_FALSE(result.rows == 0) << "must not be vacuously empty";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 10 — DEEP-CHAIN abort propagation. The leaf aborts (HintedAbortError);
// it must unwind through the child promise -> the parent's co_await await_resume
// (RethrowIfException) -> the parent promise -> the driver. EXPECT_THROW at the
// driver, no rows.
// ─────────────────────────────────────────────────────────────────────────────
TEST(CursorYield, DeepChainAbortPropagates) {
  ExecutionContext ctx;
  Frame frame{0};
  std::atomic<TransactionStatus> tx_status{TransactionStatus::TERMINATED};
  ctx.stopping_context.transaction_status = &tx_status;

  auto child = LeafPull(frame, ctx, /*row_count=*/5, /*throttle_period=*/1);
  auto parent = ParentPull(frame, ctx, child);
  ASSERT_FALSE(parent.Done());

  PullDriverScope scope{ctx, YieldMode::Enabled};
  auto ra = parent.Resume();
  EXPECT_THROW(ResumePullStep(ra, ctx), HintedAbortError)
      << "a leaf abort must propagate up through the parent hop to the driver";
}

}  // namespace
}  // namespace memgraph::query::plan
