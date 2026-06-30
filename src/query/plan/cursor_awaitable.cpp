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

#include "query/plan/cursor_awaitable.hpp"

#include <atomic>

namespace memgraph::query::plan {

// Thread-local active coroutine split policy (PR-13). Set by PullPlan from the knob immediately before
// building the cursor tree (single-threaded), read by each cursor's ctor via SelectCoroMode. Defaults
// to empty => every cursor stays Sync => byte-identical to master.
CoroSplitPolicy &ActiveCoroPolicy() noexcept {
  static thread_local CoroSplitPolicy policy{};
  return policy;
}

// Thread-local tally of cursors selected Coro while building the current plan (coroutine-region size).
uint32_t &CoroSelectedCount() noexcept {
  static thread_local uint32_t count{0};
  return count;
}

#ifndef NDEBUG
namespace {
// DEBUG-ONLY parity-test seam (see header). Default OFF => synchronous root drive == master.
std::atomic<bool> g_force_coro_root_drive{false};

// DEBUG-ONLY yield seam (S1). When enabled, PullPlan points ctx.stopping_context.yield_requested at
// g_force_yield_flag so every throttled YieldPointAwaitable check yields, maximally exercising the
// production suspend/resume drive. g_force_yield_flag stays true while enabled (no scheduler clears it;
// progress is still guaranteed because the throttle counter only fires every N checks).
std::atomic<bool> g_force_yield_enabled{false};
std::atomic<bool> g_force_yield_flag{false};
}  // namespace

void SetForceCoroRootDriveForTesting(bool enabled) noexcept {
  g_force_coro_root_drive.store(enabled, std::memory_order_relaxed);
}

bool ForceCoroRootDriveForTesting() noexcept { return g_force_coro_root_drive.load(std::memory_order_relaxed); }

void SetForceYieldForTesting(bool enabled) noexcept {
  g_force_yield_flag.store(enabled, std::memory_order_relaxed);
  g_force_yield_enabled.store(enabled, std::memory_order_relaxed);
  // Reset the throttle counter so every YieldPointAwaitable check actually yields (period=1)
  // when enabled, or restores to the production period (20) when disabled.
  ResetYieldThrottleForTesting(enabled ? 1 : 20);
}

std::atomic<bool> *ForceYieldFlagForTesting() noexcept {
  return g_force_yield_enabled.load(std::memory_order_relaxed) ? &g_force_yield_flag : nullptr;
}
#endif

// ─────────────────────────────────────────────────────────────────────────────
// ResumePullStep — drives the root generator forward by exactly ONE step.
//
// CONTRACT (single-result):
//   One call == one co_yield (HasRow), or exhaustion (Done), or a cooperative
//   yield that suspended the leaf frame before producing a row (Yielded).
//
// YIELD PROTOCOL:
//   ctx.suspended_task_handle_ptr is a pointer to a slot OWNED by the active
//   PullDriverScope (set by that RAII object, not by callers here).  It is
//   nullptr when no driver scope is active or when yield is suppressed.
//
//   • If the slot contains a non-null handle (a previously suspended leaf
//     frame), we resume that leaf frame directly — not the root generator.
//     We consume the slot first (*slot = {}) so a fresh yield during this
//     resume will re-populate it.
//   • After the resume() call returns we check the slot again: if it is now
//     non-null, the coroutine suspended at a YieldPointAwaitable before
//     producing a row → return Yielded.
//   • If the slot is null after the resume, the coroutine ran to a co_yield
//     (HasRow) or co_return (Done) → report normally.
//
// YIELD-OFF IDENTITY:
//   When ctx.suspended_task_handle_ptr == nullptr (no driver wires it yet, and
//   any PullDriverScope(Suppressed) region) the slot branch is never taken and
//   the post-resume check is also skipped — behaviour is identical to a plain
//   resume-once driver.
// ─────────────────────────────────────────────────────────────────────────────
PullRunResult ResumePullStep(PullAwaitable::ResumeAwaitable &ra, ExecutionContext &ctx) {
  // Immediate (legacy, no-frame) or already-exhausted generator: report without resuming.
  if (ra.Done()) {
    ra.RethrowIfException();
    return ra.Result() ? PullRunResult::Row() : PullRunResult::Done();
  }

  // Decide which handle to resume.
  //
  // If a PullDriverScope is active (suspended_task_handle_ptr != nullptr) AND
  // a leaf frame was stashed by a previous yield, resume that leaf directly.
  // Otherwise resume the root generator handle.
  std::coroutine_handle<> target = ra.GetHandle();
  if (ctx.suspended_task_handle_ptr && *ctx.suspended_task_handle_ptr) {
    target = *ctx.suspended_task_handle_ptr;
    *ctx.suspended_task_handle_ptr = {};  // consume; a fresh yield in this step will re-stash
  }

  target.resume();

  // Surface any exception the coroutine stored via unhandled_exception() before reporting
  // yield/row/done status. The root promise always carries propagated exceptions even if
  // the throw originated deep in a leaf (symmetric transfer unwinds to BasePromise::
  // unhandled_exception via final_suspend→SymmetricTransfer→parent chain).
  ra.RethrowIfException();

  // ── EVENT-PARK check (c3.0) — MUST come before the Yielded slot check. ──────────────────
  //
  // ProgressAwaitable::await_suspend sets ctx.event_parked = true when RegisterProgressWaiter
  // succeeds (the coro is suspended on an external progress event, NOT on a yield point).
  // It does NOT write the slot (suspended_task_handle_ptr), so the Yielded path below would
  // NOT fire — we need this independent check to distinguish the two park kinds.
  //
  // INVARIANT: only one of {event_parked, slot filled} can be true after a single resume():
  //   • YieldPointAwaitable writes the slot and does NOT touch event_parked.
  //   • ProgressAwaitable sets event_parked and does NOT write the slot.
  // Checking event_parked first is safe because if it is false the slot check is unchanged.
  if (ctx.event_parked) [[unlikely]] {
    ctx.event_parked = false;  // consume; one-shot per suspend
    return PullRunResult::EventParked();
  }

  // ── YIELD-KIND park — leaf wrote its handle into the slot. ───────────────────────────────
  // The void await_suspend in YieldPointAwaitable terminates the symmetric-transfer
  // chain, so target.resume() returns here (not at a co_yield/co_return).
  if (ctx.suspended_task_handle_ptr && *ctx.suspended_task_handle_ptr) {
    return PullRunResult::Yielded();
  }

  return ra.Result() ? PullRunResult::Row() : PullRunResult::Done();
}

}  // namespace memgraph::query::plan
