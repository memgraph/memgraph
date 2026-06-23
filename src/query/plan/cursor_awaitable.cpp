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

#include "flags/experimental.hpp"
#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

// thread_local definition for PullDriverScope's Enabled-nesting guard (declared in cursor_awaitable.hpp).
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
thread_local int PullDriverScope::enabled_depth_ = 0;

// Base Cursor seam definitions (kept out-of-line: the ctor needs the flags header, Pull() needs
// ResumePullStep, and Reset() matches the rest of the cursor cpp).

// Capture the routing decision ONCE per cursor (per query). Reading the flag here keeps the
// per-row Pull() router branch on an immutable member instead of a global flag read.
Cursor::Cursor() : use_coroutine_(flags::AreExperimentsEnabled(flags::Experiments::COROUTINE_CURSORS)) {}

void Cursor::Reset() { gen_.reset(); }

bool Cursor::Pull(Frame &f, ExecutionContext &ctx) {
  // DUAL-PATH router. flag OFF -> the cursor's verbatim legacy body (PullLegacy); the call graph
  // stays legacy because PullLegacy calls child->Pull() which re-enters this router.
  if (!use_coroutine_) return PullLegacy(f, ctx);

  // flag ON -> SINGLE-RESULT contract: resume the generator EXACTLY ONCE (one co_yield == one row).
  // Converted cursors override PullCo() to drive gen_.
  auto ra = PullCo(f, ctx);
  return ResumePullStep(ra, ctx).status == PullRunResult::Status::HasRow;
}

// Status-preserving root entry — does NOT collapse Yielded to false.
// The interpreter root driver will use this in Phase 3 so it can park and resume leaf frames.
// Zero callers today; purely an additive seam.
PullRunResult Cursor::PullRootStep(Frame &f, ExecutionContext &ctx) {
  auto ra = PullCo(f, ctx);
  return ResumePullStep(ra, ctx);
}

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
//   When ctx.suspended_task_handle_ptr == nullptr (Phase 1/2 default, and any
//   PullDriverScope(Suppressed) region) the slot branch is never taken and
//   the post-resume check is also skipped — behaviour is identical to the
//   original yield-OFF implementation.
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

  // A leaf that yielded wrote its handle into the slot during this resume call.
  // The void await_suspend in YieldPointAwaitable terminates the symmetric-transfer
  // chain, so target.resume() returns here (not at a co_yield/co_return).
  if (ctx.suspended_task_handle_ptr && *ctx.suspended_task_handle_ptr) {
    return PullRunResult::Yielded();
  }

  return ra.Result() ? PullRunResult::Row() : PullRunResult::Done();
}

}  // namespace memgraph::query::plan
