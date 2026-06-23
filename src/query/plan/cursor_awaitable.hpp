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

#pragma once

#include <atomic>
#include <coroutine>

#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/plan/cursor_awaitable_core.hpp"
#include "utils/counter.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::plan {

// ─────────────────────────────────────────────────────────────────────────────
// Deliverable 1 — YieldPointAwaitable
//
// A co_await target placed (Phase 3) at the same sites as AbortCheck.  The
// await chain funnels yield suspensions up to the outermost ResumePullStep
// driver via void await_suspend (stops symmetric transfer; control returns to
// the resuming call site, i.e. the driver).
//
// Protocol with the driver:
//   • await_ready() fires the throttled CheckAbortOrYield once.
//       – Abort  → store reason, return true (no suspend, await_resume throws).
//       – Yield  → return false (suspend path follows).
//       – Continue → return true (no suspend, no-op).
//   • await_suspend(h) — reached ONLY on the Yield path:
//       stores the LEAF handle h in *ctx_->suspended_task_handle_ptr so the
//       driver can resume that exact frame on the next ResumePullStep call.
//       Returns void so control unwinds to whoever called .resume() on h
//       (i.e. back to ResumePullStep in the driver).
//   • await_resume() — on resume, throws if an abort was recorded.
//
// When yield is OFF (suspended_task_handle_ptr == nullptr, Phase 1/2 default),
// await_ready always returns true (the nullptr guard short-circuits), so this
// is a pure no-op with zero overhead.
// ─────────────────────────────────────────────────────────────────────────────
class YieldPointAwaitable {
 public:
  struct Awaiter {
    ExecutionContext *ctx_{nullptr};
    utils::ResettableCounter *maybe_check_{nullptr};
    AbortReason abort_reason_{AbortReason::NO_ABORT};

    /// noexcept so the compiler can eliminate any EH bookkeeping at every
    /// co_await site.  Abort is deferred to await_resume() to keep this path
    /// throw-free.
    bool await_ready() noexcept {
      if (!ctx_ || !maybe_check_) [[unlikely]]
        return true;  // no context — no-op, never suspend

      auto result = ctx_->stopping_context.CheckAbortOrYield(*maybe_check_);

      if (result.action == StopOrYieldResult::Action::Abort) [[unlikely]] {
        abort_reason_ = result.abort_reason;
        return true;  // don't suspend; await_resume() will throw
      }
      if (result.action == StopOrYieldResult::Action::Yield) [[unlikely]] {
        return false;  // proceed to await_suspend
      }
      return true;  // Continue: no suspension
    }

    /// Reached ONLY when await_ready() returned false (Yield path).
    /// Returns void deliberately: this terminates the coroutine symmetric-
    /// transfer chain and hands control back to whoever last called .resume()
    /// on this handle — i.e. back to ResumePullStep in the driver.
    ///
    /// The driver inspects *ctx_->suspended_task_handle_ptr after the resume
    /// call returns to detect that a yield happened and to know which frame to
    /// resume next.
    void await_suspend(std::coroutine_handle<> h) const {
      DMG_ASSERT(ctx_->suspended_task_handle_ptr,
                 "YieldPointAwaitable::await_suspend reached but ctx_->suspended_task_handle_ptr is nullptr. "
                 "A PullDriverScope(Enabled) must be active around every coroutine pull region.");
      *ctx_->suspended_task_handle_ptr = h;
    }

    void await_resume() const {
      if (abort_reason_ != AbortReason::NO_ABORT) [[unlikely]]
        throw HintedAbortError(abort_reason_);
    }
  };

  YieldPointAwaitable(ExecutionContext &ctx, utils::ResettableCounter &maybe_check) noexcept
      : ctx_(&ctx), maybe_check_(&maybe_check) {}

  auto operator co_await() noexcept { return Awaiter{ctx_, maybe_check_}; }

 private:
  ExecutionContext *ctx_;
  utils::ResettableCounter *maybe_check_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Deliverable 3 — PullDriverScope
//
// RAII scoper that manages the two pointers the yield protocol writes through:
//   ctx.suspended_task_handle_ptr  — the leaf-handle channel
//   ctx.stopping_context.yield_requested — the "please yield" flag pointer
//
// Two modes (YieldMode enum):
//
//   Enabled  — ROOT driver scope.  Sets ctx.suspended_task_handle_ptr to
//               point at an owned slot.  Saves and restores the previous value.
//               yield_requested is left untouched (the scheduler already set it
//               for the thread via WorkerYieldRegistry).  Use this to bracket
//               the outer ResumePullStep driver loop.
//
//   Suppressed — NESTED sub-pull scope.  Sets ctx.suspended_task_handle_ptr
//               to an owned slot AND sets ctx.stopping_context.yield_requested
//               to nullptr (saving and restoring both).  CheckAbortOrYield can
//               therefore only return Continue/Abort (never Yield) for the
//               sub-pull region.  This is the correct default for any nested
//               synchronous pull that must not yield the worker mid-way.
//
// The scope is non-copyable and non-movable (pointer stability guarantee).
// ─────────────────────────────────────────────────────────────────────────────
enum class YieldMode : bool { Enabled = true, Suppressed = false };

class PullDriverScope {
 public:
  /// Construct a scope on ctx with the given yield mode.  The dtor restores
  /// both saved pointers unconditionally.
  explicit PullDriverScope(ExecutionContext &ctx, YieldMode mode = YieldMode::Enabled) noexcept
      : ctx_(ctx),
        mode_(mode),
        saved_handle_ptr_(ctx.suspended_task_handle_ptr),
        saved_yield_requested_(ctx.stopping_context.yield_requested) {
    if (mode == YieldMode::Enabled) {
      // Guard: nested Enabled scopes fight over the single suspended_task_handle_ptr slot.
      // A Suppressed scope is intentionally allowed to nest inside an Enabled one.
      DMG_ASSERT(enabled_depth_ == 0,
                 "PullDriverScope(Enabled): nested Enabled yield scope detected on this thread. "
                 "Only one Enabled scope may be active at a time; use Suppressed for inner pulls.");
      ++enabled_depth_;
    }

    // Always wire the handle-channel to our owned slot.
    ctx_.suspended_task_handle_ptr = &slot_;

    if (mode == YieldMode::Suppressed) {
      // Suppress yield: CheckAbortOrYield will never see a yield_requested.
      ctx_.stopping_context.yield_requested = nullptr;
    }
    // YieldMode::Enabled: yield_requested is left as-is (the scheduler wires
    // it per-worker; we must not disturb it).
  }

  ~PullDriverScope() noexcept {
    // Restore both pointers so the outer scope sees the previous state.
    ctx_.suspended_task_handle_ptr = saved_handle_ptr_;
    ctx_.stopping_context.yield_requested = saved_yield_requested_;

    // Decrement only for the mode we incremented (Suppressed scopes never touch enabled_depth_).
    if (mode_ == YieldMode::Enabled) {
      --enabled_depth_;
    }
  }

  // Non-copyable, non-movable (our address stability is the contract).
  PullDriverScope(const PullDriverScope &) = delete;
  PullDriverScope &operator=(const PullDriverScope &) = delete;
  PullDriverScope(PullDriverScope &&) = delete;
  PullDriverScope &operator=(PullDriverScope &&) = delete;

  /// The slot where a suspended leaf frame will write its handle.
  /// ResumePullStep inspects ctx_.suspended_task_handle_ptr (== &slot_) after
  /// each resume() call to detect a yield.
  [[nodiscard]] std::coroutine_handle<> StashedHandle() const noexcept { return slot_; }

 private:
  /// Per-thread nesting depth of Enabled scopes.  Asserted to be 0 on Enabled ctor entry.
  /// Suppressed scopes do not touch this counter.
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  static thread_local int enabled_depth_;

  ExecutionContext &ctx_;
  YieldMode mode_;
  std::coroutine_handle<> slot_{};
  /// Saved prior suspended_task_handle_ptr (restored in dtor).
  std::coroutine_handle<> *saved_handle_ptr_;
  /// Saved prior yield_requested (restored in dtor; only mutated in Suppressed mode).
  std::atomic<bool> *saved_yield_requested_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Declaration: drives the root generator forward by EXACTLY ONE step.
// See cursor_awaitable.cpp for the full contract.
// ─────────────────────────────────────────────────────────────────────────────
PullRunResult ResumePullStep(PullAwaitable::ResumeAwaitable &ra, ExecutionContext &ctx);

}  // namespace memgraph::query::plan
