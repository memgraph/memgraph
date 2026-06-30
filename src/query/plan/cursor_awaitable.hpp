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
        saved_yield_requested_(ctx.stopping_context.yield_requested),
        saved_enabled_driver_active_(ctx.enabled_driver_active) {
    if (mode == YieldMode::Enabled) {
      // Guard: two Enabled scopes on the same ExecutionContext would fight over the single
      // suspended_task_handle_ptr slot. A Suppressed scope is intentionally allowed to nest inside
      // an Enabled one. The flag is per-context (NOT thread_local) so a parked query that yielded
      // with its driver alive does not false-trip the guard when another query runs on the same
      // worker thread.
      DMG_ASSERT(!ctx_.enabled_driver_active,
                 "PullDriverScope(Enabled): an Enabled yield scope is already active on this "
                 "ExecutionContext. Only one Enabled scope may be active per context; use Suppressed "
                 "for inner pulls.");
      ctx_.enabled_driver_active = true;
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

    // Restore the per-context flag only for Enabled mode (Suppressed scopes never set it).
    if (mode_ == YieldMode::Enabled) {
      ctx_.enabled_driver_active = saved_enabled_driver_active_;
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
  ExecutionContext &ctx_;
  YieldMode mode_;
  std::coroutine_handle<> slot_{};
  /// Saved prior suspended_task_handle_ptr (restored in dtor).
  std::coroutine_handle<> *saved_handle_ptr_;
  /// Saved prior yield_requested (restored in dtor; only mutated in Suppressed mode).
  std::atomic<bool> *saved_yield_requested_;
  /// Saved prior ctx.enabled_driver_active (restored in dtor; only mutated in Enabled mode).
  bool saved_enabled_driver_active_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Declaration: drives the root generator forward by EXACTLY ONE step.
// See cursor_awaitable.cpp for the full contract.
// ─────────────────────────────────────────────────────────────────────────────
PullRunResult ResumePullStep(PullAwaitable::ResumeAwaitable &ra, ExecutionContext &ctx);

// ─────────────────────────────────────────────────────────────────────────────
// TEST-ONLY (throwaway scaffold). Forces the root driver (PullPlan::Pull) to pull the plan via the
// coroutine path — PullCo() + ResumePullStep — instead of the synchronous Pull(). This lets the parity
// harness exercise the coroutine machinery before the real, per-cursor mode selection (a later PR,
// MakeCursor-based) exists.
//
// Default OFF => the engine drives synchronously, byte-identical to master. With every cursor still in
// Sync mode (PullCo() default == Immediate(Pull())), turning it ON is ALSO byte-identical until cursors
// are converted; once a cursor has a coroutine body it is then exercised through this path.
//
// Process-global (not thread-safe against concurrent queries) — strictly a parity-test seam.
//
// DEBUG-ONLY: gated behind NDEBUG (defined in Release/RelWithDebInfo, absent in Debug). In production
// builds the hook, the interpreter branch that consults it, and the parity tests that toggle it all
// compile out entirely — so there is ZERO per-pull overhead and no way to force the coroutine path.
// The harness is a permanent Debug-build correctness gate (coroutine pull == synchronous pull), NOT
// throwaway scaffolding; it coexists with the real MakeCursor-based mode selection (a later PR).
// ─────────────────────────────────────────────────────────────────────────────
#ifndef NDEBUG
void SetForceCoroRootDriveForTesting(bool enabled) noexcept;
[[nodiscard]] bool ForceCoroRootDriveForTesting() noexcept;

// DEBUG-ONLY yield seam (S1 — coroutine-native scheduler). When enabled, the production drive points
// ctx.stopping_context.yield_requested at an always-true flag, so every throttled YieldPointAwaitable
// check actually yields — exercising the real suspend/resume drive end-to-end (the yield path has no
// production scheduler yet). ForceYieldFlagForTesting() returns that flag (nullptr when off).
// Additionally resets the thread-local YieldPointAwaitable throttle counter to period=1 (so small
// datasets fire at least one park — non-vacuous); on disable, resets to the production period.
void SetForceYieldForTesting(bool enabled) noexcept;
[[nodiscard]] std::atomic<bool> *ForceYieldFlagForTesting() noexcept;

// Reset the thread-local YieldPointAwaitable throttle counter (maybe_check_abort in operator.cpp)
// to the given period.  Called by SetForceYieldForTesting to make every checkpoint yield immediately.
// Defined in operator.cpp (where the TLS counter lives).
void ResetYieldThrottleForTesting(std::size_t period) noexcept;
#endif

}  // namespace memgraph::query::plan
