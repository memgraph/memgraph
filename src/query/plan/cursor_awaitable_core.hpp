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

#include <coroutine>
#include <cstdint>
#include <exception>
#include <utility>
#include "utils/logging.hpp"

namespace memgraph::query::plan {

/// Result status for the scheduler/driver.
struct PullRunResult {
  enum class Status : uint8_t { HasRow, Done, Yielded };
  Status status{Status::Done};

  [[nodiscard]] static PullRunResult Row() noexcept { return {Status::HasRow}; }

  [[nodiscard]] static PullRunResult Done() noexcept { return {Status::Done}; }

  [[nodiscard]] static PullRunResult Yielded() noexcept { return {Status::Yielded}; }
};

/// Base promise layout shared by all pull coroutines.
///
/// Results and exceptions live directly in the promise (on the coroutine frame),
/// not in parent-owned write-back storage. Readers access them via the child
/// handle after resumption, before any frame is destroyed.
struct BasePromise {
  bool has_more_{false};
  std::coroutine_handle<> parent_{std::noop_coroutine()};
  std::exception_ptr local_exception_{nullptr};

  static constexpr std::suspend_always initial_suspend() noexcept { return {}; }

  // Symmetric-transfer suspender.
  // NOTE: This is equivalent to a tail-call optimization for coroutines.
  struct SymmetricTransfer {
    std::coroutine_handle<> continuation;

    bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept {
      DMG_ASSERT(continuation, "symmetric transfer requiers a valid caller handler.");
      return continuation;  // Set by the caller.
    }

    void await_resume() const noexcept {}
  };

  // NOTE: By returning void, we would jump to the scheduler, this is not what we want, we want to unwind.
  // By not deleting it via suspend_never, we ensure the data is intact and can be read.
  // The frame is destroyed by the owning PullAwaitable (or ResumeAwaitable via Cursor::gen_) RAII destructor.
  auto final_suspend() noexcept { return SymmetricTransfer{parent_}; }

  void return_value(bool has_row) noexcept { has_more_ = has_row; }

  auto yield_value(bool has_row) noexcept {
    has_more_ = has_row;
    return SymmetricTransfer{parent_};
  }

  void unhandled_exception() noexcept { local_exception_ = std::current_exception(); }

  void RethrowIfException() const {
    if (local_exception_) std::rethrow_exception(local_exception_);
  }
};

/// Single awaitable type for the Query Plan.
class PullAwaitable {
 public:
  struct promise_type final : BasePromise {
    PullAwaitable get_return_object() noexcept {
      return PullAwaitable{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
  };

  /// Awaiter for one-shot helpers (InitEdges, PullInput, FindPath, etc.) that are
  /// co_await'd inline. The child handle is either moved in (rvalue co_await) or
  /// shared (lvalue co_await). Results and exceptions are read from the child's
  /// promise directly after resumption.
  struct Awaiter {
    std::coroutine_handle<promise_type> handle_{nullptr};
    bool immediate_ready_{false};
    bool immediate_value_{false};
    /// true only when the handle was moved in from a temporary PullAwaitable
    /// (operator co_await() &&).  In the lvalue case the PullAwaitable retains
    /// ownership and its own destructor will call destroy().
    bool owns_handle_{false};

    ~Awaiter() {
      // Destroy the handle when we own it (rvalue co_await path).
      // Always destroy, even if not done, to prevent leaks when parent cursors
      // are destroyed while child coroutines are suspended. The caller (cursor)
      // is responsible for ensuring proper cleanup.
      // In the lvalue co_await path (owns_handle_=false) the originating
      // PullAwaitable still holds the handle and will destroy it.
      if (owns_handle_ && handle_) {
        handle_.destroy();
      }
    }

    // handle_ is null for default-constructed or immediate PullAwaitables; await_ready() returns true
    // in both cases so await_suspend is never reached with a null handle.
    bool await_ready() const noexcept { return immediate_ready_ || !handle_; }

    // This cursor is not ready, transfer to the child and execute it's logic.
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> parent) const noexcept {
      handle_.promise().parent_ = parent;
      return handle_;  // symmetric transfer into child
    }

    bool await_resume() const {
      if (immediate_ready_) return immediate_value_;
      auto &p = handle_.promise();
      if (p.local_exception_) std::rethrow_exception(p.local_exception_);
      return p.has_more_;
    }
  };

  // PHASE-2 NOTE (cooperative yield not yet wired). ~Awaiter() destroys its handle whenever
  // owns_handle_ is set; it does NOT inspect handle_.done(). So once yield lands and a parent frame can
  // be parked over a co_await with its child leaf still suspended, YieldPointAwaitable::await_suspend
  // MUST transfer the handle out of the Awaiter (clear owns_handle_ / null handle_) before the parent
  // suspends -- otherwise tearing down the parent's Awaiter would destroy a child frame the scheduler
  // still holds (double-free). Today nothing yields, so no Awaiter is ever live across a real suspension.
  // ~PullPlan calls cursor_->Reset(), which destroys the root generator and unwinds the coroutine chain.

  PullAwaitable() = default;

  explicit PullAwaitable(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {}

  explicit PullAwaitable(bool immediate_has_row) noexcept
      : immediate_ready_(true), immediate_value_(immediate_has_row) {}

  ~PullAwaitable() {
    if (handle_) {
      handle_.destroy();
    }
  }

  // Move-only semantics
  PullAwaitable(const PullAwaitable &) = delete;
  PullAwaitable &operator=(const PullAwaitable &) = delete;

  PullAwaitable(PullAwaitable &&other) noexcept
      : handle_(std::exchange(other.handle_, nullptr)),
        immediate_ready_(other.immediate_ready_),
        immediate_value_(other.immediate_value_) {}

  PullAwaitable &operator=(PullAwaitable &&other) noexcept {
    if (this != &other) {
      if (handle_) handle_.destroy();
      handle_ = std::exchange(other.handle_, nullptr);
      immediate_ready_ = other.immediate_ready_;
      immediate_value_ = other.immediate_value_;
    }
    return *this;
  }

  /// co_await for lvalues: PullAwaitable retains ownership; Awaiter must not destroy.
  auto operator co_await() & noexcept { return Awaiter{handle_, immediate_ready_, immediate_value_, false}; }

  /// co_await for rvalues (temporaries): move the handle into the awaiter
  /// so it isn't destroyed by ~PullAwaitable() before the coroutine resumes.
  auto operator co_await() && noexcept {
    return Awaiter{std::exchange(handle_, nullptr), immediate_ready_, immediate_value_, true};
  }

  [[nodiscard]] std::coroutine_handle<promise_type> GetHandle() const noexcept { return handle_; }

  [[nodiscard]] bool Done() const noexcept { return immediate_ready_ || (handle_ && handle_.done()); }

  [[nodiscard]] bool Result() const noexcept {
    if (immediate_ready_) return immediate_value_;
    return handle_ ? handle_.promise().has_more_ : false;
  }

  void RethrowIfException() const {
    if (handle_) handle_.promise().RethrowIfException();
  }

  /// Lightweight awaitable for resuming a persistent generator.
  /// Non-owning: handle lifetime is managed by the cursor's gen_ member.
  /// Returned by Cursor::PullCo(); used as co_await target in parent cursors,
  /// or driven directly by ResumePullStep at the root.
  ///
  /// IMMEDIATE MODE (seam addition, not on the big-bang branch): a legacy child
  /// has no coroutine frame. The base Cursor::PullCo() default wraps the legacy
  /// `bool Pull()` result as an immediate ResumeAwaitable (handle_==nullptr,
  /// immediate_ready_==true). await_ready() is then true, so the parent never
  /// suspends and NO coroutine frame is allocated for that hop.
  struct ResumeAwaitable {
    std::coroutine_handle<promise_type> handle_{nullptr};
    bool immediate_ready_{false};
    bool immediate_value_{false};

    /// Wrap an already-computed legacy bool result (no coroutine frame).
    [[nodiscard]] static ResumeAwaitable Immediate(bool has_row) noexcept {
      return ResumeAwaitable{nullptr, true, has_row};
    }

    bool await_ready() const noexcept { return immediate_ready_ || !handle_ || handle_.done(); }

    // NOTE: This is called on the way down, liking back parents along the way.
    // yield_value (in Promise) is going to unwind this back.
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> parent) const noexcept {
      handle_.promise().parent_ = parent;
      return handle_;  // symmetric transfer into generator
    }

    bool await_resume() const {
      if (immediate_ready_) return immediate_value_;
      if (!handle_ || handle_.done()) {
        if (handle_) handle_.promise().RethrowIfException();
        return false;
      }
      // Defensive (mirrors PullAwaitable::Awaiter): a not-done coroutine has no stored exception
      // today (throwing runs final_suspend -> done, handled above), but keep the check so the
      // contract holds if a body ever yields after catching internally.
      handle_.promise().RethrowIfException();
      return handle_.promise().has_more_;
    }

    [[nodiscard]] std::coroutine_handle<promise_type> GetHandle() const noexcept { return handle_; }

    [[nodiscard]] bool Done() const noexcept { return immediate_ready_ || !handle_ || handle_.done(); }

    [[nodiscard]] bool Result() const noexcept {
      if (immediate_ready_) return immediate_value_;
      return handle_ ? handle_.promise().has_more_ : false;
    }

    void RethrowIfException() const {
      if (handle_) handle_.promise().RethrowIfException();
    }
  };

  /// Return a ResumeAwaitable for the live generator held by this PullAwaitable.
  ResumeAwaitable Resume() noexcept { return ResumeAwaitable{handle_}; }

 private:
  std::coroutine_handle<promise_type> handle_{nullptr};
  bool immediate_ready_{false};
  bool immediate_value_{false};
};

/// Boilerplate for a CONVERTED cursor: overrides the coroutine-resume entry point
/// (PullCo) to lazily create and resume the persistent generator held in gen_.
/// The base Cursor::PullCo() default instead immediate-wraps a legacy bool Pull().
/// Vtable dispatch on PullCo is therefore the converted/legacy predicate. Pair this
/// with `PullAwaitable DoPull(Frame &, ExecutionContext &) override;`.
#define MG_COROUTINE_CURSOR_PULLCO                                                  \
  PullAwaitable::ResumeAwaitable PullCo(Frame &f, ExecutionContext &ctx) override { \
    if (!gen_) [[unlikely]]                                                         \
      gen_ = DoPull(f, ctx);                                                        \
    return gen_->Resume();                                                          \
  }

}  // namespace memgraph::query::plan
