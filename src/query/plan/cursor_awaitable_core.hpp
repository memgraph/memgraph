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
  bool result_{false};
  std::coroutine_handle<> continuation_{std::noop_coroutine()};
  std::exception_ptr local_exception_{nullptr};

  static constexpr std::suspend_always initial_suspend() noexcept { return {}; }

  /// Suspends the current coroutine and resumes the parent via symmetric transfer.
  struct FinalSuspender {
    std::coroutine_handle<> continuation;

    bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept {
      return continuation;  // always non-null: set by RunPullToCompletion or await_suspend before resumption
    }

    void await_resume() const noexcept {}
  };

  auto final_suspend() noexcept { return FinalSuspender{continuation_}; }

  void return_value(bool has_row) noexcept { result_ = has_row; }

  // Symmetric-transfer suspender used by co_yield.
  struct YieldTransfer {
    std::coroutine_handle<> continuation;

    bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept {
      return continuation;  // always non-null: set by RunPullToCompletion or await_suspend before resumption
    }

    void await_resume() const noexcept {}
  };

  auto yield_value(bool has_row) noexcept {
    result_ = has_row;
    return YieldTransfer{continuation_};
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
    std::coroutine_handle<promise_type> child_handle_{nullptr};
    bool immediate_ready_{false};
    bool immediate_value_{false};

    ~Awaiter() {
      // The child handle was moved here from the PullAwaitable (operator co_await &&).
      // The child's ~PullAwaitable() therefore won't destroy it.  We must: if the
      // child ran to completion (done), destroy its frame now.  If it is still
      // suspended (parent being torn down mid-yield), leave it — the frame will be
      // cleaned up by PullPlan::suspended_handle_ on interpreter teardown.
      if (child_handle_ && child_handle_.done()) {
        child_handle_.destroy();
      }
    }

    bool await_ready() const noexcept { return immediate_ready_ || !child_handle_; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> parent) const noexcept {
      child_handle_.promise().continuation_ = parent;
      return child_handle_;  // symmetric transfer into child
    }

    bool await_resume() const {
      if (immediate_ready_) return immediate_value_;
      auto &p = child_handle_.promise();
      if (p.local_exception_) std::rethrow_exception(p.local_exception_);
      return p.result_;
    }
  };

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

  /// co_await for lvalues: shared ownership of the handle logic.
  auto operator co_await() & noexcept { return Awaiter{handle_, immediate_ready_, immediate_value_}; }

  /// co_await for rvalues (temporaries): move the handle into the awaiter
  /// so it isn't destroyed by ~PullAwaitable() before the coroutine resumes.
  auto operator co_await() && noexcept {
    return Awaiter{std::exchange(handle_, nullptr), immediate_ready_, immediate_value_};
  }

  [[nodiscard]] std::coroutine_handle<promise_type> GetHandle() const noexcept { return handle_; }

  [[nodiscard]] bool Done() const noexcept { return immediate_ready_ || (handle_ && handle_.done()); }

  [[nodiscard]] bool Result() const noexcept {
    if (immediate_ready_) return immediate_value_;
    return handle_ ? handle_.promise().result_ : false;
  }

  void RethrowIfException() const {
    if (handle_) handle_.promise().RethrowIfException();
  }

  /// Lightweight awaitable for resuming a persistent generator.
  /// Non-owning: handle lifetime is managed by the cursor's gen_ member.
  /// Returned by Cursor::Pull(); used as co_await target in parent cursors,
  /// or driven directly by RunPullToCompletion at the root.
  struct ResumeAwaitable {
    std::coroutine_handle<promise_type> handle_{nullptr};

    bool await_ready() const noexcept { return handle_.done(); }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> parent) const noexcept {
      handle_.promise().continuation_ = parent;
      return handle_;  // symmetric transfer into generator
    }

    bool await_resume() const {
      if (handle_.done()) {
        handle_.promise().RethrowIfException();
        return false;
      }
      return handle_.promise().result_;
    }

    [[nodiscard]] std::coroutine_handle<promise_type> GetHandle() const noexcept { return handle_; }

    [[nodiscard]] bool Done() const noexcept { return !handle_ || handle_.done(); }

    [[nodiscard]] bool Result() const noexcept { return handle_ ? handle_.promise().result_ : false; }

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

}  // namespace memgraph::query::plan
