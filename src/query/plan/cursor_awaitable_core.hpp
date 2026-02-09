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
/// To support Symmetric Transfer safely, the child writes its result and exceptions
/// into parent-owned memory (pointers) during execution/return.
struct BasePromise {
  bool result_{false};
  bool *result_ptr_{nullptr};
  std::exception_ptr *exception_ptr_{nullptr};
  std::coroutine_handle<> continuation_{nullptr};
  std::exception_ptr local_exception_{nullptr};

  static constexpr std::suspend_always initial_suspend() noexcept { return {}; }

  /// Suspends the current coroutine and resumes the parent via symmetric transfer.
  struct FinalSuspender {
    std::coroutine_handle<> continuation;

    bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept {
      return continuation ? continuation : std::noop_coroutine();
    }

    void await_resume() const noexcept {}
  };

  auto final_suspend() noexcept { return FinalSuspender{continuation_}; }

  void return_value(bool has_row) noexcept {
    result_ = has_row;
    if (result_ptr_) {
      *result_ptr_ = has_row;
    }
  }

  void unhandled_exception() noexcept {
    auto current_ex = std::current_exception();
    if (exception_ptr_) {
      *exception_ptr_ = current_ex;
    } else {
      local_exception_ = current_ex;
    }
  }

  void RethrowIfException() const {
    if (local_exception_) {
      std::rethrow_exception(local_exception_);
    }
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

  /// The Awaiter handles the transition between two coroutines.
  /// It holds the storage for results/exceptions to ensure memory safety
  /// after the child coroutine has finished.
  struct Awaiter {
    std::coroutine_handle<promise_type> child_handle_{nullptr};
    bool immediate_ready_{false};
    bool immediate_value_{false};

    // Result/Exception write-back storage
    bool result_storage_{false};
    std::exception_ptr exception_storage_{nullptr};

    bool await_ready() const noexcept {
      // If we have an immediate value or no handle, don't suspend.
      // We don't check .done() here because the driver should ensure
      // we don't await a finished coroutine.
      return immediate_ready_ || !child_handle_;
    }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> parent) noexcept {
      auto &promise = child_handle_.promise();
      promise.continuation_ = parent;

      // Hook up write-back pointers to this Awaiter's storage
      promise.result_ptr_ = &result_storage_;
      promise.exception_ptr_ = &exception_storage_;

      return child_handle_;
    }

    bool await_resume() {
      if (immediate_ready_) return immediate_value_;

      // If the child threw, propagate it now.
      if (exception_storage_) {
        std::rethrow_exception(exception_storage_);
      }

      return result_storage_;
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
    if (handle_) {
      handle_.promise().RethrowIfException();
    }
  }

 private:
  std::coroutine_handle<promise_type> handle_{nullptr};
  bool immediate_ready_{false};
  bool immediate_value_{false};
};

}  // namespace memgraph::query::plan
