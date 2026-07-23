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
#include <exception>
#include <memory>
#include <type_traits>
#include <utility>

#include "utils/logging.hpp"

namespace memgraph::utils {

/// Minimal, general-purpose LAZY coroutine task. `Task<T>` starts only when co_await'ed (or driven
/// via SyncWait): initial_suspend == suspend_always. A symmetric-transfer final_suspend jumps a
/// completing child straight into its continuation, keeping nested chains O(1) stack depth.
/// Deliberately narrow: single-awaiter, move-only, no scheduler integration -- the foundation
/// parkable Prepare builds on; scheduling awaitables layer on top.
template <typename T>
class Task;

namespace detail {

/// Storage + continuation plumbing shared by the void and non-void promise specializations.
struct PromiseBase {
  /// Coroutine to resume when this Task finishes (set by the Awaiter); null when driven directly.
  std::coroutine_handle<> continuation_{nullptr};
  std::exception_ptr exception_{nullptr};

  static std::suspend_always initial_suspend() noexcept { return {}; }

  void unhandled_exception() noexcept { exception_ = std::current_exception(); }

  /// Symmetric-transfer final awaiter: resumes the continuation, or a no-op coroutine if none.
  struct FinalAwaiter {
    bool await_ready() const noexcept { return false; }

    template <typename Promise>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> h) noexcept {
      auto continuation = h.promise().continuation_;
      return continuation ? continuation : std::noop_coroutine();
    }

    void await_resume() const noexcept {}
  };

  FinalAwaiter final_suspend() noexcept { return {}; }
};

template <typename T>
struct Promise : PromiseBase {
  // Engaged by return_value, moved out by the Awaiter/SyncWait; union avoids default-constructing
  // T and any heap allocation beyond the coroutine frame.
  union {
    T value_;
  };

  bool has_value_{false};

  Promise() noexcept {}

  ~Promise() {
    if (has_value_) value_.~T();
  }

  Task<T> get_return_object() noexcept;

  template <typename U>
  void return_value(U &&value) noexcept(std::is_nothrow_constructible_v<T, U &&>) {
    std::construct_at(&value_, std::forward<U>(value));
    has_value_ = true;
  }

  T TakeValue() {
    if (exception_) std::rethrow_exception(exception_);
    DMG_ASSERT(has_value_, "Task<T> completed without a value or an exception.");
    return std::move(value_);
  }
};

template <>
struct Promise<void> : PromiseBase {
  Task<void> get_return_object() noexcept;

  void return_void() noexcept {}

  void TakeValue() const {
    if (exception_) std::rethrow_exception(exception_);
  }
};

}  // namespace detail

template <typename T = void>
class [[nodiscard]] Task {
 public:
  using promise_type = detail::Promise<T>;

  Task() noexcept = default;

  explicit Task(std::coroutine_handle<promise_type> handle) noexcept : handle_(handle) {}

  Task(const Task &) = delete;
  Task &operator=(const Task &) = delete;

  Task(Task &&other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {}

  Task &operator=(Task &&other) noexcept {
    if (this != &other) {
      Destroy();
      handle_ = std::exchange(other.handle_, nullptr);
    }
    return *this;
  }

  ~Task() { Destroy(); }

  /// Binds this Task's completion to the awaiting coroutine and starts the lazy body via symmetric
  /// transfer on first co_await.
  struct Awaiter {
    std::coroutine_handle<promise_type> handle_;

    bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting) noexcept {
      handle_.promise().continuation_ = awaiting;
      return handle_;  // symmetric transfer: start/resume this Task's lazy body now.
    }

    T await_resume() { return handle_.promise().TakeValue(); }
  };

  Awaiter operator co_await() && noexcept {
    DMG_ASSERT(handle_, "co_await on an empty/moved-from Task.");
    return Awaiter{handle_};
  }

  /// Drive this Task to completion on the current thread and return its result (or rethrow). For
  /// non-coroutine callers (tests, sync entry points); see `SyncWait` for the ergonomic form.
  T Run() {
    DMG_ASSERT(handle_, "Run() on an empty/moved-from Task.");
    handle_.resume();
    return handle_.promise().TakeValue();
  }

  /// Start or resume this Task exactly once, WITHOUT assuming it completes (for driving a top-level
  /// Task that may park mid-body; see session.hpp's DrivePreparedRun). Caller MUST check `Done()`
  /// before `TakeValue()` -- a parked chain holds neither value nor exception yet.
  void Resume() {
    DMG_ASSERT(handle_ && !handle_.done(), "Resume() on an empty/moved-from/already-done Task.");
    handle_.resume();
  }

  /// True once the body completed (co_returned or threw), so `TakeValue()` is safe; also true for
  /// an empty/moved-from Task.
  bool Done() const noexcept { return !handle_ || handle_.done(); }

  /// Reads out a Done() Task's result, or rethrows. Precondition: `Done()`. Companion to
  /// `Resume()`/`Done()`; use `Run()`/`SyncWait` when synchronous completion is assumed.
  T TakeValue() {
    DMG_ASSERT(handle_, "TakeValue() on an empty/moved-from Task.");
    return handle_.promise().TakeValue();
  }

 private:
  void Destroy() noexcept {
    if (handle_) {
      handle_.destroy();
      handle_ = nullptr;
    }
  }

  std::coroutine_handle<promise_type> handle_{nullptr};
};

namespace detail {

template <typename T>
inline Task<T> Promise<T>::get_return_object() noexcept {
  return Task<T>{std::coroutine_handle<Promise<T>>::from_promise(*this)};
}

inline Task<void> Promise<void>::get_return_object() noexcept {
  return Task<void>{std::coroutine_handle<Promise<void>>::from_promise(*this)};
}

}  // namespace detail

/// Run a fresh lazy Task to completion on the current thread and return its value (or rethrow).
template <typename T>
T SyncWait(Task<T> &&task) {
  return task.Run();
}

}  // namespace memgraph::utils
