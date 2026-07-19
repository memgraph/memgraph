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

/// Minimal, general-purpose LAZY coroutine task type (IP-1 §3 item 1: `utils/coro_task.hpp`).
///
/// `Task<T>` does not start running when constructed -- its body only executes once it is
/// `co_await`ed (or driven via `SyncWait`), via `initial_suspend() == suspend_always`. Chaining
/// `co_await`s use a symmetric-transfer `final_suspend` so a completing child jumps directly into
/// its awaiting continuation (or `std::noop_coroutine()` if there is none) instead of returning to
/// a generic scheduler/trampoline -- this keeps nested `Task` chains O(1) stack depth regardless of
/// how many awaits are chained.
///
/// This type is deliberately narrow: single-awaiter, move-only, no scheduler/executor integration.
/// It is the foundation the parkable-Prepare coroutine work builds on (see
/// opencode-work/resource-lock-starvation/coro-prepare/ip1-design.md §3); scheduling-specific
/// awaitables (e.g. an accessor-acquire await) are layered on top, not here.
template <typename T>
class Task;

namespace detail {

/// Storage + continuation plumbing shared by the void and non-void promise specializations.
struct PromiseBase {
  /// The coroutine to resume when this Task's body finishes (set by the Awaiter on co_await).
  /// Left null when the Task is driven directly (e.g. by SyncWait) rather than awaited.
  std::coroutine_handle<> continuation_{nullptr};
  std::exception_ptr exception_{nullptr};

  static std::suspend_always initial_suspend() noexcept { return {}; }

  void unhandled_exception() noexcept { exception_ = std::current_exception(); }

  /// Symmetric-transfer final awaiter: resumes the continuation (if any) in place of returning to
  /// the caller of resume(), or hands control to a no-op coroutine when nobody is waiting.
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
  // Constructed lazily: engaged by return_value, read (and moved out) by the Awaiter/SyncWait.
  // Storing it directly on the frame (not behind a pointer) keeps the result colocated with the
  // rest of the promise -- no extra heap allocation beyond the coroutine frame itself.
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

  /// Awaiter binding this Task's completion to the awaiting coroutine's continuation. Starts the
  /// (still-suspended) lazy body via symmetric transfer on the first co_await.
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

  /// Drive this (not-yet-started) Task to completion on the CURRENT thread, without an enclosing
  /// coroutine, and return its result (or rethrow its exception). For use by non-coroutine callers
  /// (tests, sync entry points). See free function `SyncWait` below for the ergonomic form.
  T Run() {
    DMG_ASSERT(handle_, "Run() on an empty/moved-from Task.");
    handle_.resume();
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

/// Run a lazy Task to completion on the current thread and return its value (or rethrow its
/// exception). `task` must not have been co_await'd/Run() already (a fresh, not-yet-started Task).
template <typename T>
T SyncWait(Task<T> &&task) {
  return task.Run();
}

}  // namespace memgraph::utils
