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
#include <utility>

namespace memgraph::query::plan {

/// Result of running a pull to completion: has row, done, or yielded (scheduler can resume later).
struct PullRunResult {
  enum class Status : uint8_t { HasRow, Done, Yielded };
  Status status{Status::Done};
  bool has_row{false};  // meaningful when status == HasRow

  static PullRunResult Row() { return {Status::HasRow, true}; }

  static PullRunResult Done() { return {Status::Done, false}; }

  static PullRunResult Yielded() { return {Status::Yielded, false}; }
};

/// Promise type for PullAsync coroutines. Produces a single bool (has_row) on completion.
/// Uses suspend_always at final_suspend so the frame stays alive until the awaiter has read
/// the result in await_resume() (avoids use-after-free if symmetric transfer destroyed the frame).
struct PullPromise {
  struct promise_type {
    bool result_{false};
    std::coroutine_handle<> continuation_{nullptr};

    auto get_return_object() noexcept -> PullPromise;

    static constexpr auto initial_suspend() noexcept { return std::suspend_always{}; }

    auto final_suspend() noexcept {
      struct FinalSuspender {
        std::coroutine_handle<> continuation;

        bool await_ready() const noexcept { return false; }

        std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept {
          return continuation ? continuation : std::noop_coroutine();
        }

        void await_resume() const noexcept {}
      };

      return FinalSuspender{continuation_};
    }

    void return_value(bool has_row) noexcept { result_ = has_row; }

    void unhandled_exception() { throw; }
  };

  std::coroutine_handle<promise_type> handle_{};

  PullPromise() noexcept = default;

  explicit PullPromise(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {}

  PullPromise(PullPromise &&other) noexcept : handle_(other.handle_) { other.handle_ = nullptr; }

  PullPromise &operator=(PullPromise &&other) noexcept {
    if (this != &other) {
      handle_ = other.handle_;
      other.handle_ = nullptr;
    }
    return *this;
  }

  ~PullPromise() {
    if (handle_) handle_.destroy();
  }

  std::coroutine_handle<promise_type> GetHandle() const noexcept { return handle_; }

  bool Done() const noexcept { return handle_ && handle_.done(); }

  bool Result() const noexcept { return handle_ && handle_.promise().result_; }
};

inline PullPromise PullPromise::promise_type::get_return_object() noexcept {
  return PullPromise{std::coroutine_handle<promise_type>::from_promise(*this)};
}

struct PullAwaitablePromise;

/// Awaitable returned by PullAsync. When co_await'ed, runs one pull and completes with bool (has_row).
/// Can hold a coroutine (PullPromise or PullAwaitablePromise), or an immediate result (for default/sync cursor).
class PullAwaitable {
 public:
  using promise_type = PullAwaitablePromise;

  struct Awaiter {
    void *handle_ptr_{nullptr};  // PullPromise* or &awaitable_handle_
    bool immediate_ready_{false};
    bool immediate_value_{false};
    bool use_promise_{false};  // true => handle_ptr_ is PullPromise*
    /// Parent-owned result storage so we don't read child's promise after child may be invalid.
    bool result_storage_{false};

    bool await_ready() const noexcept { return immediate_ready_ || !handle_ptr_; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> parent) const noexcept;
    bool await_resume() const noexcept;
  };

  PullAwaitable() = default;

  explicit PullAwaitable(PullPromise &&p) noexcept : promise_(std::move(p)), from_promise_(true) {}

  explicit PullAwaitable(std::coroutine_handle<PullAwaitablePromise> h) noexcept
      : awaitable_handle_(h), from_awaitable_promise_(true) {}

  /// For default PullAsync: complete immediately with the given bool (no coroutine).
  explicit PullAwaitable(bool immediate_has_row) noexcept
      : immediate_ready_(true), immediate_value_(immediate_has_row) {}

  ~PullAwaitable() {
    if (from_awaitable_promise_ && awaitable_handle_) awaitable_handle_.destroy();
  }

  PullAwaitable(PullAwaitable &&other) noexcept
      : promise_(std::move(other.promise_)),
        awaitable_handle_(other.awaitable_handle_),
        immediate_ready_(other.immediate_ready_),
        immediate_value_(other.immediate_value_),
        from_promise_(other.from_promise_),
        from_awaitable_promise_(other.from_awaitable_promise_) {
    other.awaitable_handle_ = nullptr;
    other.from_awaitable_promise_ = false;
  }

  PullAwaitable &operator=(PullAwaitable &&other) noexcept {
    if (this != &other) {
      if (from_awaitable_promise_ && awaitable_handle_) awaitable_handle_.destroy();
      promise_ = std::move(other.promise_);
      awaitable_handle_ = other.awaitable_handle_;
      immediate_ready_ = other.immediate_ready_;
      immediate_value_ = other.immediate_value_;
      from_promise_ = other.from_promise_;
      from_awaitable_promise_ = other.from_awaitable_promise_;
      other.awaitable_handle_ = nullptr;
      other.from_awaitable_promise_ = false;
    }
    return *this;
  }

  auto operator co_await() & noexcept {
    Awaiter a;
    a.immediate_ready_ = immediate_ready_;
    a.immediate_value_ = immediate_value_;
    if (from_awaitable_promise_) {
      a.handle_ptr_ = &awaitable_handle_;
      a.use_promise_ = false;
    } else if (from_promise_) {
      a.handle_ptr_ = &promise_;
      a.use_promise_ = true;
    } else {
      a.handle_ptr_ = nullptr;
    }
    a.result_storage_ = false;
    return a;
  }

  auto operator co_await() && noexcept { return operator co_await(); }

  std::coroutine_handle<> GetHandle() const noexcept {
    if (from_awaitable_promise_) return std::coroutine_handle<>(awaitable_handle_);
    return promise_.GetHandle();
  }

  bool Done() const noexcept {
    if (immediate_ready_) return true;
    if (from_awaitable_promise_) return awaitable_handle_.done();
    return promise_.Done();
  }

  bool Result() const noexcept;

 private:
  PullPromise promise_;
  std::coroutine_handle<PullAwaitablePromise> awaitable_handle_{};
  bool immediate_ready_{false};
  bool immediate_value_{false};
  bool from_promise_{false};
  bool from_awaitable_promise_{false};
};

/// Promise type for coroutines that return PullAwaitable (e.g. cursor PullAsync implementations).
/// Uses suspend_always at final_suspend. When the parent awaits us, we write the result to
/// result_ptr_ (parent's storage) in return_value so the parent never reads our promise after
/// we may have been invalidated (e.g. if parent resume re-enters and resumes us at final_suspend).
struct PullAwaitablePromise {
  bool result_{false};
  bool *result_ptr_{nullptr};  // set by parent's await_suspend; we write here in return_value
  std::coroutine_handle<> continuation_{nullptr};

  PullAwaitable get_return_object() noexcept {
    return PullAwaitable{std::coroutine_handle<PullAwaitablePromise>::from_promise(*this)};
  }

  static constexpr auto initial_suspend() noexcept { return std::suspend_always{}; }

  auto final_suspend() noexcept {
    struct FinalSuspender {
      std::coroutine_handle<> continuation;

      bool await_ready() const noexcept { return false; }

      std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept {
        return continuation ? continuation : std::noop_coroutine();
      }

      void await_resume() const noexcept {}
    };

    return FinalSuspender{continuation_};
  }

  void return_value(bool has_row) noexcept {
    result_ = has_row;
    if (result_ptr_) *result_ptr_ = has_row;
  }

  void unhandled_exception() { throw; }
};

inline std::coroutine_handle<> PullAwaitable::Awaiter::await_suspend(std::coroutine_handle<> parent) const noexcept {
  if (use_promise_) {
    auto *p = static_cast<PullPromise *>(handle_ptr_);
    p->GetHandle().promise().continuation_ = parent;
    return std::coroutine_handle<>(p->GetHandle());
  }
  if (handle_ptr_) {
    auto &child_handle = *static_cast<std::coroutine_handle<PullAwaitablePromise> *>(handle_ptr_);
    child_handle.promise().continuation_ = parent;
    child_handle.promise().result_ptr_ = &const_cast<Awaiter *>(this)->result_storage_;
    return std::coroutine_handle<>(child_handle);
  }
  return std::coroutine_handle<>();
}

inline bool PullAwaitable::Result() const noexcept {
  if (immediate_ready_) return immediate_value_;
  if (from_awaitable_promise_) return awaitable_handle_.promise().result_;
  return promise_.Result();
}

inline bool PullAwaitable::Awaiter::await_resume() const noexcept {
  if (immediate_ready_) return immediate_value_;
  if (use_promise_) return static_cast<PullPromise *>(handle_ptr_)->Result();
  if (handle_ptr_) {
    // Read from parent-owned result_storage_ (child wrote it in return_value); do not read
    // child's promise in case the child was invalidated when we were resumed.
    return result_storage_;
  }
  return false;
}

/// Awaitable that completes immediately with the given bool.
class ImmediatePullAwaitable {
 public:
  struct Awaiter {
    bool value_{false};

    bool await_ready() const noexcept { return true; }

    void await_suspend(std::coroutine_handle<>) const noexcept {}

    bool await_resume() const noexcept { return value_; }
  };

  explicit ImmediatePullAwaitable(bool has_row) noexcept : value_(has_row) {}

  auto operator co_await() const noexcept { return Awaiter{value_}; }

 private:
  bool value_;
};

}  // namespace memgraph::query::plan
