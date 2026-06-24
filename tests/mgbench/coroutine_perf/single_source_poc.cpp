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

// PoC: ONE source-of-truth body -> BOTH a sync bool Pull() AND a coroutine, via control hooks.
// Business logic written exactly ONCE (PT_BODY); only PULL/EMIT/DONE differ per instantiation.
#include <coroutine>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <optional>

struct Gen {
  struct promise_type {
    bool has_more_{false};
    std::coroutine_handle<> parent_{std::noop_coroutine()};

    Gen get_return_object() { return Gen{std::coroutine_handle<promise_type>::from_promise(*this)}; }

    std::suspend_always initial_suspend() noexcept { return {}; }

    struct ST {
      std::coroutine_handle<> c;

      bool await_ready() const noexcept { return false; }

      std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept { return c; }

      void await_resume() const noexcept {}
    };

    auto final_suspend() noexcept { return ST{parent_}; }

    void return_value(bool h) noexcept { has_more_ = h; }

    auto yield_value(bool h) noexcept {
      has_more_ = h;
      return ST{parent_};
    }

    void unhandled_exception() { std::abort(); }
  };

  std::coroutine_handle<promise_type> h_{};

  struct Aw {
    std::coroutine_handle<promise_type> h_;

    bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> p) const noexcept {
      h_.promise().parent_ = p;
      return h_;
    }

    bool await_resume() const noexcept { return h_.promise().has_more_; }
  };

  Aw resume() { return Aw{h_}; }

  Gen() = default;

  explicit Gen(std::coroutine_handle<promise_type> h) : h_(h) {}

  Gen(Gen &&o) noexcept : h_(o.h_) { o.h_ = {}; }

  Gen &operator=(Gen &&o) noexcept {
    if (h_) h_.destroy();
    h_ = o.h_;
    o.h_ = {};
    return *this;
  }

  ~Gen() {
    if (h_) h_.destroy();
  }
};

struct Base {
  uint64_t out_{0};
};

// ============ SINGLE SOURCE OF TRUTH: keep-even filter logic, written ONCE ============
//   PULL_EXPR : expression yielding bool "got a row" (advances the child, fills in_->out_)
//   EMIT      : statement — a row is ready on out_
//   DONE      : statement — input exhausted
#define FILTER_BODY(PULL_EXPR, EMIT, DONE) \
  for (;;) {                               \
    bool ok = (PULL_EXPR);                 \
    if (!ok) {                             \
      DONE;                                \
    }                                      \
    if (in_->out_ % 2 == 0) {              \
      out_ = in_->out_;                    \
      EMIT;                                \
    }                                      \
  }

// =====================================================================================

struct SyncSource : Base {
  uint64_t i = 0, N;

  SyncSource(uint64_t n) : N(n) {}

  bool Pull() {
    if (i >= N) return false;
    out_ = i++;
    return true;
  }
};

struct CoSource : Base {
  uint64_t N;
  std::optional<Gen> gen_;

  CoSource(uint64_t n) : N(n) {}

  Gen Body() {
    for (uint64_t k = 0; k < N; k++) {
      out_ = k;
      co_yield true;
    }
    co_return false;
  }

  Gen::Aw pull() {
    if (!gen_) gen_ = Body();
    return gen_->resume();
  }
};

// SYNC instantiation
struct SyncFilter : Base {
  SyncSource *in_;

  SyncFilter(SyncSource *i) : in_(i) {}

  bool Pull() { FILTER_BODY(in_->Pull(), return true, return false) }
};

// COROUTINE instantiation — SAME FILTER_BODY text
struct CoFilter : Base {
  CoSource *in_;
  std::optional<Gen> gen_;

  CoFilter(CoSource *i) : in_(i) {}

  Gen Body(){FILTER_BODY(co_await in_ -> pull(), co_yield true, co_return false)} Gen::Aw resume() {
    if (!gen_) gen_ = Body();
    return gen_->resume();
  }
};

int main() {
  // drive sync
  SyncSource ss(20);
  SyncFilter sf(&ss);
  uint64_t sync_sum = 0, sync_n = 0;
  while (sf.Pull()) {
    sync_sum += sf.out_;
    ++sync_n;
  }
  // drive coroutine
  CoSource cs(20);
  CoFilter cf(&cs);
  uint64_t co_sum = 0, co_n = 0;
  for (;;) {
    auto a = cf.resume();
    if (cf.gen_->h_.done()) break;
    cf.gen_->h_.resume();
    if (!cf.gen_->h_.promise().has_more_) break;
    co_sum += cf.out_;
    ++co_n;
  }
  printf("sync : n=%llu sum=%llu\n", (unsigned long long)sync_n, (unsigned long long)sync_sum);
  printf("coro : n=%llu sum=%llu\n", (unsigned long long)co_n, (unsigned long long)co_sum);
  printf("%s\n",
         (sync_n == co_n && sync_sum == co_sum) ? "IDENTICAL -> single source produced equivalent sync + coroutine"
                                                : "MISMATCH");
  return 0;
}
