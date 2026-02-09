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
#include <optional>

#include "query/plan/cursor_awaitable_core.hpp"

#include "spdlog/spdlog.h"

#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "utils/counter.hpp"

namespace memgraph::query::plan {

/// Call at the start of the root pull coroutine so yield can store the root handle for the scheduler.
class StoreRootHandleAwaitable {
 public:
  struct Awaiter {
    ExecutionContext *ctx_{nullptr};

    bool await_ready() const noexcept { return !ctx_ || !ctx_->suspended_task_handle_ptr; }

    void await_suspend(std::coroutine_handle<> h) const {
      if (ctx_) ctx_->root_task_handle = h;
    }

    void await_resume() const noexcept {}
  };

  explicit StoreRootHandleAwaitable(ExecutionContext &ctx) noexcept : ctx_(&ctx) {}

  auto operator co_await() const noexcept { return Awaiter{ctx_}; }

 private:
  ExecutionContext *ctx_;
};

inline StoreRootHandleAwaitable StoreRootHandle(ExecutionContext &ctx) noexcept {
  return StoreRootHandleAwaitable(ctx);
}

/// Throttled check point: abort → throw, yield → suspend and return control to scheduler, else continue.
/// Use at the same call sites as AbortCheck. Pass the same thread-local counter used for throttling.
class YieldPointAwaitable {
 public:
  struct Awaiter {
    ExecutionContext *ctx_{nullptr};
    utils::ResettableCounter *maybe_check_{nullptr};

    bool await_ready() {
      if (!ctx_ || !maybe_check_) return true;
      auto result = ctx_->stopping_context.CheckAbortOrYield(*maybe_check_);
      if (result.action == StopOrYieldResult::Action::Abort) {
        spdlog::info("[yield] YieldPoint: abort (reason={})", static_cast<int>(result.abort_reason));
        throw HintedAbortError(result.abort_reason);
      }
      if (result.action == StopOrYieldResult::Action::Yield) {
        spdlog::info("[yield] YieldPoint: scheduler requested yield -> will suspend");
        return false;
      }
      // Continue path: optional test hook to force a suspend after N yield-point hits
      if (ctx_->test_yield_after_yield_point_count) {
        auto &n = *ctx_->test_yield_after_yield_point_count;
        --n;
        if (n == 0) {
          spdlog::info("[yield] YieldPoint: test yield (count reached 0) -> will suspend");
          return false;
        }
      }
      return true;
    }

    void await_suspend(std::coroutine_handle<> h) const {
      if (ctx_->suspended_task_handle_ptr) {
        *ctx_->suspended_task_handle_ptr = h;  // store the handle that suspended (any level)
        spdlog::info("[yield] YieldPoint: suspended, handle stored h={}", reinterpret_cast<uintptr_t>(h.address()));
      }
    }

    void await_resume() const noexcept {}
  };

  YieldPointAwaitable(ExecutionContext &ctx, utils::ResettableCounter &maybe_check) noexcept
      : ctx_(&ctx), maybe_check_(&maybe_check) {}

  auto operator co_await() { return Awaiter{ctx_, maybe_check_}; }

 private:
  ExecutionContext *ctx_;
  utils::ResettableCounter *maybe_check_;
};

/// Run the given pull coroutine to completion or until it yields.
/// Caller must set ctx.suspended_task_handle_ptr to the address of a handle variable before calling.
/// Returns HasRow/Done/Yielded. If Yielded, *ctx.suspended_task_handle_ptr is set; resume it later to continue.
PullRunResult RunPullToCompletion(PullAwaitable &awaitable, ExecutionContext &ctx);

/// Run until HasRow or Done; on Yielded, resume the stored handle and repeat. For sync Pull() wrapper.
bool RunPullUntilCompletion(PullAwaitable &awaitable, ExecutionContext &ctx);

/// Run until HasRow or Done; on Yielded, return to caller without resuming (scheduler can resume later).
/// Caller must set ctx.suspended_task_handle_ptr to a persistent storage location before calling.
/// @param resume_first If set, resume this handle first then run the loop (for resuming after a prior yield).
/// @return true = has row, false = done, nullopt = yielded (handle stored in *ctx.suspended_task_handle_ptr).
std::optional<bool> RunPullUntilCompletionOrYield(PullAwaitable &awaitable, ExecutionContext &ctx,
                                                  std::optional<std::coroutine_handle<>> resume_first = {});

}  // namespace memgraph::query::plan
