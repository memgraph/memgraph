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

#include "query/plan/cursor_awaitable_core.hpp"

#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "utils/counter.hpp"

namespace memgraph::query::plan {

/// Throttled yield point: abort → throw, yield → suspend and store handle for scheduler, else continue.
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
        throw HintedAbortError(result.abort_reason);
      }
      if (result.action == StopOrYieldResult::Action::Yield) {
        return false;
      }
      return true;
    }

    void await_suspend(std::coroutine_handle<> h) const {
      DMG_ASSERT(ctx_->suspended_task_handle_ptr, "ctx_->suspended_task_handle_ptr is nullptr");
      *ctx_->suspended_task_handle_ptr = h;
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

/// Run the given pull awaitable to completion or until it yields.
/// Caller must set ctx.suspended_task_handle_ptr before calling.
/// Returns HasRow, Done, or Yielded. If Yielded, *ctx.suspended_task_handle_ptr holds the handle to resume.
PullRunResult RunPullToCompletion(PullAwaitable awaitable, ExecutionContext &ctx);

}  // namespace memgraph::query::plan
