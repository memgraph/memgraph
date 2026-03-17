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

#include "query/plan/cursor_awaitable.hpp"

#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

void Cursor::Reset() { gen_.reset(); }

PullRunResult RunPullToCompletion(PullAwaitable &awaitable, ExecutionContext &ctx) {
  // 1. If we are resuming after a yield, the suspended handle is in the context.
  //    We must resume that handle (the inner coroutine that yielded), not the root.
  std::coroutine_handle<> resume_from{};
  if (ctx.suspended_task_handle_ptr && *ctx.suspended_task_handle_ptr) {
    resume_from = std::exchange(*ctx.suspended_task_handle_ptr, {});
  } else if (ctx.suspended_task_handle_ptr) {
    // TODO: do we need this?
    *ctx.suspended_task_handle_ptr = {};
  }

  decltype(awaitable.GetHandle()) handle;

  if (resume_from) {
    if (!resume_from.done()) {
      resume_from.resume();
    }
    if (ctx.suspended_task_handle_ptr && *ctx.suspended_task_handle_ptr) {
      return PullRunResult::Yielded();
    }
    if (awaitable.Done()) {
      awaitable.RethrowIfException();
      return awaitable.Result() ? PullRunResult::Row() : PullRunResult::Done();
    }
    handle = awaitable.GetHandle();
  } else {
    handle = awaitable.GetHandle();
    if (!handle || handle.done()) {
      awaitable.RethrowIfException();
      return awaitable.Result() ? PullRunResult::Row() : PullRunResult::Done();
    }
  }

  // 2. Execution loop (run until root is done or we yield again)
  while (!handle.done()) {
    handle.resume();
    if (ctx.suspended_task_handle_ptr && *ctx.suspended_task_handle_ptr) {
      return PullRunResult::Yielded();
    }
  }

  awaitable.RethrowIfException();
  return awaitable.Result() ? PullRunResult::Row() : PullRunResult::Done();
}
PullRunResult RunPullToCompletion(PullAwaitable::ResumeAwaitable &ra, ExecutionContext &ctx) {
  if (ra.Done()) return PullRunResult::Done();

  // Determined which handle to resume: either the scheduler-yielded inner handle,
  // or the root generator itself.
  std::coroutine_handle<> resume_target;
  if (ctx.suspended_task_handle_ptr && *ctx.suspended_task_handle_ptr) {
    resume_target = std::exchange(*ctx.suspended_task_handle_ptr, {});
  } else {
    resume_target = ra.GetHandle();
  }

  if (resume_target && !resume_target.done()) {
    resume_target.resume();
  }

  if (ctx.suspended_task_handle_ptr && *ctx.suspended_task_handle_ptr) {
    return PullRunResult::Yielded();
  }

  ra.RethrowIfException();
  return ra.Result() ? PullRunResult::Row() : PullRunResult::Done();
}

}  // namespace memgraph::query::plan
