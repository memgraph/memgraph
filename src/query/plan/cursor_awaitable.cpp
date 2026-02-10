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

namespace memgraph::query::plan {

PullRunResult RunPullToCompletion(PullAwaitable &awaitable, ExecutionContext &ctx) {
  // 1. Reset the suspension pointer
  if (ctx.suspended_task_handle_ptr) *ctx.suspended_task_handle_ptr = {};

  auto handle = awaitable.GetHandle();

  // 2. Immediate check for finished/null handles
  if (!handle || handle.done()) {
    awaitable.RethrowIfException();
    return awaitable.Result() ? PullRunResult::Row() : PullRunResult::Done();
  }

  // 3. Execution Loop
  while (!handle.done()) {
    handle.resume();

    // Check if the coroutine paused itself via YieldPointAwaitable
    if (ctx.suspended_task_handle_ptr && *ctx.suspended_task_handle_ptr) {
      return PullRunResult::Yielded();
    }

    // Safety: If handle.done() is true, the loop will naturally exit.
  }

  // 4. Final Result Extraction
  awaitable.RethrowIfException();
  return awaitable.Result() ? PullRunResult::Row() : PullRunResult::Done();
}

}  // namespace memgraph::query::plan
