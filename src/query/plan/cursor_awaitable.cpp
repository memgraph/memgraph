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

#include <thread>

#include "spdlog/spdlog.h"

namespace memgraph::query::plan {

namespace {
uintptr_t Tid() { return static_cast<uintptr_t>(std::hash<std::thread::id>{}(std::this_thread::get_id())); }

uintptr_t H(void *p) { return reinterpret_cast<uintptr_t>(p); }

const char *StatusStr(PullRunResult::Status s) {
  if (s == PullRunResult::Status::HasRow) return "HasRow";
  if (s == PullRunResult::Status::Done) return "Done";
  return "Yielded";
}
}  // namespace

PullRunResult RunPullToCompletion(PullAwaitable &awaitable, ExecutionContext &ctx) {
  if (ctx.suspended_task_handle_ptr) *ctx.suspended_task_handle_ptr = std::coroutine_handle<>{};

  auto handle = awaitable.GetHandle();
  spdlog::info(
      "[yield] RunPullToCompletion tid={} handle={} done={}", Tid(), H(handle.address()), handle ? handle.done() : -1);
  if (!handle) return awaitable.Result() ? PullRunResult::Row() : PullRunResult::Done();
  // Already completed (e.g. we're being called again after a prior run). No more rows.
  if (handle.done()) return PullRunResult::Done();

  while (!handle.done()) {
    spdlog::info("[yield] RunPullToCompletion tid={} resuming handle={}", Tid(), H(handle.address()));
    handle.resume();
    // Chain may have completed (we returned from resume) or yielded
    if (handle.done()) break;
    if (ctx.suspended_task_handle_ptr && *ctx.suspended_task_handle_ptr) {
      spdlog::info("[yield] RunPullToCompletion tid={} task yielded stored_handle={}",
                   Tid(),
                   H(ctx.suspended_task_handle_ptr->address()));
      return PullRunResult::Yielded();
    }
  }

  bool has_row = awaitable.Result();
  spdlog::info("[yield] RunPullToCompletion tid={} returning {} (has_row={}) handle={}",
               Tid(),
               has_row ? "HasRow" : "Done",
               has_row,
               H(handle.address()));
  return has_row ? PullRunResult::Row() : PullRunResult::Done();
}

bool RunPullUntilCompletion(PullAwaitable &awaitable, ExecutionContext &ctx) {
  std::coroutine_handle<> stored_handle;
  std::coroutine_handle<> *const saved_ptr = ctx.suspended_task_handle_ptr;
  ctx.suspended_task_handle_ptr = &stored_handle;

  const auto restore_ctx = [&ctx, saved_ptr]() { ctx.suspended_task_handle_ptr = saved_ptr; };

  int loop_count = 0;
  for (;;) {
    spdlog::info("[yield] RunPullUntilCompletion tid={} loop={} entry", Tid(), loop_count);
    auto r = RunPullToCompletion(awaitable, ctx);
    spdlog::info("[yield] RunPullUntilCompletion tid={} loop={} r={}", Tid(), loop_count, StatusStr(r.status));
    if (r.status == PullRunResult::Status::HasRow) {
      restore_ctx();
      spdlog::info("[yield] RunPullUntilCompletion tid={} return true (HasRow)", Tid());
      return true;
    }
    if (r.status == PullRunResult::Status::Done) {
      restore_ctx();
      spdlog::info("[yield] RunPullUntilCompletion tid={} return false (Done)", Tid());
      return false;
    }
    if (stored_handle) {
      spdlog::info("[yield] RunPullUntilCompletion tid={} resuming yielded task stored_handle={}",
                   Tid(),
                   H(stored_handle.address()));
      auto h = stored_handle;
      stored_handle = std::coroutine_handle<>{};
      h.resume();
      spdlog::info("[yield] RunPullUntilCompletion tid={} resume() returned", Tid());
      // Chain may have completed (root done with a row). RunPullToCompletion would see
      // handle.done() and return Done, losing the row; return the root's result directly.
      if (awaitable.Done()) {
        restore_ctx();
        bool has_row = awaitable.Result();
        spdlog::info("[yield] RunPullUntilCompletion tid={} return {} (chain completed after resume)", Tid(), has_row);
        return has_row;
      }
      auto r2 = RunPullToCompletion(awaitable, ctx);
      spdlog::info("[yield] RunPullUntilCompletion tid={} after drive r2={}", Tid(), StatusStr(r2.status));
      if (r2.status == PullRunResult::Status::HasRow) {
        restore_ctx();
        spdlog::info("[yield] RunPullUntilCompletion tid={} return true (r2 HasRow)", Tid());
        return true;
      }
      if (r2.status == PullRunResult::Status::Done) {
        restore_ctx();
        spdlog::info("[yield] RunPullUntilCompletion tid={} return false (r2 Done)", Tid());
        return false;
      }
      // Yielded again
      stored_handle = *ctx.suspended_task_handle_ptr;
      *ctx.suspended_task_handle_ptr = std::coroutine_handle<>{};
      spdlog::info("[yield] RunPullUntilCompletion tid={} yielded again new stored_handle={}",
                   Tid(),
                   H(stored_handle.address()));
    } else {
      spdlog::info("[yield] RunPullUntilCompletion tid={} Yielded but stored_handle empty", Tid());
    }
    ++loop_count;
  }
}

std::optional<bool> RunPullUntilCompletionOrYield(PullAwaitable &awaitable, ExecutionContext &ctx,
                                                  std::optional<std::coroutine_handle<>> resume_first) {
  std::coroutine_handle<> stored_handle;
  std::coroutine_handle<> *const saved_ptr = ctx.suspended_task_handle_ptr;
  ctx.suspended_task_handle_ptr = &stored_handle;
  const auto restore_ctx = [&ctx, saved_ptr]() { ctx.suspended_task_handle_ptr = saved_ptr; };

  auto copy_yielded_to_caller_and_return_nullopt = [&]() {
    if (saved_ptr) *saved_ptr = stored_handle;
    restore_ctx();
    return std::optional<bool>(std::nullopt);
  };

  if (resume_first && *resume_first) {
    spdlog::info("[yield] RunPullUntilCompletionOrYield resuming handle={}", H(resume_first->address()));
    resume_first->resume();
    // Chain may have completed (root done with a row). RunPullToCompletion would see handle.done()
    // and return Done, losing the row; so return the root's result directly when it just completed.
    if (awaitable.Done()) {
      restore_ctx();
      return awaitable.Result();
    }
    auto r = RunPullToCompletion(awaitable, ctx);
    if (r.status == PullRunResult::Status::HasRow) {
      restore_ctx();
      return true;
    }
    if (r.status == PullRunResult::Status::Done) {
      restore_ctx();
      return false;
    }
    return copy_yielded_to_caller_and_return_nullopt();
  }

  for (;;) {
    auto r = RunPullToCompletion(awaitable, ctx);
    if (r.status == PullRunResult::Status::HasRow) {
      restore_ctx();
      return true;
    }
    if (r.status == PullRunResult::Status::Done) {
      restore_ctx();
      return false;
    }
    return copy_yielded_to_caller_and_return_nullopt();
  }
}

}  // namespace memgraph::query::plan
