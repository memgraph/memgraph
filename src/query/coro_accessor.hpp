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

#include <algorithm>
#include <chrono>
#include <coroutine>
#include <functional>
#include <memory>
#include <optional>

#include "flags/run_time_configurable.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "utils/coro_task.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/park_state.hpp"
#include "utils/priority_thread_pool.hpp"

namespace memgraph::query {

namespace detail {

/// Awaitable behind AcquireAccessorCoro's park; one instance per park attempt. `await_suspend`
/// does the full single-owner arbitration: build a `ParkState` whose `on_resume` reschedules this
/// handle onto its worker; `RegisterWaiter` under the captured epoch (register-before-recheck);
/// register the deadline backstop; then re-probe once more (closes the register/release race). If
/// the re-probe acquires, take the abandon path -- proceed synchronously only if we win ClaimPark,
/// else release and genuinely suspend (a wake source is already driving our resume).
struct AcquireAwaitable {
  storage::InMemoryStorage &storage;
  utils::PriorityThreadPool &pool;
  storage::StorageAccessType rw_type;
  std::optional<storage::IsolationLevel> resolved_iso;
  storage::PendingHandle &pending;
  std::chrono::steady_clock::time_point deadline;
  size_t worker_id;
  uint64_t epoch;
  // Set iff the abandon-path re-probe won the accessor while also winning ClaimPark, without ever
  // suspending; AcquireAccessorCoro checks it right after co_await returns.
  std::optional<std::unique_ptr<storage::Accessor>> *abandon_result;
  // Opaque session-layer hook invoked right after a genuine park's handle resumes (never on the
  // synchronous fast path, where this closure is never built). Typically keeps the session alive
  // across the park and re-drives the connection loop once the Task chain completes. Empty for
  // callers that never park (tests, SyncWait); the empty case is guarded below.
  std::function<void()> on_park_resumed;

  static bool await_ready() noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> h) {
    auto ps = std::make_shared<utils::ParkState>();
    ps->worker_id = worker_id;
    ps->deadline = deadline;
    auto *pool_ptr = &pool;
    // resumed_cb lives inside ps for the whole park, keeping any session-lifetime shared_ptr it
    // captures alive across the entire park, not just while on_resume runs.
    auto resumed_cb = on_park_resumed;
    ps->on_resume = [pool_ptr, wid = worker_id, h, resumed_cb] {
      pool_ptr->RescheduleTaskOnWorker(wid, [h, resumed_cb] {
        h.resume();
        // Runs after h.resume() returns, outside the coroutine frame's execution -- safe to touch
        // caller-owned state that the frame itself couldn't (e.g. destroying the owning Task).
        if (resumed_cb) resumed_cb();
      });
    };

    auto &event = storage.main_lock_resume_event();
    if (!event.RegisterWaiter(ps, epoch)) {
      // Epoch moved since the probe: ps never entered any registry, so drop it and re-probe.
      return false;
    }

    auto &registry = pool.park_registry();
    registry.Register(ps);

    // Re-probe once more before committing to a park -- closes the race between the caller's
    // failed probe and the registration completing above.
    auto acc = storage.TryAccessWithPending(rw_type, resolved_iso, pending);
    if (acc) {
      if (utils::ClaimPark(*ps)) {
        // Won: continue synchronously with the accessor; best-effort cleanup of the registries.
        event.RemoveWaiter(ps);
        registry.Deregister(ps);
        *abandon_result = std::move(acc);
        return false;
      }
      // Lost: a wake source already claimed ps and will resume us. Release the redundant accessor;
      // the resume path re-acquires.
      acc.reset();
      return true;
    }

    // Shutdown-race closer: ShutDown() sets IsShuttingDown() (one-way flag) BEFORE its one-shot
    // Drain(), so a ps registered after that Drain would otherwise sit forever (monitor stopped, no
    // more releases). Self-claim here instead of trusting a wake that never comes; returning false
    // lets AcquireAccessorCoro's own post-co_await shutdown check throw the same clean bail.
    if (pool.IsShuttingDown()) {
      if (utils::ClaimPark(*ps)) {
        event.RemoveWaiter(ps);
        registry.Deregister(ps);
        return false;
      }
      // Lost: a real Drain/NotifyAll/Sweep already claimed ps and will resume us (and re-check
      // shutdown itself).
      return true;
    }

    return true;  // Genuinely parked; a wake source will resume us.
  }

  static void await_resume() noexcept {}
};

}  // namespace detail

/// The acquire coroutine at the heart of parkable Prepare. Resolves a storage accessor for `rw`,
/// parking the current pool worker (instead of blocking it) while a contended UNIQUE/READ_ONLY
/// acquisition is retried -- only when parking is possible (InMemory) and enabled (LOW priority,
/// flag on); otherwise it is a byte-identical blocking acquire.
///
/// Layering: resuming the parked handle propagates up the `Task<>` chain via symmetric transfer.
/// The session-aware "re-drive after the Task completes" concern is layered in via the opaque
/// `on_park_resumed` hook, supplied by the Bolt session layer -- this coroutine stays self-
/// contained and testable without it (tests/unit/coro_accessor.cpp never passes one).
///
/// @param resolved_iso     Isolation override, resolved once by the caller before this starts.
/// @param deadline         Absolute deadline; re-checked each iteration so a parked campaign still
///                         honors the ~1s `*AccessTimeout` contract.
/// @param pool             Pool this runs on -- discovers the current worker and pins the resume.
/// @param is_high_priority HIGH-priority queries never park; they take the blocking path.
/// @param on_park_resumed  Session hook threaded into every ParkState a genuine park builds; empty
///                         for callers that never park (tests, SyncWait).
inline utils::Task<std::unique_ptr<storage::Accessor>> AcquireAccessorCoro(
    storage::Storage &storage, storage::StorageAccessType rw, std::optional<storage::IsolationLevel> resolved_iso,
    std::chrono::steady_clock::time_point deadline, utils::PriorityThreadPool &pool, bool is_high_priority,
    std::function<void()> on_park_resumed = {}) {
  auto blocking_access = [&]() -> std::unique_ptr<storage::Accessor> {
    const auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::max(deadline - std::chrono::steady_clock::now(), std::chrono::steady_clock::duration::zero()));
    if (rw == storage::UNIQUE) return storage.UniqueAccess(resolved_iso, timeout);
    if (rw == storage::READ_ONLY) return storage.ReadOnlyAccess(resolved_iso, timeout);
    return storage.Access(rw, resolved_iso, timeout);
  };

  // HIGH priority / DiskStorage / flag-off all fall back to the ordinary blocking acquire,
  // never constructing a ParkState.
  if (!storage.SupportsParkAcquire() || is_high_priority || !flags::run_time::CoroPrepareAccessorYieldEnabled()) {
    co_return blocking_access();
  }

  // SupportsParkAcquire() is overridden only by InMemoryStorage, so this downcast is safe.
  auto &mem_storage = static_cast<storage::InMemoryStorage &>(storage);
  // Built once and held across every suspend/resume so UNIQUE/READ_ONLY's writer-preference stays
  // registered for the whole retry campaign, not just a single probe.
  auto pending = mem_storage.MakePendingHandle(rw);

  for (;;) {
    // Capture BEFORE the probe (lost-wakeup guard).
    const auto epoch = mem_storage.main_lock_resume_event().Epoch();

    if (auto acc = mem_storage.TryAccessWithPending(rw, resolved_iso, pending)) {
      co_return std::move(*acc);
    }

    if (std::chrono::steady_clock::now() >= deadline) {
      // Same exceptions the blocking path throws -- timeout semantics unchanged by parking.
      if (rw == storage::UNIQUE) throw storage::UniqueAccessTimeout{};
      if (rw == storage::READ_ONLY) throw storage::ReadOnlyAccessTimeout{};
      throw storage::SharedAccessTimeout{};
    }

    const auto worker_id = utils::GetCurrentWorkerId();
    DMG_ASSERT(worker_id.has_value(),
               "AcquireAccessorCoro's park path must run on a pool (LP) worker (GetCurrentWorkerId() is "
               "only published there) -- the caller must schedule this coroutine onto the pool before "
               "driving it, exactly like any other LOW-priority pool task.");

    std::optional<std::unique_ptr<storage::Accessor>> abandon_result;
    co_await detail::AcquireAwaitable{
        mem_storage, pool, rw, resolved_iso, pending, deadline, *worker_id, epoch, &abandon_result, on_park_resumed};

    if (abandon_result) {
      // Abandon-path win: the awaitable's re-probe already acquired without suspending.
      co_return std::move(*abandon_result);
    }

    // Genuinely resumed. Bail before touching storage state if tearing down (a shutdown drain's
    // on_resume may run on the draining thread, not a pool worker).
    if (pool.IsShuttingDown()) {
      throw utils::BasicException("AcquireAccessorCoro: pool is shutting down, abandoning parked accessor acquire");
    }
    // Loop back and re-probe: resume means "something changed, retry", not "acquired".
  }
}

}  // namespace memgraph::query
