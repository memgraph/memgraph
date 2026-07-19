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

/// Awaitable behind AcquireAccessorCoro's park (IP-1 design doc REVISION 3 §R3.2 / REVISION 4
/// §R4.3, opencode-work/resource-lock-starvation/coro-prepare/ip1-design.md). One instance is
/// built PER park attempt (i.e. per failed TryAccessWithPending() probe inside the acquire loop)
/// -- see AcquireAccessorCoro below for the surrounding loop.
///
/// `await_suspend` implements the FULL single-owner arbitration, not just "register and suspend":
///  1. Builds a `shared_ptr<ParkState>` whose `on_resume` reschedules THIS coroutine handle back
///     onto the CURRENT pool worker (`RescheduleTaskOnWorker`) -- resuming the handle propagates
///     up the `Task<>` chain via symmetric transfer (utils/coro_task.hpp), so nothing here needs
///     to know about session/Bolt-layer re-drive (that is a later, top-level concern -- see the
///     doc comment on AcquireAccessorCoro).
///  2. `WorkerResumeEvent::RegisterWaiter` under the captured `epoch` (B1's register-before-
///     recheck protocol). A `false` return means the epoch already moved between the caller's
///     probe and here -- `ps` never reached any registry, so there is nobody to race a claim
///     against; resume synchronously (return false, no suspend) so the acquire loop re-probes.
///  3. On successful registration, ALSO registers with the pool's deadline registry (the B2
///     timeout backstop) and re-probes the actual resource ONE more time (R1 §B1 step 4). If that
///     re-probe acquires the accessor, this is the abandon path (R4.3): only proceed
///     synchronously if this call WINS `ClaimPark` on its own `ParkState` (guarding against a
///     concurrent NotifyAll/sweep that may already be driving a resume for the very same `ps`).
///     On a LOSS, the freshly (redundantly) acquired accessor is released again -- "correct even
///     though the lock may be free, the resume just acquires it" (R4.3) -- and this call
///     genuinely suspends.
struct AcquireAwaitable {
  storage::InMemoryStorage &storage;
  utils::PriorityThreadPool &pool;
  storage::StorageAccessType rw_type;
  std::optional<storage::IsolationLevel> resolved_iso;
  storage::PendingHandle &pending;
  std::chrono::steady_clock::time_point deadline;
  size_t worker_id;
  uint64_t epoch;
  // Filled in iff the abandon-path re-probe (R4.3) itself won the accessor while ALSO winning
  // ClaimPark -- AcquireAccessorCoro checks this immediately after co_await returns, without this
  // call ever having actually suspended.
  std::optional<std::unique_ptr<storage::Accessor>> *abandon_result;
  // Session-surgery Stage B (IP-1 design doc REVISION 4 §R4.1/R4.2): opaque hook invoked by the
  // pinned reschedule closure right AFTER it resumes the parked handle -- never for the synchronous
  // (never-parked) fast path, since in that case this closure is never constructed at all. This
  // struct/AcquireAccessorCoro deliberately know nothing about what it does (typically: keep the
  // owning session alive for the whole park via a captured shared_ptr, and -- once the WHOLE Task
  // chain up to the caller's own top-level driver is done -- re-drive that caller's connection loop).
  // Empty/default-constructed std::function for any caller that never expects a park (tests, a plain
  // SyncWait) -- invoking an empty std::function is guarded below, never attempted.
  std::function<void()> on_park_resumed;

  static bool await_ready() noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> h) {
    auto ps = std::make_shared<utils::ParkState>();
    ps->worker_id = worker_id;
    ps->deadline = deadline;
    auto *pool_ptr = &pool;
    // resumed_cb is copied into BOTH the outer (on_resume) and inner (posted) closures, both of
    // which live inside ps (heap-allocated, kept alive by the registries -- see park_state.hpp) for
    // the WHOLE park, not just the invocation instant: this is what keeps a session-lifetime
    // shared_ptr captured inside resumed_cb alive across the entire park (R4.1's lifetime
    // requirement), not merely during the moment on_resume happens to run.
    auto resumed_cb = on_park_resumed;
    ps->on_resume = [pool_ptr, wid = worker_id, h, resumed_cb] {
      pool_ptr->RescheduleTaskOnWorker(wid, [h, resumed_cb] {
        h.resume();
        // Runs strictly AFTER h.resume() returns -- i.e. outside any coroutine frame's own
        // execution (the frame, if it fully completed, is merely sitting at its final_suspend by
        // now; if it re-parked instead, resumed_cb's caller-supplied logic is expected to notice
        // "not done yet" and no-op). Safe to inspect/clear caller-owned state here that would be
        // unsafe to touch from inside the coroutine body itself (e.g. destroying the very Task that
        // owns this frame).
        if (resumed_cb) resumed_cb();
      });
    };

    auto &event = storage.main_lock_resume_event();
    if (!event.RegisterWaiter(ps, epoch)) {
      // Epoch already moved (B1 waiter step 3, "register" rejected): `ps` never entered any
      // registry, so nobody can ever claim/resume it -- just drop it and re-probe on the next
      // loop iteration. No claim contest needed.
      return false;
    }

    auto &registry = pool.park_registry();
    registry.Register(ps);

    // B1 step 4 / R4.3: re-probe the real resource once more before committing to a genuine
    // park -- closes the race between the caller's original failed probe and the moment
    // registration above completed.
    auto acc = storage.TryAccessWithPending(rw_type, resolved_iso, pending);
    if (acc) {
      if (utils::ClaimPark(*ps)) {
        // Won: no wake source will ever invoke on_resume for this ps -- best-effort cleanup,
        // then continue synchronously with the accessor already in hand.
        event.RemoveWaiter(ps);
        registry.Deregister(ps);
        *abandon_result = std::move(acc);
        return false;
      }
      // Lost: some wake source already claimed ps and WILL resume us -- treat as a genuine
      // park. Release the accessor we just (redundantly) acquired; the resume path re-probes
      // and re-acquires (correct even though the lock is free right now, R4.3).
      acc.reset();
      return true;
    }

    // Shutdown-race closer (adversarial-review finding, post R4.4): PriorityThreadPool::ShutDown()
    // drains `park_registry_` exactly ONCE before stopping the monitor/workers -- a `ps` that
    // finishes registering (above) strictly AFTER that one-shot Drain() already ran would otherwise
    // sit registered forever with nothing left to ever sweep/notify it (the monitor that would run
    // Sweep() is stopped, and no more releases may occur if storage is tearing down in lockstep).
    // `pool_stop_source_::stop_requested()` is a one-way, permanently-true-once-set flag, and
    // ShutDown() sets it as the very FIRST action -- strictly before its Drain() call -- so any `ps`
    // that reaches THIS check after shutdown began (whether or not it made it into that one Drain()
    // snapshot) observes IsShuttingDown() == true here and self-claims exactly like the abandon path
    // above, instead of trusting an external wake that might never come. On a win, do NOT set
    // `abandon_result` (no accessor to hand back) -- returning false makes AcquireAccessorCoro's own
    // post-co_await `if (pool.IsShuttingDown()) throw` (already present) fire immediately, the same
    // clean bail a genuine cross-thread shutdown-drain resume would produce.
    if (pool.IsShuttingDown()) {
      if (utils::ClaimPark(*ps)) {
        event.RemoveWaiter(ps);
        registry.Deregister(ps);
        return false;
      }
      // Lost: a real Drain()/NotifyAll()/Sweep() already claimed ps and WILL resume us -- its own
      // on_resume path re-checks IsShuttingDown() and bails cleanly, exactly like above.
      return true;
    }

    return true;  // Still blocked: genuinely parked, a wake source will resume us.
  }

  static void await_resume() noexcept {}
};

}  // namespace detail

/// The acquire coroutine at the heart of parkable Prepare (IP-1 design doc REVISION 3 §R3.2,
/// REVISION 4 §R4.3, opencode-work/resource-lock-starvation/coro-prepare/ip1-design.md). Resolves
/// a storage accessor for `rw`, parking the CURRENT pool worker (instead of blocking it) while a
/// contended UNIQUE/READ_ONLY acquisition is retried, whenever parking is both possible
/// (InMemory storage) and enabled (LOW priority, flag on).
///
/// Layering note: `on_resume` here means "resume THIS suspended handle, pinned to its worker" --
/// resuming the handle propagates up the `Task<>` chain via symmetric transfer
/// (utils/coro_task.hpp). The session-aware "re-drive DoWork after the top-level Task completes"
/// concern (Session-surgery Stage B) is layered in via the opaque `on_park_resumed` hook below,
/// supplied by the Bolt session layer (communication::v2::Session::DrivePreparedRun) -- this
/// coroutine itself stays self-contained and testable without any of that (see
/// tests/unit/coro_accessor.cpp, which never passes one).
///
/// @param storage         The (per-DB) storage to acquire an accessor on.
/// @param rw               Requested access type (UNIQUE/READ_ONLY/WRITE/READ).
/// @param resolved_iso      Isolation-level override, resolved ONCE by the caller before this
///                         coroutine starts (mirrors Phase 1 in the design doc -- this coroutine
///                         never reads/resets `next_transaction_isolation_level` itself).
/// @param deadline         Absolute deadline; re-checked on every loop iteration/resume so a
///                         parked campaign still honors the ~1s `*AccessTimeout` contract.
/// @param pool              The pool this coroutine runs on -- used to discover the current
///                         worker (via `utils::GetCurrentWorkerId()`) and to pin the resume.
/// @param is_high_priority  HIGH-priority queries never park (design doc §6): they always take
///                         the ordinary blocking path below, same as flag-off/DiskStorage.
/// @param on_park_resumed   Session-surgery Stage B hook (R4.1/R4.2), threaded verbatim into every
///                         ParkState built by a genuine park attempt below -- see the doc comment
///                         on detail::AcquireAwaitable::on_park_resumed. Empty/default for callers
///                         that never expect a park (tests, a plain SyncWait).
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

  // HIGH priority never parks (§6); DiskStorage never supports parking (R4.6); flag-off must be
  // byte-identical to today's behavior (§7/C5) -- all three fall back to the ordinary blocking
  // acquire, never constructing a ParkState or registering anywhere.
  if (!storage.SupportsParkAcquire() || is_high_priority || !flags::run_time::CoroPrepareAccessorYieldEnabled()) {
    co_return blocking_access();
  }

  // storage.SupportsParkAcquire() == true implies InMemoryStorage: it is the ONLY override
  // (Storage::SupportsParkAcquire() defaults to false; DiskStorage never overrides it, R4.6) --
  // this downcast is therefore safe.
  auto &mem_storage = static_cast<storage::InMemoryStorage &>(storage);
  // Campaign-long pending scope (R4.6): built ONCE, held across every iteration of the loop below
  // -- including every suspend/resume -- so UNIQUE/READ_ONLY's writer-preference stays registered
  // for the whole retry campaign instead of just a single probe.
  auto pending = mem_storage.MakePendingHandle(rw);

  for (;;) {
    // Capture BEFORE the probe (B1's lost-wakeup guard): this is the epoch that must still hold
    // for a subsequent park (below) to be safe.
    const auto epoch = mem_storage.main_lock_resume_event().Epoch();

    if (auto acc = mem_storage.TryAccessWithPending(rw, resolved_iso, pending)) {
      co_return std::move(*acc);
    }

    if (std::chrono::steady_clock::now() >= deadline) {
      // Same exception the blocking path (`blocking_access`/CreateSharedGuard/CreateUniqueGuard)
      // throws -- observable timeout semantics are unchanged by parking.
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
      // R4.3 abandon-path win: the awaitable's own re-probe already acquired (and claimed) the
      // accessor without ever truly suspending -- nothing resumed us, we simply continued.
      co_return std::move(*abandon_result);
    }

    // Genuinely resumed -- by NotifyAll (lock release), the deadline sweep, or a shutdown drain.
    // C2: bail out cleanly BEFORE touching any further storage state if the pool is tearing down
    // (a shutdown drain's on_resume may run synchronously on the draining thread, which is not
    // necessarily a pool worker at all).
    if (pool.IsShuttingDown()) {
      throw utils::BasicException("AcquireAccessorCoro: pool is shutting down, abandoning parked accessor acquire");
    }
    // Loop back: re-probe (we may have woken spuriously, or lost a race to a third acquirer --
    // resume is a guarantee that "something changed, retry", never a guarantee of acquisition).
  }
}

}  // namespace memgraph::query
