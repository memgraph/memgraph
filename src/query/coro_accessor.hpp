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
#include "utils/on_scope_exit.hpp"
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
///  1. Builds a `shared_ptr<ParkState>` whose `on_resume` posts a resume of THIS coroutine handle
///     to any available pool worker (`PostResumeTask`) -- resuming the handle propagates up the
///     `Task<>` chain via symmetric transfer (utils/coro_task.hpp), so nothing here needs to know
///     about session/Bolt-layer re-drive (that is a later, top-level concern -- see the doc comment
///     on AcquireAccessorCoro). WHEN that resume may run is the ParkState delivery gate's job
///     (utils/park_state.hpp): not until this thread's pool task ends.
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
  std::chrono::steady_clock::time_point deadline;
  uint64_t epoch;
  // Filled in iff the abandon-path re-probe (R4.3) itself won the accessor while ALSO winning
  // ClaimPark -- AcquireAccessorCoro checks this immediately after co_await returns, without this
  // call ever having actually suspended.
  std::optional<std::unique_ptr<storage::Accessor>> *abandon_result;
  // Set unconditionally to the ParkState this attempt built, so AcquireAccessorCoro can prune it from
  // BOTH registries after a genuine resume -- see the cleanup at the bottom of the acquire loop.
  std::shared_ptr<utils::ParkState> *parked_ps;
  // Set true iff registration was refused because the wake event is CLOSED (storage tearing down), as
  // opposed to refused because the epoch moved. The former must abandon the campaign, the latter retries.
  bool *event_closed;
  // Session-surgery Stage B (IP-1 design doc REVISION 4 §R4.1/R4.2): opaque hook invoked by the
  // posted resume closure right AFTER it resumes the parked handle -- never for the synchronous
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
    ps->deadline = deadline;
    auto *pool_ptr = &pool;
    // resumed_cb is copied into BOTH the outer (on_resume) and inner (posted) closures, both of
    // which live inside ps (heap-allocated, kept alive by the registries -- see park_state.hpp) for
    // the WHOLE park, not just the invocation instant: this is what keeps a session-lifetime
    // shared_ptr captured inside resumed_cb alive across the entire park (R4.1's lifetime
    // requirement), not merely during the moment on_resume happens to run.
    auto resumed_cb = on_park_resumed;
    ps->set_on_resume([pool_ptr, h, resumed_cb] {
      // Any LP worker, never inline on the caller's thread: on_resume runs from whichever thread
      // claimed the park (a lock releaser, the deadline sweep, the shutdown drain, or this thread's
      // own arm at its task boundary), and none of those may drive a coroutine frame themselves.
      // IP-1 F6: this used to pin the resume back onto the parking worker to serialize it against
      // the parking thread; the ParkState delivery gate does that now, so the resume is free to run
      // wherever a worker is available and no longer waits out one worker's backlog.
      pool_ptr->PostResumeTask([h, resumed_cb] {
        h.resume();
        // Runs strictly AFTER h.resume() returns -- i.e. outside any coroutine frame's own
        // execution (the frame, if it fully completed, is merely sitting at its final_suspend by
        // now; if it re-parked instead, resumed_cb's caller-supplied logic is expected to notice
        // "not done yet" and no-op). Safe to inspect/clear caller-owned state here that would be
        // unsafe to touch from inside the coroutine body itself (e.g. destroying the very Task that
        // owns this frame).
        if (resumed_cb) resumed_cb();
      });
    });

    auto &event = storage.main_lock_resume_event();
    if (!event.RegisterWaiter(ps, epoch)) {
      // NB `*parked_ps` is deliberately still null here -- see below.
      // Epoch already moved (B1 waiter step 3, "register" rejected): `ps` never entered any
      // registry, so nobody can ever claim/resume it -- just drop it and re-probe on the next
      // loop iteration. No claim contest needed.
      //
      // Unless the event is CLOSED, which is a different answer to the same `false`: the storage is
      // being torn down (process shutdown, or DROP DATABASE via StopAllBackgroundTasks), so re-probing
      // is futile and would spin to the deadline. Report it so the acquire loop can bail instead. This
      // is the whole reason `Drain()` closes rather than bumping the epoch -- see RegisterWaiter.
      *event_closed = event.IsClosed();
      return false;
    }

    // Registered: from here on another thread may CLAIM `ps` at any moment. It may not yet DELIVER
    // the resume -- that is gated until this thread's pool task ends, which is what makes the rest of
    // this function (and the driver we return into) safe to keep touching frame-resident state.
    //
    // Hand `ps` back to the frame only now, AFTER a successful registration, so the caller's
    // post-resume prune runs only on a park that was actually registered. Assigning it earlier meant
    // the epoch-reject path above also paid the prune -- two mutexes and two O(N) list scans -- on
    // every iteration of a loop that already spins without backoff, and the reason a parker lands
    // there is epoch churn from a long waiter list, i.e. exactly when those scans cost most. Safe to
    // write frame memory here for the same reason as the rest of this function: the gate cannot
    // deliver a resume until this thread's task ends (utils/park_state.hpp).
    *parked_ps = ps;

    auto &registry = pool.park_registry();

    // Everything from here to the end of this block runs with `ps` ALREADY published to `event`,
    // i.e. already claimable-and-resumable by another thread. Both statements below allocate
    // (Register push_back; TryAccess constructs an Accessor with its Transaction), so both can
    // throw -- and an exception escaping await_suspend resumes and unwinds this coroutine,
    // destroying the very frame `ps->on_resume` captured, while `ps` sits registered and
    // unclaimed. The next NotifyAll/Sweep/Drain would then resume freed memory. Every other exit
    // from this function either self-claims or never published; the throwing exit is the one that
    // needs closing, and it is reachable exactly under memory pressure, which is also when
    // acquires are contended.
    // Set iff the abandon path below wins ClaimPark on our own `ps`. Must outlive the try so the
    // handler can distinguish our claim from a foreign one.
    bool self_claimed = false;
    // Publish the pending arm BEFORE anything here can throw, so that a wake source claiming `ps` in
    // the very next instant finds an arming side already accounted for. Outside the try, and that
    // placement is now sound rather than merely tidy: `PublishPendingPark` is `noexcept` because the
    // pending-park stack is intrusive (utils/park_state.hpp) and linking allocates nothing.
    //
    // It has to be unconditional-and-infallible, not just exception-safe, and this is the second time
    // that lesson was learned here. When publishing was a vector `push_back` it could throw, and the
    // `catch` below then had to reason about a park that was registered on the wake event but absent
    // from this thread's arming stack. Moving the call inside the try fixed the branch that UNWINDS,
    // and left the branch that SUSPENDS silently broken: on a lost `ClaimPark` it concluded "somebody
    // else will resume us", but nothing can ever arm a park that was never published, so the gate stuck
    // at kResumeRequested, the frame never resumed, and its campaign-long PendingHandle pinned
    // unique_pending_count above zero for the life of the process -- a bricked storage, not a leak. The
    // fix is that the failure is now unrepresentable, which is why the `published` bookkeeping flag is
    // gone: there is no longer a state in which we reached the try without having published.
    utils::PublishPendingPark(ps);
    try {
      registry.Register(ps);

      // B1 step 4 / R4.3: re-probe the real resource once more before committing to a genuine
      // park -- closes the race between the caller's original failed probe and the moment
      // registration above completed.
      // Plain TryAccess, NOT TryAccessWithPending: a SUCCESSFUL PendingScope::try_acquire() disarms
      // the scope for good (resource_lock.hpp -- `std::exchange(lock_, nullptr)`, after which every
      // call returns nullopt unconditionally). This probe can succeed and then still lose ClaimPark
      // below, in which case we park and the campaign continues -- so consuming the caller's
      // campaign-long handle here would leave every later probe failing regardless of lock state,
      // guaranteeing a spurious *AccessTimeout and dropping writer-preference for the rest of the
      // campaign. This probe only needs to close the register-vs-release race (B1 step 4); the
      // campaign's pending registration is already standing and keeps gating on our behalf.
      //
      // VERIFIED DETERMINISTICALLY (2026-07-31), not just reasoned about. A temporary thread_local
      // hook was added here -- fired in the published-but-not-yet-re-probed window -- and a test used
      // it to force the exact damaging interleaving: take ClaimPark away from this re-probe so the
      // campaign must continue, then free the lock so the re-probe succeeds anyway. With the plain
      // TryAccess below, the post-resume probe acquires in ~3ms. With TryAccessWithPending restored
      // (the original bug), the same test rides out the whole 5s deadline and throws
      // UniqueAccessTimeout on a completely free lock -- the customer-visible symptom. The hook and
      // the test were removed afterwards, per review: reproducing this needs a seam in production
      // code, and the seam is not worth carrying. Re-add both if this line is ever touched.
      auto acc = storage.TryAccess(rw_type, resolved_iso);
      if (acc) {
        if (utils::ClaimPark(*ps)) {
          // Won: no wake source will ever invoke on_resume for this ps -- best-effort cleanup,
          // then continue synchronously with the accessor already in hand.
          // Record it: ClaimPark is a one-shot exchange, so from here on OUR OWN claim is
          // indistinguishable from a foreign one, and the handler below must not mistake it for
          // "somebody will resume us" (see the note there).
          self_claimed = true;
          // Discard FIRST, then prune: RemoveWaiter/Deregister can throw (each takes a mutex), and a
          // throw after the Discard would leave `ps` on the pending-arm stack for a frame that is
          // unwinding. Same ordering rule as the catch handler below.
          utils::DiscardPendingPark(ps);  // we are the claim winner; nothing will ever ask for a resume
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
    } catch (...) {
      // Same single-owner arbitration the abandon path uses, applied to the unwind: we may only
      // let this frame die if nobody else can resume it.
      //
      // `self_claimed` is load-bearing, not defensive. ClaimPark is a one-shot exchange, so if we
      // already won it above, calling it again here returns false -- exactly as if a wake source
      // had taken it. Treating that as "somebody will resume us" and suspending would disarm every
      // wake source (NotifyAll, Sweep, both Drains all skip a claimed ParkState) with nobody left
      // to resume: a permanently suspended coroutine, no deadline backstop, the Bolt worker already
      // released, and the session's parked_prepare_ pinning the frame forever -- silently unkillable
      // and worse than the UAF this handler exists to prevent.
      if (self_claimed || utils::ClaimPark(*ps)) {
        // We own `ps`; no wake source will ever touch it. Safe to unwind and let the caller see
        // the real error (deadline/OOM handling is the acquire loop's caller's business).
        //
        // Discard FIRST, before the two registry prunes. Both of those take a mutex and can therefore
        // throw std::system_error; if one did after the Discard, `ps` would be left on this thread's
        // pending-arm stack, and the task-boundary ParkArmGuard would then arm a ParkState whose frame
        // is being destroyed by this very unwind. Ordering it first makes the stack correct no matter
        // which of the following calls fails. (Arming a self-claimed park delivers nothing -- the gate
        // is still kParking and we hold the claim -- so the residue of a throw here is at worst a stale
        // registry entry, not a resume.)
        utils::DiscardPendingPark(ps);
        event.RemoveWaiter(ps);
        registry.Deregister(ps);
        throw;
      }
      // A wake source already claimed `ps` and WILL resume this handle, so the frame must survive:
      // suspend instead of unwinding. The swallowed exception was a transient failure of an
      // opportunistic probe -- the resumed campaign re-probes and either acquires or times out
      // honestly, which is strictly better than resuming a destroyed frame.
      //
      // "WILL resume" rests on exactly one thing: `ps` is on this thread's pending-arm stack, so this
      // task's `ParkArmGuard` arms it when the task ends, completing the gate rendezvous the claim
      // winner's `RequestResume` started. That is guaranteed here because publishing happens
      // unconditionally above and cannot fail. It is NOT a self-evident property of this branch -- when
      // publishing could throw, this same `return true` was a permanent hang. Do not move the publish
      // back inside this try.
      //
      // NOTHING ELSE MAY GO HERE. An earlier revision pruned both registries on the way out as
      // "harmless defence-in-depth against a stale entry". It was not harmless: RemoveWaiter and
      // Deregister each take a mutex and can throw std::system_error, and a throw on THIS branch is
      // qualitatively different from one on the branch above. Per [expr.await]/5 the exception is
      // re-thrown at the `co_await`, so the frame unwinds and the Task temporary holding it is
      // destroyed -- while `ps` is still on the pending-arm stack with a foreign RequestResume already
      // recorded. The task-boundary ParkArmGuard then completes the rendezvous and resumes a destroyed
      // frame. Worse, std::system_error is not a utils::BasicException, so PrepareCoro's handler does
      // not match it: the session reports an ordinary query failure and carries on, and the
      // use-after-free lands later, on an unrelated worker. Silent and deferred.
      //
      // The prune was never needed: the resume is guaranteed to fire, and the resumed coroutine prunes
      // both registries at the bottom of the acquire loop before it re-probes. Leave this branch as a
      // bare `return true`.
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
        utils::DiscardPendingPark(ps);  // Discard before the prunes -- see the abandon path above.
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
/// Layering note: `on_resume` here means "resume THIS suspended handle, on whichever worker services
/// the post" -- resuming the handle propagates up the `Task<>` chain via symmetric transfer
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
/// @param pool              The pool this coroutine runs on -- used to post the resume and to reach
///                         the deadline registry. The park path requires running AS a pool task (see
///                         the assert below): the worker run loop is what arms the park.
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

  // HIGH priority never parks (§6); DiskStorage never supports parking (R4.6); flag-off keeps today's
  // behaviour (§7/C5) -- all three fall back to the ordinary blocking acquire, never constructing a
  // ParkState or registering anywhere.
  //
  // On `is_high_priority`: as of today it never fires, and that is COINCIDENCE, not structure. Every
  // query type ApproximatePreparePriority classifies as HIGH happens to leave `accessor_type_` unset,
  // so Phase 2 -- and therefore this function -- is never reached for them at all. The neighbouring
  // types in the same visitor (DescriptionQuery, DumpQuery, ShowEnumsQuery, ShowSchemaInfoQuery) DO
  // take accessors, so one reclassification makes this branch live. Keep it: it is the cheap half of
  // the contract, and the day it starts firing is the day nobody remembers it was ever dead.
  if (!storage.SupportsParkAcquire() || is_high_priority || !flags::run_time::CoroPrepareAccessorYieldEnabled()) {
    co_return blocking_access();
  }

  // `SupportsParkAcquire() == true` implies InMemoryStorage today, because it is the only override
  // (Storage::SupportsParkAcquire() defaults to false and DiskStorage never overrides it, R4.6). That
  // is a documentary invariant, not a checked one, and a `static_cast` on the strength of it is
  // undefined behaviour the moment some future subclass overrides both `TryAccess` and
  // `SupportsParkAcquire` -- silently, and in a park path where the symptom would be a corrupted
  // wake-event registration rather than an obvious crash.
  //
  // Checked instead. Deliberately NOT `MG_ASSERT` (which review suggested): aborting the whole process
  // is disproportionate for a caller/subclass bug, and this file already converted an assert to a
  // recoverable path once for that reason. Falling back to the blocking acquire is not a degraded
  // guess either -- it is exactly what every other non-parking storage does, so a mis-declaring
  // subclass gets correct behaviour plus a loud log instead of UB.
  auto *mem_storage_ptr = dynamic_cast<storage::InMemoryStorage *>(&storage);
  if (!mem_storage_ptr) [[unlikely]] {
    spdlog::error(
        "A storage type reports SupportsParkAcquire() but is not an InMemoryStorage; falling back to a "
        "blocking accessor acquire. This is a bug in that storage subclass -- the park path's wake hook and "
        "pending-handle machinery are InMemoryStorage-specific.");
    co_return blocking_access();
  }
  auto &mem_storage = *mem_storage_ptr;

  // Campaign-long pending scope (R4.6): built ONCE, held across every iteration of the loop below
  // -- including every suspend/resume -- so UNIQUE/READ_ONLY's writer-preference stays registered
  // for the whole retry campaign instead of just a single probe.
  auto pending = mem_storage.MakePendingHandle(rw);

  for (;;) {
    // Tearing down? Do not start an acquire at all. Without this the only shutdown check was
    // post-co_await, so a chain re-driven during teardown (the inline resume re-enters
    // Session::RunLoop -> Execute(), and a pipelined RUN still in the decoder buffer starts a fresh
    // Prepare) would probe, find no worker id, and take the blocking fallback below -- on the main
    // shutdown thread, for up to the whole --storage-access-timeout-sec.
    if (pool.IsShuttingDown()) {
      throw utils::BasicException("AcquireAccessorCoro: pool is shutting down, refusing to acquire a new accessor");
    }

    // Capture BEFORE the probe (B1's lost-wakeup guard): this is the epoch that must still hold
    // for a subsequent park (below) to be safe.
    const auto epoch = mem_storage.main_lock_resume_event().Epoch();

    if (auto acc = mem_storage.TryAccessWithPending(rw, resolved_iso, pending)) {
      co_return std::move(acc);
    }

    if (std::chrono::steady_clock::now() >= deadline) {
      // Same exception the blocking path (`blocking_access`/CreateSharedGuard/CreateUniqueGuard)
      // throws -- observable timeout semantics are unchanged by parking.
      if (rw == storage::UNIQUE) throw storage::UniqueAccessTimeout{};
      if (rw == storage::READ_ONLY) throw storage::ReadOnlyAccessTimeout{};
      throw storage::SharedAccessTimeout{};
    }

    // Parking is only safe where something will later ARM the park -- and the only thing that does is
    // utils::PriorityThreadPool's ParkArmGuard, wrapped around pool task bodies. Off a pool worker
    // there is no arming site, so a published park would sit registered with gate == kParking forever,
    // holding its campaign PendingHandle and thereby blocking every later acquisition on this storage
    // (utils/park_state.hpp).
    //
    // This is a RUNTIME fallback, not an assert. It used to be a DMG_ASSERT, which is compiled out
    // under NDEBUG -- i.e. absent from every release build, precisely where the consequence is a
    // permanently unusable database. Falling back to the ordinary blocking acquire is always correct:
    // it is what flag-off, HIGH priority and DiskStorage already do, and holding our own campaign
    // `pending` across it changes nothing (a scope's own count never gates its own mode --
    // can_acquire<UNIQUE> ignores unique_pending_count, and READ_ONLY's campaign registers
    // ro_pending_count). Worst case we block a non-pool thread that was never freed by parking anyway.
    if (!utils::GetCurrentWorkerId().has_value()) [[unlikely]] {
      DMG_ASSERT(false,
                 "AcquireAccessorCoro reached its park path off a pool (LP) worker -- the caller must "
                 "schedule this coroutine onto the pool before driving it. Falling back to a blocking "
                 "acquire; see the comment here for why that is safe.");
      spdlog::error(
          "Parkable accessor acquire reached its park path off a pool worker; falling back to a blocking "
          "acquire. This is a bug in the caller -- the coroutine must be driven as a pool task.");
      co_return blocking_access();
    }

    std::optional<std::unique_ptr<storage::Accessor>> abandon_result;
    std::shared_ptr<utils::ParkState> parked_ps;
    bool event_closed = false;
    co_await detail::AcquireAwaitable{mem_storage,
                                      pool,
                                      rw,
                                      resolved_iso,
                                      deadline,
                                      epoch,
                                      &abandon_result,
                                      &parked_ps,
                                      &event_closed,
                                      on_park_resumed};

    // Storage is being torn down: the wake event is closed, so no registration can ever succeed again
    // and looping would spin to the deadline. Bail the same way the shutdown checks do.
    if (event_closed) [[unlikely]] {
      throw utils::BasicException(
          "AcquireAccessorCoro: storage is shutting down (parked-waiter event closed), abandoning the acquire");
    }

    if (abandon_result) {
      // R4.3 abandon-path win: the awaitable's own re-probe already acquired (and claimed) the
      // accessor without ever truly suspending -- nothing resumed us, we simply continued.
      co_return std::move(*abandon_result);
    }

    // Prune the ParkState we were just resumed from, from BOTH registries. Whichever wake source
    // claimed it removed it from its OWN list only, and nothing else ever removes the twin entry:
    //   - a lock-release NotifyAll empties `waiters_` wholesale but leaves the deadline registry entry;
    //   - a deadline Sweep erases its own entry but leaves the `waiters_` one.
    // The second case was a real leak, and on the commonest path there is: at the shipped
    // --storage-access-timeout-sec default of 1s, a contended query that rides out its deadline is
    // resumed by the sweep, so EVERY timed-out park used to leave a fired ParkState in `waiters_`.
    // That kept `waiters_pending_` non-zero, which permanently defeated NotifyMainLockReleased's
    // cheap "nobody is parked" gate and made every later admitting transition on that storage take the
    // event mutex and bump the epoch -- and a bumped epoch fails concurrent RegisterWaiter calls, which
    // sends other parkers back around this loop with no backoff. So the stale entry did not merely
    // retain memory, it converted real parkers into spinners. (The Session it also retained is handled
    // at the source now: ParkState releases its on_resume closure when the resume fires.)
    // Both calls are best-effort and idempotent -- each is a no-op when the entry is already gone --
    // so doing both is how we stay correct without knowing which source woke us.
    if (parked_ps) {
      mem_storage.main_lock_resume_event().RemoveWaiter(parked_ps);
      pool.park_registry().Deregister(parked_ps);
    }

    // Genuinely resumed, on some pool worker -- which one is deliberately not ours to know post-F6
    // (on_resume always POSTS, never resumes inline on the claiming/draining thread). A shutdown
    // drain can be what woke us, so bail before touching storage state if the pool is tearing down.
    if (pool.IsShuttingDown()) {
      throw utils::BasicException("AcquireAccessorCoro: pool is shutting down, abandoning parked accessor acquire");
    }
    // Loop back: re-probe (we may have woken spuriously, or lost a race to a third acquirer --
    // resume is a guarantee that "something changed, retry", never a guarantee of acquisition).
  }
}

}  // namespace memgraph::query
