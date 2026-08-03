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

/// Awaitable behind AcquireAccessorCoro's park; one instance per park attempt. `await_suspend`
/// implements the full single-owner arbitration, not just "register and suspend" -- register under the
/// captured epoch, re-probe once more, and arbitrate via `ClaimPark` if that probe succeeds (the
/// ABANDON PATH). See specs/parkable-prepare.md for why each step is shaped this way.
struct AcquireAwaitable {
  storage::InMemoryStorage &storage;
  utils::PriorityThreadPool &pool;
  storage::StorageAccessType rw_type;
  std::optional<storage::IsolationLevel> resolved_iso;
  std::chrono::steady_clock::time_point deadline;
  uint64_t epoch;
  // Filled in iff the abandon-path re-probe won the accessor while ALSO winning ClaimPark;
  // AcquireAccessorCoro checks it right after co_await returns, this call never having suspended.
  std::optional<std::unique_ptr<storage::Accessor>> *abandon_result;
  // Always set to the ParkState this attempt built, so AcquireAccessorCoro can prune it from BOTH
  // registries after a genuine resume (see the cleanup at the bottom of the acquire loop).
  std::shared_ptr<utils::ParkState> *parked_ps;
  // True iff registration was refused because the wake event is CLOSED (storage tearing down) rather
  // than because the epoch moved: the former must abandon the campaign, the latter retries.
  bool *event_closed;
  // Opaque hook invoked by the posted resume closure right AFTER it resumes the parked handle -- never
  // on the synchronous never-parked path, where the closure is not even constructed. This struct knows
  // nothing of what it does (typically: keep the owning session alive across the park, and once the
  // whole Task chain is done, re-drive that caller's connection loop). Empty for callers that never
  // expect a park (tests, a plain SyncWait); invoking it is guarded below.
  std::function<void()> on_park_resumed;

  static bool await_ready() noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> h) {
    auto ps = std::make_shared<utils::ParkState>();
    ps->deadline = deadline;
    auto *pool_ptr = &pool;
    // Copied into BOTH the outer (on_resume) and inner (posted) closures, which live inside ps for the
    // WHOLE park rather than the invocation instant -- that is what keeps a session-lifetime
    // shared_ptr captured inside it alive across the park.
    auto resumed_cb = on_park_resumed;
    ps->set_on_resume([pool_ptr, h, resumed_cb] {
      // Any worker, never inline on the caller's thread: on_resume runs from whichever thread claimed
      // the park, and none of those may drive a coroutine frame. Serialising against the parking thread
      // is the gate's job, not this posting's.
      pool_ptr->PostResumeTask([h, resumed_cb] {
        h.resume();
        // Strictly AFTER h.resume() returns, i.e. outside any frame's own execution, so this may touch
        // caller-owned state that would be unsafe from inside the coroutine body -- e.g. destroying the
        // very Task that owns this frame.
        if (resumed_cb) resumed_cb();
      });
    });

    auto &event = storage.main_lock_resume_event();
    if (!event.RegisterWaiter(ps, epoch)) {
      // The epoch moved, so `ps` never entered a registry and nobody can claim it: drop and re-probe,
      // no claim contest, and `*parked_ps` deliberately stays null (see below).
      //
      // Unless the event is CLOSED -- a different answer to the same `false`. The storage is tearing
      // down, so re-probing would spin to the deadline; report it so the loop bails.
      *event_closed = event.IsClosed();
      return false;
    }

    // Registered: another thread may CLAIM `ps` from here on, but cannot DELIVER until this thread's
    // pool task ends -- which is what keeps the rest of this function, and the driver we return into,
    // safe to touch frame-resident state.
    //
    // Assigned only AFTER a successful registration, so the caller's post-resume prune runs only for a
    // park that really registered. Earlier, the epoch-reject path paid that prune too -- two mutexes
    // and two O(N) scans -- on every iteration of a loop that spins without backoff.
    *parked_ps = ps;

    auto &registry = pool.park_registry();

    // From here on `ps` is published and claimable by another thread, and both statements below
    // allocate and so can throw. An exception escaping await_suspend unwinds this coroutine, destroying
    // the frame `ps->on_resume` captured while `ps` sits registered and unclaimed -- so the next wake
    // would resume freed memory. That is what the catch below exists for; it is reachable exactly under
    // memory pressure, which is also when acquires are contended.
    //
    // Set iff the abandon path wins ClaimPark on our own `ps`. Must outlive the try so the handler can
    // tell our claim from a foreign one.
    bool self_claimed = false;
    // Publish the pending arm BEFORE anything that can throw, so a wake source claiming `ps` in the
    // next instant finds an arming side already accounted for. Outside the try, which is sound rather
    // than merely tidy: `PublishPendingPark` is `noexcept` because the pending-park stack is intrusive
    // and linking allocates nothing.
    //
    // This must be unconditional AND infallible, not just exception-safe. When publishing was a vector
    // `push_back` it could throw; moving the call inside the try fixed the branch that UNWINDS and left
    // the branch that SUSPENDS silently broken, because nothing can arm a park that was never
    // published -- the gate stuck at kResumeRequested and the frame's campaign-long PendingHandle
    // bricked the storage for the life of the process. Do not move this back inside the try.
    utils::PublishPendingPark(ps);
    try {
      registry.Register(ps);

      // Re-probe once more before committing to a genuine park, closing the race between the caller's
      // original failed probe and the completion of the registration above.
      //
      // Plain TryAccess, NOT TryAccessWithPending: a SUCCESSFUL PendingScope::try_acquire() disarms the
      // scope for good (`std::exchange(lock_, nullptr)`, after which every call returns nullopt). This
      // probe can succeed and STILL lose ClaimPark below, in which case we park and the campaign
      // continues -- so consuming the caller's campaign-long handle here would make every later probe
      // fail regardless of lock state, guaranteeing a spurious *AccessTimeout and dropping writer
      // preference for the rest of the campaign. Closing the register-vs-release race is all this probe
      // owes; the campaign's pending registration still gates on our behalf.
      //
      // Verified deterministically, not merely reasoned about: with a temporary hook forcing the
      // damaging interleaving (lose ClaimPark, then free the lock so the re-probe succeeds anyway), the
      // plain TryAccess acquires in ~3ms while TryAccessWithPending rides out the whole 5s deadline and
      // throws UniqueAccessTimeout on a completely free lock. The hook needed a seam in production code
      // and was removed with its test; re-add both if this line is ever touched.
      auto acc = storage.TryAccess(rw_type, resolved_iso);
      if (acc) {
        if (utils::ClaimPark(*ps)) {
          // Won: nothing will ever deliver for this ps, so continue synchronously. Record the win --
          // ClaimPark is one-shot, so from here our own claim is indistinguishable from a foreign one
          // and the handler below must not read it as "somebody will resume us".
          self_claimed = true;
          // Discard FIRST, then prune: RemoveWaiter/Deregister each take a mutex and can throw, and a
          // throw after the Discard would leave `ps` on the pending-arm stack for an unwinding frame.
          utils::DiscardPendingPark(ps);
          event.RemoveWaiter(ps);
          registry.Deregister(ps);
          *abandon_result = std::move(acc);
          return false;
        }
        // Lost: a wake source already claimed ps and WILL resume us, so treat this as a genuine park.
        // Release the accessor we redundantly acquired -- the resume path re-probes and re-acquires,
        // which is correct even though the lock is free right now.
        acc.reset();
        return true;
      }
    } catch (...) {
      // Single-owner arbitration applied to the unwind: this frame may only die if nobody else can
      // resume it.
      //
      // `self_claimed` is load-bearing, not defensive. ClaimPark is one-shot, so a second call after
      // winning above returns false -- indistinguishable from a wake source having taken it. Reading
      // that as "somebody will resume us" and suspending would disarm every wake source with nobody
      // left to resume: a permanently suspended coroutine, no deadline backstop, silently unkillable,
      // and worse than the UAF this handler prevents.
      if (self_claimed || utils::ClaimPark(*ps)) {
        // We own `ps`, so no wake source will touch it: safe to unwind and let the caller see the real
        // error. Discard before the prunes for the same reason as above -- a std::system_error from
        // either mutex would otherwise leave `ps` on the pending-arm stack while this frame is being
        // destroyed. (Arming a self-claimed park delivers nothing, so the worst residue of a throw here
        // is a stale registry entry, not a resume.)
        utils::DiscardPendingPark(ps);
        event.RemoveWaiter(ps);
        registry.Deregister(ps);
        throw;
      }
      // A wake source claimed `ps` and WILL resume this handle, so the frame must survive: suspend
      // rather than unwind. "WILL resume" rests on `ps` being on this thread's pending-arm stack, so
      // this task's ParkArmGuard arms it at the task boundary -- which holds only because publishing
      // above is unconditional and infallible.
      //
      // NOTHING ELSE MAY GO HERE. Do not "defensively" prune the registries: both prunes take a mutex
      // and can throw std::system_error, and per [expr.await]/5 a throw here is rethrown at the
      // `co_await`, so the frame unwinds and its Task temporary dies while `ps` is still on the
      // pending-arm stack with a foreign RequestResume recorded -- and the task-boundary ParkArmGuard
      // then resumes a destroyed frame. Worse, std::system_error is not a utils::BasicException, so
      // PrepareCoro's handler does not match: the session reports an ordinary failure and the UAF lands
      // later on an unrelated worker. The prune is also unnecessary -- the resumed coroutine prunes both
      // registries at the bottom of the acquire loop.
      return true;
    }

    // Shutdown-race closer: ShutDown() drains the registry exactly ONCE, so a `ps` that finished
    // registering strictly after that drain would sit there forever with nothing left to sweep or notify
    // it. `IsShuttingDown()` is one-way and set as ShutDown()'s first action, strictly before its drain,
    // so any `ps` reaching here self-claims rather than trusting a wake that may never come.
    //
    // On a win, deliberately do NOT set `abandon_result`: returning false with no accessor makes the
    // post-co_await shutdown check throw, the same clean bail a real shutdown-drain resume produces.
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

/// The acquire coroutine at the heart of parkable Prepare. Resolves a storage accessor for `rw`,
/// parking the CURRENT pool worker rather than blocking it while a contended UNIQUE/READ_ONLY
/// acquisition is retried -- whenever parking is both possible (InMemory storage) and enabled (LOW
/// priority, flag on).
///
/// Layering: `on_resume` here means "resume THIS handle, on whichever worker services the post", and
/// resuming propagates up the `Task<>` chain by symmetric transfer. The session-aware "re-drive the
/// connection loop once the top-level Task completes" concern is layered in through the opaque
/// `on_park_resumed` hook, supplied by the Bolt session layer -- so this coroutine stays self-contained
/// and testable without any of it (tests/unit/coro_accessor.cpp never passes one).
///
/// @param storage           The (per-DB) storage to acquire an accessor on.
/// @param rw                Requested access type (UNIQUE/READ_ONLY/WRITE/READ).
/// @param resolved_iso      Isolation-level override, resolved ONCE by the caller before this
///                          coroutine starts; it never reads/resets next_transaction_isolation_level.
/// @param deadline          Absolute deadline, re-checked every iteration and resume so a parked
///                          campaign still honours the ~1s `*AccessTimeout` contract.
/// @param pool              Used to post the resume and reach the deadline registry. The park path
///                          requires running AS a pool task -- the worker run loop is what arms a park.
/// @param is_high_priority  HIGH-priority queries never park; they take the blocking path.
/// @param on_park_resumed   Threaded verbatim into every ParkState a genuine park builds; empty for
///                          callers that never expect one (tests, a plain SyncWait).
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

  // HIGH priority, DiskStorage and flag-off all take the ordinary blocking acquire, never constructing
  // a ParkState. `is_high_priority` never fires today, but by COINCIDENCE not structure: every type
  // classified HIGH happens to leave `accessor_type_` unset, so Phase 2 is not reached for them.
  // Neighbouring types in the same visitor DO take accessors, so one reclassification makes it live.
  if (!storage.SupportsParkAcquire() || is_high_priority || !flags::run_time::CoroPrepareAccessorYieldEnabled()) {
    co_return blocking_access();
  }

  // `SupportsParkAcquire()` implies InMemoryStorage today, but that is documentary, not checked -- a
  // `static_cast` on its strength becomes UB the moment a subclass overrides both it and `TryAccess`,
  // silently, in a path whose symptom is a corrupted registration rather than a crash. Checked instead,
  // and deliberately not via MG_ASSERT: the blocking fallback is exactly what every other non-parking
  // storage does, so a mis-declaring subclass gets correct behaviour plus a loud log.
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

    // Parking is only safe where something will later ARM the park, and only the pool's ParkArmGuard
    // does. Off a pool worker a published park would sit at kParking forever and brick the storage.
    //
    // A RUNTIME fallback, not an assert: as a DMG_ASSERT this was compiled out under NDEBUG -- absent
    // from exactly the builds where the consequence is an unusable database. The blocking acquire is
    // always correct here, and holding our own campaign `pending` across it changes nothing, since a
    // scope's own count never gates its own mode.
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

    // Prune from BOTH registries: whichever wake source claimed `ps` removed it from its OWN list only,
    // and nothing removes the twin. Skipping this leaked on the COMMON path -- at the default 1s timeout
    // a query that rides out its deadline is resumed by the sweep, leaving a fired ParkState in
    // `waiters_` that kept `waiters_pending_` non-zero and turned later parkers into spinners
    // (specs/parkable-prepare.md). Both calls are best-effort and idempotent, which is how we stay
    // correct without knowing which source woke us.
    if (parked_ps) {
      mem_storage.main_lock_resume_event().RemoveWaiter(parked_ps);
      pool.park_registry().Deregister(parked_ps);
    }

    // Genuinely resumed, on some pool worker -- which one is deliberately not ours to know. A shutdown
    // drain may be what woke us, so bail before touching storage state.
    if (pool.IsShuttingDown()) {
      throw utils::BasicException("AcquireAccessorCoro: pool is shutting down, abandoning parked accessor acquire");
    }
    // Loop back: re-probe (we may have woken spuriously, or lost a race to a third acquirer --
    // resume is a guarantee that "something changed, retry", never a guarantee of acquisition).
  }
}

}  // namespace memgraph::query
