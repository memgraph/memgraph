# Parkable Prepare

Flag: `--experimental-coro-prepare-accessor-yield` (default ON, startup-only).

A query that needs a contended `UNIQUE`/`READ_ONLY` storage accessor used to block its Bolt worker
inside `try_lock_for` for up to `--storage-access-timeout-sec`. With enough such queries every worker
is pinned and the instance stops answering anything, including trivial reads. This feature suspends
the query's `Prepare` on a C++20 coroutine instead, returns the worker to the pool, and resumes the
query when the lock is actually released.

This document holds the *reasoning*: why the design is shaped this way, which alternatives failed,
and what was measured. Load-bearing invariants live in the code, next to the lines that must
preserve them. When the two disagree, the code is authoritative and this file is stale — two comments
in this feature's own history asserted behaviour that had already been reversed, so treat prose about
behaviour with suspicion and re-derive from the source.

Entry points: `src/query/coro_accessor.hpp` (the acquire coroutine), `src/utils/park_state.hpp` (the
delivery gate), `src/utils/worker_resume_event.hpp` (the per-storage wake event),
`src/utils/deadline_park_registry.hpp` (the timeout backstop), `Interpreter::PrepareCoro`.

## Why the flag is gated at install time

The flag is startup-only: no Settings registration, no `SET` support, so its value cannot change
under a live storage. `Storage`'s constructor therefore installs the `main_lock_` admit observer only
when the flag is on, and with the observer absent no release path reaches park code at all. That
makes flag-off a structural off-switch rather than a per-call early return, and it is why
`NotifyMainLockReleased` does not re-check the flag: reaching it already implies the flag.

Three things are deliberately *not* gated, because each is provably a no-op when off and a flag load
would cost more than the no-op it guards:

| Site | Cost when off |
| --- | --- |
| `ParkArmGuard` at task boundaries | one TLS read and compare; depth is always 0 because nothing publishes |
| `work_must_run_` | `.empty()`-guarded at every read |
| `park_registry_.Sweep` | one atomic load per monitor tick |

A test that flips the flag must construct its storage *afterwards*. `StorageMainLockWakeHookTest`
originally built the storage as a fixture member, before each test set the flag, which asserted
nothing once installation became conditional.

## The delivery gate: claim decides WHO, gate decides WHEN

A park is published from inside the parking thread's `await_suspend`, and that thread keeps running
afterwards: it finishes `await_suspend` (the register-before-recheck re-probe, the shutdown
self-claim), then unwinds through `coroutine_handle::resume()` into its driver, which still has
post-`Resume()` bookkeeping to do — `Session::RunLoop` must observe `!Done()` and retain the
connection's single in-flight slot. None of that may run concurrently with a resume, which re-enters
the same coroutine chain and the same session state.

The original design got that exclusion for free by **pinning** the resume onto the parking worker: a
same-worker task cannot start until that worker returns to its run loop. But pinning conflated *when*
the resume may run with *where*, and the *where* half made every parked query's wake latency hostage
to one worker's backlog — an unrelated long-running task on that worker delays the resume
arbitrarily, including past the query's own deadline.

The gate keeps the *when* and drops the *where*. It is a two-party rendezvous on one atomic between
the claim winner (`RequestResume`) and the parking thread's task boundary (`ArmPark`). Each exchanges
in its own marker and reads the other's, so whoever arrives **second** delivers, exactly once, and
neither can deliver early. The resume may then be posted to any worker.

`ClaimPark` is a separate, one-shot decision that arbitrates *which* of the several wake sources owns
a given park: a lock-release `NotifyAll`, the deadline `Sweep`, either registry's shutdown `Drain`, or
the awaitable's own abandon-path claim. Winning it does not put the winner in a position to call
anything — `on_resume_` is private precisely so "claim, then resume" cannot be written by accident.

### Why `ParkState` is heap-allocated

`claimed` must stay readable by a *losing* waker even after the winner has driven the frame to
completion and destruction. Inside the coroutine frame it would dangle; on its own allocation,
refcounted from every registry it is registered in, it cannot.

### Why `on_resume` is a closure, not a `coroutine_handle`

Resuming a parked `Prepare` is not "continue this frame". It must keep the owning session alive across
a cross-thread resume, detect pool shutdown before touching per-database state, and re-drive the
connection's post-`Execute` bookkeeping once the frame completes. Hiding that behind a
`std::function` is what lets `WorkerResumeEvent` and `DeadlineParkRegistry` stay coroutine-agnostic.

## The failure mode everything else defends against

An **unarmed park** is not one lost query. The frame holds its campaign-long `PendingHandle`, which
keeps `unique_pending_count` above zero, which makes `can_acquire<WRITE>`, `<READ>` and `<READ_ONLY>`
all permanently false on that storage. Untimed acquirers — the TTL thread, replication apply — then
block forever and the database is bricked until the process restarts.

Over-arming is harmless. Under-arming bricks the instance. Every design choice below follows from that
asymmetry.

- Arming lives in `ParkArmGuard`, which must wrap **every** site that runs a session/`Prepare` task
  body: the pool's three run-loop sites plus `PostResumeTask`'s inline fallback.
  `TaskCollection::WaitOrSteal` also runs a task body inline and deliberately has no guard — those
  are Pull-time parallel tasks that never reach the `Prepare` park path, and its caller is itself
  inside a guarded task, so any park published underneath is armed by that outer guard.
- The pending-arm stack is a **stack**, not a slot, because task execution nests: the inline-resume
  fallback re-enters the session chain, which can start a query that parks while the outer park is
  still pending its arm. With one slot the inner publish silently overwrote the outer, leaving it at
  `gate == kParking` — unarmable, hence bricked.
- The stack is **intrusive** so that publishing cannot throw. As a `std::vector` it could: `push_back`
  allocates, and a `bad_alloc` left the park registered on the wake event but absent from the stack.
  A wake source claiming in that window made the publisher lose `ClaimPark`, conclude "somebody else
  will resume us" and suspend — but nothing can arm a park that was never published. Moving the call
  inside the `try` fixed the branch that *unwinds* and left the branch that *suspends* broken. The fix
  was to make the failure unrepresentable, not to widen the `catch`.

## Two-sided wakeup protocol

The "skip `NotifyAll` when nobody is parked" fast path is sound only if both sides keep a strict
register-before-recheck / release-before-check ordering. Otherwise a release can race a park such
that the waiter misses the epoch bump *and* the releaser misses the registration — a lost wakeup that
hangs the query indefinitely rather than surfacing the access timeout, because the deadline sweep only
fires for waiters that reached a registry.

Waiter: capture the epoch **before** probing; probe; on failure register under that epoch; **re-probe
once more**; only then behave as parked. Releaser: transition the resource to released **first**, then
read the pending count, then notify unconditionally if non-zero.

The re-probe uses plain `TryAccess`, not `TryAccessWithPending`. A successful
`PendingScope::try_acquire()` disarms the scope permanently, and this probe can succeed and *still*
lose `ClaimPark`, in which case the campaign continues — so consuming the caller's campaign-long
handle here would make every later probe fail regardless of lock state, guaranteeing a spurious
timeout and dropping writer preference for the rest of the campaign. Verified with a temporary hook
forcing that interleaving: plain `TryAccess` acquires in ~3ms, `TryAccessWithPending` rides out the
whole 5s deadline and throws `UniqueAccessTimeout` on a completely free lock.

### Why `Drain()` closes rather than bumps the epoch

`Drain()` means the storage is going away. It deliberately does not bump the epoch, because a bump
only makes in-flight registrations *retry* — against a storage that is being torn down, moving the
window one iteration later. A sticky `closed_` flag, set in the same critical section that empties the
waiter list, is what terminates it. Callers distinguish "retry" from "give up" via `IsClosed()`.

### Why both registries must be pruned after a resume

A genuine park registers the same `ParkState` in two places: the pool's `DeadlineParkRegistry` and the
per-storage `WorkerResumeEvent`. Whichever wake source claims it removes it from its own list only.
A lock-release `NotifyAll` empties `waiters_` but leaves the deadline entry; a deadline `Sweep` erases
its own but leaves the `waiters_` one — and that second case was the common path, since at the default
1s access timeout a contended query that rides out its deadline is resumed by the *sweep*. Every
timed-out park therefore left a fired `ParkState` in `waiters_`, keeping `waiters_pending_` non-zero,
which permanently defeated the "nobody is parked" fast path. Every later admitting transition then
took the event mutex and bumped the epoch — and a bumped epoch fails concurrent `RegisterWaiter`
calls, sending other parkers back around the acquire loop with no backoff. The stale entry did not
merely retain memory; it turned real parkers into spinners.

## Shutdown

Parked waiters are drained **while the workers are still running**. Draining claims each `ParkState`
and requests its resume through the gate, which posts onto a still-running worker; a waiter whose
parking thread has not reached its task boundary is delivered by that arming side instead, from a
worker that is by definition still alive. Each resumed chain observes `IsShuttingDown()` and bails
cleanly rather than proceeding into per-database work.

Order: mark shutting down → drain parked (workers still looping) → stop monitor → stop workers (each
finishes its must-run queue first) → join → destroy pool.

`Storage::StopAllBackgroundTasks` runs an idempotent second pass, and is the *only* pass on the
`DROP DATABASE` path where the pool is not shutting down at all. It must not become the primary one:
a resume is posted to a worker, so draining after the pool has stopped never runs it.

### The inline-resume fallback, and its accepted delta

If every worker refuses the push because all are stopped, `PostResumeTask` runs the closure inline on
the caller's thread. Three claimants reach that: an LP worker arming from its tail (its own `try_push`
refuses too, since `stop()` has already cleared `run_`), the main shutdown thread via
`StopAllBackgroundTasks`, and any thread releasing `main_lock_` in that window.

For the latter two this runs a full session chain on a thread that is not a pool worker, and the
chain reaches `Session::RunLoop -> Execute()`, which performs a **synchronous, un-timed socket send**
before arming a fresh async read. A client with a full receive window can therefore block a
GC/TTL/replication or main-shutdown thread indefinitely. Master never did session I/O off a pool or
io thread; this is a genuine delta, accepted rather than unnoticed.

Neither obvious mitigation works. Gating on `IsShuttingDown()` is a no-op — reaching the fallback
already implies it. Dropping the resume is strictly worse: the frame stays parked holding its
`PendingHandle`, and the eventual destruction chain calls `unregister_pending()` on an
already-destroyed `main_lock_`. The real fix, if this ever has to go, is to suppress connection I/O in
the resumed hook while shutting down, letting the frame unwind and release its handle without
touching the socket.

## `PrepareCoro` and phase order

`PrepareCoro` mirrors `Prepare()` phase for phase, with the accessor acquire becoming a `co_await`.
Shared bodies were extracted (`PlanAndFinalize`, `HandlePrepareFailure`,
`QueryTransactionRequirements`, `SetupDatabaseTransactionWith`) so the two paths cannot disagree on
*what* they do. The phase **order** cannot be shared, so it is the part that drifts — and it has
drifted once already, consequentially.

`SetupInterpreterTransaction` must run in Phase 1, before the acquire. Deferring it to Phase 3 meant a
query parked on a contended accessor reported IDLE with no transaction id, so it was invisible to
`SHOW TRANSACTIONS`, un-`TERMINATE`-able, uncounted by `DROP DATABASE ... FORCE`, and its `elapsed_ms`
excluded the entire lock wait. That is exactly the diagnostic an operator reaches for during the
pile-up this feature exists to relieve. It was worse on `ON_DISK_TRANSACTIONAL`, where
`SupportsParkAcquire()` is false so the acquire blocks the full timeout while still reporting IDLE.

The deferral was justified by "never expose ACTIVE with `current_transaction_ == nullopt`", but
`SetupInterpreterTransaction` satisfies that intrinsically — it assigns `current_transaction_` before
the release-store of ACTIVE. It has no accessor dependency at all, so it may sit on either side of the
acquire, and the diagnostics decide.

Arena attribution is split rather than wrapped around the whole body: a TLS guard must never straddle
the `co_await`, or it would restore the parking thread's saved arena state onto whichever worker
resumed the frame. The accessor and `Transaction` that the acquire allocates stay unattributed;
covering them would mean teaching a deliberately DB-agnostic coroutine about arena identity.

## Session lifetime and the deliberate refcount cycle

While parked there is a real cycle: session → `parked_prepare_` → frames → `std::function` →
`shared_ptr<Session>`. Type erasure hides it from a reader, not from the refcount — `PrepareCoro`
takes the closure by value and copies it again into `AcquireAccessorCoro`, so two live frames
reference the session that transitively owns them.

It is broken by hand, by `parked_prepare_.reset()` on every resume path. That is why any path which
drops a resume leaks the `Session` and its `DatabaseAccess`, holding `Gatekeeper<Database>::count_`
above zero and stalling shutdown for minutes. The reset happens *before* anything that can throw,
which is what makes it unskippable — a non-`std` exception from `TakeValue()` previously escaped a
narrow handler, skipped the reset, and terminated the process on the way out.

`parked_prepare_` is declared **last** in `Session` so it is destroyed first: `~Task` destroys a
still-suspended frame whose locals include a `PendingHandle` registered on the storage's `main_lock_`.

## Testing

Unit coverage is one property per test across `park_state`, `worker_resume_event`,
`deadline_park_registry`, `coro_task`, `coro_accessor`, plus the pool and storage wake-hook suites.

The e2e suite runs two arms against real processes. Its discriminating shape and two empirical
findings are documented in `tests/e2e/coro_prepare_accessor_yield/common.py`, because they are
properties of *that test*, not of the feature: the contender count must exceed `--bolt-num-workers`
by one to close the HP-thread work-stealing escape hatch, and the contenders must be separate
processes because mgclient does not release the GIL during a blocking call.

The stress soak exists because both characteristic failure modes are **accumulation** bugs — a park
published but never delivered, and a resumed park left registered behind itself. Neither is visible at
the e2e's scale of three contenders for six seconds. Its leak gate watches `ActiveBoltSessions`
return to baseline after every driver is closed, since a leaked park is precisely a session outliving
its connection.

Contention design, and the mistake not to repeat: the first version saturated the pool with eight
*writer* threads and asserted the probe stayed fast. It failed at p50 ~0.96s, and that was the test's
fault. Busy writers are not blocked writers — a worker executing a real transaction is not being
wasted, and parking neither can nor should help. Worse, that design's continuously-pending `UNIQUE`
gates every new acquire of every kind, so the probe was not waiting for a worker; it was inadmissible.
The corrected shape is one long-held WRITE accessor and more `READ_ONLY` contenders than there are
workers: parking ON p50 0.0016s over 365 probes, OFF p50 0.9500s over 22.

## Known gaps

- The uncontended per-query cost of routing all `Prepare` through the coroutine driver is unmeasured.
  The Bolt gate has no contention term, so default-on routes every `Prepare` through it.
- The epoch-reject path spins without backoff. Deferred pending a measurement, since backoff trades
  against wake latency.
- Most registered e2e suites have not been run against default-on.
