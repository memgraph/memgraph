# c3 — parallel-coordinator park (design spec for verification)

Branch `feat/coroutine-scheduler-redesign` @ `a0ddaba19` (B1 + B2 core + B3 d1a/d1b/d2 done; the yield-kind
cross-task park is LIVE + TSan-clean). c3 delivers the ORIGINAL motivation (user point 7): the enterprise
parallel-query coordinator schedules ALL N branches on the pool and PARKS (frees its worker for the
scheduler) instead of running branch-0 inline + blocking in `WaitOrSteal`. The freed worker runs branches /
other work; the LAST branch to finish resumes the coordinator (pinned), which then merges.

This is the deferred "approach B" (memory [[coroutine-parallel-park-approach-b]], doc APPROACH_B.md). Its
R1–R7 constraints were VERIFIED 2026-06-23 against the p3 stack — **do NOT re-derive them**; they are
binding and carried here. This spec focuses on what's NEW: reconciling the park with the now-live d1b/d2
session-dispatch + the yield-kind mechanism.

## ⚑ VERIFICATION-CLEARED REVISIONS (2026-06-30 — concurrency-debugger + skeptic CONVERGED: "not implementable as specced; implementable WITH these changes"). These SUPERSEDE the prose below.

**FATAL-A — wrong target cursor + coro region must reach the root.** `ParallelMergeCursor` (the spec's
original target) is a PASS-THROUGH: it calls `Trigger()` once then delegates `Pull()` to `input_cursor_`;
it does NOT call `WaitOrSteal`/`ExecuteBranchesInParallel` and has nothing to park on. **The REAL
coordinators are `AggregateParallelCursor` + `OrderByParallelCursor`** (`ParallelBranchCursor` subclasses)
— they call `ExecuteBranchesInParallel` + `WaitOrSteal`. AND `WillDriveCoro()` checks the ROOT cursor's
mode; `SelectCoroMode` propagates Coro UP only if each cursor calls it. The PR-14 seam (operator.cpp:
14323-14344) forbids ALL parallel cursors from `SelectCoroMode`. **FIX:** lift that prohibition for the
COORDINATORS ONLY (`AggregateParallelCursor`/`OrderByParallelCursor`) — they own their `gen_`, resumed by
the single pull-task, so no concurrent-resume hazard; keep the prohibition on BRANCH cursors
(`ScanParallelCursor` etc. — shared `input_cursor_`, correctly Sync). Then a coro coordinator → Coro
propagates through `Produce` to the root → `WillDriveCoro()=true` → d1b dispatches the query as a
resumable task → the coordinator's `co_await` park reaches the pull-task boundary. CONVERT
`AggregateParallelCursor`/`OrderByParallelCursor` (give them a `DoPull` that schedules branches then
`co_await`s the park), NOT `ParallelMergeCursor`.

**FATAL-B — event-park does not propagate through v2's slot-based driver.** `ProgressAwaitable::
await_suspend` (priority_thread_pool.hpp) returns `bool`, unused handle, does NOT write
`*ctx.suspended_task_handle_ptr`. But `ResumePullStep` detects a suspend ONLY via that slot ⇒ the event-park
never becomes `Parked`. AND `WasParked()` is consumed/cleared by `ResumableWrapper::Run` BEFORE the d1b
pull-task body runs ⇒ the body can't read it. **FIX (new `EventParked` propagation, slot-independent):**
(1) give `ProgressAwaitable` the `ExecutionContext&` and on `await_suspend` set a new ctx flag
`ctx.event_parked` (the coro suspends, slot stays empty); (2) `ResumePullStep`, after `target.resume()`,
checks `ctx.event_parked` (BEFORE the slot/Result check) → returns a new `PullRunResult::EventParked`;
(3) `PullPlan::Pull` maps it to a new `PullOutcome::EventParked` (distinct from the yield `Parked`);
(4) the d1b pull-task body: on `EventParked` → return in the way that does NOT self-reschedule (the
`WorkerResumeEvent`/`NotifyProgress` will re-enqueue); on yield `Parked` → `return true` (self-reschedule),
unchanged. This is shared coro-driver code (also used by the working yield path) — change additively + RE-
VERIFY the yield path (d2 force-yield smoke + cursor_knob ParkResumeParity) stays green.

**HIGH-R6 — NOT enforced (spec was wrong).** `parallel_rewrite.hpp` `conflicting_types` =
{Aggregate, AggregateParallel, OrderBy, OrderByParallel} — does NOT include `ScanParallel`/`ParallelMerge`,
and there's no depth/inside-parallel guard. So nested parallel is NOT structurally prevented → two parked
coordinators could deadlock. **FIX (prerequisite, before relying on the no-deadlock argument):** add
`ScanParallel::kType` + `ParallelMerge::kType` to `conflicting_types` OR add an `inside_parallel_` depth
guard in `TryParallelizeOperator`. Forward-progress note: a parked coordinator FREES its worker (doesn't
hold one), so branches always get workers — the W<N starvation worry dissolves ONCE the region is coro
(coordinators truly park, not busy-spin) AND R6 holds.

**MEDIUM-R1 — `ProgressAwaitable::await_ready` bypasses the `in_flight_==0` barrier.** It returns true on
`Finished()` alone ⇒ the coordinator can resume + destroy the collection while a branch is still inside
`NotifyProgress` → UAF. **FIX:** `await_ready` must check `Finished() && InFlightZero()` (add the accessor),
or only `NotifyProgress` after `in_flight_` drains.

**LOW-R2 — the single-waiter `MG_ASSERT` is a process kill.** Make it a graceful `QueryException` in
release (keep the debug assert) so a planner bug / R6 violation is a query error, not a crash.

**R3/guard cleanup confirmed:** branch_contexts are sized N-1 + branch-0 inline as the merge base
(operator.cpp:14382-14539) → size-N + loop-from-0 + FRESH zeroed accumulator; REMOVE the now-dead
`main_thread` memory-tracking guard (track unconditionally for all N pool tasks).

**REVISED STAGING:** c3.0 (the `EventParked` propagation primitive in the shared driver + ProgressAwaitable
ctx wiring + await_ready in_flight check; focused harness test; RE-VERIFY yield path green) → c3.R6 (planner
nested-parallel guard + test) → c3.1 (convert AggregateParallel/OrderByParallel to coro DoPull + co_await
park + lift the coordinator-only seam guard; replace WaitOrSteal with park) → c3.2 (R3 restructure: branch-0
as task, size-N, fresh accumulator, delete postpone-hack) → c3.3 (shutdown NotifyAll cancellation) → c3.4
(TSan + workers=2,4 hammer + throughput-under-concurrency gate). Enterprise license + a forced-parallel plan
needed for c3.1+ tests.

## What c3 changes (vs APPROACH_B.md B2, adapted to live d1b/d2)
`ParallelBranchCursor::ExecuteBranchesInParallel` (operator.cpp:14382) today: branches 1..N-1 → pool tasks;
branch-0 runs INLINE on the caller; caller blocks in `collection_scheduler_->WaitOrSteal()`. Plus the
`ParallelMergeCursor::Pull` postpone-hack (operator.cpp:14290) that defers `Trigger()` until branch-0's
first input pass.
c3: branch-0 becomes a pool task like the others (ALL N equal pool tasks, R3); the coordinator becomes a
CORO cursor that `co_await`s a park awaitable until all branches finish, returning its worker meanwhile;
delete the postpone-hack + inline-branch-0 + `WaitOrSteal`. Merge (`UnifyContexts`) runs after resume,
unchanged.

## Why this composes with d1b/d2 (the key enabler)
d1b dispatches a query's pull as a resumable pool task IFF the root is coro-driven (`IsPullCoroDriven`).
Today the parallel cursors are intentionally Sync (PR-14: a SHARED `gen_` can't be resumed concurrently).
**But only the BRANCH sub-cursors share producers (ScanParallel) — the COORDINATOR is not shared.** So:
make the COORDINATOR coro (it `co_await`s park); keep the BRANCH sub-cursors Sync (each runs `cursor->Pull()`
on its own pool task, exactly as today). A coro coordinator ⇒ the query is coro-driven ⇒ dispatched as a
resumable task (d1b) ⇒ the coordinator's `co_await` park suspends up the coro chain to the pull-task
boundary ⇒ worker freed. This is precisely the d1b/d2 machinery, now consumed by the coordinator.

## THE NEW SUBTLETY — yield-kind vs event-kind park (must reconcile with d1b)
d1b's pull-task body, on `parked` (the bool out-param), does `return true` ⇒ B1 `ScheduleResumableTask`
wrapper RESCHEDULES it pinned (self-reschedule) — that is the YIELD kind (resume = immediate, the query
just relinquished its slice). The COORDINATOR park is the EVENT kind: it must NOT self-reschedule; it
registers a `WorkerResumeEvent`/progress waiter and stays parked until the LAST branch fires
`NotifyProgress`, which re-enqueues the continuation pinned.
B1 already has both paths: the yield path stashes a leaf handle (YieldPointAwaitable); the event path is
`CollectionScheduler::ProgressAwaitable` → `RegisterProgressWaiter` → `CurrentResumableTask::SetParked`
(TLS parked flag) → `WrapTask::WasParked()` ⇒ the wrapper returns WITHOUT rescheduling; `NotifyProgress`
re-enqueues. **The reconciliation:** when PullPlan::Pull returns Parked, the pull-task must distinguish
which kind it was and react correctly:
  - YIELD (YieldPointAwaitable, yield_requested): self-reschedule (d1b `return true`). Unchanged.
  - EVENT-PARK (coordinator co_await ProgressAwaitable): do NOT self-reschedule — the progress waiter is
    already registered (CurrentResumableTask::SetParked was called under the pool mutex); the wrapper's
    WasParked() path returns without reschedule and NotifyProgress re-enqueues.
Design: carry the kind on the Parked signal. The cleanest is to let the EVENT-park go through B1's
CurrentResumableTask path (which sets the WrapTask parked flag) and have the d1b pull-task check
`CurrentResumableTask::WasParked()` (or an equivalent ctx kind-flag set by the awaitable) BEFORE its
`return true`: if event-parked → `return false`-equivalent that the wrapper treats as "parked, do not
reschedule" (B1's existing WasParked semantics); else (yield) → `return true`. VERIFY this interplay end to
end: ProgressAwaitable.await_suspend (deep in the coro) → RegisterProgressWaiter → SetParked → suspend →
PullPlan drive returns Parked → pull-task body → wrapper WasParked → return-no-reschedule → last branch
NotifyProgress → RescheduleTaskOnWorker(pinned) → pull-task re-runs → SessionHL::Pull re-enters → coro
resumes the coordinator → merge → Finished.

## Binding constraints (APPROACH_B.md R1–R7 — carried, do not weaken)
- **R1 (HIGH):** park-join gates resume on `Finished() && in_flight_ == 0` (2-phase barrier), NOT
  `Finished()` alone — else a worker still inside `NotifyProgress` touches `progress_cv_` after the
  coordinator resumed + destroyed the collection → UAF. (B1's `in_flight_` already implements this; confirm
  the coordinator's resume waits for it.)
- **R2 (HIGH):** single progress waiter per `TaskCollection` (release-build MG_ASSERT). Use a distinct
  collection per park level. Nested parallel is R6-forbidden so one level is the norm.
- **R3 (HIGH):** branch-0 becomes a pool task ⇒ size `branch_contexts`/collectors **N** (was N-1), loop
  `i=0`, `metadata_i=i`, and a FRESH zeroed accumulator as the merge base (else branch-0's
  hops/counters/collectors/profiling are silently dropped). The `main_thread`-steal memory-tracking guard
  (operator.cpp:14419) must be revisited (no inline branch-0 / main-thread anymore).
- **R6:** nested parallel forbidden at plan time (`InsideSubqueryArm` / C1 planner rule) ⇒ a parked
  coordinator can't be a branch of another parked coordinator ⇒ no all-workers-parked deadlock. CONFIRM
  the rule still holds on this branch's planner.
- **R7:** the coordinator must `Trigger()` (SetCollection+SetPool, schedule all N) BEFORE parking, else
  single-threaded collapse. The SCHEDULED task state is overloaded (fresh-dispatch vs resume) — do not
  conflate.
- **pool_ atomic** (C0): `CollectionScheduler::pool_` is atomic (already on this branch via B1).

## Shutdown-while-parked (the d2-deferred item lands HERE — event-park makes it reachable)
A coordinator parked on a `WorkerResumeEvent` holds `shared_from_this` (session alive) + a suspended
coroutine (the PullPlan). If the client disconnects / `ShutDown()` runs while parked, the event may never
fire ⇒ the continuation never runs ⇒ Session + coroutine frame LEAK (concurrency-debugger d1b finding #4/#5).
Fix: `PriorityThreadPool::ShutDown()` must, for every live `WorkerResumeEvent`, `NotifyAll()` (or a
cancellation token) so parked continuations are released and can observe shutdown + tear down. Needs a
registry of live events (or route through the pool). Plus the `~PullPlan` shutdown guard (c-core-2) covers
the coroutine teardown once the continuation is released. VERIFY no UAF/leak on disconnect-mid-park.

## Staged implementation
- **c3.1** — make ONE coordinator (ParallelMerge) coro + park: convert `ParallelMergeCursor` to co_await
  the ProgressAwaitable; reconcile the yield-vs-event-park kind in the d1b pull-task; keep
  ExecuteBranchesInParallel's branch scheduling but route the coordinator's wait through park instead of
  WaitOrSteal. Gate: a single ParallelMerge query (enterprise) returns identical results; the coordinator
  actually parks (probe) + resumes; TSan.
- **c3.2** — R3 restructure: branch-0 as a pool task (size-N, fresh accumulator); delete inline-branch-0 +
  the postpone-hack. Apply to AggregateParallel/OrderByParallel coordinators too. Gate: parallel results ==
  serial corpus across grouped/orderby/distinct; workers=2,4 hammer (the p3 C3 race case).
- **c3.3** — shutdown-while-parked NotifyAll cancellation + disconnect-mid-park test (ASan/TSan, no leak).
- **c3.4** — TSan campaign + the concurrent-parallel-load throughput gate (APPROACH_B.md B4: park should
  improve throughput-under-concurrency with no single-query regression).

## Test/verification notes
- Enterprise: needs `MEMGRAPH_ORGANIZATION_NAME`/`MEMGRAPH_ENTERPRISE_LICENSE` + a plan that actually
  parallelizes (ScanParallel/ParallelMerge). Confirm how to force a parallel plan (num_threads / the
  parallel planner rule).
- The force-park seam does NOT apply (that drives YIELD); c3's park is event-driven by branch completion,
  so the test is a real parallel query (branches genuinely run) — the coordinator parks because it's
  waiting, not because forced.
- Reuse the d2 real-Bolt smoke harness (neo4j driver) for the enterprise parallel queries.
