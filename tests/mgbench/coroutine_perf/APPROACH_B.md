# Approach B — parallel-branch self-park scheduling (design + plan)

Branch: `feat/coroutine-parallel-park-approach-b` (off the gate branch). This implements the
**throughput** optimization deferred from PR-14: the parallel-query coordinator self-parks (frees its
worker) while branches run, instead of blocking in `WaitOrSteal`.

> The design + risk register below was **verified on 2026-06-23** against the p3 stack (artifact
> `opencode-work/coroutine-cursors-split/2026-06-23--P3.2-design-verification.html`). It is **not**
> re-derived here — it is ported into v2 terms. The constraints R1–R7 are binding.

## Status: correctness vs throughput

Approach A (shipped, PR-14) is **correct and deadlock-free** — a blocked coordinator steals its own
branches inline, so progress is guaranteed even if every worker is a blocked coordinator. Approach B
is therefore a **pure throughput optimization, not a correctness fix.** Under concurrent load it stops
a parallel query from tying up a worker while it waits.

## What approach B changes

Approach A (`ParallelBranchCursor::ExecuteBranchesInParallel`, operator.cpp): branches 1..N-1 →
pool; branch 0 runs inline on the caller; caller **blocks** in `collection_scheduler_->WaitOrSteal()`
(steal-then-futex-`Wait()`).

Approach B (uniform): branch 0 becomes a pool task like the others (**all N** are equal pool tasks);
the caller becomes a pure coordinator that **`co_await`s a progress/park awaitable** until all
branches finish, returning its worker to the pool meanwhile (and possibly running another branch on
the freed thread). Merge (`UnifyContexts`) runs after resume, unchanged.

## The v2 ⇄ p3 gap (important)

The p3 stack built the park infrastructure **in the pool**; v2 did **not** port it. v2's
`src/utils/priority_thread_pool.{hpp,cpp}` is the simpler master pool (233 lines) with only
`Wait()`/`WaitOrSteal()` (futex). p3's (897d8e277, 475 lines) additionally has:

- `WorkerResumeEvent` — a multi-waiter resume event.
- `CollectionScheduler::RegisterProgressWaiter(resume_task, pool, worker_id, ...)` + `NotifyProgress()`.
- `ParkAwaiter`-style self-park (`RegisterWaiter(event, observed_epoch)`, TLS parked flag, `ClearParked`).
- `progress_cv_` + the `in_flight_` 2-phase gate.

**So approach B on v2 = (B1) port that proven pool infra, then (B2) apply the verified
ParallelBranchCursor restructuring.** The yield protocol already on v2 (`YieldPointAwaitable` /
`PullDriverScope` / `ResumePullStep`'s `Yielded`) is the *serial-cursor* mid-pull yield — a SEPARATE,
still-dormant concern. Approach B does not depend on it.

## Verified constraints (R1–R7) — binding

- **R1 (HIGH):** park-join must gate resume on `Finished() && in_flight_ == 0`, **not** `Finished()`
  alone — else a worker still inside `NotifyProgress` touches `progress_cv_` after the coordinator
  resumed and destroyed the collection → UAF. Mirror `Wait()`'s 2-phase barrier (p3
  priority_thread_pool.cpp ~595-607): `in_flight_` incremented before the FINISHED store, decremented
  at `WrapTask` exit after `NotifyProgress`, on every path including exceptions.
- **R2 (HIGH):** `RegisterProgressWaiter` asserts a single waiter (release-build **abort** if two
  parks hit the same `TaskCollection`). Use a distinct `TaskCollection` per park level, or the
  multi-waiter `WorkerResumeEvent` list.
- **R3 (HIGH):** today `branch_contexts`/collectors are sized **N-1** and branch 0 mutates the parent
  `context` as the merge base (`UnifyContexts`). Making branch 0 a pool task requires: size **N**,
  loop `i = 0`, `metadata_i = i`, and a **fresh zeroed accumulator** for the merge base — else
  branch 0's hops/counters/collectors/profiling are silently dropped.
- **R4:** the `main_thread != this_thread` memory-tracking guard is already correct for a worker-task
  branch 0 — no inversion needed (the original design overclaimed). Only the structural loop change.
- **R6:** **nested parallel is forbidden at plan time** (`InsideSubqueryArm`, the C1 planner rule), so
  a parked coordinator can never itself be a branch of another parked coordinator → the
  "all-workers-parked" deadlock is structurally impossible. Confirm this rule still holds on v2's
  planner before relying on it.
- **R7:** the coordinator must `Trigger()` (SetCollection + SetPool then schedule) **before** parking,
  else single-threaded collapse. The `SCHEDULED` task state is overloaded (fresh-dispatch vs
  resume) — do not conflate.
- **pool_ race** (p3 C0): `CollectionScheduler::pool_` must be atomic (Trigger nulls it while a worker
  may read it). Port that fix with the infra.

## PR breakdown

- **B1 — port the pool park infrastructure.** Bring `WorkerResumeEvent`, `RegisterProgressWaiter`,
  `NotifyProgress`, the self-park awaiter, `in_flight_` (R1), atomic `pool_`, multi-waiter (R2) from
  p3's `priority_thread_pool.{hpp,cpp}` into v2's. Standalone; port p3's pool unit tests too. No
  cursor changes. Gate: the pool tests + TSan on park/resume/notify in isolation.
- **B2 — restructure `ExecuteBranchesInParallel`.** All N branches as pool tasks (R3), coordinator
  `co_await` park (R7) gated on `Finished() && in_flight_==0` (R1), distinct collection per level (R2).
  Gate: flag-ON parallel == serial corpus; results invariant.
- **B3 — gates.** TSan on coordinator-park + ScanParallel; the 320-run hammer (grouped/nested/orderby
  aggregates @ workers = 2 and 4 — the case the p3 C3 race tripped); abort/interrupt mid-parallel;
  undersized-pool (W < N) progress; flag-OFF parallel e2e byte-identical.
- **B4 — perf.** Re-run `parallel_ab.sh` + a concurrent-load harness (many simultaneous parallel
  queries) on the perf box: approach B should improve throughput-under-concurrency with no
  single-query regression.

## Empirical gate (the throughput problem to demonstrate)

Approach B's benefit is concentrated under **concurrent parallel load** (R6 rules out nested). The B4
harness must show: with C concurrent parallel queries and a W-worker pool, approach A wastes a worker
per blocked coordinator while approach B keeps workers busy → higher aggregate throughput. A
single-query parallel run must not regress.
