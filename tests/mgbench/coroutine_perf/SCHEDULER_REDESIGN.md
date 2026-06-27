# Coroutine-native scheduler redesign (supersedes APPROACH_B mechanism choice)

Branch: `feat/coroutine-scheduler-redesign` (off the approach-B branch, which is off the gate branch).
Goal: redesign parallel execution + the thread pool to **assume cooperative coroutine yield**, dropping
the machinery that exists only because today's tasks cannot yield. Subsumes approach B (the parallel
self-park) — the same park/resume primitive serves both the parallel coordinator and serial-cursor
yield.

> Precondition (user): the coro + split foundation is **sound** — perf-gated on bare metal (SPLIT
> ~+1% instr/query vs ALL ~+10%), `cursor_parity` proves sync≡coro, knob/interpreter/profile green.
> **One caveat this redesign must confront first:** the *yield* path has never fired in production
> (no `PullDriverScope(Enabled)` in the live drive; nothing sets `yield_requested`). The redesign
> depends on yield working, so **making yield real is step 1** (S1), before any scheduler simplification.

## Thesis: today's pool is built around NON-yielding tasks

`src/utils/priority_thread_pool.{hpp,cpp}` (v2, close to master):

- **Mixed (LP) workers + a separate HP worker pool.** HP exists as an escape hatch — code comment:
  *"ideally tasks yield and this isn't needed."* Each worker owns a `priority_queue<Work>` and
  spins (Phase 3) then `cv_.wait` (Phase 4) when idle.
- **A 100ms monitor (`sched_mon`)** that detects a worker *stuck on a task* (`last_task_` unchanged
  AND `working_` AND `has_pending_work_`) and **migrates** the head of its queue to a hot LP thread or
  an HP thread. Pure workaround: a long task blocks everything queued behind it, so the monitor
  reshuffles the queue out from under it.
- **Parallel coordinator blocks** in `WaitOrSteal` (steal-then-futex-`Wait`), tying up its thread.

Every one of these is a symptom of "a task, once running, holds its worker until it returns." Coro
yield removes the premise.

## Redesigned scheduler (coroutine-native)

1. **No HP thread pool.** A single uniform worker class. Priority still ordered within each worker's
   `priority_queue` (HP id bit retained). HP work no longer needs a dedicated pool because LP workers
   are never stuck for long — they yield. (Removes `hp_workers_`, the `Priority::HIGH` worker
   template instantiation, and the HP branch in scheduling.)
2. **Cooperative yield replaces monitor-migration.** Instead of the monitor *moving queued work off* a
   stuck worker, a worker with work queued behind a long-running coro task signals `yield_requested`
   for that task; the task parks at its next `YieldPointAwaitable`, the worker drains its queue, and
   the parked task is resumed later. The monitor shrinks from "lock+migrate queue entries" to
   "set a yield flag" (or disappears if workers self-detect queue depth at yield checkpoints).
3. **Park/resume primitive (shared).** Both (a) the parallel coordinator (approach B) and (b) a
   yielded long task suspend via the same mechanism: the running task's continuation is captured, the
   worker returns to its loop to run other work, and a resume event reschedules the continuation. This
   is the one genuinely new concurrency primitive; everything else is deletion/simplification.
4. **Different sleep.** With park, a worker **never blocks inside a task** — it only sleeps when the
   *global* supply of runnable work is empty. Sleep becomes purely demand-driven (one wait on global
   emptiness), not per-task futex waits scattered through `WaitOrSteal`/`Wait`. Workers parking a task
   immediately look for the next runnable item rather than sleeping.

Net: delete the HP pool + the migrate-on-stuck monitor + the per-task futex waits; add one park/resume
primitive + a yield-request signal. The scheduler gets smaller and the worker loop simpler.

## Hard sequencing (this is the spine of the plan)

Nothing about the scheduler can be simplified until yield demonstrably works in production, because the
simplifications *rely* on tasks yielding. So:

- **S1 — make yield real on the SERIAL path + prove it.** Wire `PullDriverScope(Enabled)` into the
  production drive (`PullPlan::Pull`), have the drive loop handle the `Yielded` result from
  `ResumePullStep` (re-resume the stashed leaf), and provide a way to raise `yield_requested` for a
  running query. Prove with a real query that it yields mid-pull and resumes to byte-identical results
  (extend `cursor_yield` / `cursor_knob`). **No scheduler change yet** — yield just has to fire and be
  correct. This is the foundation and the highest-risk-retirement step.
- **S2 — park/resume primitive in the pool.** A `WorkerResumeEvent`-style park: capture the running
  task's continuation, return the worker to its loop, resume on event. Carry the **verified
  approach-B constraints**: R1 (resume gated on `Finished() && in_flight_==0`, 2-phase barrier — no
  `progress_cv_` UAF), R2 (single-waiter abort → distinct collection per level or multi-waiter list),
  atomic `pool_`. TSan in isolation.
- **S3 — parallel coordinator self-park (approach B, subsumed).** All N branches as pool tasks (R3:
  size N, loop i=0, fresh accumulator), coordinator `co_await`s the S2 park instead of `WaitOrSteal`.
  R7 (Trigger before park), R6 (nested parallel forbidden at plan time — re-confirm on v2 planner).
- **S4 — drop the HP worker pool.** Single worker class; HP ordering stays intra-queue.
- **S5 — replace monitor-migration with the yield signal.** Monitor (or a cheaper mechanism) sets
  `yield_requested` on workers with queued work behind a long task instead of migrating queue entries.
- **S6 — redesign sleep** around global-emptiness + park, removing per-task futex waits.
- **S7 — gates.** TSan across park/resume/yield/ScanParallel; the 320-run grouped/nested/orderby
  hammer @ workers=2&4; abort/interrupt mid-pull and mid-parallel; undersized-pool progress; flag-OFF
  byte-identical; perf: single-query no-regression + concurrent-load throughput win.

S1–S3 deliver the throughput payoff (yield + parallel park). S4–S6 are the scheduler simplification
the coro model unlocks. Each S is independently buildable + TSan-gated; ship in that order.

## Risk register (concurrency-critical)

- **Yield correctness (new in prod):** S1 is the first time `await_suspend` stashes a real leaf handle
  and the drive re-resumes it under load. Mid-pull state must live in cursor members (it does — the
  conversions were built for this), and abort/exception across a suspend must surface (the
  `await_resume` throw path + `RethrowIfException` in `ResumePullStep` cover it — re-verify under a
  forced-yield abort test).
- **R1/R2/R6/R7 + atomic `pool_`** from the verified approach-B register (APPROACH_B.md) — binding for
  S2/S3.
- **HP removal regressions:** anything that relied on HP latency (e.g. cancellation tasks, snapshot
  triggers) must be re-checked — confirm what currently enqueues `Priority::HIGH` before deleting the
  HP pool (S4 gating audit).
- **Monitor removal:** the migrate-on-stuck monitor also covers *non-coro* (Sync) long tasks (e.g. a
  big synchronous scan below the split point). Those still can't yield. So S5 cannot blindly delete
  migration — Sync subtrees below the split still need *some* anti-starvation story (keep a reduced
  monitor for the Sync region, or ensure the split keeps Sync regions short). **Open question — must
  resolve in S5 design.**

## Open questions for the user / next design pass
- S5's Sync-region starvation story (above): keep a slimmed monitor, or rely on the split keeping Sync
  subtrees bounded? This gates how much of the monitor can actually be deleted.
- Worker count: drop to a single uniform pool sized to cores, or keep a small reserved set?
