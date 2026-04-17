# Parallel Resumable Branch Fix

## Goal

Fix the parallel aggregation correctness regression introduced after enabling
cooperative suspension in parallel execution paths, by making branch execution
use the same scheduler-managed resumable-task model as the session/query entry
loop.

The immediate target is the failing global aggregate case:

```cypher
USING PARALLEL EXECUTION
MATCH (n)
RETURN count(*), sum(n.p), avg(n.p), min(n.p), max(n.p)
```

## Why This Follow-Up Exists

The first regressing commit is:

- `b0f44a087` `query: localize ScanParallel producer batches`

That change made the `ScanParallelCursor` producer path suspendable while
building the next batch locally. The producer-side change itself is reasonable,
but it exposed a mismatch in how parallel worker branches are driven.

Before this series, branch tasks effectively ran synchronously to a row or to
completion. Now a branch can suspend in the middle of `ScanParallel` while
waiting for producer progress, but the branch driver still treats that state as
if the branch had finished.

## Current Diagnosis

### What works

- serial aggregation is correct
- `ScanParallelCursor` producer publishes a complete frame and chunk set only
  after the upstream pull completes
- active branches can accumulate rows correctly
- some branches complete correctly and post-process `COUNT` and `AVG`

### What fails

Some worker branches suspend in `ScanParallelCursor::ProducerProgressAwaitable`
while waiting for the producer to advance, but the branch driver records them
as:

- `status=Done`
- `delta=0`

even though those branches already accumulated partial aggregation state.

That leaves branch-local `AggregateCursor` state in a half-finished form:

- `sum/min/max` reflect processed rows
- `count(*)` is still zero because post-processing did not run
- `avg` still equals the accumulated sum because it also was not finalized

Example observed branch-local rows:

- `agg2=0 agg3=1769 agg4=1769 agg5=65 agg6=77`
- `agg2=0 agg3=2610 agg4=2610 agg5=14 agg6=100`

These are not valid final aggregate rows; they are pre-post-processing state.

### Second bug exposed by reuse

`AggregateParallelCursor` reuses branch-local aggregation buffers. For a reused
cursor with `group_by_.empty()`:

- `ProcessAll()` can return `false`
- `aggregation_` can still be non-empty because it was reused
- `AggregateCursor::DoPull()` then initializes `aggregation_it_` and yields the
  stale row anyway

So we currently have both:

1. suspended branches misclassified as done
2. reused aggregate state yielded after a no-input pull

## Root Cause

Worker branches are driven through:

- `Cursor::Pull(...) -> PullAwaitable::ResumeAwaitable`
- a branch-local resume step that resumes either:
  - the stored inner suspended handle from `YieldPointAwaitable`
  - or the root branch generator handle

Historically this path only knew how to interpret one suspension protocol correctly:

- scheduler yield through `YieldPointAwaitable`, which stores the suspended
  handle in `ExecutionContext::suspended_task_handle_ptr`

But `ProducerProgressAwaitable` suspends differently:

- it registers the coroutine handle in `producer_waiters_`
- it does not mark `suspended_task_handle_ptr`

So the branch step resumed the branch, saw no explicit scheduler yield marker,
found `Result() == false`, and concluded `Done` instead of `Yielded`.

That is the semantic mismatch.

## Desired Design

Match the session/query scheduling model:

- each branch is scheduled as a resumable pool task
- the task returns `true` when it yielded and must be rescheduled
- the task returns `false` only when it is actually complete or failed
- the scheduler owns pause/resume semantics uniformly

This is the same shape as the communication/session work loop:

- run
- if yielded, reschedule on the same worker
- otherwise finish

In practice the branch path needs to distinguish two different suspension modes:

1. immediate scheduler yield
   - a worker-local preemption point
   - should be rescheduled immediately, preferably on the same worker
2. parked on external progress
   - the branch cannot continue until `ScanParallel` producer progress occurs
   - should not be requeued immediately
   - should instead be resumed by the producer-progress event itself

The current bug exists because worker branches only have an implicit two-way
interpretation:

- yielded
- done

but `ProducerProgressAwaitable` really creates the third case.

## Strategy

### Implementation strategy

Fix this as a follow-up on top of `b0f44a087`, then cherry-pick forward into the
later commits in the coroutine-parallel stack once validated.

Why:

- this is the first bad commit in the bisection
- the regression is easier to reason about at the point where it first appears
- later commits should receive the fix as a clean follow-up rather than hiding it
  inside unrelated slices

### Technical strategy

Refactor `ParallelBranchCursor` so that worker branches remain true
scheduler-owned resumable tasks, but can also park on external progress without
being mistaken for completed work.

Concretely:

1. preserve branch-local state inside the resumable worker lambda:
   - local frame
   - local execution context
   - persistent `ResumeAwaitable`
   - pre-pull/post-pull lifecycle state
   - suspended inner coroutine handle owned by the branch task
2. extend resumable-task scheduling with an explicit parked-on-event path:
   - immediate scheduler yield keeps the current same-worker reschedule behavior
   - producer-progress suspension parks the task until the event wakes it
3. make `ProducerProgressAwaitable` resume the branch task, not the inner cursor
   coroutine in isolation
4. only mark branch completion after a real row/exhaustion result
5. remove the accidental "suspended means done" interpretation from the
   branch-specific driver
6. guard `AggregateCursor::DoPull()` so a `ProcessAll() == false` path cannot
   emit reused stale aggregation rows

## Detailed Implementation Plan

### Slice 1: Generic resumable-task self-park support in `utils`

Goal:

- let any resumable task register itself to be resumed by an external event
- keep this generic so future parallel operators can reuse it

Changes:

- add a generic current-resumable-task context in `PriorityThreadPool`
  / `TaskCollection` execution wrappers
- expose a helper that allows the currently running resumable task to register
  itself on a `WorkerResumeEvent`
- keep the existing resumable task signature unchanged for now
- have the pool/task wrapper detect:
  - normal completion
  - immediate scheduler yield
  - self-park on external progress

Expected result:

- a resumable task can return control to the pool without being treated as
  finished
- the wakeup event, not the pool wrapper, becomes responsible for rescheduling
  the parked task

Tests:

- extend `tests/unit/utils_priority_thread_pool.cpp`
- add unit coverage that:
  - a resumable task can self-park and later continue
  - self-parked tasks are not immediately requeued
  - wakeup resumes the task on the original worker when possible

### Slice 2: Distinguish parked cursor suspension from scheduler yield

Goal:

- allow cursor awaiters to say "the current task parked on external progress"
  without redefining scheduler yield semantics

Changes:

- add a small execution-context marker for "task parked during this pull step"
- make `ProducerProgressAwaitable`:
  - save the suspended cursor handle
  - register the current resumable task on the producer-progress event
  - mark the task as parked
- keep `YieldPointAwaitable` unchanged as the scheduler-yield path

Expected result:

- `RunPullToCompletion(...)` can continue to mean "cursor suspended"
- branch/task drivers can distinguish:
  - ordinary scheduler yield
  - parked-on-event suspension

Tests:

- add focused unit coverage near `cursor_awaitable` or query-yield tests if a
  minimal isolated case is practical

### Slice 3: Refactor `ParallelBranchCursor` to use parked-task semantics

Goal:

- remove the accidental "producer wait means branch is done" behavior

Changes:

- update the branch worker closure in `ExecuteBranchesInParallel()` to:
  - preserve branch-local pull state
  - treat parked pulls as "task still live"
  - skip post-pull/unification completion work until a real row or real done
- preserve current behavior for genuine scheduler-driven yields

Expected result:

- branches that wait for `ScanParallel` producer progress remain live
- post-processing runs only after the branch really reaches row/done

Tests:

- add or update targeted parallel-query tests when practical

## Current Validation

Validated so far:

- `./build/tests/unit/utils_priority_thread_pool`
  - passes
  - includes new self-park coverage for both standalone resumable tasks and
    `TaskCollection` tasks
- user-confirmed targeted previously failing parallel e2e correctness test now
  passes after the branch self-park fix

Still blocked locally:

- `cmake --build build -j3 --target interpreter`
  - currently still fails in this workspace because of the unrelated existing
    module/compiler mismatch around `memgraph.utils.fnv.pcm`
  - this prevents local execution of the new interpreter regression test even
    though the targeted e2e behavior is now reported as fixed
- validate against the existing failing aggregate repro

### Slice 4: Harden `AggregateCursor` reuse

Goal:

- prevent stale reused aggregate rows after a no-input pull

Changes:

- make `AggregateCursor::DoPull()` return `false` immediately when
  `ProcessAll()` returns `false`, even for global aggregation

Expected result:

- reused branch aggregation state is never emitted after a no-input pull

Tests:

- add the smallest unit or query-plan regression test that exercises
  aggregate reuse if practical

### Slice 5: Validate and cherry-pick forward

Goal:

- confirm the fix on the first bad commit, then propagate it cleanly

Changes:

- rebuild `memgraph`
- rerun the targeted parallel aggregate repro
- rerun broader parallel correctness coverage if the targeted case passes
- record the exact validation and cherry-pick guidance in the docs

## Open Questions To Confirm During Implementation

1. Can the resumable-task wrapper expose enough context for external progress
   awaiters to reschedule the task itself, without widening unrelated pool APIs?
2. Should the parked-task path live in `TaskCollection`, `PriorityThreadPool`,
   or a small resumable-task TLS/context helper used by both?
3. Can we keep `ParallelBranchCursor::ExecuteBranchesInParallel()` source
   compatible for other parallel operators while improving only the branch task
   lifecycle?

## Validation Plan

Primary:

- rebuild `memgraph`
- rerun the targeted parallel aggregate repro

Expected indicators of success:

- no branch with processed rows ends as `status=Done` before post-processing
- all branch-local rows reaching merge have finalized `COUNT` and `AVG`
- no reused branch emits stale rows after `ProcessAll() == false`
- merged result matches serial execution:
  - `count(*) = 203`
  - `sum(n.p) = 10100`
  - `avg(n.p) = 50.5`
  - `min(n.p) = 1`
  - `max(n.p) = 100`

Secondary:

- rerun the broader parallel correctness workload if the targeted case passes

## Status

Current status:

- slices 1-3 (self-park, parked cursor, branch refactor) are working
- slice 4 (AggregateCursor reuse hardening) applied as fix F7 + process-all guard
- slice 5 (validation) in progress

## New Bug: TryExecuteOneIdleTask throws on yielded branch tasks

### Symptom

`test_exception_single_invalid` fails ~15% of runs with 8 workers:

```
DatabaseError: An unknown exception occurred, this is unexpected.
Real message: WaitOrSteal cannot handle yielding tasks. Use co_await Finished() instead.
```

### Root cause

`ExecuteBranchesInParallel` wait loop calls
`collection_scheduler_->TryExecuteOneIdleTask()` at `operator.cpp:10997`.
This steals an IDLE branch task and runs it inline on the main thread.

Since commit `ed148a02f` ("enable scheduler yields in background branches"),
branch tasks can yield via `AbortCheck` / `YieldPointAwaitable` when the
scheduler sets `yield_requested`. The task lambda returns `true` (yielded),
and `TryExecuteOneIdleTask` throws `runtime_error`.

This only fires under concurrent load (8+ workers) when the scheduler
preempts a branch task that `TryExecuteOneIdleTask` is trying to run inline.

### Fix plan

Change `TryExecuteOneIdleTask` to accept an optional `PriorityThreadPool*`
parameter. When a task yields and the pool is available:

1. transition the task state from `STOLEN` to `SCHEDULED`
2. schedule `WrapTask(index, pool)` on the pool so the task resumes on a
   worker thread (preserving the same-worker resumption guarantee)
3. return `true` (progress was made)

When the pool is null (legacy `WaitOrSteal` sync path), throw as before.

`CollectionScheduler::TryExecuteOneIdleTask()` passes its `pool_` member,
which was set by `SetPool(context.worker_pool)` in the branch join setup.

### Call sites

| Location | Pool available? | Fix behavior |
|---|---|---|
| `operator.cpp:10997` wait loop | Yes (via `CollectionScheduler`) | Reschedule on pool |
| `operator.cpp:10935` teardown (removed at branch tip) | No | N/A (removed) |
| `priority_thread_pool.cpp:517` `WaitOrSteal()` | No | Throw (unchanged) |
