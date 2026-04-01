# Coroutine Parallel Yield Enablement

## Goal

Allow scheduler-driven yield to work end-to-end across coroutine cursors, including
parallel execution paths, without breaking correctness, continuation lifetime, or
worker-pool fairness.

The current system already supports yield in ordinary coroutine cursor chains when
every layer remains `co_await`-based. The remaining blockers are the places where
we explicitly detach the yield signal or where we still rely on blocking joins and
synchronous nested cursor execution.

## Current status

Completed slices:
- `8beda2a55` `docs: outline coroutine parallel yield enablement plan`
- `63e18349a` `query: make parallel branch joins wait cooperatively`
- `d55a7389f` `query: wait for branch progress during cooperative joins`
- `9270ac03b` `query: coordinate ScanParallel batch producers`

What is true now:
- the parent parallel join no longer blocks in `WaitOrSteal()`
- the cooperative join now waits on branch progress instead of blind thread-yield polling
- `ScanParallelCursor` now has explicit producer/waiter coordination for shared batch publication
- branch tasks still do **not** yield
- the shared upstream pull inside `ScanParallelCursor` still temporarily disables yield
- nested synchronous cursor consumers such as `EvaluatePatternFilter` are unchanged

Immediate next goal:
- replace polling-style branch waiting with a true coroutine-friendly branch completion wait
- only after that, enable branch yields and continue the `ScanParallelCursor` refactor

## Working rules for this series

Every new implementation slice should do all of the following before commit:
- add or update tests for the new logic when practical in the same slice
- run the most targeted validation available and record what was run
- update this task document with:
  - completed work
  - changed assumptions or design adjustments
  - known remaining risks
- keep commits scoped to the files changed by that slice only

PR / review summaries should explain:
- the problem being addressed
- the behavior change
- why this approach is safe as an incremental step
- what still remains intentionally out of scope

## Local build status

Current build command used during this work:

```bash
cmake --build build -j3 --target memgraph
```

Current local failure:
- unrelated planner/module build mismatch, not caused by the coroutine parallel-yield changes
- failing files:
  - `src/planner/include/planner/core/eclass.hpp`
  - `src/planner/include/planner/core/enode.hpp`
- observed error shape:
  - module file built from a different branch / compiler mismatch
  - subsequent `ENodeId` resolution failures in planner sources

This means local full-build verification for this series is currently blocked by unrelated
planner module state. Targeted validation and user-local builds are still useful until that
separate issue is cleared.

## Current blockers

### 1. Parallel branch join still blocks

Code:
- `src/query/plan/operator.cpp` `ParallelBranchCursor::ExecuteBranchesInParallel`
- `src/utils/priority_thread_pool.cpp` `TaskCollection::WaitOrSteal`

Current behavior:
- branch worker contexts explicitly set `yield_requested = nullptr`
- the parent branch join uses `WaitOrSteal()`
- `WaitOrSteal()` throws if a task yields

Why it matters:
- parallel sections become scheduler black holes
- the main query worker is occupied while waiting for sibling branches
- this prevents parallel sections from cooperating with the limited worker pool

### 2. Shared `ScanParallelCursor` state cannot survive suspension

Code:
- `src/query/plan/operator.cpp` `ScanParallelCursor::DoPull`

Current behavior:
- the shared upstream pull temporarily nulls `yield_requested`
- this avoids suspending while holding branch-sensitive shared scan state

Why it matters:
- only one branch can safely advance the shared input batch today
- losing branches effectively wait on the shared scan path instead of yielding
- continuation ownership is mixed together with shared scan coordination state

### 3. Nested synchronous cursor execution cannot propagate yield

Code:
- `src/query/plan/operator.cpp` `EvaluatePatternFilterCursor`
- `src/query/trigger.cpp`

Current behavior:
- nested cursor execution is driven with synchronous `RunPullToCompletion(...)`
- `EvaluatePatternFilter` explicitly nulls `yield_requested` and `suspended_task_handle_ptr`
- triggers intentionally run synchronously and likely should remain non-yielding

Why it matters:
- expression evaluation still contains a synchronous execution island
- not every consumer of a cursor chain is currently coroutine-aware

## Design principles

1. Preserve the existing coroutine model.
   Yield already works when every layer stays `co_await`-based. We should extend
   that model rather than inventing a second yield mechanism.

2. Distinguish preemption from blocked progress.
   There are two different reasons to stop running:
   - scheduler preemption: another task should run
   - local blocked progress: this task cannot currently make progress

3. Avoid holding workers while waiting on other workers.
   Blocking joins and busy waits should be replaced with resumable waiting or, as
   an incremental step, cooperative polling with yield checkpoints.

4. Separate shared coordination state from continuation-owned state.
   A coroutine that may suspend must not borrow mutable shared state that another
   branch can overwrite before resumption.

## Phased implementation plan

### Phase 1: Make branch join cooperative without enabling branch yield yet

Objective:
- stop blocking the main worker in `WaitOrSteal()`
- keep background branches non-yielding for now

Changes:
- add a non-blocking `TaskCollection` API that can steal and run only idle tasks
- replace `collection_scheduler_->WaitOrSteal()` with a coroutine loop that:
  - executes any idle branch tasks locally
  - checks `Finished()`
  - hits `AbortCheck(context)` while waiting
  - yields the OS thread briefly when no progress is available

Expected result:
- the waiting branch join becomes scheduler-cooperative
- branch workers still run to completion
- this is a safe stepping stone before enabling yield inside branches

Risks:
- temporary spin/yield loop may still be less efficient than a proper awaitable join
- but it is simpler and isolates the first behavioral change

Status:
- completed, but evolved slightly during implementation
- first step replaced blocking join with cooperative polling
- second step replaced blind polling with progress-aware waiting

### Phase 2: Introduce a resumable branch completion wait

Objective:
- replace cooperative polling with a true awaitable join

Changes:
- extend `TaskCollection` with completion notifications appropriate for coroutine waiting
- add a branch-join awaitable or equivalent `co_await Finished()`
- remove the need for blocking wait and manual polling in `ParallelBranchCursor`

Expected result:
- parent branch coroutine suspends while waiting for branch completion
- worker is free to run other work

### Phase 3: Allow yielding branch tasks

Objective:
- remove `yield_requested = nullptr` in branch contexts

Changes:
- teach `TaskCollection` / pool logic to tolerate yielded resumable tasks in branch collections
- keep branch continuation state valid across resumes
- preserve exception propagation and context unification

Expected result:
- branch execution itself becomes scheduler-cooperative

Open design point:
- distinguish "yield because preempted" from "yield because blocked on local condition"
- the latter should not necessarily be pinned back to the same worker as the first low-priority continuation

### Phase 4: Split `ScanParallelCursor` shared vs per-branch state

Objective:
- let branches suspend safely while interacting with the parallel scan source

Changes:
- separate shared batch/chunk assignment from per-branch coroutine progress
- avoid suspending while borrowing mutable shared frame/generator state
- optionally introduce a blocked-progress signal when a branch cannot currently obtain work

Expected result:
- branches waiting on shared scan progress can stop occupying workers

### Phase 5: Revisit `ParallelMerge`

Objective:
- remove hacky scheduling assumptions inherited from the blocking model

Changes:
- reevaluate the "schedule after first upstream pass" behavior
- ensure scheduling boundaries match coroutine semantics rather than legacy fork/join constraints

### Phase 6: Decide policy for nested synchronous cursor consumers

Objective:
- clearly define whether every cursor consumer should support yield

Changes:
- likely keep triggers intentionally non-yielding
- decide whether `EvaluatePatternFilter` should remain synchronous or gain an async expression path

## Implementation checklist

### Slice A
- [x] Add reusable non-blocking task-steal API to `TaskCollection`
- [x] Add `CollectionScheduler` wrapper for the new API
- [x] Replace `ParallelBranchCursor` blocking join with cooperative wait loop
- [x] Improve cooperative wait loop to wait for branch progress instead of blind thread-yield polling
- [x] Add unit coverage for `TaskCollection::TryExecuteOneIdleTask()` and `TaskCollection::WaitForProgress()`
- [x] Verify the targeted scheduler/task-collection behavior with dedicated unit tests

### Slice B
- [ ] Add coroutine-friendly branch completion wait primitive
- [ ] Remove polling join loop
- [ ] Keep branches non-yielding until branch resume model is ready

### Slice C
- [ ] Enable branch yields
- [ ] Verify exception handling, context merge, and profiling merge still work
- [ ] Verify high-priority interruptions during branch execution

### Slice D
- [ ] Refactor `ScanParallelCursor` state ownership
- [x] Coordinate a single batch producer and waiter handoff to reduce shared-mutex pileups
- [ ] Allow branch-side yield while waiting on shared scan progress
- [ ] Verify no duplicate or missing rows under stress

### Slice E
- [ ] Revisit `ParallelMerge` trigger model
- [ ] Audit remaining synchronous `RunPullToCompletion(...)` sites

## Verification plan

### Correctness
- serial vs parallel result equality under mixed workload
- no duplicate/missing rows under repeated resume
- branch exception propagation preserved
- branch context merge preserved

### Scheduling behavior
- query can yield while waiting for branch completion
- worker pool continues executing other work during join waits
- no deadlock when high-priority work arrives during parallel execution

### Stress coverage
- parallel execution stress test
- focused `pattern_match` and aggregation/order-by cases
- long-running parallel scans with concurrent readers/writers

## Commit strategy

1. task document and first join-side infrastructure
2. cooperative join in parallel branches
3. coroutine-friendly branch completion primitive
4. branch yield enablement
5. `ScanParallelCursor` refactor
6. follow-up cleanup / tests

Each commit should:
- keep behavior understandable on its own
- include the reasoning in the commit message
- avoid mixing unrelated cleanup

## Change log

### `63e18349a` `query: make parallel branch joins wait cooperatively`
- Added a small `TaskCollection` helper to run one idle task locally.
- Replaced blocking `WaitOrSteal()` in `ParallelBranchCursor` with a cooperative wait loop.
- Intention: stop occupying the current worker in a fully blocking join while branch tasks still remain non-yielding.

### `d55a7389f` `query: wait for branch progress during cooperative joins`
- Added progress notifications to `TaskCollection`.
- Replaced blind `std::this_thread::yield()` join polling with bounded waiting for branch progress.
- Intention: reduce CPU waste and make the cooperative join less noisy while still staying incremental.

### `9270ac03b` `query: coordinate ScanParallel batch producers`
- Added explicit producer/waiter coordination inside `ScanParallelCursor`.
- One branch now owns shared upstream batch publication while other branches wait on a condition variable.
- Intention: reduce lock pileups on the shared scan mutex without enabling scan-side yield yet.

## Validation log

### Targeted unit-test build and run
- Build command:

```bash
cmake --build build -j3 --target memgraph__unit__utils_priority_thread_pool
```

- Test command:

```bash
./tests/unit/utils_priority_thread_pool
```

- Result:
  - build succeeded
  - all 32 tests passed
  - includes new coverage for:
    - `TaskCollection::TryExecuteOneIdleTask()`
    - `TaskCollection::WaitForProgress()`
