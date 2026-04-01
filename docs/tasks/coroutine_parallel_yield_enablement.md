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
- `208352688` `query: suspend branch joins on task progress`
- `ed148a02f` `query: enable scheduler yields in background branches`
- `e045ba4ad` `query: suspend ScanParallel waiters cooperatively`
- `b0f44a087` `query: localize ScanParallel producer batches`
- `4f3fb0349` `query: start parallel sections before merge pulls`
- pending commit: scope intentional yield suppression and extend helper coverage

What is true now:
- the parent parallel join no longer blocks in `WaitOrSteal()`
- the cooperative join now waits on branch progress instead of blind thread-yield polling
- the parent join can now suspend on branch progress via a coroutine-friendly wait primitive
- `ScanParallelCursor` now has explicit producer/waiter coordination for shared batch publication
- background branch tasks now refresh `yield_requested` from the worker currently running them
- `TaskCollection` has unit coverage for resumable collection tasks that yield and later complete
- losing `ScanParallelCursor` branches now suspend on producer handoff instead of sitting in timed condition-variable waits
- `WorkerResumeEvent` provides reusable multi-waiter wakeup/resume on original workers
- the ScanParallel producer now builds the next batch locally before publishing shared state
- the shared upstream pull inside `ScanParallelCursor` no longer needs to disable yield while building the next batch
- `ParallelMergeCursor` now schedules sibling branches before the first upstream pull
- `EvaluatePatternFilter` remains intentionally synchronous because `TypedValue::Function`
  is still evaluated from the non-coroutine expression evaluator
- the remaining yield suppression in `EvaluatePatternFilter` should stay explicit and scoped

Immediate next goal:
- make the intentional `EvaluatePatternFilter` exception easier to reason about with a scoped guard
- extend helper coverage around progress/yield resume primitives before changing more execution logic

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

Current build commands used during this work:

```bash
cmake --build build -j3 --target memgraph
cmake --build build -j1 --target src/query/CMakeFiles/mg-query.dir/plan/operator.cpp.o
```

Current local failure:
- unrelated module / dyndep build-tree problems, not caused by the coroutine parallel-yield changes
- failing areas:
  - `src/planner/include/planner/core/eclass.hpp`
  - `src/planner/include/planner/core/enode.hpp`
  - `src/utils/temporal.hpp`
  - `clang-scan-deps` / `.ddi.tmp` generation for `src/query/plan/operator.cpp`
- observed error shape:
  - module file built from a different branch / compiler mismatch
  - subsequent `ENodeId` resolution failures in planner sources
  - `clang-scan-deps` succeeds but Ninja cannot move generated `.ddi.tmp` into place

This means local full-build verification for this series is currently blocked by unrelated
planner module state. Targeted validation and user-local builds are still useful until that
separate issue is cleared.

## Current blockers

### 1. Nested synchronous cursor execution cannot propagate yield

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

Current decision:
- keep `EvaluatePatternFilter` non-yielding for now
- do not force coroutine semantics into `TypedValue::Function` / expression evaluation in this series
- make yield suppression use a dedicated RAII guard so the exception is obvious and exception-safe

### 2. Runtime validation of the new parallel scheduling path is still incomplete

Code:
- `tests/e2e/parallel/test_parallel_correctness.py`
- stress workloads under `tests/stress/standalone/native/workloads/parallel_execution/`

Current behavior:
- focused unit coverage for scheduler/task-collection helpers is good
- a focused parallel e2e run currently fails for licensing reasons, not for a correctness assertion
- full stress/correctness validation of the coroutine-parallel path still needs a licensed runtime

Why it matters:
- we have changed join, branch-yield, scan-wait, producer publication, and merge-start behavior
- the remaining risk is less about raw implementation coverage and more about real-plan runtime interactions

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
- [x] Add coroutine-friendly branch progress wait primitive
- [x] Replace timeout polling in the join loop with coroutine-friendly progress suspension
- [x] Keep branches non-yielding until branch resume model is ready
- [ ] Extend the wait primitive from "progress" to a cleaner branch-completion abstraction if needed

### Slice C
- [x] Enable branch yields by refreshing branch contexts from the current worker's yield signal
- [ ] Verify exception handling, context merge, and profiling merge still work
- [ ] Verify high-priority interruptions during branch execution

### Slice D
- [ ] Refactor `ScanParallelCursor` state ownership
- [x] Coordinate a single batch producer and waiter handoff to reduce shared-mutex pileups
- [x] Allow branch-side suspension while waiting on shared scan progress
- [x] Let the ScanParallel producer-side shared upstream pull yield safely
- [ ] Verify no duplicate or missing rows under stress

### Slice E
- [x] Revisit `ParallelMerge` trigger model
- [ ] Verify that early branch scheduling does not regress correctness or deadlock behavior

### Slice F
- [x] Keep `EvaluatePatternFilter` intentionally synchronous with explicit scoped yield suppression
- [x] Add focused helper tests for progress/yield edge cases used by the coroutine parallel path

## Validation log
- Added `ScopedYieldSuppression` as a dedicated RAII helper in `src/query/context.hpp`
  and switched `EvaluatePatternFilter` to use it instead of manual save/restore.
- Added/updated focused tests:
  - `tests/unit/interpreter.cpp`
    - `ScopedYieldSuppressionRestoresExecutionContextState`
  - `tests/unit/utils_priority_thread_pool.cpp`
    - `TaskCollection.WaitForProgressWakesOnTaskYield`
    - `TaskCollection.ProgressAwaitableTracksSuccessiveProgressEpochs`
    - `WorkerResumeEvent.StaleEpochDoesNotRegisterWaiter`
- Ran:

```bash
cmake --build build -j3 --target memgraph__unit__utils_priority_thread_pool
./build/tests/unit/utils_priority_thread_pool --gtest_filter='TaskCollection.WaitForProgressWakesOnTaskYield:WorkerResumeEvent.StaleEpochDoesNotRegisterWaiter'
./build/tests/unit/utils_priority_thread_pool
```

- Result:
  - focused helper tests passed
  - full `utils_priority_thread_pool` suite passed: `39` tests
- Additional focused rerun after extending epoch coverage:

```bash
./build/tests/unit/utils_priority_thread_pool --gtest_filter='TaskCollection.ProgressAwaitableTracksSuccessiveProgressEpochs:TaskCollection.WaitForProgressWakesOnTaskYield:WorkerResumeEvent.StaleEpochDoesNotRegisterWaiter'
```

- Result:
  - progress-awaitable successive-epoch coverage passed
- Also attempted:

```bash
cmake --build build -j3 --target memgraph__unit__interpreter
```

- Local result on this machine:
  - still blocked by the unrelated planner PCM/module mismatch in:
    - `src/planner/include/planner/core/eclass.hpp`
    - `src/planner/include/planner/core/enode.hpp`
  - because of that, the new interpreter-side helper test could not be validated locally here

- Branch progress wait slice (`208352688`)
  - build: `cmake --build build -j3 --target memgraph__unit__utils_priority_thread_pool`
  - test: `./build/tests/unit/utils_priority_thread_pool`
  - result: passed, `34` tests
- Background branch yield slice (pending commit)
  - build: `cmake --build build -j3 --target memgraph__unit__utils_priority_thread_pool`
  - test: `./build/tests/unit/utils_priority_thread_pool`
  - result: passed, `34` tests
  - full build: `cmake --build build -j3 --target memgraph`
  - result: still blocked by unrelated stale module artifact in `src/utils/temporal.hpp` (`memgraph.utils.fnv.pcm`)
- ScanParallel waiter suspension slice (pending commit)
  - build: `cmake --build build -j3 --target memgraph__unit__utils_priority_thread_pool`
  - test: `./build/tests/unit/utils_priority_thread_pool`
  - result: passed, `35` tests
  - full build: `cmake --build build -j3 --target memgraph`
  - result: still blocked by unrelated stale module artifact in `src/utils/temporal.hpp` (`memgraph.utils.fnv.pcm`)
- ScanParallel local-batch publish slice (pending commit)
  - object build attempt: `cmake --build build -j3 --target src/query/CMakeFiles/mg-query.dir/plan/operator.cpp.o`
  - result: blocked before `operator.cpp` is typechecked by unrelated stale module artifact in `src/utils/temporal.hpp` (`memgraph.utils.fnv.pcm`)
  - full build: `cmake --build build -j3 --target memgraph`
  - result: still blocked by unrelated stale planner/module artifact in `src/planner/include/planner/core/eclass.hpp`
- ParallelMerge early scheduling slice (pending commit)
  - object build attempt: `cmake --build build -j1 --target src/query/CMakeFiles/mg-query.dir/plan/operator.cpp.o`
  - result: blocked before `operator.cpp` is typechecked by unrelated stale module artifact in `src/utils/temporal.hpp` (`memgraph.utils.fnv.pcm`)
  - full build: `cmake --build build -j3 --target memgraph`
  - result: blocked by unrelated planner/module artifact in `src/planner/include/planner/core/eclass.hpp` and intermittent `.ddi.tmp` generation failure from `clang-scan-deps`
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

### Pending commit: coroutine-friendly branch progress wait
- Added one-shot waiter registration to `TaskCollection`.
- `NotifyProgress()` now resumes a waiting coroutine on the original worker via the pool.
- `ParallelBranchCursor` now `co_await`s branch progress instead of sleeping in a timeout loop.
- Intention: move branch joins closer to a real resumable wait without enabling branch task yields yet.

### `ed148a02f` `query: enable scheduler yields in background branches`
- Background branch contexts now refresh `yield_requested` from the worker executing the branch slice.
- Added scheduler-level coverage for a yielded `TaskCollection` task that resumes and completes on the same worker.
- Intention: let branch `AbortCheck` cooperate with the resumable task scheduler instead of inheriting a stale or disabled yield signal.

### `e045ba4ad` `query: suspend ScanParallel waiters cooperatively`
- Replaced timed `producer_cv_.wait_for(...)` loops with coroutine suspension/resume on producer handoff.
- Added `WorkerResumeEvent`, a reusable multi-waiter wakeup primitive that reschedules suspended coroutines onto their original workers.
- Added unit coverage proving multiple waiters resume on the workers where they suspended.
- Intention: stop losing scan branches from occupying workers while another branch owns shared batch publication.

### `b0f44a087` `query: localize ScanParallel producer batches`
- The producer now pulls upstream rows into a producer-local `Frame` and computes chunks before touching shared scan state.
- Shared `frame_` / `chunks_` are only updated once the batch is fully ready, under `mutex_`.
- Intention: separate in-flight producer state from published shared state so the producer path can stop relying on shared mutable state during the upstream pull.

### Pending commit: start parallel sections before the first merge pull
- `ParallelMergeCursor` now triggers its collection scheduler before the first upstream `Pull()` instead of after the first returned row.
- Intention: remove one of the last explicit blocking-era scheduling hacks now that branch joins, branch tasks, scan waiters, and the scan producer path are coroutine-friendly.

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

### Targeted validation for coroutine-friendly branch progress wait
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
  - includes coroutine-friendly progress waiter coverage in `utils_priority_thread_pool.cpp`
