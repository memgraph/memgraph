# B1 — port the p3 pool park infra into v2's pool (merge spec)

Ref to port FROM: `897d8e277` (p3 stack). Target: current branch `feat/coroutine-scheduler-redesign`.
Constraint (user): **KEEP v2's HP threads + `sched_mon` monitor** ("leave all threads"). So v2 is the
BASE; we ADD p3's park infra on top — not adopt p3's pool wholesale (p3 deliberately removed HP+monitor).

## Why the merge is tractable
`work_pinned_` is a **separate** priority_queue on each Worker. v2's steal phase and the `sched_mon`
monitor only ever touch `work_` (the stealable queue), so pinned continuations are invisible to both by
construction — no special-casing needed in steal/monitor. The HP `if constexpr` branches and the monitor
stay exactly as in v2.

## Files
- **NEW** `src/utils/worker_yield_signal.hpp` — `WorkerYieldRegistry` (verbatim from ref). Foundational; the
  park's same-worker pinning reads `WorkerYieldRegistry::GetCurrentWorkerId()`. (B1.0)
- **MERGE** `src/utils/priority_thread_pool.{hpp,cpp}` — add park infra to v2 base. (B1.1/B1.2)
- **WIRE** `src/memgraph.cpp` (~847) construct a `WorkerYieldRegistry` that OUTLIVES the pool (declare
  before it); pass into ctor. Fix all ctor call sites + `TaskCollection::AddTask` (void→`[]{t();return false;}`)
  + `ScheduledCollection` (→`ScheduleResumableTask`). (B1.3)
- **TESTS** port `tests/unit/utils_worker_yield_registry.cpp` (B1.0), `utils_priority_thread_pool.cpp`
  resumable parts + `cursor_yield_pool.cpp` (B1.4) + CMake.

## What to ADD to v2 (from ref, faithfully — carries EC-2..EC-7 / P4/P9/P10 fixes + lifetime barriers)
1. `using ResumableTaskSignature = std::move_only_function<bool()>;`
2. **TaskCollection → resumable**: `Task` holds `ResumableTaskSignature`; 5 states
   IDLE/SCHEDULED/PARKED/STOLEN/FINISHED; `in_flight_` 2-phase lifetime barrier (R1); `progress_*`
   members + `progress_cv_`; methods `WrapTask(idx,pool)`, `Wait(timeout)`, `WaitOrSteal(timeout)`,
   `TryExecuteOneIdleTask(pool)`, `WaitForProgress`, `ProgressEpoch`, `Finished`/`AllTerminal`/
   `HasNonTerminalTasks`, `RegisterProgressWaiter`(MG_ASSERT single-waiter R2)/`NotifyProgress`.
   `enable_shared_from_this` (WrapTask captures `weak_from_this().lock()`).
3. `WorkerResumeEvent` (epoch + waiters; `RegisterTaskWaiter` stores PARKED **release** before listing;
   `NotifyAll` dispatches pinned via `RescheduleTaskOnWorker`).
4. `CurrentResumableTask` + the `current_resumable_task_stack` TLS + `CurrentResumableTaskScope`.
5. `PriorityThreadPool::ScheduleResumableTask` (+ `ResumableWrapper::Run`: scope → run → WasParked?return
   / yielded?reschedule-same-worker / shutdown-inline-drain) and `RescheduleTaskOnWorker(wid,task)`
   (`push(...,pinned=true)`, reuses `last_task_` id).
6. `Worker`: `work_pinned_` queue; `push(task,id,pinned=false)`; `pop_task` prefers higher id across both
   queues; Phase 1B + Phase 4 check both queues; steal + monitor untouched (ignore `work_pinned_`); the
   yield_registry clear/re-arm in Phase 1A + `SetCurrentWorker`/`ClearCurrentWorker` at loop start/end;
   `yield_registry_`/`worker_id_` published in ctor BEFORE the start barrier.
7. ctor: KEEP `(mixed, hp, init_cb)` + ADD trailing `WorkerYieldRegistry* yield_registry=nullptr`.
   Worker `operator()` gains a `WorkerYieldRegistry*` param (both LOW+HIGH explicit instantiations).
8. `CollectionScheduler`: atomic `pool_` (C0 fix), `ProgressAwaitable` (`await_suspend`→
   `RegisterProgressWaiter`, busy-spin fallback), `Trigger` nulls pool atomically.

## Binding invariants (do not weaken)
R1 in_flight 2-phase gate · R2 single progress waiter (MG_ASSERT) · R7 Trigger-before-park · atomic pool_ ·
PARKED stored **release** (read out-of-mutex by Finished/AllTerminal acquire) · EC-2 stolen task (no
worker_id) must NOT park (busy-spin) · pinned tasks never trigger HP preemption / never stolen / never
migrated.

## Behavior must stay identical after B1 (yield + park DORMANT)
Nothing calls `ScheduleResumableTask` for real query work yet (that's B2). AddTask-based tasks wrap to
`return false` (never yield/park). `RequestYieldForWorker` is only armed by HP-task arrival, same as
today's HP path — keep it gated so production behavior is unchanged until B2 wires a consumer.

## Verify
B1.0: build + `utils_worker_yield_registry` green. B1.1-3: full build green; `utils_priority_thread_pool`
green; existing pool consumers unaffected. B1.4: resumable/park tests green + **TSan zero warnings** on
park/resume/notify in isolation (also re-run under TSan to confirm no regression of the C0 pool_ race).
