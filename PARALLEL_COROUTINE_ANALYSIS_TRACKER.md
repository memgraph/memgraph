# Memgraph Parallel Coroutine Execution Analysis Tracker

**Status:** Complete
**Last Updated:** 2026-04-17
**Analyst:** OpenCode

---

## ANALYZED COMPONENTS (COMPLETE)

### Core Coroutine Infrastructure
- [x] **PullAwaitable** - Single awaitable type for Query Plan
- [x] **BasePromise** - Shared promise layout with symmetric transfer
- [x] **Cursor** base class - Pull() vs DoPull() lifecycle
- [x] **ResumeAwaitable** - Lightweight persistent generator resumption

### Yield Mechanism
- [x] **YieldPointAwaitable** - Throttled yield points
- [x] **AbortCheck** - Integration with yield mechanism
- [x] **StoppingContext** - CheckAbortOrYield implementation
- [x] **WorkerYieldRegistry** - Per-worker yield signals (TLS)
- [x] **Symmetric Transfer** - Tail-call optimization in promises

### Task Management & States
- [x] **Task State Machine** - IDLE/SCHEDULED/PARKED/STOLEN/FINISHED
- [x] **TaskCollection** - Batch task management
- [x] **WrapTask** - Task wrapping for pool scheduling
- [x] **ResumableWrapper** - Yield detection and rescheduling
- [x] **CurrentResumableTask** - TLS-based current task tracking

### Thread Safety & Synchronization
- [x] **tasks_mutex_** - Protects TaskCollection vector access
- [x] **progress_mutex_** - Protects progress_epoch_, waiters
- [x] **State atomics** - CAS transitions with acq_rel ordering
- [x] **Exception propagation** - Store-before-flag pattern

### Parallel Execution
- [x] **ExecuteBranchesInParallel** - Structured parallel join (~300 lines)
- [x] **CollectionScheduler** - Scheduler integration
- [x] **HasNonTerminalTasks()** - Correct join predicate
- [x] **AllTerminal()** - Structured join invariant

### Coroutine Frame Lifecycle
- [x] **Frame allocation/deallocation** - Compiler-allocated via operator new on coroutine creation
- [x] **Handle ownership semantics** - PullAwaitable owns handle; Awaiter conditionally owns (rvalue co_await)
- [x] **Coroutine chain unwinding** - Cursor::Reset() → gen_.reset() → ~PullAwaitable() → handle_.destroy()
- [x] **Exception frame unwinding** - unhandled_exception() stores in promise; rethrown on await_resume
- [x] **CoroutineTracker** - Wrapper that tracks coroutine destruction via active_coroutines_ counter

### Park Mechanism Details
- [x] **WorkerResumeEvent::RegisterWaiter** - Epoch check prevents lost wakeups
- [x] **WorkerResumeEvent::NotifyAll** - Swaps waiters, increments epoch, reschedules on original worker
- [x] **Park vs Yield distinction** - Yield=scheduler-driven fairness; Park=event-driven I/O waits
- [x] **CurrentResumableTaskScope** - TLS stack tracks parked state per resumable task
- [x] **ProducerProgressAwaitable** - ScanParallelCursor's parking with shutdown/interrupt checks

### Specific Operator Implementations
- [x] **ScanParallelCursor** - CoroutineTracker for safe destruction; ProducerProgressAwaitable for parking
- [x] **AggregateParallelCursor** - Parallel aggregation via ExecuteBranchesInParallel; pre_/post_pull_func pattern
- [x] **OrderByParallel** - Parallel sort using k-way merge; min-heap for result ordering
- [x] **ParallelMerge** - Triggers CollectionScheduler before first upstream pull; lightweight wrapper

### Advanced Threading Scenarios
- [x] **Work stealing algorithm** - Idle workers steal from busy workers' work_ queue (not work_pinned_)
- [x] **High priority task injection** - HP tasks signaled via yield_registry; preempt LP via RequestYieldForWorker
- [x] **Pinned tasks** - worker_pinned_ queue never stolen; used for continuations after yield
- [x] **HotMask** - Bitmap tracks "hot" workers (not sleeping) for task assignment; GetHotElement() finds first hot

### Memory & Resource Management
- [x] **CrossThreadMemoryTracking** - RAII class tracks memory per thread; StartTracking/StopTracking for branch boundaries
- [x] **Frame memory management** - utils::MemoryResource for cursor allocation; per-query memory tracking
- [x] **Heap-allocated shared storage** - shared_ptr workarounds in ExecuteBranchesInParallel ensure data outlives yields
- [x] **OOM handling** - OOMExceptionEnabler in branch tasks; CrossThreadMemoryTracking catches OOM per branch

### Edge Cases & Error Handling
- [x] **Interrupt() semantics** - Wakes PARKED tasks via WorkerResumeEvent::NotifyAll(); sets interrupted_ flag
- [x] **Shutdown() vs Reset()** - Shutdown destroys cursors; Reset clears gen_ (coroutines) but keeps cursor state
- [x] **Timeout handling** - AsyncTimer in StoppingContext; MustAbort() checks timer expiry
- [x] **Transaction termination** - TERMINATED state in MustAbort(); causes HintedAbortError

### Testing & Verification
- [x] **Unit test patterns** - 55 tests in utils_priority_thread_pool.cpp; Synchronized<> for thread-safe test state
- [x] **Stress test strategies** - ConcurrentStealingAndScheduling test; multiple iterations with timing variations
- [x] **ASan/TSan findings** - Heap-buffer-overflow in WaitOrSteal (fixed); tasks_mutex_ added for thread safety
- [x] **E2E parallel tests** - 27 parallel correctness tests; require enterprise license

---

## ISSUES DISCOVERED

### FIXED Issues (from memory palace)

| Issue | Location | Root Cause | Fix |
|-------|----------|------------|-----|
| **Heap-buffer-overflow in WaitOrSteal** | `priority_thread_pool.cpp` | Concurrent unsynchronized access to `tasks_` vector between `ScheduledCollection()` and `WaitOrSteal()` | Added `tasks_mutex_` to protect vector access; locked `TryExecuteOneIdleTask()`; fixed move constructor |
| **Premature coroutine resumption** | `operator.cpp` | Join loop used `!Finished()` which doesn't wait for PARKED tasks | Changed to `HasNonTerminalTasks()` predicate |
| **TryExecuteOneIdleTask yields** | `priority_thread_pool.cpp` | Stolen task could yield, but no pool to reschedule | Added optional `PriorityThreadPool*` parameter; transition STOLEN→SCHEDULED and reschedule |
| **Exception visibility race** | `operator.cpp` | Exception not visible when `exception_occurred` flag read | Store exception BEFORE `fetch_or(true, release)` on flag |
| **Move constructor race** | `priority_thread_pool.hpp` | Move accessed `tasks_` without holding lock | Hold `tasks_mutex_` during move operations |

### POTENTIAL Issues Under Investigation

| ID | Issue | Location | Risk | Notes |
|----|-------|----------|------|-------|
| **P1** | Double-resume of coroutine handle | `priority_thread_pool.cpp:NotifyAll()` | MEDIUM | Check `handle.done()` but race window between check and resume. No lock held. |
| **P2** | Unsafe coroutine destruction | `cursor_awaitable_core.hpp:~PullAwaitable()` | HIGH | `handle_.destroy()` called unconditionally - UB if coroutine suspended. `CoroutineTracker` pattern mitigates. |
| **P3** | Stale suspended_handle after branch destruction | `operator.cpp:ExecuteBranchesInParallel` | MEDIUM | Branch task captures `&suspended_handle` by reference. If task yields and parent destroys, UAF on resume. |

### OPEN Questions / Potential Issues - ANSWERED

| ID | Question | Severity | Status |
|----|----------|----------|--------|
| **Q1** | What happens if `suspended_handle` in branch task is destroyed while task is PARKED? | HIGH | **ANSWERED**: `CoroutineTracker` pattern ensures cursor waits for coroutine destruction. Structured join required. |
| **Q2** | Can `WorkerResumeEvent::NotifyAll()` miss a waiter? | MEDIUM | **ANSWERED**: Epoch check prevents lost wakeups - registration fails if epoch changed. |
| **Q3** | Is `ScopedYieldSuppression` sufficient? | MEDIUM | **ANSWERED**: Used in EvaluatePatternFilterCursor to prevent yields during synchronous pattern evaluation. Clears yield_requested and suspended_task_handle_ptr. |
| **Q4** | What prevents double-resume? | HIGH | **ANSWERED**: `WasParked()` early return + `handle.done()` check, but race window exists. |
| **Q5** | Coroutine destruction while suspended? | HIGH | **ANSWERED**: `~PullAwaitable()` calls `destroy()` - UB if suspended. `CoroutineTracker` defers destruction. |
| **Q6** | `exception_occurred` overflow? | LOW | **ANSWERED**: uint8_t with fetch_or - overflow not a concern (just needs non-zero check). |
| **Q7** | ABA issues with Task state CAS? | MEDIUM | **ANSWERED**: Not a concern - only 5 states, CAS validates expected state. |
| **Q8** | ProgressAwaitable race? | MEDIUM | **ANSWERED**: Epoch check in `RegisterProgressWaiter` - returns false if epoch changed. |

---

## KEY FILES REFERENCE

| File | Lines | Purpose |
|------|-------|---------|
| `cursor_awaitable_core.hpp` | 236 | PullAwaitable, BasePromise, ResumeAwaitable |
| `cursor_awaitable.hpp` | 84 | YieldPointAwaitable, RunPullToCompletion |
| `cursor_awaitable.cpp` | 48 | Cursor::Reset(), RunPullToCompletion |
| `context.hpp` | 213 | ExecutionContext, StoppingContext, StopOrYieldResult, ScopedYieldSuppression |
| `worker_yield_signal.hpp` | 150 | WorkerYieldRegistry, per-worker signals |
| `priority_thread_pool.hpp` | 426 | TaskCollection, Task states, CollectionScheduler, HotMask |
| `priority_thread_pool.cpp` | 755 | WrapTask, WaitOrSteal, worker thread loop, work stealing |
| `operator.cpp` | 12046 | ExecuteBranchesInParallel (~10866-11159), parallel cursors |
| `query_memory_control.hpp` | 104 | CrossThreadMemoryTracking, OOM handling |
| `utils_priority_thread_pool.cpp` | 1898 | 55 unit tests for thread pool |

---

## CRITICAL PATTERNS

### 1. Structured Parallel Join
```cpp
while (collection_scheduler_->HasNonTerminalTasks()) {
  if (collection_scheduler_->TryExecuteOneIdleTask()) continue;
  co_await collection_scheduler_->WaitForProgressAwaitable();
}
// All tasks FINISHED before return
```

### 2. CoroutineTracker Safety
```cpp
struct CoroutineTracker {
  PullAwaitable awaitable;
  ~CoroutineTracker() {
    awaitable = PullAwaitable{};  // Destroys coroutine
    cursor->active_coroutines_--; // Track destruction
  }
};
```

### 3. Epoch-Based Race Prevention
```cpp
// Observer
uint64_t epoch = event.Epoch();
if (condition_ready()) return false;  // Don't suspend
if (!event.RegisterWaiter(handle, epoch)) return false;  // Epoch changed
return true;  // Suspend

// Notifier
std::lock_guard lock(mutex_);
++epoch_;  // Increment epoch
wake_all_waiters();
```

### 4. ScopedYieldSuppression
```cpp
// Used when synchronous completion required
ScopedYieldSuppression scoped_yield_suppression{context};
// Now AbortCheck won't suspend, RunPullToCompletion runs to completion
```

### 5. Interrupt for PARKED Tasks
```cpp
void Interrupt() override {
  for (auto &cursor : branch_cursors_) {
    cursor->Interrupt();  // Wakes PARKED tasks
  }
}
// Critical: Without this, Wait() deadlocks during exception handling
```

---

## RACE CONDITION HOTSPOTS

1. **TryExecuteOneIdleTask** vs pool worker task execution
2. **WrapTask** state CAS (multiple threads may attempt)
3. **NotifyProgress** vs **RegisterProgressWaiter**
4. **WorkerResumeEvent::NotifyAll** vs **RegisterWaiter**
5. Cursor destruction while coroutine suspended
6. Double-resume of coroutine handles (P1)
7. Stale `suspended_handle` after coroutine destruction (P3)

---

## MEMORY ORDERING RULES

- **State transitions**: `acq_rel` CAS
- **Flag setting**: `release` store
- **Flag checking**: `acquire` load
- **Exception visibility**: Store → Release-store flag → Acquire-load flag → Load exception
- **tearing_down**: `release` store before tasks check

---

## RELATED KNOWLEDGE GRAPH NODES

- `memgraph.coroutine.base_promise`
- `memgraph.coroutine.pull_awaitable`
- `memgraph.coroutine.yield_point`
- `memgraph.coroutine.frame_lifecycle`
- `memgraph.parallel_execution.task_states`
- `memgraph.parallel_execution.yield_mechanism_detailed`
- `memgraph.parallel_execution.park_mechanism`
- `memgraph.parallel_execution.thread_safety`
- `memgraph.parallel_execution.execute_branches`
- `memgraph.parallel_execution.coroutine_tracker`
- `memgraph.parallel_execution.epoch_pattern`

## RELATED MEMORY PALACE DRAWERS

- `memgraph/parallel-execution`: "COROUTINE PARALLEL EXECUTION ARCHITECTURE"
- `memgraph/parallel-execution`: "CRITICAL ATOMICS AND RACE CONDITIONS ANALYSIS"
- `memgraph/parallel-execution`: "COROUTINE FRAME LIFECYCLE & PARK MECHANISM"
- `memgraph/parallel-execution`: "PARALLEL OPERATOR PATTERNS"

---

## ANALYSIS COMPLETE

All components from the initial tracker have been analyzed:
- ✅ Core coroutine infrastructure
- ✅ Yield and park mechanisms
- ✅ Task management and states
- ✅ Thread safety and synchronization
- ✅ Parallel execution patterns
- ✅ All three parallel operator types (ScanParallel, AggregateParallel, OrderByParallel)
- ✅ Work stealing and advanced threading
- ✅ Memory management and OOM handling
- ✅ Edge cases and error handling
- ✅ Testing patterns

**3 potential issues remain under investigation (P1-P3).**

---

*Analysis completed 2026-04-17*
