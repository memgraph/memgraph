# Issue P4: Silent Task Dropping in RescheduleTaskOnWorker During Pool Shutdown

## Executive Summary
**Severity:** CRITICAL
**Location:** `src/utils/priority_thread_pool.cpp:271-288`
**Issue Type:** Deadlock - Task resume closures silently dropped during shutdown
**Status:** OPEN - Needs immediate fix

---

## Problem Description

When `PriorityThreadPool::RescheduleTaskOnWorker()` is called during pool shutdown (i.e., after `AwaitShutdown()` has been called), it silently returns without executing the task. This causes **permanent deadlock** when:

1. A parallel query is executing with PARKED branch tasks
2. Query is interrupted/exception occurs
3. `Interrupt()` → `NotifyAll()` tries to wake PARKED tasks
4. `RescheduleTaskOnWorker()` sees `pool_stop_source_.stop_requested()` and **silently drops** the resume closure
5. Task remains in PARKED state forever
6. `TaskCollection::Wait()` blocks forever waiting for task completion
7. Main thread tries to join worker threads → **DEADLOCK**

### Vulnerable Code

```cpp
void PriorityThreadPool::RescheduleTaskOnWorker(uint16_t worker_id, TaskSignature task) {
  spdlog::trace("RescheduleTaskOnWorker: worker_id={}", worker_id);
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    // BUG: Silently returns without executing the task!
    // The task is lost forever - caller assumes it will run
    return;
  }
  // ... normal scheduling logic
}
```

---

## Deadlock Call Graph

```
[Main Thread]                         [Worker Thread 4]
    |                                         |
    |--- Query execution starts -------------->|
    |                                         |--- ExecuteBranchesInParallel
    |                                         |      └─ branch_tasks[i]->DoPull()
    |                                         |         └─ co_await ProducerProgressAwaitable
    |                                         |            ├─ RegisterWaiter(producer_waiters_)
    |                                         |            ├─ Check interrupted_ → false
    |                                         |            └─ await_suspend returns true
    |                                         |               (coroutine suspended, state=PARKED)
    |                                         |
    |--- Exception/Interrupt detected          |
    |    └─ PullPlan::~PullPlan()              |
    |         └─ cursor_->Reset()              |
    |              └─ ParallelBranchCursor::Reset()
    |                   └─ Interrupt()
    |                        ├─ interrupted_ = true
    |                        └─ producer_waiters_.NotifyAll()
    |                             └─ For each PARKED task:
    |                                  └─ RescheduleTaskOnWorker(worker_id, resume_closure)
    |                                       └─ pool_stop_source_.stop_requested()?
    |                                            └─ YES (shutdown initiated)
    |                                                 └─ **SILENTLY RETURNS**
    |                                                    (resume_closure dropped!)
    |                                         |
    |                                         |--- Task still PARKED, never notified
    |                                         |
    |--- AwaitShutdown()                       |
    |    └─ workers_.clear()                   |
    |         └─ jthread::join()               |
    |              └─ Waiting for Worker 4 ----X--- DEADLOCK (Worker 4 stuck in Wait())
```

---

## Code Path Analysis

### Where RescheduleTaskOnWorker is Called During Shutdown

| Caller | Location | Purpose |
|--------|----------|---------|
| `WorkerResumeEvent::NotifyAll()` | priority_thread_pool.cpp:732-734 | Wake PARKED branch tasks on interrupt |
| `TaskCollection::NotifyProgress()` | priority_thread_pool.cpp:661-663 | Wake parent coroutine waiting on progress |

Both paths are called during teardown to wake up parked tasks, but the closures are silently dropped.

---

## Fix Options

### Option 1: Execute Inline on Shutdown (RECOMMENDED)

When pool is stopping, execute the task inline on the calling thread instead of dropping it:

```cpp
void PriorityThreadPool::RescheduleTaskOnWorker(uint16_t worker_id, TaskSignature task) {
  spdlog::trace("RescheduleTaskOnWorker: worker_id={}", worker_id);
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    spdlog::warn("RescheduleTaskOnWorker: pool stopping, executing inline");
    task();  // Execute synchronously instead of dropping
    return;
  }
  // ... normal scheduling logic
}
```

**Pros:**
- Simple, minimal change
- Guaranteed task execution
- Maintains ordering (task runs before RescheduleTaskOnWorker returns)

**Cons:**
- May execute on wrong thread (calling thread vs target worker)
- Could cause brief delay in shutdown path

### Option 2: Use Emergency Queue

Add a separate "shutdown queue" that bypasses the normal worker pool:

```cpp
void PriorityThreadPool::RescheduleTaskOnWorker(uint16_t worker_id, TaskSignature task) {
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    std::lock_guard lock(shutdown_mutex_);
    shutdown_tasks_.push_back(std::move(task));
    shutdown_cv_.notify_one();
    return;
  }
  // ... normal scheduling
}
```

**Pros:**
- Preserves async semantics
- Can be drained during final shutdown phase

**Cons:**
- More complex
- Requires additional synchronization

### Option 3: Fail Loudly

Change silent return to an assertion or exception:

```cpp
void PriorityThreadPool::RescheduleTaskOnWorker(uint16_t worker_id, TaskSignature task) {
  MG_ASSERT(!pool_stop_source_.stop_requested(),
            "Cannot reschedule task during pool shutdown");
  // ... normal scheduling
}
```

**Pros:**
- Immediately reveals the bug

**Cons:**
- Doesn't fix the deadlock
- Causes crash instead of hang

---

## Recommended Fix

**Implement Option 1** - Execute inline during shutdown. This is the minimal, safest fix.

### Files to Modify
- `src/utils/priority_thread_pool.cpp:271-288` - RescheduleTaskOnWorker

### Verification
1. Run parallel e2e tests in a loop
2. Verify no deadlock after fix
3. Add logging to confirm inline execution path is hit

---

## Related Issues

| Issue | Relationship | Status |
|-------|-------------|--------|
| P1 | Contributing factor - race can corrupt state | Partially Fixed |
| P5 | Related - orphaned closure race | Open |
| P6 | Related - ProgressAwaitable raw handle | Open |
| **P4** | **This issue - PRIMARY deadlock cause** | **Open** |

---

## Testing Strategy

1. **Deadlock Reproduction**:
   - Run parallel e2e tests in while loop
   - Confirm deadlock occurs within N iterations

2. **Fix Verification**:
   - Apply Option 1 fix
   - Run same loop test
   - Verify no deadlock after M iterations (M >> N)

3. **Regression Test**:
   - Add unit test that calls RescheduleTaskOnWorker during shutdown
   - Verify task is executed

---

## Document History

| Date | Change |
|------|--------|
| 2025-01-17 | Initial analysis from live deadlock observation |
| 2025-01-17 | Subagent verification confirmed root cause |
