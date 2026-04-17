# Issue P1: Double-Resume Race in WorkerResumeEvent::NotifyAll()

## Executive Summary
**Severity:** MEDIUM
**Location:** `src/utils/priority_thread_pool.cpp:711-746`
**Issue Type:** Race condition - TOCTOU (Time-of-Check-Time-of-Use)
**Status:** POTENTIALLY FIXED - See "Fix Verification" section below

---

## Fix Verification (NEW)

A potential fix was found in the git diff for `src/query/plan/operator.cpp`. The problematic code path in `ScanParallelCursor::ProducerProgressAwaitable::await_suspend()` was removed.

### What Was Changed
The fix removes the fallback path that registered raw coroutine handles with `producer_waiters_`. The comment in the code explains:

```cpp
// No CurrentResumableTaskScope: registering the raw coroutine handle in
// producer_waiters_ would create a double-resume race. suspended_handle in
// the branch lambda closure also holds this handle; both NotifyAll() and the
// next WrapTask invocation would call resume() on the same frame concurrently.
// Fall through to busy-spin: DoPull will re-check immediately on resume.
clear_handle();
return false;
```

### Subagent Verification Results

After subagent review, the following findings were confirmed:

1. **ScanParallelCursor fix is CORRECT** - The busy-spin fallback is safe
2. **SECOND double-resume path found** - `CollectionScheduler::ProgressAwaitable` at operator.cpp:11107 still registers raw coroutine handles
3. **WorkerResumeEvent::NotifyAll() still has TOCTOU** - The function itself was not hardened, only one caller was removed

### NEW CRITICAL FINDING: ProgressAwaitable Double-Resume Path

**Location:** `src/utils/priority_thread_pool.hpp:360` (called from operator.cpp:11107)

```cpp
bool await_suspend(std::coroutine_handle<> handle) const {
    return scheduler_ && scheduler_->RegisterProgressWaiter(handle, observed_epoch_);
}
```

This creates the same race pattern in `ExecuteBranchesInParallel`:
- Path A: `TaskCollection::NotifyProgress()` resumes via `RescheduleTaskOnWorker`
- Path B: `ResumableWrapper::Run()` yield path may also reschedule

### Recommended Actions
1. Apply same fix pattern to `ProgressAwaitable`
2. Harden `NotifyAll()` and `NotifyProgress()` with atomic CAS guards
3. Deprecate raw-handle registration API

### Files Modified in Fix
- `src/query/plan/operator.cpp`: Removed ~14 lines of problematic code path

---

## Original Problem Description

---

## Problem Description

The `NotifyAll()` function checks if a coroutine handle is done before resuming it, but there's a race window between the check and the resume operation. Another thread could resume and complete the coroutine in this window, leading to undefined behavior when `resume()` is called on an already-completed coroutine.

### Vulnerable Code
```cpp
void WorkerResumeEvent::NotifyAll() {
  std::vector<Waiter> waiters;
  {
    auto lock = std::lock_guard(mutex_);
    ++epoch_;
    waiters.swap(waiters_);
  }  // Lock released here

  for (auto &waiter : waiters) {
    if (waiter.handle) {
      if (waiter.handle.done()) {  // <-- CHECK
        continue;
      }
      // RACE WINDOW: Another thread could resume here
      if (waiter.pool && waiter.worker_id) {
        waiter.pool->RescheduleTaskOnWorker(*waiter.worker_id, [handle = waiter.handle]() mutable {
          if (handle && !handle.done()) handle.resume();  // <-- Second check in lambda
        });
      } else {
        waiter.handle.resume();  // <-- USE (direct resume)
      }
    }
  }
}
```

---

## Call Graph

### Thread 1: Notifier (NotifyAll)
```
WorkerResumeEvent::NotifyAll()
  ├─ Lock mutex_
  ├─ ++epoch_
  ├─ waiters.swap(waiters_)
  └─ Unlock mutex_
      └─ For each waiter:
          ├─ Check: waiter.handle.done()?
          │   └─ If false (not done):
          │       ├─ [RACE WINDOW START]
          │       ├─ RescheduleTaskOnWorker() OR
          │       └─ waiter.handle.resume() directly
          │       └─ [RACE WINDOW END]
          └─ Continue
```

### Thread 2: Concurrent Resumer (Pool Worker)
```
PriorityThreadPool::Worker::operator()()
  └─ Execute task
      └─ ResumableWrapper::Run()
          └─ (*task)()  // Task returns true (yielded)
              └─ WrapTask() continuation
                  └─ RescheduleTaskOnWorker()
                      └─ Push to worker queue
                          └─ Worker wakes, pops task
                              └─ Task lambda executes
                                  └─ resume_target.resume()
                                      └─ Coroutine completes
                                          └─ handle.done() = true
```

### Thread 3: Event Signaler
```
SomeEvent::Complete()
  └─ WorkerResumeEvent::NotifyAll()
      └─ (Same as Thread 1 - multiple concurrent NotifyAll possible?)
```

---

## Race Scenario Walkthrough

### Scenario: Concurrent Resume via Pool

**Initial State:**
- Task is PARKED on WorkerResumeEvent
- One waiter registered with handle H
- Worker W is about to call NotifyAll()

**Timeline:**

| Time | Thread 1 (NotifyAll) | Thread 2 (Pool Worker) |
|------|---------------------|----------------------|
| T1 | Lock mutex | - |
| T2 | ++epoch | - |
| T3 | Swap waiters | - |
| T4 | Unlock mutex | - |
| T5 | Check H.done() → false | - |
| T6 | [RACE WINDOW] | Yield signal set |
| T7 | - | RescheduleTaskOnWorker(H) |
| T8 | - | Worker W2 pops task |
| T9 | - | resume H → completes |
| T10 | - | H.done() = true |
| T11 | RescheduleTaskOnWorker(H) | - |
| T12 | (Later) Worker W3 pops task | - |
| T13 | resume H ← UB! | - |

**Result:** Undefined behavior from resuming completed coroutine

---

## Root Cause Analysis

### Why This Can Happen

1. **Multiple Resume Paths**: A parked task can be resumed via:
   - `NotifyAll()` from event completion
   - `RescheduleTaskOnWorker()` from yield recovery
   - Direct resume in `NotifyAll()` for non-pool cases

2. **Lock Scope**: The mutex only protects `waiters_` vector and `epoch_`. Once waiters are swapped out, they can be accessed concurrently.

3. **No Synchronization on Handle**: The coroutine handle itself has no synchronization - `done()` and `resume()` are not atomic operations.

### Standard C++ Coroutine Behavior

From C++20 standard:
- `resume()` on a done coroutine: undefined behavior
- `done()` returns true if coroutine has reached final suspend
- These operations are not thread-safe by default

---

## Potential Impact

### Immediate
- Undefined behavior (likely crash or corruption)
- Double-execution of coroutine cleanup code
- Potential use-after-free if frame destroyed

### In Practice
This may be rare because:
1. `RescheduleTaskOnWorker` path has second `done()` check
2. Most uses have single event source
3. Parked tasks typically have single resumer

### But Still Dangerous
- Race conditions are probabilistic
- High-load scenarios increase likelihood
- Future changes could make it more common

---

## Proposed Fix Approaches

### Option 1: Atomic State Tracking (Recommended)

Track handle state with atomic:
```cpp
struct Waiter {
  std::coroutine_handle<> handle;
  std::atomic<bool> *resumed_flag;  // New
  // ...
};

void NotifyAll() {
  for (auto &waiter : waiters) {
    if (waiter.handle && waiter.resumed_flag) {
      bool expected = false;
      if (waiter.resumed_flag->compare_exchange_strong(expected, true)) {
        // We won the race, safe to resume
        if (!waiter.handle.done()) {
          waiter.handle.resume();
        }
      }
      // Else: another thread already resumed, skip
    }
  }
}
```

**Pros:**
- Definitive race resolution
- Clear ownership of resume

**Cons:**
- Additional memory per waiter
- Requires plumbing flag through registration

### Option 2: Try-Resume Pattern

Use CAS on a "resuming" state:
```cpp
struct Waiter {
  std::coroutine_handle<> handle;
  std::atomic<ResumeState> state{ResumeState::Pending};
  // ...
};

enum class ResumeState { Pending, Resuming, Done };

bool TryResume(Waiter &waiter) {
  auto expected = ResumeState::Pending;
  if (!waiter.state.compare_exchange_strong(expected, ResumeState::Resuming)) {
    return false;  // Already being resumed or done
  }

  if (!waiter.handle.done()) {
    waiter.handle.resume();
  }
  waiter.state.store(ResumeState::Done);
  return true;
}
```

**Pros:**
- State machine is clearer
- Can detect completed vs in-progress

**Cons:**
- More complex
- Still requires storage

### Option 3: Document and Assert

If analysis shows race is impossible in practice:
```cpp
void NotifyAll() {
  for (auto &waiter : waiters) {
    if (waiter.handle) {
      DMG_ASSERT(!waiter.handle.done(),
                 "Race detected: handle became done between check and resume");
      waiter.handle.resume();
    }
  }
}
```

**Pros:**
- Simple
- No runtime cost in release

**Cons:**
- Doesn't fix the bug
- Only catches in debug builds

---

## Verification Questions

1. **Can multiple NotifyAll() run concurrently?**
   - If yes, race is more likely
   - Check if mutex is held during iteration

2. **Can a task be both yielded and event-resumed?**
   - If yes, double-resume is possible
   - Need to check intersection of yield and park paths

3. **Is there already synchronization I'm missing?**
   - Check if handle has internal synchronization
   - Verify pool task execution guarantees

---

## Files to Modify

1. `src/utils/priority_thread_pool.hpp`
   - Add atomic flag to Waiter struct

2. `src/utils/priority_thread_pool.cpp`
   - Modify NotifyAll() to use CAS
   - Update RegisterWaiter to accept/return flag

3. `src/query/plan/operator.cpp` (if needed)
   - Update parking awaitables to provide flag

---

## Deadlock Connection Analysis (NEW)

After observing a **live deadlock** in parallel e2e tests, subagent verification revealed:

### What the Deadlock Showed
- Thread 4 (Worker): Stuck in `TaskCollection::Wait()` waiting for PARKED task
- Thread 1 (Main): Attempting `AwaitShutdown()` → trying to join worker
- Root cause: **Resume closures being silently dropped during pool shutdown**

### Connection to P1
P1 correctly identified the double-resume race pattern, but the **observed deadlock is NOT directly caused by P1**. Instead:

1. **P1 is a CONTRIBUTING factor** - The race can corrupt task state machine
2. **Primary cause is NEW** - `RescheduleTaskOnWorker` silently drops tasks when pool stopping

### Related New Issues
- **P4**: Silent task dropping in `RescheduleTaskOnWorker` - PRIMARY deadlock cause
- **P5**: Orphaned closure race in `ProducerProgressAwaitable` - P1-related
- **P6**: ProgressAwaitable raw handle (second P1 path) - separate fix needed

---

## Testing Strategy

1. **Stress Test**: High-concurrency scenario with rapid park/resume cycles
2. **Sanitizer Run**: TSan to detect data races
3. **Code Review**: Verify all resume paths use new synchronization
4. **Deadlock Reproduction**: Run parallel e2e tests in loop, verify fix resolves stuck state
