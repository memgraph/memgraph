# Issue P5: Orphaned Closure Race in ProducerProgressAwaitable

## Executive Summary
**Severity:** HIGH
**Location:** `src/query/plan/operator.cpp:9961-9997`
**Issue Type:** Race condition - Orphaned closure after failed suspend
**Status:** OPEN - Needs fix

---

## Problem Description

In `ProducerProgressAwaitable::await_suspend()`, there's a race window between successfully registering a waiter with `producer_waiters_` and checking `interrupted_` under the lock. If `Interrupt()` runs in this window:

1. Thread A (branch): `RegisterWaiter()` succeeds - closure added to `producer_waiters_`
2. Thread B (parent): `Interrupt()` acquires lock, sets `interrupted_ = true`, calls `NotifyAll()`
3. Thread A: Recheck under lock sees `interrupted_` is true
4. Thread A: Returns `false` (doesn't suspend)
5. **Problem**: `NotifyAll()` already consumed the closure and dispatched a `ResumableWrapper::Run` task
6. Result: `WrapTask` executes on a coroutine that never suspended → state machine corruption

This can leave the task in an unrecoverable state, potentially contributing to deadlocks.

### Vulnerable Code

```cpp
bool await_suspend(std::coroutine_handle<> handle) const {
  // ... store_handle lambda ...

  // RACE WINDOW STARTS HERE
  if (utils::CurrentResumableTask::RegisterWaiter(cursor_->producer_waiters_, observed_epoch_)) {
    // RACE WINDOW: Interrupt() can run here

    // Check if shutdown was requested between await_ready() and now
    {
      const std::lock_guard lock(cursor_->mutex_);
      if (cursor_->shutting_down_ || cursor_->interrupted_) {
        // We return false (don't suspend), but NotifyAll() may have already
        // consumed our closure from producer_waiters_!
        if (context_->task_parked_ptr) {
          *context_->task_parked_ptr = false;
        }
        return false;  // Don't suspend
      }
    }
    return true;  // Actually suspend
  }
  // ... fallback to busy-spin ...
}
```

---

## Race Condition Timeline

```
[Thread A: Branch Task]                    [Thread B: Parent/Interrupt]
    |                                               |
    |--- RegisterWaiter(producer_waiters_)         |
    |    (closure added to queue)                  |
    |                                               |
    |    [RACE WINDOW OPEN]                         |
    |                                               |--- Interrupt()
    |                                               |    ├─ lock(mutex_)
    |                                               |    ├─ interrupted_ = true
    |                                               |    └─ unlock(mutex_)
    |                                               |
    |                                               |--- producer_waiters_.NotifyAll()
    |                                               |    └─ Takes Thread A's closure
    |                                               |    └─ RescheduleTaskOnWorker()
    |                                               |         └─ Dispatches WrapTask
    |                                               |              (will run on Thread C)
    |                                               |
    |--- lock(mutex_)                              |
    |--- interrupted_? → TRUE                      |
    |--- unlock(mutex_)                            |
    |                                               |
    |--- return false  // Don't suspend!           |
    |                                               |
    |    [COROUTINE CONTINUES WITHOUT SUSPENDING]  |
    |                                               |
    |--- [Thread C: WrapTask runs]                 |
         └─ Tries to resume coroutine              |
            that's already running!                |
            → STATE MACHINE CORRUPTION              |
```

---

## Impact Analysis

### When This Race Occurs
- High-contention parallel queries
- Rapid query cancellation
- Exception thrown during parallel execution

### Consequences
| Scenario | Outcome |
|----------|---------|
| `WrapTask` runs on running coroutine | Undefined behavior, potential crash |
| Coroutine state corrupted | Task stuck in intermediate state |
| `TaskCollection::Wait()` confusion | May wait for task that won't complete properly |

---

## Fix Options

### Option 1: Unregister on Failed Suspend (RECOMMENDED)

When the recheck detects `interrupted_` after successful `RegisterWaiter`, explicitly remove the registered closure:

```cpp
bool await_suspend(std::coroutine_handle<> handle) const {
  store_handle();
  if (utils::CurrentResumableTask::RegisterWaiter(cursor_->producer_waiters_, observed_epoch_)) {
    // Check if shutdown was requested while we were registering
    {
      const std::lock_guard lock(cursor_->mutex_);
      if (cursor_->shutting_down_ || cursor_->interrupted_) {
        // CRITICAL FIX: Remove our closure before returning
        cursor_->producer_waiters_.RemoveWaiter(handle, observed_epoch_);

        if (context_->task_parked_ptr) {
          *context_->task_parked_ptr = false;
        }
        return false;
      }
    }
    return true;
  }
  clear_handle();
  return false;
}
```

**Pros:**
- Clean semantic: if we don't suspend, we don't leave a waiter
- Prevents orphaned closure from being notified

**Cons:**
- Requires adding `RemoveWaiter()` method to `WorkerResumeEvent`
- Still has brief race window (between RegisterWaiter and RemoveWaiter)

### Option 2: Atomic Check-and-Register

Combine the register and check into a single atomic operation:

```cpp
bool await_suspend(std::coroutine_handle<> handle) const {
  store_handle();

  const std::lock_guard lock(cursor_->mutex_);
  if (cursor_->shutting_down_ || cursor_->interrupted_) {
    return false;  // Don't even try to register
  }

  // Register while holding lock - guaranteed not interrupted
  if (utils::CurrentResumableTask::RegisterWaiter(cursor_->producer_waiters_, observed_epoch_)) {
    return true;
  }

  clear_handle();
  return false;
}
```

**Pros:**
- Eliminates race entirely
- Simple and clear

**Cons:**
- Holds mutex during registration (may affect performance)
- Changes locking pattern

### Option 3: Epoch-Based Guard

Use the epoch to detect if our closure was already consumed:

```cpp
bool await_suspend(std::coroutine_handle<> handle) const {
  store_handle();
  uint64_t my_epoch = observed_epoch_;

  if (utils::CurrentResumableTask::RegisterWaiter(cursor_->producer_waiters_, observed_epoch_)) {
    {
      const std::lock_guard lock(cursor_->mutex_);
      if (cursor_->shutting_down_ || cursor_->interrupted_) {
        // Check if NotifyAll already happened
        if (cursor_->producer_waiters_.GetEpoch() > my_epoch) {
          // NotifyAll already ran, closure was consumed
          // We MUST suspend and let the scheduled task wake us
          return true;
        }
        // Safe to return false
        return false;
      }
    }
    return true;
  }
  // ...
}
```

**Pros:**
- Handles the case where NotifyAll already ran
- No need to remove closure

**Cons:**
- More complex
- Requires adding GetEpoch() method
- Still has edge cases

---

## Recommended Fix

**Implement Option 1** - Unregister on failed suspend. This is the cleanest semantic fix.

### Implementation Steps

1. Add `RemoveWaiter()` method to `WorkerResumeEvent`:
```cpp
void WorkerResumeEvent::RemoveWaiter(std::coroutine_handle<> handle, uint64_t epoch) {
  auto lock = std::lock_guard(mutex_);
  // Remove waiter with matching handle and epoch
  std::erase_if(waiters_, [&](const Waiter& w) {
    return w.handle == handle && epoch_ == epoch;
  });
}
```

2. Update `ProducerProgressAwaitable::await_suspend()` to call `RemoveWaiter()` when not suspending due to interrupt/shutdown.

### Files to Modify
- `src/utils/priority_thread_pool.hpp` - Add RemoveWaiter declaration
- `src/utils/priority_thread_pool.cpp` - Implement RemoveWaiter
- `src/query/plan/operator.cpp` - Call RemoveWaiter in await_suspend

---

## Related Issues

| Issue | Relationship | Status |
|-------|-------------|--------|
| P1 | Parent issue - double-resume race | Partially Fixed |
| **P5** | **This issue - orphaned closure sub-problem** | **Open** |
| P4 | Related - silent task dropping can make this worse | Open |

---

## Testing Strategy

1. **Unit Test**:
   - Create mock scenario where RegisterWaiter succeeds then interrupt occurs
   - Verify closure is properly removed

2. **Stress Test**:
   - Run parallel queries with rapid cancellation
   - Verify no state corruption

3. **Sanitizer Run**:
   - TSan to verify no data races in fixed code

---

## Document History

| Date | Change |
|------|--------|
| 2025-01-17 | Identified during deadlock investigation |
| 2025-01-17 | Subagent verification confirmed race exists |
