# Parallel Execution Deadlock Investigation Summary

## Problem
During exception handling in parallel query execution, worker threads deadlock in `TaskCollection::Wait()` waiting for PARKED tasks that never reach FINISHED state.

## Root Cause Analysis

### The Re-parking Problem
When `Interrupt()` is called during exception teardown:
1. `shutting_down_` and `interrupted_` flags are set
2. `NotifyAll()` wakes PARKED tasks
3. Tasks resume execution
4. Tasks go back to the start of `DoPull` loop
5. Tasks see `producer_active_ && !all_pulled_` still true
6. Tasks try to park AGAIN on `ProducerProgressAwaitable`
7. Even though `await_ready()` should return true (don't suspend), tasks are still parking

### Evidence from Logs
```
[14:36:47.918] WrapTask[1]: starting with state=2  ← Resumed from PARKED
[14:36:47.918] ScanParallelCursor::DoPull: resumed from ProducerProgressAwaitable
[14:36:47.918] ScanParallelCursor::DoPull: about to await ProducerProgressAwaitable (epoch=1)
[14:36:47.918] NotifyAll: waking 0 waiters
```

Task[1] was PARKED (state=2), got woken, resumed, and immediately tried to park again. The `NotifyAll: waking 0 waiters` suggests the task successfully registered as a waiter again.

## Attempted Fixes

### 1. ParallelBranchCursor::Interrupt() (be67a71a1)
**Status:** ✅ Committed
- Forwards `Interrupt()` to all branch cursors
- Ensures signal reaches `ScanParallelCursor`

### 2. ScanParallelCursor::Interrupt() (0393444fa)
**Status:** ✅ Committed  
- Sets `shutting_down_ = true` before `NotifyAll()`
- Added `interrupted_` flag to prevent re-parking
- Checks both flags in `await_ready()`

### 3. await_suspend Race Checks (0393444fa)
**Status:** ✅ Committed
- Path 1: Checks `shutting_down_ || interrupted_` after registration
- Path 2: Fixed to return false when shutting_down after registration

### 4. Additional Debugging (ef8cfce67)
**Status:** ✅ Committed
- Added trace logging to `await_ready`, `await_suspend`, `DoPull`
- Logs show tasks are still re-parking despite fixes

## Why Fixes Aren't Working

### Theory 1: Multiple NotifyAll() Calls
Looking at the logs, there are MULTIPLE `NotifyAll()` calls with different epoch values:
- First: `NotifyAll: waking 1 waiters` (epoch 0)
- Second: `NotifyAll: waking 0 waiters` (epoch 1)

Each `NotifyAll()` increments the epoch. If a task parks AFTER the first NotifyAll but BEFORE checking `interrupted_`, it would register with epoch 1. Then when the second `NotifyAll(0)` runs (from a different code path?), the epoch doesn't match.

### Theory 2: await_ready() Not Being Called
In C++ coroutines, `await_ready()` is always called before `await_suspend()`. But if `await_ready()` returns false (should suspend), the coroutine should call `await_suspend()`. 

The issue may be that `await_ready()` is checking the flags correctly, but there's a race between the check in `await_ready()` and the registration in `await_suspend()`.

### Theory 3: Different Coroutine Instances
Each parallel branch has its own coroutine instance. The `interrupted_` flag is on the `ScanParallelCursor`, which is shared across all branches. But the `await_ready()` check happens in `ProducerProgressAwaitable` which is created per-branch.

## Next Steps

### Option 1: Prevent Re-parking at DoPull Level
Instead of relying on `await_ready()`, check `interrupted_` in the `DoPull` loop BEFORE getting `producer_epoch`:

```cpp
while (true) {
  {
    std::unique_lock lock(mutex_);
    if (interrupted_) {
      co_return false;  // Exit immediately if interrupted
    }
    if (producer_active_ && !all_pulled_) {
      producer_epoch = producer_waiters_.Epoch();
    }
  }
  // ...
}
```

### Option 2: Force FINISHED State in WrapTask
When a PARKED task is resumed, if it was interrupted, force it to FINISHED immediately:

```cpp
// In WrapTask
if (expected == Task::State::PARKED) {
  // Check if we should actually run or just finish
  if (was_interrupted) {
    state->store(Task::State::FINISHED);
    return false;
  }
}
```

### Option 3: Fix Race in await_suspend Path 2
Currently Path 2 returns `true` (suspend) if epoch doesn't match after registration. It should return `false`:

```cpp
if (cursor_->producer_waiters_.RegisterWaiter(..., observed_epoch)) {
  // Check if epoch changed while we were registering
  // If so, DON'T suspend - the NotifyAll already happened
  return false;  // Currently returns true in some paths
}
```

## Current Code State

Branch: `coroutine_cursors`
Commits:
- be67a71a1: ParallelBranchCursor::Interrupt()
- 0393444fa: ScanParallelCursor fixes with interrupted_ flag
- ef8cfce67: Additional debug logging
- 60f855013: Initial trace logging

## Test Status

Tests are still deadlocking. The `interrupted_` flag approach is not preventing re-parking as expected.

## Recommended Fix

The most reliable fix is **Option 1**: Check `interrupted_` at the start of each `DoPull` loop iteration BEFORE attempting to park. This ensures that once `Interrupt()` has been called, no task can ever park again, regardless of race conditions in the awaitable mechanism.