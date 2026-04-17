# Parallel Execution Deadlock Analysis

## Executive Summary

During exception handling in parallel query execution, worker threads can deadlock in `TaskCollection::Wait()` waiting for PARKED tasks that never reach FINISHED state. The root cause is that PARKED tasks in `ScanParallelCursor` may not receive the wake signal when an exception occurs in another parallel branch.

## Deadlock Call Graph

```
ExecuteBranchesInParallel (operator.cpp:11121)
    ↓ calls WaitOrSteal()
TaskCollection::WaitOrSteal() (priority_thread_pool.cpp:531)
    ↓ calls Wait()
TaskCollection::Wait() (priority_thread_pool.cpp:516) ← STUCK HERE
    ↓ waits for
Task.state_->wait(FINISHED) ← Never reaches FINISHED
```

## Complete Call Chain Analysis

| Function | File:Line | What It Does | Deadlock Role | Current State | Fix Required |
|----------|-----------|--------------|---------------|---------------|--------------|
| `ExecuteBranchesInParallel` | operator.cpp:11084 | Main parallel execution coordinator. Creates tasks, runs them, waits for completion. | Calls `WaitOrSteal()` in teardown phase after exception. | ✅ Working | None |
| `ParallelBranchCursor::Interrupt` | operator.cpp:10830 | NEW: Forwards `Interrupt()` to all branch cursors to wake PARKED tasks. | Propagates wake signal down cursor chain. | ✅ Added | Ensure it calls all cursors |
| `ScanParallelCursor::Interrupt` | operator.cpp:10148 | NEW: Sets `shutting_down_=true` and calls `NotifyAll()` to wake parked coroutines. | Wakes tasks waiting on `producer_waiters_` | ✅ Added | Must set flag BEFORE NotifyAll |
| `TaskCollection::Wait` | priority_thread_pool.cpp:516 | Waits for all tasks to reach FINISHED state via atomic wait. | **DEADLOCK LOCATION**: Blocks forever if task never reaches FINISHED. | ⚠️ Partial | Should check for cancellation |
| `WrapTask` catch block | priority_thread_pool.cpp:504-507 | When task throws, sets FINISHED and rethrows. | Exception path DOES set FINISHED correctly. | ✅ Working | None |
| `ProducerProgressAwaitable::await_ready` | operator.cpp:9949 | Checks `shutting_down_` - returns true if tearing down (don't suspend). | Prevents parking if shutdown requested. | ✅ Working | None |
| `ProducerProgressAwaitable::await_suspend` | operator.cpp:9955 | Registers task as waiter on `producer_waiters_`. | May park task even if shutdown is imminent (race window). | ⚠️ Edge case | Check shutting_down_ after registering |
| `WorkerResumeEvent::NotifyAll` | priority_thread_pool.cpp:693 | Wakes all registered waiters by rescheduling them. | Wakes PARKED tasks. | ✅ Working | None |
| `CurrentResumableTask::RegisterWaiter` | priority_thread_pool.cpp:223 | Registers current task as waiter on WorkerResumeEvent. | Links task to wake mechanism. | ✅ Working | None |
| `TaskCollection::WrapTask` | priority_thread_pool.cpp:457 | Wraps task execution, handles state transitions. | Exception in catch block sets FINISHED. | ✅ Working | None |

## Deadlock Scenario

1. **Branch A** is executing normally
2. **Branch B** is PARKED waiting on `ProducerProgressAwaitable` (scanning for more data)
3. **Branch A** throws an exception (e.g., type error in aggregation)
4. Exception propagates through `WrapTask` catch block → sets FINISHED for Branch A ✅
5. `ExecuteBranchesInParallel` teardown loop calls `Interrupt()` on all cursors
6. `ParallelBranchCursor::Interrupt()` → `AggregateParallelCursor::Interrupt()` (inherited empty) → Wait, no forwarding!

**THE BUG**: Before the fix, the chain was:
```
ExecuteBranchesInParallel::Interrupt()
    ↓ calls
AggregateParallelCursor::Interrupt() → inherits Cursor::Interrupt() which is EMPTY
    ↓ does NOT call
ScanParallelCursor::Interrupt()
```

So PARKED tasks in `ScanParallelCursor` never received `NotifyAll()` and stayed PARKED forever.

## The Fix

### Fix 1: ParallelBranchCursor::Interrupt() (operator.cpp:10830)
```cpp
void Interrupt() override {
  for (const auto &cursor : branch_cursors_) {
    cursor->Interrupt();
  }
}
```
Ensures all branch cursors receive the interrupt signal.

### Fix 2: ScanParallelCursor::Interrupt() (operator.cpp:10148)
```cpp
void Interrupt() override {
  input_cursor_->Interrupt();
  // Signal teardown before waking so PARKED tasks check the flag and exit.
  {
    const std::lock_guard lock(mutex_);
    shutting_down_ = true;
  }
  producer_waiters_.NotifyAll();
}
```
Sets `shutting_down_` BEFORE waking tasks, so when they resume they check the flag and exit.

### Fix 3: ProducerProgressAwaitable Race Check (operator.cpp:9973-9980)
```cpp
// Check if shutdown was requested between await_ready() and now.
{
  const std::lock_guard lock(cursor_->mutex_);
  if (cursor_->shutting_down_) {
    if (context_->task_parked_ptr) {
      *context_->task_parked_ptr = false;
    }
    return false;  // Don't suspend
  }
}
```
Handles the race between checking `shutting_down_` in `await_ready()` and registering as waiter in `await_suspend()`.

## Race Condition Explanation

The race window:
```
Time →

Task:                      Interrupt Thread:
─────────────────          ─────────────────────────
await_ready() {
  lock(mutex)
  return shutting_down_; ←  lock(mutex)
  unlock(mutex)              shutting_down_ = true;
}                            NotifyAll();
                           unlock(mutex)
await_suspend() {
  RegisterWaiter()  ←─── Now epoch changed!
  return true; // Suspend
}
// Task is now PARKED but
// missed the NotifyAll()
```

The fix adds a second check inside `await_suspend()` (after locking) to catch this race.

## Verification

Test: `test_exception_matrix[ExceptionOrigin.FIRST-2]` with 16 workers
- Before fix: Deadlock in TaskCollection::Wait()
- After fix: Test passes ✅

## Test Results

| Test Suite | Before Fix | After Fix |
|------------|------------|-----------|
| Correctness (27 tests) | ✅ Pass | ✅ Pass |
| Exceptions (44 tests) | ❌ Deadlock | ✅ Pass |
| Termination | ❌ Deadlock | ✅ Pass |

## Files Modified

1. `src/query/plan/operator.cpp`:
   - Added `ParallelBranchCursor::Interrupt()` at line 10830
   - Modified `ScanParallelCursor::Interrupt()` at line 10148
   - Added race check in `ProducerProgressAwaitable::await_suspend()` at line 9973

## Alternative Approaches Considered

1. **Modify TaskCollection::Wait() to check exception_occurred**: This would couple the thread pool to query semantics, which is architecturally unclean.

2. **Force FINISHED on all tasks during teardown**: Would require exposing internal task state, breaking encapsulation.

3. **Use wait_for() instead of wait()**: Would add polling overhead and complexity.

The chosen solution (proper Interrupt() propagation) is the cleanest as it:
- Uses existing mechanisms (WorkerResumeEvent, NotifyAll)
- Maintains clear separation of concerns
- Follows the existing cursor pattern
- Requires minimal code changes
