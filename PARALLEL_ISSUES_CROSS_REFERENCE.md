# Parallel Execution Issues - Cross Reference Matrix

## Overview

This document cross-references all identified issues in the parallel query execution system, from the initial P1-P3 analysis through the deadlock investigation.

---

## Issue Summary

| Issue | Severity | Status | Type | Primary File |
|-------|----------|--------|------|--------------|
| **P1** | Medium | **Fixed** | Double-resume race | priority_thread_pool.cpp |
| **P2** | Low | Closed (FP) | False positive | cursor_awaitable_core.hpp |
| **P3** | Low | Closed (FP) | False positive | operator.cpp |
| **P4** | **Critical** | **Open** | **Deadlock** | **priority_thread_pool.cpp** |
| **P5** | High | Open | Race condition | operator.cpp |
| **P6** | Medium | **Fixed** | Double-resume pattern | priority_thread_pool.hpp |
| **P7** | Medium | Open | Defensive improvement | priority_thread_pool.cpp |

---

## Cross-Reference Matrix

### How Issues Relate to Each Other

```
P1 (Double-Resume Race - FIXED)
├── Fixed paths: ScanParallelCursor, ProgressAwaitable
├── Related to: P4 (can contribute to state corruption)
├── Related to: P5 (orphaned closure sub-problem)
└── Similar to: P6 (same pattern, different location - also FIXED)

P4 (Silent Task Dropping - PRIMARY DEADLOCK CAUSE)
├── Caused deadlock observed in: 2025-01-17
├── Related to: P1 (state corruption makes worse)
├── Related to: P5 (orphaned closures can trigger)
└── Defensive measure: P7 (timeout can help detect)

P5 (Orphaned Closure Race)
├── Sub-problem of: P1
├── Can trigger: P4 deadlock scenario
└── Location: ProducerProgressAwaitable

P6 (ProgressAwaitable Raw Handle - FIXED)
├── Same pattern as: P1
├── Fix applied: 2025-01-17 (busy-spin fallback)
└── Status: Both P1 and P6 now use safe pattern

P7 (TaskCollection Timeout)
├── Defensive measure for: P4
└── Doesn't fix root cause but prevents infinite hangs
```

---

## Issue Dependencies

### Fix Order Recommendation

1. **First: P4** (Critical - fixes deadlock)
   - Execute task inline in RescheduleTaskOnWorker during shutdown
   - Blocks: None
   - Unblocks: Testing of other fixes

2. **Second: P5** (High - fixes race)
   - Unregister waiter on failed suspend
   - Depends on: P4 (to test properly)
   - Related to: P1

3. **Third: P6** (Medium - code quality)
   - Apply same fix pattern as P1
   - Depends on: P4 stable

4. **Fourth: P7** (Medium - defensive)
   - Add timeout to TaskCollection::Wait()
   - Can be done in parallel with others

---

## Code Location Cross-Reference

| File | Lines | Issues Affected |
|------|-------|-----------------|
| `priority_thread_pool.cpp` | 271-288 | P4 (RescheduleTaskOnWorker) |
| `priority_thread_pool.cpp` | 532 | P4, P7 (TaskCollection::Wait) |
| `priority_thread_pool.cpp` | 651-672 | P4, P6 (NotifyProgress) |
| `priority_thread_pool.cpp` | 711-746 | P1, P4 (NotifyAll) |
| `priority_thread_pool.hpp` | 360 | P6 (ProgressAwaitable) |
| `operator.cpp` | 9961-9997 | P5 (ProducerProgressAwaitable) |
| `operator.cpp` | 11107 | P6 (ExecuteBranchesInParallel) |

---

## Deadlock Timeline Reconstruction

Based on GDB analysis and issue cross-referencing:

```
T0: Parallel ORDER BY query starts
    └─ ExecuteBranchesInParallel begins

T1: Branch tasks start executing
    └─ Task A: DoPull() → co_await ProducerProgressAwaitable
        ├─ RegisterWaiter(producer_waiters_) [P5 race window]
        ├─ Check interrupted_ → false
        └─ Suspend, state=PARKED

T2: Query interrupted (exception or timeout)
    └─ Parent calls Interrupt()
        ├─ interrupted_ = true
        ├─ NotifyAll() wakes PARKED tasks
        │   └─ RescheduleTaskOnWorker() for Task A
        │       └─ [P4] pool_stop_source_.stop_requested()=true
        │           └─ **SILENTLY RETURNS - TASK NEVER RUNS**
        └─ [P5] If race occurred, orphaned closure exists

T3: ExecuteBranchesInParallel calls Wait()
    └─ Task A still in PARKED state
    └─ state.wait(PARKED) blocks forever [P7 would timeout here]

T4: Main thread AwaitShutdown()
    └─ Tries to join worker thread
    └─ Worker thread stuck in Wait()
    └─ **DEADLOCK**
```

---

## Subagent Findings Summary

### Initial Analysis (P1-P3)
- P1: Double-resume race identified
- P2: False positive (destroying suspended coroutine is legal)
- P3: False positive (lambda closure members are heap-allocated)

### Deadlock Investigation
- Live deadlock observed in parallel e2e tests
- GDB attached to stuck memgraph process
- Thread analysis revealed stuck worker in TaskCollection::Wait()

### Subagent Verification
- Confirmed P4 is PRIMARY cause (silent task dropping)
- Confirmed P5 is contributing factor (orphaned closures)
- Noted P1 fix is correct but incomplete (ProgressAwaitable still has issue)
- Identified P7 as defensive measure

---

## Recommended Fixes Summary

### P4 (Critical)
```cpp
void PriorityThreadPool::RescheduleTaskOnWorker(uint16_t worker_id, TaskSignature task) {
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    spdlog::warn("Pool stopping, executing inline");
    task();  // Execute inline instead of dropping
    return;
  }
  // ... normal scheduling
}
```

### P5 (High)
```cpp
// In ProducerProgressAwaitable::await_suspend
if (cursor_->shutting_down_ || cursor_->interrupted_) {
  cursor_->producer_waiters_.RemoveWaiter(handle, observed_epoch_);  // ADD THIS
  return false;
}
```

### P6 (Medium)
Apply same pattern as P1 fix to ProgressAwaitable

### P7 (Medium)
Add timeout parameter to TaskCollection::Wait()

---

## Testing Recommendations

| Test | Purpose | Issues Covered |
|------|---------|----------------|
| Parallel e2e loop test | Reproduce/deadlock | P4, P7 |
| Rapid cancellation stress | Race conditions | P5, P1 |
| Pool shutdown timing | Task dropping | P4 |
| ORDER BY with interrupt | Integration | P4, P6, P7 |

---

## Document References

- P1: `P1_DOUBLE_RESUME_RACE_ANALYSIS.md`
- P2: `P2_UNSAFE_COROUTINE_DESTRUCTION.md`
- P3: `P3_STALE_SUSPENDED_HANDLE.md`
- P4: `P4_SILENT_TASK_DROPPING_DEADLOCK.md`
- P5: `P5_ORPHANED_CLOSURE_RACE.md`
- P6: `P6_PROGRESS_AWAITABLE_RAW_HANDLE.md`
- P7: `P7_TASK_COLLECTION_TIMEOUT.md`
- Master tracker: `PARALLEL_COROUTINE_ANALYSIS_TRACKER.md`

---

## Document History

| Date | Change |
|------|--------|
| 2025-01-17 | Initial P1-P3 analysis |
| 2025-01-17 | Deadlock observed in parallel e2e tests |
| 2025-01-17 | Subagent verification of deadlock |
| 2025-01-17 | Created P4-P7, cross-reference matrix |
