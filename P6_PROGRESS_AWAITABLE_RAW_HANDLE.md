# Issue P6: ProgressAwaitable Raw Coroutine Handle Registration

## Executive Summary
**Severity:** MEDIUM
**Location:** `src/utils/priority_thread_pool.hpp:360`
**Issue Type:** Race condition - Raw coroutine handle in ProgressAwaitable
**Status:** OPEN - Needs same fix pattern as P1

---

## Problem Description

`CollectionScheduler::ProgressAwaitable` registers raw coroutine handles with `TaskCollection`, creating the same double-resume race pattern identified in P1. When `ExecuteBranchesInParallel` calls `co_await collection_scheduler_->WaitForProgressAwaitable()`:

1. The parent coroutine's raw handle is stored in `TaskCollection::progress_waiter_`
2. `NotifyProgress()` can resume this handle via `RescheduleTaskOnWorker`
3. Other paths could also attempt to resume the same handle
4. This creates the same TOCTOU race as P1

### Vulnerable Code

```cpp
// src/utils/priority_thread_pool.hpp:360
class CollectionScheduler {
  struct ProgressAwaitable {
    bool await_suspend(std::coroutine_handle<> handle) const {
      // BUG: Registers raw coroutine handle
      return scheduler_ && scheduler_->RegisterProgressWaiter(handle, observed_epoch_);
    }
    // ...
  };
};
```

### Usage Location

```cpp
// src/query/plan/operator.cpp:11107
auto ExecuteBranchesInParallel(...) -> PullAwaitable {
  // ...
  co_await collection_scheduler_->WaitForProgressAwaitable();  // <-- Uses vulnerable awaitable
  // ...
}
```

---

## Race Condition

Similar to P1, but for the parent coroutine in parallel execution:

```
[Parent Coroutine Path A]              [Parent Coroutine Path B]
    |                                          |
    |--- Suspend at WaitForProgressAwaitable   |
    |    └─ RegisterProgressWaiter             |
    |         └─ Store raw handle              |
    |                                          |
    |--- NotifyProgress runs ----------------->|
    |    └─ RescheduleTaskOnWorker             |
    |         └─ handle.resume()               |
    |                                          |
    |--- Other resume path ----X               |
         └─ Also tries resume()
              → DOUBLE-RESUME RACE
```

Note: Subagent verification confirmed that the parent coroutine in this case is NOT wrapped in `ResumableWrapper`, so the claimed "Path B" from P1 analysis may not exist here. However, the raw handle registration is still a code smell and potential future bug.

---

## Fix Recommendation

Apply the **same fix pattern as P1**:

1. Check for `CurrentResumableTaskScope` availability
2. If available, use safe `CurrentResumableTask::RegisterWaiter()` path
3. If not available, fall through to busy-spin (return false from await_suspend)

### Proposed Fix

```cpp
bool await_suspend(std::coroutine_handle<> handle) const {
    // Try safe task-registration path first (like P1 fix)
    if (utils::CurrentResumableTask::IsInScope()) {
        // Use safe registration via CurrentResumableTask
        return utils::CurrentResumableTask::RegisterWaiter(
            scheduler_->GetProgressEvent(), observed_epoch_);
    }

    // No CurrentResumableTaskScope: fall through to busy-spin
    // (same pattern as the fixed ProducerProgressAwaitable in P1)
    return false;
}
```

However, this requires `TaskCollection` to work with `CurrentResumableTask` paradigm, which may need refactoring.

### Alternative Fix: Add CurrentResumableTaskScope

Wrap the parent coroutine execution in a `CurrentResumableTaskScope`:

```cpp
// In ExecuteBranchesInParallel or its caller
auto scope = utils::CurrentResumableTaskScope::Create(
    // ... task signature ...
);

// Now ProgressAwaitable can use safe registration
```

---

## Relation to P1

| Aspect | P1 (Fixed) | P6 (This Issue) |
|--------|-----------|-----------------|
| Location | `ScanParallelCursor::ProducerProgressAwaitable` | `CollectionScheduler::ProgressAwaitable` |
| Register | `producer_waiters_` (WorkerResumeEvent) | `progress_waiter_` (TaskCollection) |
| Status | **FIXED** with busy-spin fallback | **OPEN** - needs same pattern |
| Risk | Was causing issues | Lower - different code path |

---

## Files to Modify

- `src/utils/priority_thread_pool.hpp` - ProgressAwaitable::await_suspend
- `src/utils/priority_thread_pool.cpp` - Potentially TaskCollection changes
- `src/query/plan/operator.cpp` - May need CurrentResumableTaskScope

---

## Testing Strategy

1. **Code Review**: Verify no double-resume can occur with current code paths
2. **Stress Test**: Run parallel ORDER BY queries extensively
3. **Refactoring**: If fix is applied, verify ProgressAwaitable uses safe path

---

## Document History

| Date | Change |
|------|--------|
| 2025-01-17 | Identified as second double-resume path during P1 analysis |
| 2025-01-17 | Subagent verification noted parent coroutine may not have ResumableWrapper |
