# Issue P7: TaskCollection Missing Cancellation/Timeout Mechanism

## Executive Summary
**Severity:** MEDIUM
**Location:** `src/utils/priority_thread_pool.cpp:524-538` (Wait), `594-599` (WaitForProgress)
**Issue Type:** Defensive improvement - Missing cancellation/timeout
**Status:** OPEN - Defensive enhancement

---

## Problem Description

`TaskCollection::Wait()` and `TaskCollection::WaitForProgress()` can block indefinitely if a task gets stuck in PARKED state. This was observed in the live deadlock (Issue P4). There's no mechanism to:

1. Cancel a waiting operation
2. Timeout after a reasonable period
3. Force-unblock waiters during emergency shutdown

### Vulnerable Code

```cpp
void TaskCollection::Wait() {
  for (auto& task : tasks_) {
    auto state = task.state_->load();
    while (state != Task::State::FINISHED) {
      if (state == Task::State::PARKED) {
        // BLOCKS FOREVER if task never wakes up
        task.state_->wait(Task::State::PARKED);
      }
      state = task.state_->load();
    }
  }
}
```

---

## Why This Matters

Even after fixing P4 (silent task dropping), defensive programming suggests adding safeguards:

1. **Hung task detection**: If a task is PARKED for >N seconds, something is wrong
2. **Graceful degradation**: Allow query to fail with error instead of hanging forever
3. **Observability**: Add hooks to detect and report stuck waits

---

## Fix Options

### Option 1: Add Timeout to Wait() (RECOMMENDED)

```cpp
bool TaskCollection::Wait(std::chrono::milliseconds timeout = std::chrono::milliseconds(30000)) {
  auto deadline = std::chrono::steady_clock::now() + timeout;

  for (auto& task : tasks_) {
    auto state = task.state_->load();
    while (state != Task::State::FINISHED) {
      if (state == Task::State::PARKED) {
        // Use timed wait instead of indefinite wait
        auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
          spdlog::error("TaskCollection::Wait() timeout - task stuck in PARKED state");
          return false;  // Indicate timeout
        }

        task.state_->wait_for(Task::State::PARKED, deadline - now);
      }
      state = task.state_->load();
    }
  }
  return true;  // All tasks finished
}
```

### Option 2: Add Cancellation Token

```cpp
class TaskCollection {
  std::atomic<bool> cancelled_{false};

public:
  void Cancel() {
    cancelled_.store(true);
    // Wake all waiters
    for (auto& task : tasks_) {
      task.state_->notify_all();
    }
  }

  bool Wait() {
    for (auto& task : tasks_) {
      auto state = task.state_->load();
      while (state != Task::State::FINISHED && !cancelled_.load()) {
        if (state == Task::State::PARKED) {
          task.state_->wait(Task::State::PARKED);
        }
        state = task.state_->load();
      }

      if (cancelled_.load()) {
        return false;  // Cancelled
      }
    }
    return true;
  }
};
```

### Option 3: Add Heartbeat/Health Check

Add periodic logging when waiting for extended periods:

```cpp
void TaskCollection::Wait() {
  auto last_log = std::chrono::steady_clock::now();

  for (auto& task : tasks_) {
    auto state = task.state_->load();
    while (state != Task::State::FINISHED) {
      // Log if waiting for too long
      auto now = std::chrono::steady_clock::now();
      if (now - last_log > std::chrono::seconds(5)) {
        spdlog::warn("TaskCollection::Wait() still waiting for task in state={}",
                     static_cast<int>(state));
        last_log = now;
      }

      if (state == Task::State::PARKED) {
        task.state_->wait_for(Task::State::PARKED, std::chrono::seconds(1));
      }
      state = task.state_->load();
    }
  }
}
```

---

## Recommended Fix

**Implement Option 1** (timeout) + **Option 3** (heartbeat logging) as defensive measures.

This won't fix the root cause (P4), but will:
1. Prevent infinite hangs
2. Provide clear error messages when issues occur
3. Enable faster detection of problems

---

## Implementation Notes

### Files to Modify
- `src/utils/priority_thread_pool.hpp` - Add timeout parameter, cancellation flag
- `src/utils/priority_thread_pool.cpp` - Implement timeout logic

### Callers to Update
- `ExecuteBranchesInParallel` - Handle Wait() returning false
- Other TaskCollection users - May need to handle timeout

---

## Related Issues

| Issue | Relationship | Status |
|-------|-------------|--------|
| P4 | Root cause - if P4 fixed, this is less critical | Open |
| **P7** | **This issue - defensive safeguard** | **Open** |

---

## Testing Strategy

1. **Unit Test**:
   - Create TaskCollection with stuck PARKED task
   - Verify Wait() times out after specified duration

2. **Integration Test**:
   - Simulate task getting stuck
   - Verify error is reported, not infinite hang

---

## Document History

| Date | Change |
|------|--------|
| 2025-01-17 | Identified as defensive improvement during deadlock analysis |
