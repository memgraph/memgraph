# Issue P2: Unsafe Coroutine Destruction in ~PullAwaitable()

## Executive Summary
**Severity:** REVISED - Original claim was incorrect
**Location:** `src/query/plan/cursor_awaitable_core.hpp:143-147`
**Issue Type:** False Positive - destroying suspended coroutine IS legal per C++ standard
**Status:** ✅ CLOSED (main claim incorrect, but see "Real Concern" below)

### Subagent Verification Results

**VERDICT: FALSE POSITIVE** - The central claim that destroying a suspended coroutine is UB is **incorrect**.

Per C++20 N4860 [coroutine.handle.destroy]: Precondition is that the coroutine **IS suspended**. `destroy()` on a suspended coroutine is the correct cleanup mechanism.

### Real Concern Identified (Different Issue)

**NEW FINDING:** Parallel branch destruction while tasks are RUNNING (not suspended) on worker threads is real UB. This can occur in `~PullPlan()` → `cursor_->Reset()` → branch cursor destruction.

This is a separate issue from the original P2 claim. See "Parallel Branch Destruction Race" section below.

---

## Problem Description

`~PullAwaitable()` unconditionally calls `handle_.destroy()` if a handle exists. According to the C++ standard, destroying a coroutine handle while the coroutine is suspended (not done) is undefined behavior. This can happen if:

1. A cursor is destroyed while its coroutine is suspended (yielded or parked)
2. A parent coroutine is destroyed while a child coroutine is suspended
3. An exception causes early exit without proper cleanup

### Vulnerable Code
```cpp
~PullAwaitable() {
  if (handle_) {
    handle_.destroy();  // <-- UB if coroutine suspended!
  }
}
```

### C++ Standard Reference (CORRECTED)
From C++20 N4860 [coroutine.handle.destroy]:
> "Preconditions: *this refers to a coroutine that is **suspended**."

The standard **REQUIRES** the coroutine to be suspended before calling `destroy()`. The original claim in this document was incorrect - the quoted "Requires" text does not exist in the standard.

**What IS undefined behavior:**
- Calling `destroy()` on a **running** (not suspended) coroutine
- Calling `destroy()` on an already-destroyed coroutine
- Calling `resume()` on a done coroutine

**What is WELL-DEFINED:**
- Calling `destroy()` on a **suspended** coroutine - this is the intended cleanup mechanism

---

## Call Graph - Destruction Scenarios

### Scenario 1: Cursor Destruction While Suspended

```
Query Execution
  └─ PullPlan::Pull()
      └─ cursor_->Pull(frame, context)
          └─ PullAwaitable::ResumeAwaitable.Resume()
              └─ RunPullToCompletion()
                  └─ resume_target.resume()  // Runs coroutine
                      └─ co_await AbortCheck(context)  // Yields!
                          └─ YieldPointAwaitable::await_suspend()
                              └─ *ctx.suspended_task_handle_ptr = handle
                              └─ return void  // Suspends here

[Coroutine now suspended at AbortCheck]

[External event: Query cancelled]
  └─ PullPlan::~PullPlan()
      └─ cursor_->Reset()
          └─ Cursor::Reset()
              └─ gen_.reset()  // Destroys PullAwaitable
                  └─ ~PullAwaitable()
                      └─ handle_.destroy()  // <-- UB! Coroutine suspended
```

### Scenario 2: Parent Cursor Destroyed While Child Suspended

```
Parent Cursor (Expand)
  └─ DoPull()
      └─ co_await input_cursor->Pull()  // Child coroutine
          └─ Child::DoPull()
              └─ co_await AbortCheck()  // Child yields
                  └─ Child suspended

[Parent still running, but now an exception occurs]
  └─ Exception thrown
      └─ Stack unwinding
          └─ ~Child::PullAwaitable()  // Child destroyed
              └─ handle_.destroy()  // <-- UB! Child suspended
          └─ ~Parent::PullAwaitable()
              └─ handle_.destroy()
```

### Scenario 3: Branch Task After ExecuteBranchesInParallel Returns

```
ExecuteBranchesInParallel
  └─ Schedule branch tasks
      └─ Branch task starts
          └─ cursor->Pull()
              └─ co_await AbortCheck()  // Branch yields
                  └─ suspended_handle set
                  └─ return true (yielded)

[Main thread]
  └─ Structured join loop
      └─ HasNonTerminalTasks() = true
      └─ co_await WaitForProgressAwaitable()
          └─ Suspends main coroutine

[Error condition]
  └─ exception_occurred = true
      └─ Break from join loop
      └─ WaitOrSteal()  // Drain tasks
          └─ Branch task resumes and completes
      └─ ExecuteBranchesInParallel returns

[Stack unwinding through caller]
  └─ ~AggregateParallelCursor()
      └─ ~ParallelBranchCursor()
          └─ ~branch_cursors_[]
              └─ ~ScanParallelCursor::gen_map_
                  └─ ~CoroutineTracker
                      └─ awaitable = PullAwaitable{}
                          └─ ~PullAwaitable()
                              └─ handle_.destroy()
                                  // If branch still suspended, UB!
```

---

## Root Cause Analysis

### Why This Happens

1. **Coroutine Lifetime ≠ Cursor Lifetime**: Cursors can be destroyed while coroutines are suspended
2. **No Suspend-State Tracking**: `PullAwaitable` doesn't track if coroutine is suspended
3. **RAII Destruction**: `~PullAwaitable()` is called automatically, no chance to check state

### Current Mitigation: CoroutineTracker Pattern

`ScanParallelCursor` uses a wrapper to defer destruction:
```cpp
struct CoroutineTracker {
  PullAwaitable awaitable;
  ScanParallelCursor *cursor;

  ~CoroutineTracker() {
    awaitable = PullAwaitable{};  // Destroys here
    cursor->active_coroutines_--; // Track destruction
  }
};
```

**But this only helps if:**
- Cursor properly waits for `active_coroutines_` to reach 0
- Structured join guarantees all tasks complete
- No exceptions during teardown

### Why It's Still Dangerous

1. **Not All Cursors Use CoroutineTracker**: Most cursors just have `std::optional<PullAwaitable> gen_`

2. **Exception Paths**: Exceptions can cause destruction before structured join completes

3. **Nested Coroutines**: Parent destruction destroys children even if children suspended

4. **Manual Reset()**: `Cursor::Reset()` destroys generator, potentially while suspended

---

## Detailed Call Graph - Complete Lifecycle

### Normal Lifecycle (Safe)
```
Cursor Creation
  └─ gen_ = std::nullopt

First Pull()
  └─ gen_.emplace(DoPull())  // Coroutine created
      └─ Compiler allocates frame
      └─ Initial suspend
      └─ Returns PullAwaitable with handle

Subsequent Pulls
  └─ gen_->Resume()
      └─ await_suspend returns handle
      └─ Symmetric transfer to coroutine
      └─ Runs until co_yield/co_return
          ├─ co_yield: Suspend, return true
          └─ co_return: Final suspend, done() = true

Reset() / Destructor
  └─ gen_.reset()  // When done() = true
      └─ ~PullAwaitable()
          └─ handle_.destroy()  // Safe - coroutine done
```

### Dangerous Lifecycle (UB)
```
Cursor Creation
  └─ gen_ = std::nullopt

First Pull()
  └─ gen_.emplace(DoPull())
      └─ Coroutine created and suspended at initial_suspend

Pull()
  └─ Resume()
      └─ Coroutine runs
      └─ co_await AbortCheck()
          └─ await_ready() = false (yield requested)
          └─ await_suspend stores handle
          └─ Returns to caller (coroutine suspended)

[External trigger: Query cancelled]
  └─ ~Cursor()
      └─ gen_.reset()
          └─ ~PullAwaitable()
              └─ handle_.destroy()  // UB! Coroutine suspended at AbortCheck
```

---

## Proposed Fix Approaches

### Option 1: Check Before Destroy (Minimal Fix)

```cpp
~PullAwaitable() {
  if (handle_) {
    // Check if we can safely destroy
    if (handle_.done()) {
      // Safe: coroutine completed
      handle_.destroy();
    } else {
      // Dangerous: coroutine suspended
      // Options:
      // 1. Log and skip (leak)
      // 2. Assert in debug (catch bugs)
      // 3. Try to resume and let it complete
      DMG_ASSERT(false, "Destroying suspended coroutine - this is a bug!");
    }
  }
}
```

**Pros:**
- Simple
- Catches bugs early in debug

**Cons:**
- Doesn't fix underlying issue
- Release builds still UB
- What do we do in release if not done?

### Option 2: Safe Destroy with Resume (Complex)

```cpp
~PullAwaitable() {
  if (handle_) {
    if (!handle_.done()) {
      // Need to let coroutine complete
      // But we're in a destructor - can't suspend
      // Can only synchronously run to completion

      // Set a flag that coroutine checks
      handle_.promise().request_abort = true;

      // Resume to let it clean up
      while (!handle_.done()) {
        handle_.resume();
      }
    }
    handle_.destroy();
  }
}
```

**Pros:**
- Actually completes coroutine

**Cons:**
- Synchronous resume in destructor is dangerous
- Could cause deadlock
- May violate async assumptions
- Infinite loop if coroutine doesn't check flag

### Option 3: Ownership Transfer (Best Practice)

Use `CoroutineTracker` pattern everywhere:
```cpp
class Cursor {
 protected:
  std::optional<CoroutineTracker> gen_;  // Instead of PullAwaitable

  struct CoroutineTracker {
    PullAwaitable awaitable;
    std::atomic<size_t> *active_count;

    ~CoroutineTracker() {
      // Wait for coroutine to complete
      if (!awaitable.GetHandle().done()) {
        // Problem: How do we wait without blocking?
      }
      awaitable = PullAwaitable{};
      if (active_count) active_count->fetch_sub(1);
    }
  };
};
```

**Pros:**
- Explicit lifetime tracking
- Can implement proper wait

**Cons:**
- Major refactoring
- Requires all cursors to track
- Still has "how to wait" problem

### Option 4: Structured Concurrency Guarantee (Recommended)

Ensure coroutine completes before destruction by design:
```cpp
class PullAwaitable {
 public:
  ~PullAwaitable() {
    if (handle_) {
      DMG_ASSERT(handle_.done(),
        "PullAwaitable destroyed while coroutine suspended. "
        "This indicates a bug in structured concurrency. "
        "Ensure AllTerminal() before cursor destruction.");
      handle_.destroy();
    }
  }
};
```

**Plus fix callers:**
1. `ExecuteBranchesInParallel` must ensure `AllTerminal()` before return
2. `Cursor::Reset()` must drain coroutines first
3. Exceptions must trigger proper teardown

**Pros:**
- Catches design bugs
- No runtime overhead in release
- Forces correct structured concurrency

**Cons:**
- Requires fixing all callers
- Debug-only detection

### Option 5: Reference Counting (Complex but Safe)

```cpp
class PullAwaitable {
  struct SharedState {
    std::coroutine_handle<> handle;
    std::atomic<size_t> refs{1};
    std::atomic<bool> destroying{false};
    std::condition_variable cv;
    std::mutex mtx;
  };

  std::shared_ptr<SharedState> state_;

 public:
  ~PullAwaitable() {
    if (state_ && --state_->refs == 0) {
      state_->destroying = true;
      if (!state_->handle.done()) {
        // Signal coroutine to complete
        // Wait for completion
        std::unique_lock lock(state_->mtx);
        state_->cv.wait(lock, [&] {
          return state_->handle.done();
        });
      }
      state_->handle.destroy();
    }
  }
};
```

**Pros:**
- Actually safe
- Can wait properly

**Cons:**
- High complexity
- Performance overhead
- May deadlock

---

## Recommended Approach: Option 4 + Audit

1. **Add assertion in ~PullAwaitable()** (debug builds)
2. **Audit all cursor destruction paths** to ensure coroutines complete first
3. **Fix ExecuteBranchesInParallel** to guarantee AllTerminal()
4. **Fix Cursor::Reset()** to properly drain coroutines

### Verification Checklist

- [ ] All parallel operators wait for `AllTerminal()` before return
- [ ] `Cursor::Reset()` checks `gen_->Done()` before reset
- [ ] Exception paths trigger proper task draining
- [ ] Nested cursors complete children before parent destruction
- [ ] No synchronous resume in destructors

---

## Testing Strategy

1. **ASan/UBSan Build**: Run with `handle_.destroy()` instrumented
2. **Stress Test**: Rapid create/destroy cursors with suspended coroutines
3. **Exception Test**: Throw exceptions during suspended coroutines
4. **Code Review**: Check all cursor destruction paths

---

## Appendix: Parallel Branch Destruction Race (Real Concern)

### The Issue

While destroying a **suspended** coroutine is legal, destroying a **running** coroutine is UB. In parallel execution:

1. Branch tasks execute on worker pool threads
2. A branch task's coroutine can be RUNNING (not suspended) while:
   - `~PullPlan()` is called on the main thread
   - `cursor_->Reset()` cascades to branch cursor destruction
   - `~PullAwaitable()` calls `handle_.destroy()` on a RUNNING coroutine

### Vulnerable Interleaving

```
[Worker Thread]                          [Main Thread]
    |                                          |
    |--- Branch task executing --------------->|
    |    (coroutine RUNNING)                   |
    |                                          |--- Query cancelled
    |                                          |--- ~PullPlan()
    |                                          |    └─ cursor_->Reset()
    |                                          |         └─ Destroy branch cursor
    |                                          |              └─ ~PullAwaitable()
    |                                          |                   └─ handle_.destroy()
    |<----------------------------------------X  <-- UB! Destroying running coroutine
```

### Mitigation Status

- `ExecuteBranchesInParallel` has `DMG_ASSERT(AllTerminal())` before return
- `Interrupt()` signals tasks to exit
- `WaitOrSteal()` drain attempts to complete tasks
- **BUT**: `~PullPlan()` does NOT go through this structured join - it directly calls `Reset()`

### Fix Options

1. **Ensure structured join in ~PullPlan()**: Call `Interrupt()` + wait before `Reset()`
2. **Check active_coroutines_ in Reset()**: Wait for completion before clearing `gen_map_`
3. **Use CoroutineTracker pattern**: Spin-wait in destructor on `active_coroutines_` counter

### Severity

| Scenario | Severity |
|----------|----------|
| Single-threaded cursor destruction while suspended | NONE (legal) |
| Nested cursor destruction while children suspended | NONE (legal) |
| Parallel branch cursor destruction while branch RUNNING | **HIGH** (real UB) |

---

## Document History

| Date | Change |
|------|--------|
| Original | Identified P2 as UB when destroying suspended coroutine |
| Post-Review | **CLOSED** - Main claim incorrect per C++ standard; Added parallel race concern as separate issue |
