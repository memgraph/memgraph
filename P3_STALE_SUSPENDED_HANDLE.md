# Issue P3: Stale suspended_handle Reference in Branch Tasks

## Executive Summary
**Severity:** CLOSED - False Positive
**Location:** `src/query/plan/operator.cpp:10919` (branch task lambda capture)
**Issue Type:** Initially identified as Use-After-Free, but subagent verification found the analysis incorrect
**Status:** ❌ FALSE POSITIVE - Lambda closure member is heap-allocated, address is stable

### Subagent Verification Results

**VERDICT: FALSE POSITIVE** - The central claim is incorrect.

The `suspended_handle` is **NOT a stack variable** - it is a **closure data member** captured by value in the lambda. Lambda closures are heap-allocated when stored in `std::function` (via `ResumableTaskSignature`). The address of closure members is stable across invocations.

Additionally, `suspended_task_handle_ptr` is cleared to `nullptr` before the lambda returns, and the structured join (`AllTerminal()`) guarantees all tasks finish before `TaskCollection` destruction.

---

## Problem Description

In `ExecuteBranchesInParallel`, branch tasks capture `&suspended_handle` by reference. The `suspended_handle` is a stack variable in the lambda that gets stored into `context.suspended_task_handle_ptr`. If the branch task yields and the parent coroutine (ExecuteBranchesInParallel) is destroyed before the task resumes, the `suspended_handle` reference becomes dangling. When the task eventually resumes, it writes to freed memory.

### Vulnerable Code

```cpp
// operator.cpp:10919
PullAwaitable ExecuteBranchesInParallel(...) {
  // ...

  tasks.AddResumableTask([this,
                          i,
                          // ... other captures ...
                          awaitable_opt = std::optional<plan::PullAwaitable::ResumeAwaitable>(),
                          suspended_handle = std::coroutine_handle<>(),  // Stack variable!
                          task_parked = false,
                          // ...
                          ]() mutable -> bool {
    // ...
    context.suspended_task_handle_ptr = &suspended_handle;  // Stored for yield
    // ...

    // If task yields:
    if (!awaitable_opt) {
      // ...
    }

    if (suspended_handle) {  // Reading stack variable
      result = plan::PullRunResult::Yielded();
    }
    // ...

    if (result.status == plan::PullRunResult::Status::Yielded) {
      return true;  // Task yields, will be rescheduled
    }
    // ...
  });

  // After adding all tasks:
  collection_scheduler_->Trigger();  // Tasks may start executing

  // Branch 0 runs on main thread
  co_await cursor->Pull(frame, context);  // May yield!

  // Wait for branches with structured join
  while (collection_scheduler_->HasNonTerminalTasks()) {
    // If exception occurs here, ExecuteBranchesInParallel coroutine destroyed
    // while branch tasks still have &suspended_handle references!
    co_await collection_scheduler_->WaitForProgressAwaitable();
  }
}
```

---

## Call Graph - Race to Destruction

### Timeline: Exception During Structured Join

```
Main Thread (ExecuteBranchesInParallel Coroutine)
  ├─ Create TaskCollection
  ├─ Add branch tasks 1..N-1
  │   └─ Each task captures &suspended_handle (stack ref in lambda)
  ├─ Trigger scheduler
  ├─ Run branch 0
  │   └─ co_await cursor->Pull()
  │       └─ May complete or yield
  ├─ Enter structured join loop
  │   └─ while (HasNonTerminalTasks())
  │       ├─ TryExecuteOneIdleTask()
  │       ├─ Check exception_occurred
  │       └─ co_await WaitForProgressAwaitable()  // <-- Suspend here

Worker Thread (Branch Task 1)
  ├─ Start executing task lambda
  ├─ context.suspended_task_handle_ptr = &suspended_handle
  ├─ cursor->Pull()
  │   └─ co_await AbortCheck()
  │       └─ yield_requested = true
  │       └─ await_suspend stores handle in *suspended_task_handle_ptr
  │       └─ Suspends branch task coroutine
  ├─ Return true (yielded) to ResumableWrapper
  └─ RescheduleTaskOnWorker() schedules continuation
      └─ Task now waiting in pool queue

[Exception Occurs in another branch]
  ├─ Branch 2 throws exception
  ├─ exception_occurred.fetch_or(true, release)
  └─ Main thread sees exception in join loop

Main Thread (continued)
  ├─ Break from join loop due to exception
  ├─ HasNonTerminalTasks() = true (branch 1 still SCHEDULED)
  ├─ Enter teardown phase
  │   ├─ tearing_down.store(true)
  │   ├─ Interrupt() all cursors
  │   │   └─ Wake PARKED tasks
  │   └─ WaitOrSteal()  // Drain remaining tasks
  │       └─ Branch 1 task completes here
  │           └─ Check tearing_down, exit early
  │           └─ Return false (finished)
  ├─ DBG_ASSERT(AllTerminal()) passes
  ├─ ExecuteBranchesInParallel returns
  └─ Coroutine frame destroyed!
      └─ Lambda stack variables destroyed
          └─ suspended_handle destroyed!

[But what if branch task wasn't drained?]
Alternative Scenario:
  ├─ WaitOrSteal() doesn't get all tasks (race?)
  ├─ Some task still SCHEDULED in pool queue
  ├─ ExecuteBranchesInParallel returns
  ├─ Lambda destroyed, suspended_handle freed
  └─ Later: Pool worker picks up scheduled task
      └─ Task resumes with &suspended_handle (dangling!)
      └─ Writes to freed memory ← UAF!
```

---

## Detailed Memory Layout

### Lambda Capture Layout

```cpp
// Conceptual layout of branch task lambda
struct BranchTaskLambda {
  // Captured by value (copied into lambda frame)
  size_t i;
  shared_ptr<vector<exception_ptr>> exceptions;
  shared_ptr<vector<ExecutionContext>> branch_contexts;
  // ... other shared_ptr captures ...

  // Captured by value (primitives in lambda frame)
  optional<PullAwaitable::ResumeAwaitable> awaitable_opt;
  coroutine_handle<> suspended_handle;  // <-- IN LAMBDA FRAME
  bool task_parked;
  size_t frame_size;
  thread::id main_thread;
  // ...
};
```

### Where Each Piece Lives

| Variable | Location | Lifetime |
|----------|----------|----------|
| `suspended_handle` | Lambda frame (stack when executing) | Lambda execution |
| `context.suspended_task_handle_ptr` | ExecutionContext pointer | Branch task execution |
| `*suspended_task_handle_ptr` | Points to `suspended_handle` | While task running |
| Lambda itself | Heap (std::function) | Until Task FINISHED |

### The Problem

```cpp
// Inside task lambda:
context.suspended_task_handle_ptr = &suspended_handle;  // Points to lambda frame

// After yield and reschedule:
// - Lambda returns true (yielded)
// - State = SCHEDULED
// - Lambda frame destroyed (function call ended)
// - suspended_handle destroyed!

// Later when rescheduled:
// - New invocation of lambda
// - New suspended_handle instance
// - But YieldPointAwaitable may have stored to old address!
```

---

## Why It May Not Manifest

### Current Protection: Structured Join

```cpp
while (collection_scheduler_->HasNonTerminalTasks()) {
  // Wait for ALL tasks to be FINISHED
}
// Only return when AllTerminal() = true
```

This ensures:
1. All branch tasks reach FINISHED state
2. Task lambdas complete
3. `suspended_handle` no longer accessed

### But Race Window Exists

```cpp
if (collection_scheduler_->HasNonTerminalTasks()) {
  // Teardown phase
  tearing_down.store(true);
  for (auto &cursor : branch_cursors_) {
    cursor->Interrupt();  // Wake PARKED tasks
  }
  active_collection->WaitOrSteal();  // <-- What if task not FINISHED here?
}

#ifndef NDEBUG
DMG_ASSERT(active_collection->AllTerminal(), ...);  // Catches if tasks not done
#endif
```

The assertion catches it in debug, but:
1. Not in release
2. Race between `HasNonTerminalTasks()` check and `WaitOrSteal()`
3. What if task yields DURING teardown?

---

## Root Cause Analysis

### Fundamental Issue

Branch task lambda captures stack variables by reference (implicitly via `this` for member access, explicitly via `&` for pointer storage). When the lambda is invoked, those references point to valid stack memory. But:

1. When lambda returns (yield), stack frame is destroyed
2. References become dangling
3. If lambda re-invoked (after reschedule), new stack frame, new variables
4. But external code (YieldPointAwaitable) may have stored to old address

### Why This Design?

The design assumes:
1. Lambda is single-invocation (no yields)
2. Or if yields, parent waits for completion
3. Structured join ensures completion before parent destruction

But coroutine yields break assumption #1.

---

## Call Graph: Complete Flow with Potential UAF

### Normal Flow (Safe)
```
ExecuteBranchesInParallel
  └─ Create branch tasks
      └─ Task lambda captures suspended_handle by value

  ├─ Branch task runs
  │   └─ context.suspended_task_handle_ptr = &suspended_handle
  │   └─ cursor->Pull()
  │       └─ Runs to completion (no yield)
  │   └─ return false (finished)
  │   └─ State = FINISHED

  └─ Structured join
      └─ HasNonTerminalTasks() = false quickly
      └─ Returns

  └─ All lambdas complete
      └─ suspended_handle properly destroyed
```

### Dangerous Flow (UAF Risk)
```
ExecuteBranchesInParallel
  └─ Create branch tasks

  ├─ Branch task runs
  │   └─ context.suspended_task_handle_ptr = &suspended_handle
  │   └─ cursor->Pull()
  │       └─ co_await AbortCheck()  // Yields!
  │           └─ await_suspend
  │               └─ *ctx->suspended_task_handle_ptr = handle
  │               └─ Stores to suspended_handle
  │           └─ Returns to caller
  │   └─ return true (yielded)
  │   └─ Lambda returns, stack destroyed
  │   └─ suspended_handle destroyed
  │   └─ State = SCHEDULED

  └─ Structured join
      └─ co_await WaitForProgressAwaitable()
          └─ Suspend main coroutine

  [Exception]
  └─ ExecuteBranchesInParallel destroyed
  └─ Lambda frames destroyed

  [Later - Pool Worker]
  └─ Task rescheduled
  └─ Lambda re-invoked (NEW suspended_handle)
  └─ resume_target.resume()
      └─ Coroutine resumes at AbortCheck
      └─ await_resume completes

  [But YieldPointAwaitable already stored to OLD suspended_handle!]
  └─ Old suspended_handle memory may be reused
  └─ Undefined behavior
```

---

## Proposed Fix Approaches

### Option 1: Heap-Allocated Handle Storage (Recommended)

Instead of stack variable, use heap storage that outlives lambda invocations:

```cpp
tasks.AddResumableTask([this,
                        i,
                        // ...
                        suspended_handle_storage = make_shared<coroutine_handle<>>(),
                        task_parked_storage = make_shared<bool>(false),
                        // ...
                        ]() mutable -> bool {
  // ...
  context.suspended_task_handle_ptr = suspended_handle_storage.get();
  context.task_parked_ptr = task_parked_storage.get();

  // ... use as before, but storage survives across yields ...

  if (*suspended_handle_storage) {
    result = PullRunResult::Yielded();
  }

  if (*task_parked_storage) {
    return false;
  }
  // ...
});
```

**Pros:**
- Storage lives until lambda destroyed (Task FINISHED)
- No UAF risk
- Minimal code changes

**Cons:**
- Additional heap allocation per task
- shared_ptr overhead

### Option 2: Use Existing Shared Storage

`suspended_handle` is already captured, but we need it to persist. Use the existing `shared_ptr` pattern:

```cpp
// Add to heap-allocated captures
auto shared_state = make_shared<struct {
  coroutine_handle<> suspended_handle;
  bool task_parked;
}>();

tasks.AddResumableTask([shared_state, ...]() mutable -> bool {
  context.suspended_task_handle_ptr = &shared_state->suspended_handle;
  context.task_parked_ptr = &shared_state->task_parked;
  // ... rest same ...
});
```

**Pros:**
- Single heap allocation per task
- Clean, explicit lifetime

**Cons:**
- Slightly more refactoring

### Option 3: Don't Store Handle Across Yield (Complex)

Redesign to not need persistent suspended_handle:

```cpp
// Instead of storing handle and checking later,
// determine yielded status differently

// YieldPointAwaitable could set a flag instead of storing handle
// Then check flag instead of checking suspended_handle
```

**Pros:**
- No lifetime issues
- Simpler state management

**Cons:**
- Major redesign
- May break other code relying on suspended_task_handle_ptr

### Option 4: Verify Structured Join (Documentation)

Add stronger verification that structured join prevents this:

```cpp
#ifndef NDEBUG
// Before destroying ExecuteBranchesInParallel frame
for (const auto &task : tasks_) {
  DMG_ASSERT(task.state_->load() == Task::State::FINISHED,
             "Task not finished before parent destruction - "
             "stack variables may be dangling!");
}
#endif
```

**Pros:**
- Catches bugs in debug
- No runtime cost in release

**Cons:**
- Doesn't fix underlying issue
- Release builds still at risk

---

## Recommended Approach: Option 1 or 2

Use heap-allocated storage for variables that must persist across yields:

```cpp
// In ExecuteBranchesInParallel

// Before adding tasks, create shared storage
auto branch_states = make_shared<vector<struct BranchState>>(num_branches);

for (size_t i = 1; i < num_branches; i++) {
  auto &state = (*branch_states)[i];

  tasks.AddResumableTask([this, i, branch_states, &state, ...]() mutable -> bool {
    // Use state.suspended_handle instead of local variable
    context.suspended_task_handle_ptr = &state.suspended_handle;
    context.task_parked_ptr = &state.task_parked;
    // ...
  });
}
```

This ensures:
1. Storage lives until `branch_states` destroyed
2. `branch_states` is captured by value (shared_ptr)
3. Parent coroutine destruction is safe

---

## Testing Strategy

1. **Stress Test**: High-frequency yield/exception scenarios

---

## Resolution: Why This is a False Positive

### Fundamental Error in Analysis

The original analysis conflated a lambda's **call stack frame** with its **closure object**.

**What the code actually does:**
```cpp
tasks.AddResumableTask([this, i, ..., suspended_handle = std::coroutine_handle<>(), ...]() mutable -> bool {
    context.suspended_task_handle_ptr = &suspended_handle;
    // ...
});
```

**Key facts:**
1. `suspended_handle` is a **closure data member** (captured by value)
2. The lambda closure is **heap-allocated** when converted to `ResumableTaskSignature`
3. The address `&suspended_handle` is stable - it points to the closure, not the stack
4. `suspended_task_handle_ptr` is cleared to `nullptr` before the lambda returns
5. Structured join (`DMG_ASSERT(AllTerminal())`) guarantees tasks complete before `TaskCollection` destruction

### Why the Race Cannot Occur

| Condition | Protection |
|-----------|------------|
| Address stability | Closure is heap-allocated |
| Use-after-free | `suspended_task_handle_ptr` cleared before return |
| Parent destruction race | `AllTerminal()` ensures completion |
| Concurrent access | Single-threaded per task (worker owns task during execution) |

### Conclusion

The original concern about `suspended_handle` becoming a dangling reference is **not valid**. The lambda closure pattern used in `ExecuteBranchesInParallel` is correct and safe.

---

## Document History

| Date | Change |
|------|--------|
| Original | Identified P3 as use-after-free from stale reference |
| Post-Review | **CLOSED** - Analysis incorrect; lambda closure members are heap-allocated with stable addresses |
2. **ASan Build**: Detect use-after-free
3. **Fuzzing**: Random exception injection during parallel execution
4. **Static Analysis**: Check for reference captures in coroutine lambdas
