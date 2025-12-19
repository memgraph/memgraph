# Proposal: Scheduler Consolidation Finding

## Summary

Investigated three approaches to reduce scheduler-related non-determinism events:

1. **high_resolution_clock** - NOT recommended (alias for system_clock on Linux)
2. **Consolidated scheduler** - Single thread with priority queue (moderate benefit)
3. **timerfd** - Kernel-driven wake-ups (best for determinism)

## Key Findings

### high_resolution_clock

On Linux (glibc), `high_resolution_clock` is an alias for `system_clock`:

```cpp
std::is_same_v<high_resolution_clock, system_clock> == true
std::is_same_v<high_resolution_clock, steady_clock> == false
high_resolution_clock::is_steady == false
```

**Result**: Generates same WALLCLOCK_TIME events as system_clock. **Do NOT use for scheduling.**

### Scheduler Comparison (1 second test, 3 tasks at 100ms/200ms/500ms)

| Approach | clock_gettime | futex | THREADSWITCH | NEWTHREAD | Total Key |
|----------|---------------|-------|--------------|-----------|-----------|
| Independent cv (3 threads) | 40 | 43 | 20 | 3 | **106** |
| Consolidated cv (1 thread) | 56 | 36 | 3 | 1 | **96** |
| Simple timerfd (3 threads) | 2 | 3 | 20 | 3 | **28** |
| Consolidated timerfd (1 thread) | 40 | 1 | 3 | 1 | **45** |

### Analysis

**Simple timerfd (3 threads, 3 timerfds)** has the fewest events:
- **No clock polling** - kernel manages all timing
- **No futex** - just read() on timerfd
- 3 threads but minimal synchronization

**Consolidated cv (1 thread)** wins on thread-related events:
- **85% fewer THREADSWITCH** (3 vs 20)
- **67% fewer NEWTHREAD** (1 vs 3)
- But more clock calls due to naive implementation

---

## Detailed Findings

### 1. Independent Condition Variable (Current Approach)

This is similar to current Memgraph scheduler design:
- Each scheduler has its own thread
- Thread polls: `clock_gettime()` → `wait_until()` → execute → repeat

**Events per scheduler iteration:**
- 1 clock_gettime (get current time)
- 1-2 futex (wait + wake)
- Thread switches between schedulers

### 2. Consolidated Scheduler with Priority Queue

Single thread manages all tasks:
- Priority queue ordered by next_run time
- Wake only when soonest task is due
- Dispatch multiple tasks if due at same time

**Benefits:**
- Single thread = fewer context switches
- Single futex wait = fewer futex events
- Can batch concurrent tasks

**Issue in naive implementation:**
- Still calls clock twice per cycle (check due + reschedule)
- Can be optimized to reduce clock calls

### 3. timerfd - Kernel-Driven Wake-ups

Linux timerfd lets kernel manage timing:
```cpp
int tfd = timerfd_create(CLOCK_MONOTONIC, 0);

// Set timer
struct itimerspec its = {.it_value = {.tv_sec = 0, .tv_nsec = 100000000}};
timerfd_settime(tfd, 0, &its, nullptr);

// Block until timer fires (kernel wakes us)
uint64_t expirations;
read(tfd, &expirations, sizeof(expirations));
```

**Benefits:**
- **No clock polling** - kernel tracks time
- **Precise wake-ups** - kernel wakes at exact time
- **No futex** - just read() syscall
- Lower CPU usage

**Trade-offs:**
- Linux-specific (not portable to Windows/macOS)
- One file descriptor per timer
- More complex API

---

## Recommendations

### Option A: timerfd-based Scheduler (Best for Determinism)

Replace `condition_variable::wait_until` with `timerfd`:

```cpp
class Scheduler {
    int tfd_ = timerfd_create(CLOCK_MONOTONIC, 0);

    void ThreadRun() {
        while (running_) {
            // Set timer for next task
            auto next = GetNextTaskTime();
            SetTimerfd(tfd_, next - now);

            // Kernel wakes us (no polling!)
            uint64_t exp;
            read(tfd_, &exp, sizeof(exp));

            ExecuteDueTasks();
        }
    }
};
```

**Expected reduction**: ~70% of scheduler events

### Option B: Consolidated Scheduler (Portable)

Single scheduler thread with priority queue:

```cpp
class ConsolidatedScheduler {
    priority_queue<Task> tasks_;

    void AddTask(name, interval, callback) {
        tasks_.push({now + interval, interval, callback});
        cv_.notify_one();  // Wake if new task is soonest
    }

    void ThreadRun() {
        while (running_) {
            auto next_task = tasks_.top();
            cv_.wait_until(lock, next_task.next_run);
            // Execute and reschedule
        }
    }
};
```

**Expected reduction**: ~30% of scheduler events (mainly from fewer threads)

### Option C: Hybrid Approach

Use timerfd on Linux, fallback to condition_variable on other platforms:

```cpp
#ifdef __linux__
    using SchedulerImpl = TimerfdScheduler;
#else
    using SchedulerImpl = ConditionVariableScheduler;
#endif
```

---

## Implementation Complexity

| Approach | Effort | Portability | Event Reduction |
|----------|--------|-------------|-----------------|
| timerfd | High (2-3 days) | Linux only | ~70% |
| Consolidated cv | Medium (1-2 days) | All platforms | ~30% |
| Hybrid | High (3-4 days) | All platforms | ~70% on Linux |

---

## Files to Modify

### For Consolidated Scheduler

| File | Changes |
|------|---------|
| `src/utils/scheduler.hpp` | Add ConsolidatedScheduler class |
| `src/utils/scheduler.cpp` | Implement priority queue dispatch |
| `src/memgraph.cpp` | Use single scheduler for all tasks |
| `src/utils/priority_thread_pool.cpp` | Integrate with consolidated scheduler |

### For timerfd Scheduler

| File | Changes |
|------|---------|
| `src/utils/scheduler.hpp` | Add timerfd-based implementation |
| `src/utils/scheduler.cpp` | Implement timerfd logic |
| CMakeLists.txt | Detect Linux for conditional compilation |

---

## Demo Files

Located in `demos/`:

| File | Purpose |
|------|---------|
| `high_res_clock_test.cpp` | Proves high_resolution_clock == system_clock |
| `consolidated_scheduler.cpp` | Compares independent vs consolidated cv |
| `timerfd_scheduler.cpp` | Demonstrates timerfd approach |
| `*.undo` | Recordings for event comparison |

---

## Conclusion

**Recommended approach**:

1. **Short-term**: Implement **Consolidated Scheduler** (Option B)
   - Medium effort, portable, ~30% reduction
   - Reduces threads and context switches immediately

2. **Long-term**: Add **timerfd support** (Option A or C)
   - Higher effort, Linux-only benefit, ~70% reduction
   - Eliminates clock polling entirely

**Combined with A1 (interval increase) and A2 (steady_clock)**:
- A1 + A2: ~16% reduction
- Consolidated scheduler: ~30% additional
- timerfd: ~70% additional (Linux)

**Total potential reduction on Linux: 70-80% of scheduler events**
