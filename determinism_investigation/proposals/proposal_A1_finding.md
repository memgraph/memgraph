# Proposal A1 Finding: Thread Pool Monitoring Interval

## Summary

**Recommendation: Increase monitoring interval from 100ms to 1000ms**

The `PriorityThreadPool` monitoring runs every 100ms to detect stuck tasks. This is likely too aggressive for most workloads and generates unnecessary events.

## Current Implementation

**File**: `src/utils/priority_thread_pool.cpp`

### Line 105: Monitoring Interval

```cpp
monitoring_.SetInterval(std::chrono::milliseconds(100));
```

### Lines 106-142: Monitoring Callback

The monitoring callback runs named "sched_mon" and does:
1. Iterates through all worker threads
2. Checks if a worker is stuck on the same task
3. If stuck, moves pending tasks to other workers

```cpp
monitoring_.Run("sched_mon", [this, workers_num = workers_.size(), ...] {
    for (auto &worker : workers_) {
        // Check if worker stuck on same task
        if (worker_last_task == worker->last_task_ && worker->working_ && worker->has_pending_work_) {
            // Move task to a different queue
            ...
        }
    }
});
```

## Analysis

### Purpose of 100ms Interval

The monitoring detects when a worker is "stuck" on a task. A task is considered stuck if:
- Worker is still working on the same task ID as last check
- Worker has pending work in queue

The 100ms interval means:
- A stuck task can be detected and moved within 100ms
- 10 monitoring cycles per second
- 10 sets of clock_gettime + futex events per second

### Event Impact

From Stage 2 analysis, the scheduler ("sched_mon") is a major contributor to events:
- clock_gettime: 25,295 (18.76% of total)
- futex: 20,580 (15.26% of total)

Part of these events come from the thread pool monitoring.

### Trade-off Analysis

| Interval | Stuck Detection Latency | Events/second |
|----------|------------------------|---------------|
| 100ms | 100-200ms | 10 |
| 500ms | 500-1000ms | 2 |
| 1000ms | 1000-2000ms | 1 |

**Question**: Is sub-second stuck detection important?

For most workloads:
- Queries typically complete in <100ms or >1s (bimodal distribution)
- A task that takes >1s is already slow; 100ms detection adds nothing
- Long-running tasks (analytics, import) don't benefit from 100ms detection

**Answer**: For most workloads, 1000ms detection is sufficient.

## Proposed Change

### Option 1: Increase to 1000ms (Recommended)

```cpp
// src/utils/priority_thread_pool.cpp line 105
monitoring_.SetInterval(std::chrono::milliseconds(1000));  // Was 100ms
```

**Pros:**
- 10x reduction in monitoring events
- Minimal impact on stuck task detection
- Simple change

**Cons:**
- Stuck tasks detected 0.9s slower

### Option 2: Make Configurable

```cpp
// src/flags/general.cpp
DEFINE_uint64(thread_pool_monitoring_interval_ms, 1000,
              "Thread pool monitoring interval in milliseconds");

// src/utils/priority_thread_pool.cpp
monitoring_.SetInterval(std::chrono::milliseconds(FLAGS_thread_pool_monitoring_interval_ms));
```

**Pros:**
- Users can tune for their workload
- Can disable in determinism mode

**Cons:**
- More complex
- Requires flag definition

## Expected Impact

### Event Reduction

Current (100ms, assuming 60s test):
- Monitoring cycles: 600
- Events per cycle: ~3 (clock_gettime + futex)
- Total: ~1,800 events from monitoring

Proposed (1000ms, 60s test):
- Monitoring cycles: 60
- Events per cycle: ~3
- Total: ~180 events from monitoring

**Reduction: ~1,620 events (~90% reduction in monitoring events)**

### As Percentage of Total

From Stage 2, monitoring is part of "sched_mon" scheduler events.
- Conservative estimate: 5-10% of scheduler events
- Total reduction: ~2,500-5,000 events

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Stuck tasks detected slower | Medium | Low | Most tasks are <100ms or >1s |
| Performance regression | Low | Low | Only affects edge case (stuck tasks) |
| Behavior change | Low | Low | Only timing, not correctness |

## Verification Plan

1. Run existing unit tests: `./tests/unit/priority_thread_pool`
2. Run integration tests with modified interval
3. Benchmark query latency with both intervals
4. Compare event counts with live-recorder

## Conclusion

**Recommendation: PROCEED with 1000ms interval**

- Low effort (single line change)
- Low risk (stuck detection is edge case)
- Medium reward (~10% reduction in scheduler events)
- Category A change (beneficial for all deployments)

## Additional Notes

### Related Code

The monitoring thread is a `Scheduler` instance (`monitoring_`), which uses the same scheduler infrastructure analyzed in Stage 3-5.

### Code Flow

```
PriorityThreadPool constructor
└── monitoring_.SetInterval(100ms)
└── monitoring_.Run("sched_mon", callback)
    └── Scheduler::ThreadRun (scheduler.cpp)
        └── system_clock::now() (line 164)
        └── condition_variable_.wait_until (line 175)
        └── Repeat every 100ms
```

The monitoring thread contributes to:
- clock_gettime from system_clock::now()
- futex from condition_variable::wait_until()
- WALLCLOCK_TIME from system_clock usage

If Proposal A2 (steady_clock) is also implemented, monitoring events would be further reduced by removing WALLCLOCK_TIME events.
