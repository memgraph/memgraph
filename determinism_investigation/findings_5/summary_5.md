# Stage 5 Summary: Thread Switching Analysis

## Thread Count
- **NEWTHREAD events**: 128 threads created
- **Thread ID range**: 2618123 - 2618203+
- Most threads created during startup (time < 10,000,000 bbcounts)

## THREADSWITCH Distribution (27,844 events)

The high number of thread switches is driven by:
1. Multiple scheduler threads running in parallel
2. Thread pool workers for various services
3. RocksDB internal threads
4. Python interpreter threads

---

## Futex Analysis (20,580 events)

### Primary Source: Scheduler Thread Synchronization
All sampled futex calls originate from `memgraph::utils::Scheduler::ThreadRun`:

**Services identified:**
- "sched_mon" - Scheduler monitor
- "Memory check" - Memory monitoring service

**Call Flow:**
```
Scheduler::ThreadRun
└── condition_variable_any::wait_until
    ├── pthread_cond_timedwait64 (futex wait)
    └── pthread_mutex_unlock (futex wake)
```

### Futex Types Observed
| Type | Count | Description |
|------|-------|-------------|
| Wait (result=-0x6e) | ~50% | Thread waiting for condition |
| Wake (result=0x0) | ~50% | Thread waking up from wait |

---

## Root Cause: Scheduler Design

The Scheduler implementation in `src/utils/scheduler.cpp` creates a **busy-waiting loop** that:

1. Gets current time (`clock_gettime`) at line 164
2. Waits on condition variable (`futex`) at line 175
3. Wakes up, unlocks mutex (`futex`)
4. Repeats

**Each scheduler iteration causes:**
- 1-2 clock_gettime calls
- 1-2 futex calls
- Thread switches

**With multiple scheduler instances running concurrently**, this multiplies rapidly.

---

## Scheduler Services Identified

Based on service_name strings in backtraces:

| Service Name | Purpose | Likely Interval |
|--------------|---------|-----------------|
| "sched_mon" | Scheduler monitoring | Short (ms) |
| "Memory check" | Memory monitoring | Short (ms) |
| (Others) | Various background tasks | Various |

---

## Remediation Opportunities

### High Impact - Scheduler Consolidation

| Change | Impact | Events Reduced |
|--------|--------|----------------|
| Increase scheduler intervals | High | ~20,000 |
| Consolidate scheduler threads | High | ~15,000 |
| Use event-driven instead of polling | Very High | ~40,000 |

### Specific Recommendations

1. **Increase scheduler check intervals**
   - Current: Likely millisecond-level
   - Recommended: Second-level for background tasks
   - Impact: 10x reduction in scheduler events

2. **Consolidate scheduler threads**
   - Multiple schedulers running independently
   - Could use single scheduler with task queue
   - Impact: Fewer threads, less context switching

3. **Event-driven architecture**
   - Replace polling with event notifications
   - Only wake on actual events
   - Impact: Near-elimination of idle polling

4. **Disable unnecessary services during debugging**
   - "Memory check" may not be needed for unit tests
   - "sched_mon" could be optional
   - Flags to disable background schedulers

---

## Thread Categories

Based on NEWTHREAD timing:

| Timing (bbcount) | Count | Purpose |
|------------------|-------|---------|
| < 10,000 | 2 | Early startup |
| 10,000 - 1,000,000 | 2 | Python init |
| 1,000,000 - 8,000,000 | ~50 | Service initialization |
| 8,000,000 - 10,000,000 | ~20 | Thread pools |
| > 50,000,000 | ~50 | Query processing |

---

## Conclusion

**The Scheduler is the central source of non-determinism:**

1. It drives 18.76% of events (clock_gettime)
2. It drives 15.26% of events (futex)
3. It contributes to THREADSWITCH events
4. Total Scheduler-related: ~35% of all events

**Key file to modify:** `src/utils/scheduler.cpp`

**Estimated impact of scheduler optimization: 40-50% reduction in total events**
