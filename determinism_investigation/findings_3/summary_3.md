# Stage 3 Summary: Time Operations Investigation

## clock_gettime Sources (25,295 events)

### Primary Source: Scheduler
**File**: `src/utils/scheduler.cpp`

All sampled `clock_gettime` calls originate from `memgraph::utils::Scheduler::ThreadRun`:

1. **Line 164**: `const auto now = std::chrono::system_clock::now();`
   - Called at the start of every scheduler loop iteration
   - Used to determine when the next scheduled task should run

2. **Line 175**: `condition_variable_.wait_until(lk, token, next, ...)`
   - The timed wait internally calls `clock_gettime` multiple times
   - This is in the std::condition_variable_any implementation

### Identified Scheduler Services:
- "sched_mon" - Scheduler monitor
- "Memory check" - Memory monitoring service

### Call Flow:
```
Scheduler::ThreadRun
└── std::chrono::system_clock::now()
    └── clock_gettime (syscall)
```

---

## gettimeofday Sources (7,605 events)

### Primary Source: RocksDB
All sampled `gettimeofday` calls originate from RocksDB's internal timer:

1. **rocksdb::PosixClock::NowMicros()** - RocksDB's platform clock
2. **rocksdb::Timer::Run()** - RocksDB's internal timer thread
3. **rocksdb::EnvLogger::Flush()** - Log flushing operations

### Call Flow:
```
rocksdb::Timer::Run
├── rocksdb::PosixClock::NowMicros()
│   └── gettimeofday (syscall)
└── rocksdb::EnvLogger::Flush()
    └── rocksdb::PosixClock::NowMicros()
        └── gettimeofday (syscall)
```

### Note:
RocksDB is initialized even in in-memory storage mode for certain features. This is an external dependency.

---

## Remediation Opportunities

### High Impact - Memgraph Scheduler
| Issue | Impact | Solution |
|-------|--------|----------|
| Frequent time checks | 25,295 events | Use monotonic clock instead of wall clock |
| Multiple schedulers | Multiplied calls | Consolidate scheduler threads |
| Short intervals | More iterations | Increase scheduler check intervals |

**Specific Recommendations for `scheduler.cpp`:**

1. **Use steady_clock instead of system_clock**:
   - `std::chrono::steady_clock::now()` uses CLOCK_MONOTONIC
   - More efficient for timing (doesn't need wall clock)
   - However, may still trigger similar syscalls

2. **Reduce scheduler frequency**:
   - Review if "sched_mon" and "Memory check" need such frequent checks
   - Increase intervals from default values

3. **Combine schedulers**:
   - Consider a single scheduler thread with multiple tasks
   - Reduces thread overhead and synchronization

### Low Impact - RocksDB (External)
RocksDB's time operations are in its internal code:
- Cannot be directly modified
- Could potentially be reduced by:
  - Disabling RocksDB statistics
  - Reducing log flush frequency
  - Using in-memory mode configurations that minimize RocksDB activity

---

## Key Files to Review

1. **`src/utils/scheduler.cpp`** - Main scheduler implementation (lines 164, 175)
2. **`src/utils/scheduler.hpp`** - Scheduler header and interval configuration
3. Callers of `Scheduler::Run()` - To understand which services use schedulers

## Estimated Impact of Fixes

| Fix | Events Reduced | Complexity |
|-----|---------------|------------|
| Use steady_clock | Minimal | Low |
| Increase scheduler intervals 10x | ~23,000 | Low |
| Consolidate scheduler threads | ~12,000 | Medium |
| Disable RocksDB timers | ~7,600 | Medium |

**Total potential reduction: ~42,600 events (~31% of all events)**
