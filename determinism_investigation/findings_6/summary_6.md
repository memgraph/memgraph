# Non-Determinism Investigation: Final Report

## Executive Summary

This investigation analyzed non-determinism sources in Memgraph using Undo's live-recorder and udb tools. The goal was to identify events that could be eliminated, reduced, or made less frequent to improve recording predictability for time-travel debugging.

### Key Finding
**The Scheduler (`src/utils/scheduler.cpp`) is the central source of non-determinism**, driving ~35-40% of all recorded events through its polling-based design.

---

## Event Distribution

### Total Events: ~134,857

| Category | Events | % | Primary Source |
|----------|--------|---|----------------|
| Time Operations | 52,161 | 38.7% | Scheduler |
| Thread Operations | 50,813 | 37.7% | Scheduler + Thread Pools |
| File System | 20,720 | 15.4% | File checks, RocksDB |
| RNG | 17 | 0.01% | Startup only |
| Other | ~11,146 | 8.2% | Various |

### Top 10 Event Types

| Rank | Event | Count | % | Source |
|------|-------|-------|---|--------|
| 1 | THREADSWITCH | 27,844 | 20.65% | OS scheduling |
| 2 | clock_gettime | 25,295 | 18.76% | **Scheduler** |
| 3 | futex | 20,580 | 15.26% | **Scheduler** |
| 4 | WALLCLOCK_TIME | 18,990 | 14.08% | Time syscalls |
| 5 | newfstatat | 15,616 | 11.58% | File operations |
| 6 | gettimeofday | 7,605 | 5.64% | RocksDB |
| 7 | access | 3,555 | 2.64% | File checks |
| 8 | gettid | 2,134 | 1.58% | Thread IDs |
| 9 | read | 1,549 | 1.15% | File I/O |
| 10 | getrandom | 17 | 0.01% | UUID generation |

---

## Root Cause Analysis

### 1. Scheduler Design (Primary Issue)

**File**: `src/utils/scheduler.cpp`

The scheduler uses a polling loop that continuously:
1. Gets current time (line 164)
2. Waits on condition variable with timeout (line 175)
3. Repeats

**Scheduler services identified:**
- "sched_mon" - Scheduler monitoring
- "Memory check" - Memory usage monitoring

**Impact per scheduler iteration:**
- 1-2 clock_gettime calls
- 1-2 futex calls
- Thread context switches

### 2. RocksDB Timer (Secondary)

RocksDB maintains an internal timer thread calling `gettimeofday` for:
- Log flushing
- Statistics collection
- Background operations

### 3. Thread Pool Design

~128 threads created during startup for:
- Worker pools
- Scheduler threads
- RocksDB threads
- Python interpreter

---

## Remediation Recommendations

### Priority 1: Scheduler Optimization (Est. 40% reduction)

| Action | Implementation | Impact |
|--------|----------------|--------|
| Increase intervals | Set scheduler periods to 1+ seconds | -20,000 events |
| Consolidate schedulers | Single scheduler with task queue | -15,000 events |
| Add debug flag | `--disable-background-schedulers` | -40,000 events |

**Code changes for `src/utils/scheduler.cpp`:**

1. Add configurable minimum interval:
```cpp
// Minimum 1 second for background tasks in debug mode
static constexpr auto kMinDebugInterval = std::chrono::seconds(1);
```

2. Add flag to disable schedulers:
```cpp
DEFINE_bool(disable_background_schedulers, false,
            "Disable background scheduler threads for deterministic debugging");
```

### Priority 2: RocksDB Timer (Est. 5% reduction)

| Action | Implementation | Impact |
|--------|----------------|--------|
| Disable statistics | RocksDB options | -3,000 events |
| Reduce log flush | Increase flush interval | -4,000 events |

**RocksDB configuration:**
```cpp
options.stats_dump_period_sec = 0;  // Disable stats dumps
options.info_log_level = rocksdb::InfoLogLevel::ERROR_LEVEL;
```

### Priority 3: Thread Pool Reduction (Est. 10% reduction)

| Action | Implementation | Impact |
|--------|----------------|--------|
| Reduce thread count | Lower pool sizes | -5,000 switches |
| Use single-threaded mode | For debugging/testing | -20,000 switches |

**Add flag:**
```cpp
DEFINE_bool(single_threaded_mode, false,
            "Run with minimal threads for deterministic debugging");
```

### Priority 4: UUID Seeding (Low priority)

| Action | Implementation | Impact |
|--------|----------------|--------|
| Fixed UUIDs | Deterministic UUID generation for tests | -17 events |

**For testing only:**
```cpp
DEFINE_string(fixed_run_id, "", "Use fixed run ID for deterministic testing");
```

---

## Implementation Roadmap

### Phase 1: Quick Wins (1-2 days)
1. Add `--disable-background-schedulers` flag
2. Add `--scheduler-min-interval` flag
3. Document debug flags for deterministic testing

### Phase 2: Scheduler Refactoring (3-5 days)
1. Consolidate multiple scheduler instances
2. Implement event-driven wake-up for schedulers
3. Add per-service interval configuration

### Phase 3: RocksDB Optimization (2-3 days)
1. Configure RocksDB for minimal timer activity
2. Add debug configuration preset
3. Document disk storage timing behavior

### Phase 4: Thread Model Review (5+ days)
1. Audit thread pool configurations
2. Implement single-threaded mode
3. Profile thread switching patterns

---

## Testing Recommendations

### For Deterministic Debugging Sessions

Recommended Memgraph flags:
```bash
./build/memgraph \
    --disable-background-schedulers \
    --scheduler-min-interval=60 \
    --single-threaded-mode \
    --storage-gc-aggressive=false \
    --telemetry-enabled=false
```

### Expected Event Reduction

| Configuration | Events | Reduction |
|--------------|--------|-----------|
| Current | ~135,000 | - |
| Priority 1 only | ~80,000 | 40% |
| Priority 1+2 | ~70,000 | 48% |
| All optimizations | ~50,000 | 63% |

---

## Files to Modify

### High Priority
| File | Changes |
|------|---------|
| `src/utils/scheduler.cpp` | Add interval controls, debug mode |
| `src/utils/scheduler.hpp` | Add configuration options |
| `src/memgraph.cpp` | Add command-line flags |

### Medium Priority
| File | Changes |
|------|---------|
| `src/storage/v2/disk/rocksdb_storage.cpp` | RocksDB timer config |
| Thread pool configurations | Reduce default sizes |

### Low Priority
| File | Changes |
|------|---------|
| `src/utils/uuid.cpp` | Optional deterministic mode |
| `src/glue/run_id.cpp` | Fixed run ID option |

---

## Conclusion

The investigation revealed that **non-determinism in Memgraph is primarily driven by the scheduler implementation**, not by random number generation as initially suspected. The polling-based scheduler design causes frequent time checks and thread synchronization events.

**Key insight:** RNG (getrandom) accounts for only 0.01% of events, while scheduler-related operations account for ~40%.

**Recommended first action:** Add a `--disable-background-schedulers` flag for debugging sessions to immediately reduce events by ~40%.

---

## Appendix: Event Statistics Raw Data

See `findings_2/event_stats.txt` for complete event statistics.

## Appendix: Full Backtraces

- `findings_3/clock_gettime_traces.txt` - Time operation call stacks
- `findings_3/gettimeofday_traces.txt` - gettimeofday call stacks
- `findings_4/getrandom_traces.txt` - RNG call stacks
- `findings_5/futex_traces.txt` - Thread synchronization call stacks
- `findings_5/newthread_events.txt` - Thread creation events
