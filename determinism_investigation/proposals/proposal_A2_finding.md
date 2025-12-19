# Proposal A2 Finding: steady_clock vs system_clock

## Summary

**Recommendation: Use `steady_clock` instead of `system_clock` in scheduler**

Using `steady_clock` instead of `system_clock` reduces live-recorder events by 50% for time operations.

## Experiment

### Setup

Created standalone test program `demos/clock_separate_test.cpp` that calls each clock type 10,000 times.

### Results

| Clock Type | clock_gettime | WALLCLOCK_TIME | Total Events |
|------------|---------------|----------------|--------------|
| steady_clock | 10,000 | 0 | ~10,000 |
| system_clock | 10,000 | 10,000 | ~20,000 |

**Finding: system_clock generates 2x the events!**

### Why?

- `steady_clock` uses `CLOCK_MONOTONIC` - only records the syscall event
- `system_clock` uses `CLOCK_REALTIME` - records both the syscall AND wall-clock time
- live-recorder tracks WALLCLOCK_TIME separately for determinism replay

### Recording Files

- `demos/steady_only.undo` - steady_clock test recording
- `demos/system_only.undo` - system_clock test recording

---

## Clock Behavior Analysis

### steady_clock Properties

```cpp
std::chrono::steady_clock::is_steady = true  // Guaranteed monotonic
period = 1/1000000000 seconds                // Nanosecond precision
```

**Pros:**
- Monotonic (never goes backwards)
- Not affected by NTP adjustments
- Ideal for measuring intervals, timeouts, benchmarks
- **50% fewer live-recorder events**

**Cons:**
- Cannot convert to calendar time (`to_time_t` not available)
- Epoch is unspecified (usually system boot time)

### system_clock Properties

```cpp
std::chrono::system_clock::is_steady = false  // May not be monotonic
period = 1/1000000000 seconds                 // Nanosecond precision
```

**Pros:**
- Convertible to calendar time via `to_time_t()`
- Represents wall-clock time

**Cons:**
- Can jump forward/backward during NTP adjustments
- **2x more live-recorder events**
- May cause unexpected behavior in timeout calculations

---

## Current Memgraph Usage

### `src/utils/scheduler.cpp`

```cpp
// Line 64: Current implementation uses system_clock
const auto now = std::chrono::system_clock::now();
```

```cpp
// Line 164: Also in ThreadRun loop
const auto now = std::chrono::system_clock::now();
```

### Analysis

The scheduler uses `system_clock::now()` for:
1. Calculating when to run the next task
2. Waiting on condition variables

**Neither use case requires calendar time conversion.**

The scheduler only needs to measure elapsed time for intervals, making `steady_clock` the correct choice.

---

## Proposed Change

### Before

```cpp
// src/utils/scheduler.cpp
const auto now = std::chrono::system_clock::now();
condition_variable_.wait_until(lock, next_run);
```

### After

```cpp
// src/utils/scheduler.cpp
const auto now = std::chrono::steady_clock::now();
condition_variable_.wait_until(lock, next_run);
```

### Files to Modify

| File | Change |
|------|--------|
| `src/utils/scheduler.cpp` | Replace `system_clock` with `steady_clock` |
| `src/utils/scheduler.hpp` | Update type aliases if needed |

---

## Expected Impact

### Event Reduction

Based on Stage 2 findings:
- clock_gettime: 25,295 events (18.76% of total)
- WALLCLOCK_TIME: 18,990 events (14.08% of total)

If most WALLCLOCK_TIME events are from scheduler's use of system_clock:
- **Potential reduction: ~18,990 events (~14% of total)**

### Performance

- No performance impact (both clocks use vDSO)
- Slightly more correct behavior (monotonic guarantee)

### Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Break calendar time features | Low | Low | Scheduler doesn't need calendar time |
| Break condition_variable | None | N/A | Both clock types work with wait_until |
| Existing tests fail | Low | Low | Run existing scheduler tests |

---

## Verification Plan

1. Build Memgraph with change applied
2. Record with live-recorder (same test queries as Stage 1)
3. Compare `info event-stats` to baseline
4. Verify WALLCLOCK_TIME events reduced
5. Run scheduler unit tests: `./tests/unit/utils_scheduler`

---

## Conclusion

**Recommendation: PROCEED with implementation**

- Low effort change (2-4 hours including testing)
- Low risk (scheduler doesn't need calendar time)
- Medium reward (~14% event reduction)
- Additional correctness benefit (monotonic guarantee)

This is a Category A change (general production improvement) - beneficial for all deployments, not just determinism mode.
