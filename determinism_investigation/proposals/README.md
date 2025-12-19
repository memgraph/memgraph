# Non-Determinism Reduction Proposals

This folder contains research and experiments for reducing non-determinism in Memgraph recordings.

## Summary of Proposals

### Category A: General Production Improvements

| ID | Proposal | Status | Effort | Reward |
|----|----------|--------|--------|--------|
| A1 | Increase thread pool monitoring interval | **Researched** | Low | Medium |
| A2 | Use steady_clock in scheduler | **Researched** | Low | Medium |
| A3 | Consolidate memory logging scheduler | Pending | Low | Low |
| A4 | RocksDB statistics optimization | Pending | Medium | Medium |

### Category B: High-Determinism Debug Mode

| ID | Proposal | Status | Effort | Reward |
|----|----------|--------|--------|--------|
| B1 | `--determinism-mode` master flag | Pending | Low | High |
| B2 | Disable background schedulers | **Researched** | Medium | Very High |
| B3 | `--scheduler-min-interval` flag | Pending | Medium | High |
| B4 | Single-threaded worker pools | Pending | High | High |
| B5 | Deterministic UUID generation | Pending | Low | Very Low |
| B6 | Disable telemetry in determinism mode | Pending | Very Low | Low |

## Key Findings

### Demo 1: Clock Comparison (proposal_A2_finding.md)

**Finding: `system_clock` generates 2x more live-recorder events than `steady_clock`**

| Clock Type | clock_gettime | WALLCLOCK_TIME | Total |
|------------|---------------|----------------|-------|
| steady_clock | 10,000 | 0 | ~10,000 |
| system_clock | 10,000 | 10,000 | ~20,000 |

**Recommendation**: Use `steady_clock` in scheduler (reduces events by ~50% for time operations)

### Demo 2: Scheduler Timing

**Finding: Event count scales with number of wake-ups, not duration**

Both short polling (100ms x 5) and long polling (1000ms x 5) produce the same number of events (~17 clock_gettime, 5 futex) because they have the same number of iterations.

**Implication**: Increasing scheduler interval by 10x reduces events by ~10x for the same duration.

| Interval | Events per second | Events over 60s |
|----------|-------------------|-----------------|
| 100ms | 10 | 600 |
| 1000ms | 1 | 60 |

### Clock Type in wait_until

**Finding**: `system_clock` in `wait_until` generates additional `WALLCLOCK_TIME` events

- 3 wait_until with steady_clock: 0 WALLCLOCK_TIME events
- 3 wait_until with system_clock: 6 WALLCLOCK_TIME events

### Demo 3: high_resolution_clock

**Finding**: On Linux, `high_resolution_clock` is an alias for `system_clock`, NOT `steady_clock`!

```cpp
std::is_same_v<high_resolution_clock, system_clock> == true   // On Linux
std::is_same_v<high_resolution_clock, steady_clock> == false
```

**Recommendation**: Never use `high_resolution_clock` for scheduling - use `steady_clock` instead.

### Demo 4 & 5: Scheduler Consolidation

**Finding**: Different scheduler architectures have vastly different event profiles

| Approach | clock_gettime | futex | THREADSWITCH | NEWTHREAD |
|----------|---------------|-------|--------------|-----------|
| Independent cv (3 threads) | 40 | 43 | 20 | 3 |
| Consolidated cv (1 thread) | 56 | 36 | **3** | **1** |
| Simple timerfd (3 threads) | **2** | **3** | 20 | 3 |
| Consolidated timerfd (1 thread) | 40 | **1** | **3** | **1** |

**Key insights**:
- **timerfd eliminates clock polling** - kernel manages timing, ~2 clock events vs 40-56
- **Consolidated scheduler reduces threads** - 85% fewer THREADSWITCH events
- **Best approach**: timerfd + consolidated = minimal events on all fronts

## Demos

Located in `demos/` folder:

| Demo | Purpose | Build |
|------|---------|-------|
| `clock_comparison` | Compare steady vs system clock | `make clock_comparison` |
| `clock_separate_test` | Isolate clock behavior | `make clock_separate_test` |
| `scheduler_timing` | Compare polling strategies | `make scheduler_timing` |
| `high_res_clock_test` | Analyze high_resolution_clock | `make high_res_clock_test` |
| `consolidated_scheduler` | Compare cv-based schedulers | `make consolidated_scheduler` |
| `timerfd_scheduler` | Kernel-driven wake-ups | `make timerfd_scheduler` |

### Usage

```bash
cd demos
make all

# Run demos
./clock_comparison
./scheduler_timing short
./scheduler_timing long

# Record with live-recorder
make record-clock_comparison
make record-scheduler_timing

# Analyze recording
make analyze-clock_comparison
```

## Research Documents

| File | Content | Status |
|------|---------|--------|
| `proposal_A1_finding.md` | Thread pool monitoring interval | Complete |
| `proposal_A2_finding.md` | steady_clock vs system_clock | Complete |
| `proposal_B2_finding.md` | Background schedulers inventory | Complete |
| `proposal_scheduler_consolidation_finding.md` | Consolidated scheduler & timerfd | Complete |

## Experiment Recordings

Located in `demos/` after running tests:

- `steady_only.undo` - steady_clock test recording
- `system_only.undo` - system_clock test recording
- `sched_short.undo` - 100ms polling recording
- `sched_long.undo` - 1000ms polling recording
- `sched_clock.undo` - Clock type in wait_until test
- `high_res_test.undo` - high_resolution_clock test
- `sched_independent.undo` - Independent cv schedulers (3 threads)
- `sched_consolidated.undo` - Consolidated cv scheduler (1 thread)
- `timerfd_simple.undo` - Simple timerfd (3 threads)
- `timerfd_consolidated.undo` - Consolidated timerfd (1 thread)

## Next Steps

### Ready for Implementation (Quick Wins)

1. **Implement A2** - Change scheduler from `system_clock` to `steady_clock`
   - Impact: ~14% event reduction (WALLCLOCK_TIME elimination)
   - Risk: Low
   - Files: `src/utils/scheduler.cpp`

2. **Implement A1** - Increase thread pool monitoring from 100ms to 1000ms
   - Impact: ~10% reduction in monitoring events
   - Risk: Low
   - Files: `src/utils/priority_thread_pool.cpp`

3. **Implement B1+B2** - Add `--determinism-mode` flag with scheduler disabling
   - Impact: ~20% total event reduction
   - Risk: Low (opt-in only)
   - Files: `src/flags/general.cpp`, `src/memgraph.cpp`, `src/utils/priority_thread_pool.cpp`

### Future Improvements (Higher Effort)

4. **Consolidated Scheduler** - Single thread with priority queue
   - Impact: ~30% reduction (fewer threads, fewer context switches)
   - Effort: Medium (1-2 days)
   - Portable to all platforms

5. **timerfd-based Scheduler** - Kernel-driven wake-ups (Linux only)
   - Impact: ~70% reduction (eliminates clock polling)
   - Effort: High (2-3 days)
   - Best for determinism but Linux-specific

### Combined Expected Reduction

| Change | Event Reduction |
|--------|-----------------|
| A2 alone | ~14% |
| A1 alone | ~2% |
| B2 alone | ~20% |
| Quick wins combined | **~35%** |
| + Consolidated scheduler | **~50%** |
| + timerfd (Linux) | **~70%** |
