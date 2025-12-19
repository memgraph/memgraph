# Proposal B2 Finding: Background Schedulers Analysis

## Summary

Memgraph has **13+ scheduler instances** running background tasks. Disabling non-essential schedulers in determinism mode would significantly reduce events.

## Complete Scheduler Inventory

### Core Schedulers (memgraph.cpp)

| Scheduler | Interval | Purpose | Configurable | Essential |
|-----------|----------|---------|--------------|-----------|
| `python_gc_scheduler` | `FLAGS_storage_python_gc_cycle_sec` | Python garbage collection | Yes | No |
| `mem_log_scheduler` | 3 seconds | Memory warning logging | Partially* | No |
| `jemalloc_purge_scheduler` | `FLAGS_storage_gc_cycle_sec` | jemalloc memory purge | Yes | No |

*Only runs if `FLAGS_memory_warning_threshold > 0`

### Thread Pool Monitoring (priority_thread_pool.cpp)

| Scheduler | Interval | Purpose | Configurable | Essential |
|-----------|----------|---------|--------------|-----------|
| `monitoring_` | 100ms | Stuck task detection | **No** | Low** |

**Can be disabled for debugging - stuck task detection not critical for short sessions

### Storage Schedulers (storage/v2/inmemory/storage.cpp)

| Scheduler | Interval | Purpose | Configurable | Essential |
|-----------|----------|---------|--------------|-----------|
| `gc_runner_` | `config_.gc.interval` | Transaction garbage collection | Yes | Medium |
| `snapshot_runner_` | `config_.durability.snapshot_interval` | Periodic snapshots | Yes | Medium |
| `ttl_` | `ttl_info.period` | TTL expiration | Yes | No |

### Replication/Coordination Schedulers

| Scheduler | Interval | Purpose | Configurable | Essential |
|-----------|----------|---------|--------------|-----------|
| `instance_checker_` | configurable | Instance health checks | Yes | High (HA only) |
| `replica_checker_` | `replica_check_frequency_` | Replica health checks | Yes | High (HA only) |

### Telemetry/License Schedulers

| Scheduler | Interval | Purpose | Configurable | Essential |
|-----------|----------|---------|--------------|-----------|
| Telemetry `scheduler_` | exp. backoff | Telemetry reporting | Yes | No |
| LicenseChecker `scheduler_` | 5 minutes | License validation | Yes | No* |
| LicenseSender `scheduler_` | exp. backoff | License reporting | Yes | No |

*Not essential for debugging sessions

### Audit Scheduler

| Scheduler | Interval | Purpose | Configurable | Essential |
|-----------|----------|---------|--------------|-----------|
| AuditLog `scheduler_` | configurable ms | Buffer flushing | Yes | Medium |

---

## Event Impact Analysis

### From Stage 2 Event Stats

- **clock_gettime**: 25,295 (18.76% of total)
- **futex**: 20,580 (15.26% of total)
- **Total scheduler-related**: ~35-40% of all events

### Event Breakdown by Scheduler (Estimated)

| Scheduler | Events/cycle | Cycles/min | Events/min |
|-----------|-------------|------------|------------|
| Thread pool monitoring (100ms) | ~3 | 600 | 1,800 |
| Memory check (3s) | ~3 | 20 | 60 |
| Python GC (varies) | ~3 | varies | varies |
| jemalloc purge (varies) | ~3 | varies | varies |
| Storage GC (varies) | ~3 | varies | varies |
| Snapshots (varies) | ~5 | varies | varies |
| License check (5min) | ~3 | 0.2 | 0.6 |
| Telemetry (backoff) | ~5 | ~0.1 | 0.5 |

**Thread pool monitoring is the dominant source** due to 100ms interval.

---

## Schedulers Safe to Disable in Determinism Mode

### Category 1: Completely Safe to Disable

| Scheduler | Reason |
|-----------|--------|
| `python_gc_scheduler` | Python GC can be triggered manually if needed |
| `mem_log_scheduler` | Warning logs not needed for debugging |
| `jemalloc_purge_scheduler` | Memory can be purged manually |
| Telemetry `scheduler_` | Already has `--telemetry-enabled=false` |
| License schedulers | Not needed for debugging |
| `ttl_` | TTL not critical for debugging |

### Category 2: Safe to Disable for Short Sessions

| Scheduler | Reason |
|-----------|--------|
| Thread pool `monitoring_` | Stuck task detection not critical for <1min debugging |
| `gc_runner_` | GC can be manual for short sessions |
| `snapshot_runner_` | Snapshots not needed for debugging |

### Category 3: Should NOT Disable

| Scheduler | Reason |
|-----------|--------|
| `instance_checker_` | Critical for HA functionality |
| `replica_checker_` | Critical for HA functionality |
| AuditLog `scheduler_` | May lose audit data |

---

## Implementation Proposal

### B2.1: Add `--determinism-mode` Flag

```cpp
// src/flags/general.cpp
DEFINE_bool(determinism_mode, false,
    "Enable high-determinism mode for debugging (reduces background activity)");
```

### B2.2: Conditionally Disable Schedulers

```cpp
// src/memgraph.cpp

// Python GC - only start if not in determinism mode
if (!FLAGS_determinism_mode) {
  memgraph::utils::Scheduler python_gc_scheduler;
  python_gc_scheduler.SetInterval(std::chrono::seconds(FLAGS_storage_python_gc_cycle_sec));
  python_gc_scheduler.Run("Python GC", [] { memgraph::query::procedure::PyCollectGarbage(); });
}

// Memory check - only start if not in determinism mode
if (!FLAGS_determinism_mode && FLAGS_memory_warning_threshold > 0) {
  mem_log_scheduler.SetInterval(std::chrono::seconds(3));
  mem_log_scheduler.Run("Memory check", [] { ... });
}

// jemalloc purge - only start if not in determinism mode
if (!FLAGS_determinism_mode) {
  jemalloc_purge_scheduler.SetInterval(std::chrono::seconds(FLAGS_storage_gc_cycle_sec));
  jemalloc_purge_scheduler.Run("Jemalloc purge", [] { ... });
}
```

### B2.3: Modify Thread Pool Monitoring

```cpp
// src/utils/priority_thread_pool.cpp

// In determinism mode, don't start monitoring
#ifndef NDEBUG  // Or check a flag
if (!FLAGS_determinism_mode) {
  monitoring_.SetInterval(std::chrono::milliseconds(100));
  monitoring_.Run("sched_mon", ...);
}
#endif
```

### B2.4: Modify Storage Schedulers

```cpp
// src/storage/v2/inmemory/storage.cpp

// GC - increase interval in determinism mode
if (FLAGS_determinism_mode) {
  gc_runner_.SetInterval(std::chrono::seconds(3600));  // 1 hour
} else {
  gc_runner_.SetInterval(config_.gc.interval);
}

// Snapshots - disable in determinism mode
if (!FLAGS_determinism_mode) {
  snapshot_runner_.SetInterval(config_.durability.snapshot_interval);
  snapshot_runner_.Run(...);
}
```

---

## Expected Event Reduction

### Baseline (from Stage 2)

- Total events: ~135,000
- Scheduler-related: ~47,000 (35%)

### With `--determinism-mode`

| Scheduler | Events Before | Events After | Reduction |
|-----------|---------------|--------------|-----------|
| Thread pool monitoring | 18,000 | 0 | 18,000 |
| Memory check | 600 | 0 | 600 |
| Python GC | 300 | 0 | 300 |
| jemalloc purge | 300 | 0 | 300 |
| Storage GC | 300 | 30 | 270 |
| Snapshots | 100 | 0 | 100 |
| Telemetry | 50 | 0 | 50 |
| License | 10 | 0 | 10 |
| **Total** | ~20,000 | ~30 | **~19,970** |

**Expected reduction: ~15-20% of total events**

Combined with A2 (steady_clock) reduction of ~14%, total reduction could be **~30-35%**.

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Memory leaks (no Python GC) | Low | Low | Short sessions; manual GC if needed |
| Memory growth (no jemalloc purge) | Low | Low | Short sessions |
| Stuck tasks not detected | Low | Medium | Acceptable for debugging |
| No snapshots | None | None | Not needed for debugging |
| License issues | None | None | License still valid |

---

## Verification Plan

1. Add `--determinism-mode` flag
2. Implement conditional scheduler startup
3. Test basic functionality with flag enabled
4. Record with live-recorder (same test queries)
5. Compare `info event-stats` to baseline
6. Verify significant event reduction

---

## Conclusion

**Recommendation: PROCEED with implementation**

- Medium effort (4-8 hours)
- Low risk (determinism mode is opt-in)
- Very high reward (~20% event reduction)
- Category B change (debug mode only)

This is the **highest-impact change** for determinism improvement.

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/flags/general.cpp` | Add `--determinism-mode` flag |
| `src/memgraph.cpp` | Conditional scheduler startup |
| `src/utils/priority_thread_pool.cpp` | Conditional monitoring |
| `src/storage/v2/inmemory/storage.cpp` | Conditional GC/snapshot |
| `src/telemetry/telemetry.cpp` | Respect determinism mode |
