# Timer Backend Comparison Finding

## Summary

Compared three timer backends for non-determinism event counts under Undo live-recorder:

1. **condition_variable::wait_until** (current Memgraph approach)
2. **timerfd** (Linux kernel-driven timer)
3. **POSIX timer** (timer_create + sigwaitinfo)

## Test Setup

- 5 iterations at 100ms interval
- Single-threaded
- Recorded with live-record, analyzed with `info events`

## Results

| Backend | clock_gettime | futex | Other wait | Total (excl. XSAVE) |
|---------|---------------|-------|------------|---------------------|
| CV (steady_clock) | 8 | 5 | - | 15 |
| timerfd | 2 | 0 | 5 read | 12 |
| POSIX timer | 2 | 0 | 5 sigtimedwait | 12 |

### Event Breakdown

**condition_variable (cv)**:
```
18 XSAVE
 8 clock_gettime
 5 futex
 1 write
 1 fstat
```

**timerfd**:
```
17 XSAVE
 5 read
 2 clock_gettime
 1 write
 1 timerfd_settime
 1 timerfd_create
 1 fstat
 1 close
```

**POSIX timer**:
```
17 XSAVE
 5 rt_sigtimedwait
 2 clock_gettime
 1 write
 1 timer_settime
 1 timer_delete
 1 timer_create
 1 rt_sigprocmask
 1 fstat
```

## Analysis

### clock_gettime Events

| Backend | clock_gettime | Reduction |
|---------|---------------|-----------|
| CV | 8 | baseline |
| timerfd | 2 | **-75%** |
| POSIX | 2 | **-75%** |

The CV backend polls with `clock_gettime` on each iteration:
- Get current time
- Compare to deadline
- Call futex to wait

timerfd and POSIX timer only call `clock_gettime` twice total (start/end of test).

### futex Events

| Backend | futex | Alternative |
|---------|-------|-------------|
| CV | 5 | - |
| timerfd | 0 | read() |
| POSIX | 0 | rt_sigtimedwait() |

CV uses `futex` for the condition_variable wait. Both kernel timers eliminate this entirely.

### Wait Mechanism

| Backend | Wait syscall | Blocking? |
|---------|--------------|-----------|
| CV | futex | Yes (polling first) |
| timerfd | read() | Yes (kernel wakes) |
| POSIX | rt_sigtimedwait | Yes (signal wakes) |

## Recommendation

**Use timerfd for ConsolidatedScheduler** because:

1. **Fewest non-deterministic events**
   - 75% reduction in `clock_gettime`
   - No `futex` calls
   - Simple `read()` for blocking

2. **Simpler than POSIX timer**
   - No signal handling complexity
   - No need to block SIGRTMIN
   - Single file descriptor per timer

3. **Kernel-managed timing**
   - No polling loop
   - Precise wake-ups
   - Lower CPU usage

4. **Well-supported on Linux**
   - Available since Linux 2.6.25
   - Works with epoll for multiplexing
   - No special permissions needed

### Trade-offs

| Aspect | timerfd | POSIX timer |
|--------|---------|-------------|
| Complexity | Lower | Higher (signals) |
| Portability | Linux only | POSIX systems |
| Multiplexing | epoll/select | sigwaitinfo |
| File descriptors | 1 per timer | 0 |

## Implementation Notes

### timerfd Pattern

```cpp
#include <sys/timerfd.h>

int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);

// Set one-shot or repeating timer
struct itimerspec its{};
its.it_value.tv_sec = seconds;
its.it_value.tv_nsec = nanoseconds;
its.it_interval = its.it_value;  // For repeating
timerfd_settime(tfd, 0, &its, nullptr);

// Block until timer fires
uint64_t expirations;
read(tfd, &expirations, sizeof(expirations));
```

### For ConsolidatedScheduler

Use single timerfd with dynamic deadline updates:

```cpp
class ConsolidatedScheduler {
  int timer_fd_;
  int cancel_fd_;  // eventfd for cancellation
  int epoll_fd_;   // Wait on both

  void UpdateDeadline(steady_clock::time_point next) {
    auto duration = next - steady_clock::now();
    struct itimerspec its{};
    its.it_value.tv_sec = duration_cast<seconds>(duration).count();
    its.it_value.tv_nsec = (duration % seconds(1)).count();
    timerfd_settime(timer_fd_, 0, &its, nullptr);
  }
};
```

## Demo Files

- `timer_backend_comparison.cpp` - Test program
- `cv_timer.undo` - CV recording
- `timerfd_timer.undo` - timerfd recording
- `posix_timer.undo` - POSIX timer recording
