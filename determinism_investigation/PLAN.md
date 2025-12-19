# Non-Determinism Investigation Plan

## Objective
Identify, categorize, and prioritize sources of non-determinism in Memgraph to make Undo recordings smaller and more predictable for time-travel debugging.

## Tools
- **live-record**: `~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/live-record`
- **udb**: `~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/udb`
- **Build**: RelWithDebInfo (for symbol resolution)

## Stages

### Stage 1: Recording
Record memgraph execution with test queries under live-recorder.
- Files: `STAGE_1.md`, `script_1.sh`, `findings_1/`

### Stage 2: Event Statistics Overview
Get overall event statistics to understand distribution of non-determinism sources.
- Files: `STAGE_2.md`, `script_2.sh`, `findings_2/summary_2.md`

### Stage 3: Time Operations Investigation
Deep dive into gettimeofday and clock_gettime call stacks.
- Files: `STAGE_3.md`, `script_3.sh`, `findings_3/summary_3.md`

### Stage 4: RNG Investigation
Investigate getrandom and other random number sources.
- Files: `STAGE_4.md`, `script_4.sh`, `findings_4/summary_4.md`

### Stage 5: Thread Switching Analysis
Analyze THREADSWITCH events and patterns.
- Files: `STAGE_5.md`, `script_5.sh`, `findings_5/summary_5.md`

### Stage 6: Recommendations
Final summary and remediation recommendations.
- Files: `STAGE_6.md`, `findings_6/summary_6.md`

## Expected Event Types
Based on initial Release build investigation:

| Event Type | Count | % | Notes |
|------------|-------|---|-------|
| THREADSWITCH | 25,603 | 53.3% | Dominant source |
| gettimeofday | 4,629 | 9.6% | Time operations |
| clock_gettime | 3,536 | 7.4% | Time operations |
| gettid | 2,109 | 4.4% | Thread ID lookups |
| WALLCLOCK_TIME | 1,375 | 2.9% | Wall clock captures |
| futex | 1,369 | 2.9% | Thread sync |
| getrandom | 16 | 0.03% | Startup seeding only |

## Key Source Files (suspected)
- `src/utils/scheduler.hpp` - Scheduler timing
- `src/utils/timestamp.hpp` - Timestamp generation
- `src/utils/skip_list.cpp` - Skip list RNG seeding
- `src/utils/uuid.cpp` - UUID generation
