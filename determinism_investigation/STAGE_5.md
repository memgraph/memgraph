# Stage 5: Thread Switching Analysis

## Objective
Analyze THREADSWITCH and futex events to understand thread patterns and contention.

## Method
1. Count threads created (NEWTHREAD events)
2. Sample THREADSWITCH events for patterns
3. Analyze futex contention points

## Events to Investigate
- THREADSWITCH: 27,844 events (20.65%)
- futex: 20,580 events (15.26%)
- NEWTHREAD: 128 events
- clone3: 127 events

## Script
Run `script_5.sh` to capture thread analysis.

## Output
- `findings_5/thread_analysis.txt` - Thread switch patterns
- `findings_5/futex_traces.txt` - Futex contention points
- `findings_5/summary_5.md` - Analysis summary
