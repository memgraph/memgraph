# Stage 3: Time Operations Investigation

## Objective
Deep dive into clock_gettime and gettimeofday call stacks to identify which Memgraph code is responsible.

## Method
Use UDB to navigate to time-related events and capture backtraces.

## Events to Investigate
- clock_gettime: 25,295 events (18.76%)
- gettimeofday: 7,605 events (5.64%)

## Script
Run `script_3.sh` to capture sample backtraces.

## Output
- `findings_3/clock_gettime_traces.txt` - Sample backtraces for clock_gettime
- `findings_3/gettimeofday_traces.txt` - Sample backtraces for gettimeofday
- `findings_3/summary_3.md` - Analysis summary
