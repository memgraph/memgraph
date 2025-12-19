# Stage 4: RNG Investigation

## Objective
Investigate getrandom syscall sources to understand RNG seeding patterns.

## Method
Use UDB to capture backtraces for all getrandom events.

## Events to Investigate
- getrandom: 17 events (0.01%)

Note: Low count indicates RNG is seeded once at startup.

## Script
Run `script_4.sh` to capture backtraces.

## Output
- `findings_4/getrandom_traces.txt` - Backtraces for getrandom calls
- `findings_4/summary_4.md` - Analysis summary
