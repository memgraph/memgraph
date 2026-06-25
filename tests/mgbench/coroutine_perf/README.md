# Coroutine-cursor performance microbench (coroutine cursors v2 / PR-13 perf gate)

Measures the per-pull cost of the **coroutine cursor** pull path and validates the
`--query-coroutine-yield-ops` **split-point knob** as the mitigation for it.

Background: the coroutine pull path has a real per-cursor-boundary instruction-count cost.
Driving the *whole* plan as coroutines regresses pull-dominated reads (scan/expand) by a
double-digit percentage, while aggregate/order-by plans are ~free. The knob exists so we run
coroutines only where they are cheap (or, later, where cooperative yield pays off): coroutine
from the **root down to a yield-worthy split point** (Aggregate / OrderBy), and plain
synchronous `Pull()` below it — so the deep scan/expand region pays no boundary cost.

These scripts run the **same binary** three ways and compare:

| Arm   | `--query-coroutine-yield-ops` | Meaning |
|-------|-------------------------------|---------|
| OFF   | *(empty, the default)*        | every cursor Sync — byte-identical to master (baseline) |
| ALL   | `All`                         | whole plan coroutine — the worst-case the knob avoids |
| SPLIT | `Aggregate,OrderBy`           | coroutine root→split-point only; scan/expand stay Sync |

All queries are **plain serial Cypher** (no `USING PARALLEL EXECUTION`), so **no enterprise
license is required**; the result isolates the per-pull coroutine machinery on the serial read
path.

## Files
- `p33_microbench.py PORT TAG` — chain graph (100k `:N`, 100k chain `:R` edges); times 8
  pull-dominated queries (scan/filter/agg/expand/expand2/project/orderby), median of 40,
  writes `/tmp/p33_<TAG>.json`.
- `p33_fanout.py PORT TAG` — high-fan-out graph (1000 `:S` × 100 `:T`); isolates the
  per-helper-call component. Writes `/tmp/fan_<TAG>.json`.
- `hammer.py PORT QUERY SECS` — single-connection tight query loop, to generate steady CPU
  load for `perf` to sample on a box with a real PMU.
- `run_ab.sh [binary] [port]` — runs the 3-way A/B and prints the overhead table.

## Build
```bash
source /opt/toolchain-v7/activate
cmake --build build --target memgraph -j$(nproc)     # RelWithDebInfo (-O2 -g -DNDEBUG) is fine
```
A `-DCMAKE_BUILD_TYPE=Release` build is most production-representative, but RelWithDebInfo
(`-O2 -DNDEBUG`, asserts compiled out) is already representative.

## Run
```bash
source tests/ve3/bin/activate          # provides mgclient
tests/mgbench/coroutine_perf/run_ab.sh build/memgraph 7799
```

## Reading the result
- **ALL%** is the cost of running everything as coroutines: largest on `expand*`/`scan`,
  near-zero on `sum_agg`/`orderby`.
- **SPLIT%** should be ≈0 on the scan/expand rows (those subtrees stay Sync below the split
  point) while the aggregate/order-by rows still run their region as coroutines. That gap —
  ALL regressing pull-heavy reads vs SPLIT not — is the knob's whole reason to exist.

## Recorded run (nested aarch64 VM — Lima on Apple Silicon, RelWithDebInfo) — DIRECTIONAL

Chain microbench, median of 40, overhead vs the OFF (sync) baseline:

| query         | OFF(ms) | ALL%   | SPLIT% |
|---------------|--------:|-------:|-------:|
| scan_count    |   3.609 | +18.6% |  +4.6% |
| filter_count  |   7.951 | +28.2% |  +2.1% |
| sum_agg       |   7.898 | +15.9% |  -0.2% |
| group_agg     |   8.236 | +15.5% |  +3.1% |
| expand_count  |  10.014 | +11.3% |  +0.8% |
| expand2_count |  16.930 | +14.3% |  +0.4% |
| project_10k   |   7.195 |  +5.8% |  -1.8% |
| orderby_1k    |  17.975 |  +9.8% |  -0.9% |

The thesis holds: running the whole plan as coroutines (ALL) costs **+6% to +28%** on
pull-dominated reads, while the **SPLIT** setting collapses that to **≈0% (−2% .. +5%)** on
those same reads — because the deep scan/expand region stays synchronous below the split
point. (sum_agg/orderby SPLIT being marginally faster is within run-to-run noise.) These are
inflated nested-VM numbers; the *relative* ALL-vs-SPLIT gap is the result, not the absolute %.

## Hardware note (authoritative numbers)
The original investigation (see `INVESTIGATION.md` on the reference commit and
`ADRs/008_coroutine_cursors.md`) found the dominant cost is per-pull coroutine
resume/suspend **dispatch**, and that a **nested aarch64 VM (Lima on Apple Silicon)** both
inflates it and lacks a usable PMU (`perf stat -e cycles` = `<not supported>`). Treat numbers
from such a VM as **directional only**; the perf-gate decision (P3.3 → flip default) must be
taken from a **bare-metal** run. The `All` arm is a benchmarking lever for exactly that
measurement and is not a recommended production setting for pull-heavy plans.
