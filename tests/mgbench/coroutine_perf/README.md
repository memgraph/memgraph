# Coroutine-cursor performance microbench (P3.3 perf gate)

Reproduces the P3.3 performance investigation for the **coroutine cursors** work
(`COROUTINE_CURSORS` flag). Full context, findings, and decisions:
[`ADRs/008_coroutine_cursors.md`](../../../ADRs/008_coroutine_cursors.md).

The flip to make the coroutine pull path the default (P3.4) is gated on a performance
budget: **a couple of percent overhead is acceptable; >5% is not.** These scripts measure
the overhead by running the **same binary** twice — flag-OFF (`PullLegacy`, byte-identical
to master) vs flag-ON (`--experimental-enabled=coroutine-cursors`, the coroutine path).

All queries here are **plain serial Cypher** (no `USING PARALLEL EXECUTION`), so **no
enterprise license is required**, and the result isolates the per-pull coroutine
machinery on the serial read path.

## Files
- `p33_microbench.py PORT TAG` — chain graph (100k `:N`, 100k chain `:R` edges); times 8
  pull-dominated queries (scan/filter/agg/expand/expand2/project/orderby), median of 40,
  writes `/tmp/p33_<TAG>.json`.
- `p33_fanout.py PORT TAG` — high-fan-out graph (1000 `:S` × 100 `:T` = 100k edges, ~1000
  `InitEdges` calls); used to isolate the per-helper-call component from the per-result
  dispatch base. Writes `/tmp/fan_<TAG>.json`.
- `hammer.py PORT QUERY SECS` — single-connection tight query loop, to generate steady CPU
  load for `perf` to sample.
- `run_ab.sh [binary] [port]` — runs both microbenches flag-OFF vs flag-ON and prints the
  overhead table.
- `profile_perf.sh <off|on> [binary] [port]` — HW-counter profiling (needs a real PMU; see
  below).

## Build
```bash
source /opt/toolchain-v7/activate
cmake --build build --target memgraph -j$(nproc)     # RelWithDebInfo (-O2 -g -DNDEBUG) is fine
```
For the most production-representative numbers, a `-DCMAKE_BUILD_TYPE=Release` build is
ideal, but RelWithDebInfo (`-O2 -DNDEBUG`, asserts compiled out) is already representative.

## Run the A/B
```bash
source tests/ve3/bin/activate          # provides mgclient
tests/mgbench/coroutine_perf/run_ab.sh build/memgraph 7799
```
Each query prints `OFF / ON / ovhd%`. Single-threaded; mgclient round-trips are negligible
because the queries return few rows (server-side pull dominates).

## Baseline (nested aarch64 VM — Lima on Apple Silicon, RelWithDebInfo)
These are the numbers from the original investigation; the bare-metal run should be
compared against them. **Hardware PMU is unavailable on this VM** (`perf stat -e cycles`
= `<not supported>`), which is exactly why a bare-metal box is needed.

| Query class                 | flag-ON overhead (this VM) |
|-----------------------------|----------------------------|
| Expansion (1-hop / 2-hop)   | ~17–20%                    |
| Raw scan / filter / sum-agg | ~10–13%                    |
| Grouped aggregation         | ~6%                        |
| Projection / order-by-limit | ~2–4%                      |
| Fan-out expand (isolation)  | ~12.8%                     |

Component split (from chain ~17–20% vs fanout ~12.8%):
**~5–7% per-helper-call** (one-shot helper coroutines like `InitEdgesCo` allocate a frame
per call — arch-independent, fixable by inlining) **+ ~12.8% per-result dispatch base**
(the per-pull coroutine resume/suspend itself).

Locally ruled out as the *dominant* cause (no meaningful change): hoisting per-row setup,
removing the inner-loop yield-point suspension, and a pooled coroutine-frame allocator.

## Profile on bare metal (the actual goal)
On a box with a real PMU (`perf stat -e cycles,instructions` must NOT say
`<not supported>`):
```bash
# may need: sudo sysctl kernel.perf_event_paranoid=1   (and kernel.kptr_restrict=0)
tests/mgbench/coroutine_perf/profile_perf.sh off build/memgraph 7799
tests/mgbench/coroutine_perf/profile_perf.sh on  build/memgraph 7799
```
Compare flag-ON vs flag-OFF **cycles**, **IPC**, and **branch-misses**.

## Decision criteria
- **Hypothesis:** the ~12–15% dispatch base is largely a nested-VM artifact — coroutine
  resume is an indirect branch, and this VM predicts indirect branches poorly. On bare
  metal (good BTB) it should be far smaller.
- If bare-metal flag-ON overhead is **≤ ~5%** → P3.3 gate passes; proceed to P3.4 (flip
  default, delete the dual path).
- If still **> 5%** with elevated branch-misses → the per-pull dispatch is a real cost;
  revisit the dispatch design (reduce suspension points / batch resumes) and/or land the
  arch-independent helper-coroutine inlining first.
