# c3.4 Perf-Gate RESULTS — scheduler park redesign (bare-metal run)

Executed-results companion to [`HANDOFF.md`](./HANDOFF.md) §6 and
[`c34-perf-box-runbook.md`](./c34-perf-box-runbook.md). Records the actual measurements for the
**parallel scheduler park-vs-block** gate (Gate 1), plus a re-confirmation of the serial
coroutine-cursor gate (Gate 2, whose original bare-metal run is in [`../RESULTS.md`](../RESULTS.md)).

- **Branch:** `handoff/coroutine-scheduler-perf-gate`
- **Code tip:** c3.2b `8a9e25c40` (docs commit `2287c56ba` on top — code identical).
- **Verified binary:** `memgraph version 3.11.0+62~2287c56ba704` — embedded hash **matches HEAD
  `2287c56ba704`**. The pre-existing build (`80d3a16f0117`, 2026-06-26) was **stale** and was
  rebuilt from HEAD before any measurement. Stale-binary risk cleared.
- **Date of run:** 2026-07-06.

---

## 1. Environment

| Property | Value |
|---|---|
| Machine | **bare metal** — 11th Gen Intel i7-11800H, no `hypervisor` cpuid flag |
| Cores | 16 logical (8 physical × 2 SMT); siblings `core_N = (N, N+8)` |
| `perf` PMU | available after `sudo sysctl kernel.perf_event_paranoid=1` (was 4) |
| Governor | `performance` |
| **Turbo** | **ON** (`intel_pstate/no_turbo=0`) — see the noise note below |
| Toolchain | `/opt/toolchain-v7`, Clang 20.1.7, C++23 |
| Build type | RelWithDebInfo (`-O2 -g -DNDEBUG`), `MG_ENTERPRISE=ON`, no sanitizers |
| License | valid Enterprise (`SHOW LICENSE INFO` → `is_valid=True`, FOREVER) |
| Parallel plan | confirmed: `EXPLAIN USING PARALLEL EXECUTION 4 …` → `P ScanAll (n, threads: 4)` |

### ⚠️ Measurement trap that shaped the methodology — turbo was left ON

Unlike the 2026-06-26 cursors-v2 run (which disabled turbo), **turbo was ON** for this session
(the box had been reverted to defaults). On this laptop-class i7-11800H a measured core's turbo
frequency **wanders with neighbouring-core activity and thermals**, and a persistent background
process (`opencode`, ~1 full core) injected a **±6–9 % run-to-run wall-clock noise floor** — larger
than the 2 % budget the serial gate must resolve.

**Mitigation:** the primary metric for every per-query conclusion is **PMU instructions/query**,
which is **frequency- and contention-independent** (`perf stat -p <mg-pid> -- <fixed-N hammer>`,
count ÷ N). Wall-clock is reported as corroborating evidence with explicit run-to-run ranges. The
throughput A/B (§3) saturates all cores where all-core turbo is already capped, and is a same-box
relative comparison, so it is far less turbo-sensitive; it was additionally repeated pinned and
unpinned.

Arms compared (same binary, set via `--query-coroutine-yield-ops`):

| Gate | Arm | flag | meaning |
|---|---|---|---|
| Gate 2 (serial) | OFF | *(empty)* | every cursor Sync — byte-identical to master |
| Gate 2 (serial) | ALL | `All` | whole plan coroutine — worst case the split knob avoids |
| Gate 2 (serial) | SPLIT | `Aggregate,OrderBy` | coroutine root→split-point only |
| Gate 1 (parallel) | BLOCK | *(empty)* | sync coordinator blocks in `WaitOrSteal` (work-stealing) |
| Gate 1 (parallel) | PARK | `Aggregate,OrderBy,Accumulate,Distinct,HashJoin` | coroutine coordinator **parks**, frees its worker |

---

## 2. Gate 2 — serial coroutine-cursor A/B (re-confirmation) → ✅ PASS

`run_ab.sh` chain + fanout microbench, 5 runs (3 unpinned + 2 pinned to physical cores 2–7).

### 2.1 Wall-clock overhead vs OFF (mean over 5 runs; per-run range in brackets)

| query | ALL % (mean) | SPLIT % (mean) | SPLIT range |
|---|---:|---:|---|
| scan_count | **+17.5%** | **+0.5%** | [−6.1, +8.5] |
| expand_count | +7.6% | −0.3% | [−7.2, +5.2] |
| expand2_count | +10.7% | −1.3% | [−3.1, +1.0] |
| expand_fanout | **+19.9%** | **+1.3%** | [−3.2, +8.7] |
| sum_agg | −0.6% | +0.0% | [−5.4, +6.1] |
| group_agg | +3.4% | −0.9% | [−2.2, +0.2] |
| orderby_1k | +6.2% | −0.2% | [−9.5, +7.1] |

Every SPLIT mean is within ±2.4 %; the per-run spread is the turbo noise floor (see §1).

### 2.2 PMU instructions/query — the definitive, frequency-independent number (N=200)

| arm | expand2 insn/q | Δ vs OFF | expand1 insn/q | Δ vs OFF | branch-misses (expand2) |
|---|---:|---:|---:|---:|---:|
| OFF | 425.58 M | — | 252.52 M | — | 11.1 M |
| ALL | 463.15 M | **+8.8%** | 273.75 M | **+8.4%** | **39.9 M (3.6×)** |
| SPLIT | 425.53 M | **−0.01%** | 252.51 M | **−0.00%** | 11.1 M (identical) |

**SPLIT is instruction-for-instruction identical to OFF** on the pull-heavy scan/expand region
(those cursors stay Sync below the split point → byte-identical code). ALL's cost is real: +8.5 %
instructions and **3.6× branch-misses** from the per-boundary coroutine dispatch.

**Verdict (PERF_GATE.md §4):** SPLIT ≤ ~2 % everywhere → **green-light the endgame** (flip the
default to the split policy; delete the dual `Immediate(Pull())` path). Consistent with the
2026-06-26 run in [`../RESULTS.md`](../RESULTS.md).

---

## 3. Gate 1 — parallel scheduler PARK vs BLOCK → ⚠️ FAIL (do NOT ship as-is)

`throughput_gate.py`, dataset 400k `:N`, `USING PARALLEL EXECUTION 4`.

### 3.1 Single-query latency (p50, ms) — PARK vs BLOCK

| shape | run 1 | verify A | verify B | verdict |
|---|---|---|---|---|
| agg | −8.4% | +12.5% | +4.3% | neutral (within turbo noise) |
| grp | +1.4% | +4.2% | −1.8% | neutral |
| ord | +4.2% | −0.1% | +3.1% | neutral |

Single-query latency **does not regress** beyond noise — PARK is fine at concurrency 1.

### 3.2 Throughput under concurrency (parallel queries/sec), Δ = PARK vs BLOCK

| clients | run 1 (pinned) | verify A (pinned) | verify B (unpinned, 16 cores) |
|---:|---:|---:|---:|
| 1 | −8.5% | −6.8% | ±0% |
| 2 | −12.3% | — | — |
| 4 | −22.7% | −21.6% | −19.3% |
| 8 | −30.0% | — | — |
| 16 | **−43.2%** | **−44.2%** | **−49.0%** |

**PARK regresses throughput, monotonically worsening with concurrency**, reproducible across 3
runs. Unpinning to the full 16 cores did **not** remove it (−49 % at 16 clients) → **not** an
oversubscription artifact of core-pinning. This is the exact failure mode HANDOFF §6.2 flags.

### 3.3 Root cause — exact-N parallel-query PMU (single client, N=200, frequency-independent)

Reproduce with [`scripts/pmu_parallel_park_vs_block.sh`](./scripts/pmu_parallel_park_vs_block.sh)
(exact query count on one client → per-query counts immune to turbo/thermal wander).

| per-query metric | BLOCK | PARK | Δ |
|---|---:|---:|---:|
| instructions | 794.7 M | 1027.3 M | **+29.3%** |
| cycles | 309.9 M | 426.7 M | +37.7% |
| branches | 151.3 M | 212.6 M | +40.5% |
| branch-miss rate | 0.04% | 0.03% | equal |
| context-switches (1 client) | 2605 | 2476 | equal |

The park drive (`co_await` suspend/resume dispatch, `NotifyProgress`, `RegisterProgressWaiter`, the
`Finished() && InFlightZero()` barrier re-check loop, and coro-frame machinery) executes **+29 %
instructions and +40 % branches per parallel query**. The branches are **well-predicted** (miss
rate unchanged) and context-switches are **equal at 1 client** — so it is **not** branch
misprediction and **not** park/wake thrashing. It is raw per-query CPU **work volume**.

**Why it only bites under concurrency:** at low concurrency spare cores absorb the extra work (PARK
even spreads onto *more* cores — 4.39 vs 3.49 CPUs utilized — so latency stays neutral). At high
concurrency the box is CPU-saturated, so +29 % work-per-query directly displaces queries that would
otherwise complete → throughput falls, worsening with client count.

### 3.4 Verdict (HANDOFF §6.2 / runbook §3 decision criteria)

⚠️ **Do NOT ship the park redesign as-is.** It holds single-query latency but **regresses
throughput-under-concurrency by 20–49 %**, the opposite of the APPROACH_B hypothesis. The redesign's
core justification (better worker sharing → higher throughput) is **not** met on this hardware.

**Direction for the fix (per APPROACH_B.md caveat):** the park machinery is too heavy per query
(+29 % instructions). Options to explore, cheapest first:
1. **Cut the park-drive instruction cost** — the coordinator's whole drive runs coroutine to enable
   the park; profile the +29 % (suspend/resume dispatch vs `NotifyProgress`/barrier re-check) and
   inline/shrink the hot path before re-testing.
2. **Park conditionally** — only park when the pool has *other* queued work to run on the freed
   worker (parking pays off only if the freed worker is immediately useful); when the pool is
   saturated with this query's own branches, blocking/work-stealing (BLOCK) is strictly better.
3. **Revisit split-point placement** — a coroutine region extending over heavy work inflates the
   per-query coro cost; a lower/tighter split may shrink it.

---

## 4. What was NOT run this session

- **Full end-to-end TSan campaign (§6.1).** Not executed here. The authoritative pool state-machine
  TSan test (`utils_priority_thread_pool`, 41/41, 0 races) already passed on the dev box; the e2e
  confirmation remains open. It is moot for *shipping* until the Gate 1 throughput regression is
  resolved, but should be run once a revised park design clears Gate 1.
- **PMU for the serial cursors gate** beyond instructions/query — the full counter set is already in
  [`../RESULTS.md`](../RESULTS.md) (2026-06-26); not repeated.

## 5. Box state left behind (revert when done)

```bash
sudo sysctl kernel.perf_event_paranoid=4        # was set to 1 for perf
# governor already 'performance'; turbo was left ON (no_turbo=0) — unchanged
```
The enterprise license was supplied via a session-local env file (not committed).

## 6. One-paragraph summary

On bare-metal i7-11800H at HEAD `2287c56ba704`: **Gate 2 (serial coroutine cursors) PASSES** —
SPLIT is instruction-identical to OFF (−0.01 %) while ALL costs +8.5 % instructions / 3.6×
branch-misses; green-light flip-default + dual-path deletion. **Gate 1 (parallel scheduler PARK)
FAILS** — single-query latency neutral, but throughput-under-concurrency regresses 20–49 %
(reproducible, pinned and unpinned), because the park drive costs **+29 % instructions per query**
that saturates the CPU under load. Do not ship the park redesign as-is; cut the park-drive cost or
park only when a freed worker has other work, then re-gate.
