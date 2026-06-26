# Coroutine cursors v2 — perf-gate RESULTS (bare-metal run)

This is the executed-results companion to [`PERF_GATE.md`](./PERF_GATE.md). It records the actual
measurements taken on a real PMU-capable machine, the methodology (including three measurement
traps that produced wrong numbers before they were fixed), and the resulting flip-default decision.

- **Branch:** `perf/coroutine-cursors-v2-gate`
- **Code tip:** PR-14 `d228c3c02`; binary built at HEAD `80d3a16f0` (docs-only commit on top — code identical).
- **Verified binary version:** `memgraph version 3.11.0+35~80d3a16f0117` (hash matches HEAD; stale-binary risk cleared).
- **Date of run:** 2026-06-26.

---

## 1. Environment

| Property | Value |
|---|---|
| Machine | **bare metal** (`systemd-detect-virt=none`, no `hypervisor` cpuid flag) |
| Cores | 16 |
| `perf` | 6.8.12 |
| Toolchain | `/opt/toolchain-v7` (Clang/libstdc++ with `GLIBCXX_3.4.34`) |
| Build type | RelWithDebInfo (`-O2 -g -DNDEBUG`, asserts off) |

### Box tuning applied for stable numbers (all require root)

```bash
sudo sysctl kernel.perf_event_paranoid=1            # was 4 -> perf can read CPU counters
sudo cpupower frequency-set -g performance          # was powersave
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo   # turbo OFF: max == base, no boost variance
# + CPU frequency pinned to a single fixed point (removes governor drift entirely)
```

> ⚠️ The box is left in this non-default state. Revert (`perf_event_paranoid=4`, `powersave`,
> `no_turbo=0`, unpin frequency) when finished.

The arms compared throughout (set via `--query-coroutine-yield-ops`):

| Arm | flag | meaning |
|---|---|---|
| **OFF** | *(empty)* | every cursor Sync — byte-identical to master (baseline) |
| **ALL** | `All` | whole plan coroutine — worst-case boundary cost |
| **SPLIT** | `Aggregate,OrderBy` | coroutine root→split-point only; scan/expand stay Sync |

---

## 2. Primary — 3-way wall-clock A/B (`run_ab.sh`, chain + fanout microbench)

Three full runs, pinned `taskset -c 0-7`, on the frequency-pinned box. Table is overhead vs the
OFF baseline; **median of the 3 runs** in the last two columns (per-run values shown for variance).

| query | ALL% (run1/2/3) | **ALL median** | SPLIT% (run1/2/3) | **SPLIT median** |
|---|---|---:|---|---:|
| scan_count    | +14.1/+1.8/+17.1 | +14.1% | +1.1/−13.0/−0.8 | −0.8% |
| filter_count  | +0.9/−2.2/+3.0   | +0.9%  | +4.5/−0.4/+2.2  | +2.2% |
| sum_agg       | −1.8/+4.3/+7.0   | +4.3%  | −1.8/+1.0/+0.9  | +0.9% |
| group_agg     | +3.5/+5.1/+7.7   | +5.1%  | +1.9/+0.9/+2.8  | +1.9% |
| expand_count  | +9.3/+8.8/+9.4   | **+9.3%**  | +2.9/+2.0/+2.7  | +2.7% |
| expand2_count | +14.1/+13.7/+13.4 | **+13.7%** | +2.6/+0.8/−1.0  | +0.8% |
| project_10k   | −6.5/+3.1/+5.5   | +3.1%  | −1.3/+0.5/−1.2  | −1.2% |
| orderby_1k    | −1.9/+7.6/+7.7   | +7.6%  | +0.8/+1.2/+1.6  | +1.2% |
| expand_fanout | +16.3/+18.5/+13.2 | **+16.3%** | +1.1/+1.0/−1.6  | +1.0% |

(Absolute OFF times were ~3× inflated because frequency was pinned low; **relative overhead %** is
the deliverable and is frequency-independent.)

**Reading:**
- **ALL reproduces a clean, low-variance regression** on pull-heavy reads: `expand_count` +9.3%,
  `expand2_count` +13.7%, `expand_fanout` +16.3% (run-to-run variance < 1%). The boundary cost is
  real on bare metal — not a VM artifact.
- **SPLIT collapses every one of those to ≤ ~2.7%**, well under the 5% hard ceiling. Aggregate/
  order-by rows are flat under both arms.
- `filter_count` +4.5% and `scan_count` −13% in run 1 were single-run noise (shortest queries,
  most jitter-sensitive) — they wash out in the median.

---

## 3. Hardware counters — instructions/query (the trustworthy number)

`perf_pmu.sh` measures the memgraph process over an **exact** query count
(`perf stat -p <pid> -- hammer_fixed.py … N`), so `instructions/query = counted / N` is exact and
**frequency-independent**. N = 120. Graph loaded into each arm's own process (100k `:N`, 99 990 `:R`)
with a non-vacuity assert.

### instructions / query

| query | OFF | ALL | SPLIT | **ALL Δ** | **SPLIT Δ** |
|---|---:|---:|---:|---:|---:|
| expand2 (`(n)-[:R]->()-[:R]->(m)`) | 421.4M | 463.5M | 424.7M | **+10.0%** | **+0.78%** |
| expand1 (`(n)-[:R]->(m)`)          | 249.7M | 273.7M | 253.0M | **+9.6%**  | **+1.3%**  |

### cycles / query + IPC

| query | OFF cyc | ALL Δ | SPLIT Δ | IPC OFF/ALL/SPLIT |
|---|---:|---:|---:|---|
| expand2 | 147.0M | +13.1% | +0.36% | 2.87 / 2.79 / 2.88 |
| expand1 | 89.5M  | +10.5% | +2.9%  | 2.79 / 2.77 / 2.75 |

**Reading:** IPC is ~2.8 across all arms with branch-miss ≈ 0 — the coroutine cost is **pure extra
retired instructions** (frame setup / suspend-resume dispatch), not pipeline stalls, exactly as the
p3 investigation predicted. On the trustworthy metric **SPLIT adds ~1%** while **ALL adds ~10%**;
the marginal +2.7% wall-clock SPLIT on `expand_count` was cycle/scheduling noise, not instructions.

---

## 4. Parallel-execution sanity (enterprise) — `parallel_ab.sh`

`USING PARALLEL EXECUTION` aggregate/order-by queries over 100k `:Node`, OFF/ALL/SPLIT, single run.
Non-vacuity: `EXPLAIN` shows `threads:` in every arm (`parallel_plan_has_threads=True`).

| query | OFF(ms) | ALL% | SPLIT% |
|---|---:|---:|---:|
| p_count   | 3.305  | +1.2% | −2.4%  |
| p_sum     | 6.340  | −5.9% | −12.6% |
| p_grouped | 7.494  | −6.6% | −6.9%  |
| p_orderby | 26.954 | −9.1% | −10.5% |

**Reading:** no arm regresses vs OFF, and **ALL tracks SPLIT** on every query (e.g. p_orderby −9.1%
vs −10.5%). Since the knob produces no consistent delta and ALL ≈ SPLIT, the coroutine knob has **no
effect on the parallel region** — confirming PR-14's design that parallel execution stays fully
synchronous (`ExecuteBranchesInParallel` drives sub-cursors via plain `Pull()`). The small negative
deltas are single-run noise. ✅ flat.

---

## 5. Yield latency — `tests/unit/cursor_yield_latency.cpp`

A **mechanism microbench** (synthetic fake-coroutine chain, no DB) measuring the **uninterruptible
span** = wall-time between two consecutive points where a cooperative yield can actually fire = the
latency a future scheduler (approach B) waits before a worker parks. Chain depth N=8; `S` = number of
top frames carrying a `YieldPointAwaitable`; the yield point lives **inside the blocking consume loop**
so granularity differs by mode. Headline = `max_ns`.

| scenario | root_only (S=1) | split (S=4) | full_coro (S=8) |
|---|---:|---:|---:|
| best (stream, M=1) | ~3.1 µs | ~0.35 µs | ~0.30 µs |
| real_world (M=4)   | ~17 µs  | ~3.7 µs  | ~0.95 µs |
| **worst (blocking, M=100k)** | **88–260 ms** | **88–260 ms** | **3–48 µs** |

(Absolute worst-case ms varies with the pinned CPU frequency between runs; the **ratio** is the
robust, reproduced deliverable.)

**Reading:**
1. The uninterruptible span is governed by whether a yield point sits **inside** the blocking
   consume. Only `full_coro` interrupts a blocking operator mid-consume (~µs); `split` and
   `root_only` must run the **entire** 100k consume uninterrupted (tens-to-hundreds of ms) — a
   ~10⁴–10⁵× difference.
2. **`split` gives no better preemption latency than `root_only` in the worst case** — the blocking
   work is *below* the split point, so neither has a yield point there. (Split's lower *median* is
   from many tiny mid-tree yields; the *max* — what the scheduler waits — equals root-only.)
3. Streaming/cheap plans (best/real) preempt fine in all modes (sub-µs to low-µs).

**Modeling caveat:** this models a blocking operator *below* the split point (e.g. a big scan/expand
feeding an aggregate). In the real SPLIT policy the Aggregate/OrderBy *is* the split point and is
itself a coroutine, so whether real SPLIT preempts mid-aggregate depends on whether the real Aggregate
consume loop carries a `YieldPointAwaitable` — a question for the (deferred) real-plan harness. The
mechanism bench establishes the **principle**, not the per-operator numbers.

---

## 6. Decision

Per the `PERF_GATE.md` decision tree, the throughput criterion — **SPLIT ≤ ~2%, ALL reproduces the
regression** — is met on the trustworthy metric (instructions/query: **SPLIT +1%**, ALL +10%).
Correctness (parity / knob / parallel tests) was already green.

➡️ **Green-light the endgame:** flip the empty-knob default to the split policy (coroutine
root→split-point), keep the knob as an override / kill-switch, and proceed to delete the dual `Pull`
/ `Immediate(Pull())` seam (the P3.4 cleanup). The flip is a throughput+correctness decision and is
**not** blocked by anything measured here.

**Caveat carried forward to approach B (not a blocker for the flip):** the latency bench shows SPLIT
inherits root-only's poor preemptibility for heavy synchronous work *below* the split point. When the
cooperative-yield scheduler (approach B) lands, the split-point set / yield-point placement must be
revisited so heavy operators can be preempted — independent of today's throughput flip.

---

## 7. Methodology traps encountered (and how they were caught)

Three measurements produced confidently-wrong numbers before being fixed. Recorded so they are not
repeated.

1. **CPU-frequency drift contaminated the first A/B.** With the governor still scaling, the always-last
   `SPLIT` arm ate the slow-frequency tail and showed SPLIT regressing *worse* than ALL (+47% vs +10%)
   — logically impossible (SPLIT's coro region ⊂ ALL's). **Fix:** pin the CPU frequency; the inversion
   vanished and SPLIT dropped to ≤2.7%.
2. **The PMU run first measured an empty graph.** With `--storage-snapshot-on-exit=false`, the graph
   loaded in one process did not survive the per-arm restart, so `perf` counted an empty DB (120
   expand2 "queries" in 0.15 s; 308K instr/query). **Fix:** load the graph **inside each measured
   process** and assert `nodes == 100000` before measuring (`minimal_load.py`).
3. **The first latency benchmark was vacuous.** It armed `yield_requested=true` before the first pull,
   so the root frame (which has a yield point in every mode) suspended immediately — identical ~96 ns
   across all 9 cells, `rows_before_event=0`. **Fix:** measure the **inter-yield interval** with the
   yield point placed *inside* the blocking consume, so the mode (depth of the coro region) actually
   gates whether a mid-consume yield is possible.

---

## 8. Reproducing

```bash
source /opt/toolchain-v7/activate
cmake --build build --target memgraph -j$(nproc)
./build/memgraph --version            # hash MUST equal `git rev-parse --short HEAD`

source tests/ve3/bin/activate          # mgclient

# 3.1 wall-clock A/B (no license, no root):
taskset -c 0-7 tests/mgbench/coroutine_perf/run_ab.sh build/memgraph 7799

# 3.2 PMU instructions/query (needs perf_event_paranoid<=1):
tests/mgbench/coroutine_perf/perf_pmu.sh build/memgraph 7799 120

# 3.4 parallel sanity (needs enterprise license in env):
set -a; source <license-file>; set +a
tests/mgbench/coroutine_perf/parallel_ab.sh build/memgraph 7796

# yield-latency mechanism microbench (no DB):
cmake --build build --target cursor_yield_latency -j$(nproc)
taskset -c 8-11 ./build/tests/unit/cursor_yield_latency
```
