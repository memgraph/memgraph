# Coroutine cursors v2 — perf gate & bare-metal verification plan

This is the hand-off doc for the **performance investigation** of the coroutine-cursor v2
stack. It records what was built, what is already verified, and the **exact measurements +
decision criteria** that must be produced on a real (bare-metal, PMU-capable) machine before
we can flip the coroutine pull path on by default.

- **Branch:** `perf/coroutine-cursors-v2-gate` (this branch), forked off the implementation
  tip `d228c3c02` on `pr/coroutine-cursors-v2-01-runtime`.
- **Status:** implementation complete (PR-1…PR-14), all dormant by default, master untouched,
  nothing pushed. The only thing left before "flip default" is **this perf gate**.
- **Run instructions for the benchmark itself:** see [`README.md`](./README.md). This doc is the
  *plan + decision criteria* layered on top of it.

---

## 1. What was built (and why it should be ~free by default)

The goal: make every query-plan cursor able to run as a **stackless coroutine** (so a worker can
cooperatively yield/park mid-pull — the foundation for better parallel scheduling), **without**
regressing the ordinary synchronous pull path.

The design is a **dual-path seam**, default-dormant:

- Every cursor keeps its existing `bool Pull(Frame&, ExecutionContext&)` as the **synchronous
  body, byte-identical to master**. Nothing was renamed.
- A virtual `PullCo()` was added. Its **base default is `Immediate(Pull())`** — i.e. an
  unconverted (or Sync-mode) cursor pulled through the coroutine entry point just runs its
  synchronous `Pull()` with no coroutine frame allocated.
- A "converted" cursor additionally has a `PullAwaitable DoPull(...)` coroutine twin (added via
  the `MG_COROUTINE_CURSOR_PULLCO` macro) that drives a persistent `gen_` generator. It is only
  used when that cursor's `mode()` is `Coro`.
- Per-cursor `CursorMode { Sync, Coro }`. **Default Sync ⇒ behaviour is exactly master.**

### The split-point knob (the crux of the perf story)

The coroutine pull path has a real **per-cursor-boundary instruction-count cost** (allocate/lay
out a frame, suspend/resume dispatch). On the original (p3) investigation
(`ADRs/008_coroutine_cursors.md` + `INVESTIGATION.md`, on the old `pr/coroutine-cursors-p3-unify`
branch @ `897d8e277`) the cost concentrated on **shallow, high-row scans/expands** and was ~0 on
aggregate/order-by. So we do **not** run the whole plan as coroutines. Instead:

> Run coroutines only from the **root down to (and including) a yield-worthy "split point"
> operator** (e.g. Aggregate / OrderBy); everything **below** the split point stays a plain
> synchronous `Pull()` and pays no boundary cost.

This is controlled by a **runtime gflag**:

```
--query-coroutine-yield-ops=<comma-separated operator kinds>
```

- **empty (default)** → every cursor Sync → byte-identical to master, zero overhead.
- `Aggregate,OrderBy` → the recommended "SPLIT" setting: coroutine root→split-point, sync below.
- `All` (or `*`) → whole plan coroutine — the **worst-case** arm; a benchmarking lever, not a
  production setting.
- case-insensitive, whitespace-trimmed, unknown names warned + ignored.

Selection happens at cursor construction (bottom-up, no tree walk): each converted cursor calls
`SelectCoroMode({child cursors}, CoroOp::X)` and goes `Coro` iff its op-kind is a split point
under the active policy **or** any of its co-awaited children is `Coro`. This yields a
contiguous Coro region from the root down to the deepest split point. `PullPlan::Pull` drives the
root via `PullCo + ResumePullStep` iff `root.mode()==Coro`, else plain `Pull()`.

### Enterprise parallel cursors (PR-14)

The 6 parallel cursors (`ScanParallel`, `DistinctParallel`, `ParallelMerge`, `ParallelBranch`,
`AggregateParallel`, `OrderByParallel`) intentionally have **no coroutine body** and ride the base
`PullCo()=Immediate(Pull())`. `ExecuteBranchesInParallel` drives branch sub-cursors via plain
synchronous `Pull()` and blocks on `collection_scheduler_->WaitOrSteal()` (work-stealing) — there
is no `PullCo`/`co_await` anywhere in the parallel region. They must never get the `gen_` macro
(`ScanParallel`/`DistinctParallel` are shared producers pulled concurrently — one coroutine frame
can't be resumed concurrently). The cooperative-yield "self-park" coordinator model is the
deferred **approach B** (a throughput optimization, out of scope for this gate).

### Commit map (off master)

| PR | Commit | Group |
|----|--------|-------|
| 1  | `b120cb4f2` | dormant pull runtime |
| 2  | `a801ff734` | per-cursor mode seam |
| 3  | `a7eddb5e9` | coroutine root-drive seam |
| 4  | `82cd0c3cc` | parity corpus |
| 5  | `81e9688b2` | leaf/simple (Once, Produce, Filter, Limit, Skip) |
| 6  | `faaf1c0b1` | scan/expand (ScanAll, ScanAllByEdge, Expand, ExpandVariable, EdgeUniquenessFilter, ConstructNamedPath) |
| 7  | `8237b6102` | write (Create*, Delete, Set*, Remove*) |
| 8  | `77624fcdb` | multi-child (Apply, Merge, Optional, Union, Cartesian, HashJoin, IndexedJoin) |
| —  | `841a74b52` | gate parity harness behind NDEBUG (Debug-only) |
| 9  | `9f2676767` | breaker (Accumulate, EmptyResult, Unwind, Distinct, OrderBy, Aggregate, RollUpApply, CallProcedure) |
| 10 | `c29ffa4fd` | shortest-path (ST/SingleSource/Weighted/AllShortest/KShortest) |
| 11 | `78d3cea2a` | IO/table/Foreach (LoadCsv/Parquet/Jsonl, OutputTable*, Foreach) |
| 12 | `23903aab2` | PeriodicCommit + PeriodicSubquery |
| 13a| `74e7c4211` | per-cursor split-policy wiring (dormant) |
| 13b| `e90dd67ef` | runtime knob + production coro drive |
| 13c| `b2ea524d5` | `All` knob token + 3-way microbench |
| 14 | `d228c3c02` | enterprise parallel cursors (approach A) |

---

## 2. What is already verified (correctness — NOT performance)

Run locally (RelWithDebInfo, nested VM), all green:

- **`cursor_parity`** (Debug-only, NDEBUG-gated) — a corpus of ~45 read + ~17 write queries run
  synchronously vs. with every cursor force-driven coroutine; rendered results identical. This is
  a mechanical proof that **coroutine pull == synchronous pull** across the operator surface.
- **`cursor_knob`** (production build) — `--query-coroutine-yield-ops` results are invariant
  knob-off vs Aggregate/OrderBy/All/`*`/combined/whitespace/unknown. Verified **non-vacuous** (a
  temporary root-mode probe confirmed valid knobs drive the root Coro; absent/unknown ops stay
  Sync).
- **`CursorKnobParallelTest`** (MG_ENTERPRISE) — real `PriorityThreadPool` + license; `USING
  PARALLEL EXECUTION` aggregate/grouped/order-by queries are result-invariant across knob
  OFF/All/Aggregate,OrderBy, with a non-vacuity guard asserting the plan actually parallelized
  (`EXPLAIN` shows `threads:`).
- `cursor_seam` 5/5, `cursor_yield` 12/12.

**Directional (nested aarch64 VM, RelWithDebInfo — inflated, no usable PMU) 3-way microbench:**

| query class            | ALL overhead | SPLIT overhead |
|------------------------|-------------:|---------------:|
| scan / filter / expand | +11%..+28%   | +0.4%..+4.6%   |
| sum / group / order-by | +10%..+16%   | −0.9%..+3.1%   |

The *shape* (ALL regresses pull-heavy reads; SPLIT collapses it to ~0%) is the result; the
absolute numbers are not trustworthy from a VM. **That is exactly what the perf box must settle.**

---

## 3. What MUST be verified on the perf box (the gate)

The flip-default decision is gated on a **performance budget**: historically agreed as
**a couple of percent overhead is acceptable; >5% is not.** Produce the following, on bare metal,
with a real PMU.

### 3.0 Build (production-representative)

```bash
source /opt/toolchain-v7/activate
# Most representative: a Release build.
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target memgraph -j$(nproc)
# RelWithDebInfo (-O2 -g -DNDEBUG, asserts off) is acceptable and what the VM numbers used.
```

⚠️ **Verify the binary matches the branch before trusting any number:**
```bash
./build/memgraph --version   # hash must equal `git rev-parse --short HEAD`
```
(Stale-binary repros have burned us before.)

### 3.1 Primary: the 3-way wall-clock A/B

```bash
source tests/ve3/bin/activate                       # provides mgclient
tests/mgbench/coroutine_perf/run_ab.sh build/memgraph 7799
```
This loads the 100k chain graph and the fan-out graph and prints, per query, the median over 40
runs for **OFF / ALL / SPLIT** plus the overhead %. Capture the full table.

**What to confirm / answer:**
1. **SPLIT overhead is within budget** (target ≤ ~2%, hard ceiling 5%) on *every* query class,
   especially the pull-heavy `scan_count`, `filter_count`, `expand_count`, `expand2_count`.
2. **ALL reproduces a meaningful regression** on those same pull-heavy queries (sanity that the
   coroutine cost is real and that SPLIT is actually avoiding it — not that the machine is just
   too fast to see anything).
3. The aggregate/order-by rows are ~flat under both ALL and SPLIT (expected).
4. Run it **2–3 times**; record run-to-run variance. Pin the process (`taskset`/`nice`), disable
   turbo/frequency scaling if possible, and run on an otherwise-idle box.

### 3.2 Hardware counters (the "why", and the trustworthy number)

The VM could not do this (`perf stat -e cycles` = `<not supported>`). On the perf box:

```bash
# steady-state load for sampling (single connection, tight loop):
tests/mgbench/coroutine_perf/hammer.py 7799 "MATCH (n:N)-[:R]->()-[:R]->(m) RETURN count(*)" 30 &
# then, against the memgraph PID:
perf stat -e instructions,cycles,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses -p <pid> sleep 20
```
Do this for OFF vs ALL vs SPLIT (restart memgraph with the relevant `--query-coroutine-yield-ops`
each time; see `run_one()` in `run_ab.sh` for the flag plumbing). Report **instructions/query** and
**cycles/query** deltas — instruction count is the most stable cross-run metric and is what the p3
investigation used to attribute the cost to per-boundary work vs per-helper-call work.

> Reference component split from p3 (chain ~17–20% vs fan-out ~12.8% on an i7-11800H): roughly
> ~5–7% per-helper-call + the remainder per-result dispatch. Confirm whether v2's numbers match,
> and whether the dispatch base is a real cost or (as suspected on the VM) a nested-virtualization
> indirect-branch artifact that mostly disappears on bare metal.

### 3.3 Split-point tuning (the product question)

The knob currently recognises **only `Aggregate` and `OrderBy`** as split points. With real
numbers, answer:
- Is `Aggregate,OrderBy` the right **default split set**? Are there plan shapes where the
  coroutine region is still large enough to cost > budget (deep coroutine region above the split
  point)?
- Do we need **more split-point op kinds** (e.g. Accumulate, Distinct) to push the split lower in
  more plans? (Adding one = a new `CoroOp` enum value + a name in `CoroOpFromName` in
  `src/query/interpreter.cpp` + the `SelectCoroMode(..., CoroOp::X)` call on that cursor.)
- Does any realistic query regress under SPLIT? If so, capture its `EXPLAIN` and the coro region.

### 3.4 Parallel execution (does the seam touch parallel throughput?)

PR-14 keeps parallel execution fully synchronous, so there should be **no** parallel throughput
change. Confirm with a parallel workload (needs an enterprise license — export
`MEMGRAPH_ORGANIZATION_NAME` + `MEMGRAPH_ENTERPRISE_LICENSE`; see the stress workload at
`tests/stress/standalone/native/workloads/parallel_execution/`). Compare OFF vs SPLIT vs ALL on `USING PARALLEL EXECUTION`
aggregate/order-by queries — expect ~flat. If ALL shows a parallel regression, that is information
for the deferred **approach B** (self-park) work, not a blocker for this gate.

### 3.5 Macro check (optional, end-to-end)

Run a representative slice of the standard mgbench (e.g. pokec) OFF vs SPLIT to confirm there is no
surprise at the whole-query level beyond the microbench. Pokec dataset is cached at
`tests/mgbench/.cache/datasets/pokec/`.

---

## 4. Decision tree (what each outcome unlocks)

- **SPLIT ≤ ~2% everywhere** → green-light the endgame:
  1. **Flip default** — change the empty-knob default so the coroutine path is selected per the
     split policy (root→split-point Coro). Keep the knob as an override / kill-switch.
  2. **Delete the dual path** — once coroutine-first is settled, make `Pull` non-virtual / remove
     the `Immediate(Pull())` seam and the `mode()` branch (the p3 "P3.4" cleanup).
- **SPLIT 2–5%** → ship coroutine-capable but keep default **off** (knob opt-in); revisit the
  split-point set / kernel hot-path (the p3 `await_resume` / `InitEdgesCo` inlining already
  landed — see if more is needed). Do **not** flip default.
- **SPLIT > 5% on real hardware** → the split model is insufficient as-is; the coroutine pull path
  stays opt-in only, and the cost needs another kernel-level pass before any flip.
- Regardless: the correctness work (PR-1…14) stands on its own and is dormant/safe; nothing here
  blocks merging the seam as opt-in.

---

## 5. Gotchas / environment notes

- **`cursor_parity` is Debug-only** (NDEBUG-gates the force-coro hook). In Release/RelWithDebInfo
  it compiles to a single skipped test. The production knob path (`cursor_knob`) is **not** gated
  and runs everywhere — that, plus the parallel test, is the production-build correctness gate.
- **Unit-test scratch dir:** `mkdir -p build/tmp && TMPDIR=build/tmp ./build/tests/unit/<t>`.
- **Enterprise license** (for parallel runs):
  `export MEMGRAPH_ORGANIZATION_NAME='Memgraph'`
  `export MEMGRAPH_ENTERPRISE_LICENSE='<your-enterprise-license-key>'`
- **The knob is read fresh per query construction**, so it is runtime-tunable via
  `gflags::SetCommandLineOption` / per-restart; `run_ab.sh` sets it per server start.
- Prior art to read first: `ADRs/008_coroutine_cursors.md` and
  `tests/mgbench/coroutine_perf/INVESTIGATION.md` — both live on the **old** branch
  `pr/coroutine-cursors-p3-unify` @ `897d8e277` (`git show 897d8e277:<path>`), not on this branch.

---

## 6. One-paragraph summary to paste into the perf-box session

> We have a dual-path coroutine-cursor seam on `perf/coroutine-cursors-v2-gate` (off
> `d228c3c02`). Default = synchronous = master. `--query-coroutine-yield-ops=Aggregate,OrderBy`
> runs coroutines only root→split-point ("SPLIT"); `=All` runs the whole plan coroutine (worst
> case). Correctness is proven (parity + knob + parallel tests green). The open question is purely
> performance: on bare metal with a PMU, run `tests/mgbench/coroutine_perf/run_ab.sh`, confirm
> SPLIT is within ~2% of OFF on pull-heavy scans/expands (hard ceiling 5%) while ALL reproduces the
> regression, capture `perf stat` instructions/query for OFF/ALL/SPLIT, and decide whether to flip
> the default + delete the dual path.
