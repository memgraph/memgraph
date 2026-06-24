# Coroutine Cursors (Cooperative Query Execution)

**Author**
Andreja Tonev (https://github.com/andrejtonev)

**Status**
IN PROGRESS — Phase 1 & 2 landed on their stacked branches; Phase 3 (P3.1 serial
yield-wiring, P3.2 parallel cursors) implemented and verified locally on
`pr/coroutine-cursors-p3-unify`; P3.3 performance gate open (see below); P3.4
(default flip + dual-path deletion) not started. Nothing pushed.

**Date**
2026-06-24

---

## Problem

Memgraph executes a query plan as a tree of `Cursor`s driven by a synchronous
`bool Cursor::Pull(Frame&, ExecutionContext&)` pull loop. A single `Pull` runs to
completion on its worker thread; there is no way to **cooperatively yield** mid-pull.
That has two consequences:

1. **Fairness / head-of-line blocking.** A long-running query (large scan, deep
   expansion) monopolises its worker thread until it finishes a pull. Short queries
   queued behind it wait. There is no preemption point inside a pull.
2. **Thread-blocking primitives.** Operators that must wait (e.g. the enterprise
   parallel cursors waiting on branch tasks) block their worker thread, tying up a
   pool slot for the duration.

The goal of the **coroutine cursors** work is to make cursor pulls *resumable
coroutines* that can `co_await` a yield point and suspend back to a scheduler, so the
worker is freed and the query is rescheduled later — cooperative multitasking for query
execution.

This is a large, invasive change to the hottest code path in the engine. It must land
incrementally, provably preserve correctness, and not regress performance.

## Context — the migration strategy (3-phase stack)

The original prototype (`coroutine_cursors` branch, "big bang") converted *every*
cursor to a coroutine in one step: `Pull()` itself became the coroutine driver and
there was no fallback. That is unreviewable and unrevertable. We decomposed it into a
stacked, flag-gated migration:

- **Phase 1 — dual-path seam.** Every cursor gains a second implementation behind a
  runtime flag (`flags::Experiments::COROUTINE_CURSORS`, gflag
  `--experimental-enabled=coroutine-cursors`, default OFF):
  - `PullLegacy()` — the original synchronous body, verbatim (flag-OFF path).
  - `DoPull()` — the coroutine twin (flag-ON path), driven once per row via `PullCo()`
    / a persistent generator `gen_`.
  - The base `Cursor::Pull()` becomes a *router*: flag-OFF → `PullLegacy()`; flag-ON →
    drive the coroutine. Converted cursors do **not** override `Pull()`.
  - A parity harness (`tests/unit/cursor_parity.cpp`) proves flag-OFF ≡ flag-ON over a
    query corpus. This is the core safety property of the whole migration.
- **Phase 2 — scheduling primitives.** `WorkerYieldRegistry`, a uniform priority
  thread pool with explicit task states (IDLE/SCHEDULED/PARKED/STOLEN/FINISHED), and the
  yield primitive: `StoppingContext`, `YieldPointAwaitable`, `PullDriverScope`.
- **Phase 3 — unify & convert.** Wire the actual yield trigger into the serial cursors
  (P3.1), convert the remaining enterprise parallel cursors to the seam (P3.2),
  performance-gate the coroutine path (P3.3), then flip the default ON and delete the
  dual path (P3.4).

The coroutine machinery (`src/query/plan/cursor_awaitable_core.hpp`,
`cursor_awaitable.hpp`) uses **symmetric transfer** (tail-call) for both descent into a
child (`Awaiter`/`ResumeAwaitable::await_suspend` returns the child handle) and unwind
back to the parent (`BasePromise::final_suspend → SymmetricTransfer{parent_}`). Cursor
generators are **long-lived**: `gen_` is created once (`MG_COROUTINE_CURSOR_PULLCO`:
`if (!gen_) gen_ = DoPull(...); return gen_->Resume();`) and resumed per pull.

## Decisions

### D1 — Dual-path seam, flag-gated, parity-proven (Phase 1)

Each cursor carries both implementations during the migration. Correctness is anchored
by the parity harness (flag-OFF byte-identical to master; flag-ON proven identical to
flag-OFF). The dual path is temporary: P3.4 deletes `PullLegacy()`, the router, and
makes `Pull()` non-virtual. This trades a transitional `use_coroutine_` branch +
duplicated bodies for a safe, reviewable, revertable rollout.

### D2 — P3.2 parallel cursors: Approach A (blocking DoPull), not the park redesign

The 6 enterprise parallel cursors (`DistinctParallel`, `ScanParallel`, `ParallelMerge`,
`AggregateParallel`, `OrderByParallel`, and the `ParallelBranchCursor` base) were the
last cursors still overriding `bool Pull`. Two ways to put them on the seam were
considered:

- **Approach A (chosen):** keep master's exact execution model — branch 0 runs inline on
  the calling thread, branches 1..N-1 are pool tasks, the caller blocks in
  `CollectionScheduler::WaitOrSteal()` (work-stealing its own branch tasks inline) — and
  only add coroutine-seam compatibility: rename `Pull`→`PullLegacy` and override `PullCo`
  to **immediate-wrap** `PullLegacy` (`ResumeAwaitable::Immediate(PullLegacy(...))`). No
  parking, no worker freed.
- **Approach B (deferred):** the "uniform-coordinator-park" redesign — the coordinator
  becomes a coroutine that `co_await`-parks until branches finish, freeing its worker;
  all N branches become symmetric pool tasks.

**Why A:** a concurrency adjudication (recorded in the design-verification artifact)
proved Approach A is **correct and deadlock-free** in flag-ON mode: a blocked
coordinator steals and runs its own branch tasks inline, so progress is guaranteed even
if every pool worker is a blocked coordinator. Therefore Approach B is a *throughput
optimization*, not a correctness requirement, and was deferred to a follow-up PR. The
park risk register (R1 drain barrier, R2 single-waiter, R3 merge survivor, R6
nested-parallel, R7 collapse) is fully documented for that future PR.

**Critical implementation detail:** the parallel cursors must **not** use the persistent
`gen_` coroutine pattern. `ScanParallel`'s cursor is *shared* across branches (reused via
`plan_creation_helper_`) and pulled **concurrently** by multiple branch workers; a single
coroutine frame cannot be `Resume()`d concurrently (corrupts coroutine state). The
immediate-`PullCo` form has no shared coroutine frame and is re-entrant (the underlying
`PullLegacy` is mutex-guarded where shared). Yield is suppressed for the per-branch serial
sub-cursors via a coordinator-level `PullDriverScope(Suppressed)` over
`ExecuteBranchesInParallel` plus a per-branch `Suppressed` scope (so each concurrent
branch gets its own `suspended_task_handle_ptr` slot — otherwise concurrent branches race
on a shared slot).

Commits:
- **C0** `2d8f63e40` — `CollectionScheduler::pool_` made `std::atomic` (release/acquire). A
  pre-existing TSan data race (`Trigger()` nulls `pool_` while a worker reads it in
  `RegisterProgressWaiter()`), surfaced during P3 TSan work. Standalone, independent of
  the rest.
- **C1** `8539b17dd` — **plan-time prohibition of parallel sections inside subquery arms**
  (`InsideSubqueryArm` guard in `parallel_rewrite.hpp`), plus a missing `Union` branch in
  `ConflictingOperators`. This *fixes a pre-existing silent-data-loss bug*: parallelizing
  an `Aggregate`/`OrderBy` inside an `Apply`/`CALL{}` subquery arm corrupted the enclosing
  `Apply` and dropped main-branch rows (e.g. `count(n)` collapsed to 0 vs the correct
  value), even single-level. It also removes the nested-parallel hazard for the future
  park redesign.
- **C3** `ddfc95def` — the 5 parallel cursors on the seam via immediate-`PullCo` + the
  suppression scopes above. 0 `bool Pull` overrides remain (P3.4 can make `Pull`
  non-virtual).

### D3 — Performance gate before flipping the default (P3.3)

The coroutine default flip (P3.4) is gated on an explicit performance budget: **a couple
of percent overhead is acceptable; >5% is not justifiable.** See the Performance section.

## Verification (correctness)

- **Parity harness** `cursor_parity` (flag-OFF ≡ flag-ON) — green.
- **Forced-yield integration** `cursor_yield_real`, `cursor_yield_interpreter`,
  `cursor_yield_pool` (multi-worker, real coroutine machinery under external yield
  contention) — green; the multi-worker yield→reschedule→resume path is **TSan-clean**.
- **Parallel cursors:** flag-OFF parallel e2e 35/35 (byte-identical to master),
  including a new permanent regression class `TestSubqueryArmParallelism` guarding the C1
  data-loss fix; flag-ON parallel == serial corpus all-correct; flag-ON grouped/nested/
  order-by aggregations 320/320 runs at `--bolt-num-workers` 2 and 4 (a concurrency bug
  found and fixed during C3 — concurrent branches sharing a coroutine-handle slot).
- **Broad correctness sweep:** the full `gql_behave` suite run flag-ON vs flag-OFF
  (`continuous_integration --coroutine-cursors`). Result: **identical** across the whole
  in-memory corpus — `memgraph_V1` 1180/1180, `openCypher_M09` 778/891 (the gap is the
  known TCK delta, identical in both arms), `stackoverflow` 2/2, `unstable` 2/6; on-disk
  suites also pass flag-ON. This is the broad net behind "all serial cursors wired".
  (Harness note: the suite pins `behave==1.2.6`; the venv had drifted to 1.3.3 — reinstall
  1.2.6. And between back-to-back orchestrator runs, ensure port 7687 is free, or a stale
  server contaminates results.)

## Performance (P3.3) — open

### Measurement

Measured flag-ON vs flag-OFF on the **same** binary `ddfc95def` (flag-OFF = `PullLegacy`
= byte-identical to master). Single-threaded python-mgclient microbench, 100k-node graph,
median of 40 iterations × multiple reps, RelWithDebInfo (`-O2 -g -DNDEBUG`) + jemalloc.
Pull-intensive read queries (few result rows → server-side pull cost dominates):

| Query (pull-dominated)        | Overhead (flag-ON vs flag-OFF) |
|-------------------------------|--------------------------------|
| Expansion (1-hop, 2-hop)      | ~17–20%                        |
| Raw scan / filter / sum-agg   | ~10–13%                        |
| Grouped aggregation           | ~6%                            |
| Projection / order-by-limit   | ~2–4%                          |

Overhead scales with **pull density** — highest where the per-row body is tiny
(expansion emits one edge per pull) and lowest where per-row work dominates. The ~17–20%
on hops exceeds the 5% budget, so this is currently a **blocker for P3.4**.

### What was investigated and ruled out (locally)

The expectation (from the original-branch experience) is that long-lived coroutines +
symmetric transfer give ~0 overhead. Both properties are present in our implementation,
so the overhead must be elsewhere. Three hypotheses were prototyped and measured; **all
were ruled out as the dominant cause**:

1. **Per-row setup reconstruction.** Our `DoPull` bodies recreate
   `OOMExceptionEnabler` / `SCOPED_PROFILE_OP` / `frame_writer` / helper lambdas each
   result row (for master-per-`Pull` fidelity), whereas the original hoists them once per
   coroutine. Hoisting them out of the loop → **no change**. (`SCOPED_PROFILE_OP` is a
   no-op `optional`=nullopt when `!is_profile_query`, so it was cheap regardless.)
2. **The inner-loop yield-point suspension.** Replacing the per-pull
   `co_await YieldPointAwaitable` with a plain (non-suspending) `AbortCheck()` → **no
   change**.
3. **Coroutine-frame allocation.** A thread-local segregated freelist for coroutine
   frames (via `promise_type::operator new/delete`) → **marginal/noise** (expand +14.7%
   vs +17.4%; scan/fanout unchanged).

A **zero-code fanout experiment** isolated the components: a high-fan-out graph (100k
edges, ~1000 `InitEdges` calls) gives **+12.8%** vs the chain graph (100k edges, ~100k
`InitEdges` calls) at **+17–20%**. So overhead ≈ **~5–7% per-helper-call** (the one-shot
`InitEdgesCo`/`PullInputCo`-style coroutines allocate a frame per call; arch-independent,
inlinable) **+ ~12.8% per-result coroutine dispatch base** (`Aggregate co_await
PullChild(Expand)` → resume → `co_yield`, per row).

### Initial hypothesis (VM artifact) — RAISED, then REFUTED on bare metal

The original (nested-VM) reading was that the dominant ~12–15% is the **per-pull
coroutine resume/suspend dispatch** — an **indirect branch** (jump to the stored resume
point), with symmetric transfer adding more indirect jumps — and that this was **largely
an artifact of the measurement environment** (nested aarch64 VM on Apple Silicon, poor
indirect-branch prediction / no real BTB), hence near-free on bare metal. The VM had no
hardware PMU (`perf stat -e cycles,instructions` = `<not supported>`) so this could not be
confirmed there; a bare-metal box was required.

### Bare-metal resolution (P3.3, 2026-06-24) — overhead is REAL, not microarchitectural

Re-ran the A/B and hardware counters on a **bare-metal Intel i7-11800H (Tiger Lake),
x86_64, not virtualized**, `perf_event_paranoid=1`, `performance` governor, turbo off.
Microbench harness: `tests/mgbench/coroutine_perf/`. **The overhead did not collapse — it
reproduced, and on the pull-dense expansions it was as large or larger than on the VM**
(`expand_count` +20.4%, `expand2_count` +34.2%, `scan_count` +24.5%, fanout +27.0% vs the
VM's ~12.8%). A fixed cost growing to a *larger* percentage on the faster machine is the
opposite of what a misprediction artifact predicts.

Hardware counters settle it. A self-contained run (queries counted + `perf stat` over the
exact same window, single pinned core, `MATCH (n:N)-[:R]->(m) RETURN count(*)`):

| metric            | flag-OFF | flag-ON | per-query Δ |
|-------------------|----------|---------|-------------|
| queries / 15 s    | 321      | 270     | **throughput −18.9%** |
| instructions/query| 251.0 M  | 301.3 M | **+20.0%**  |
| cycles/query      | 108.4 M  | 128.5 M | **+18.5%**  |
| IPC               | 2.32     | 2.35    | ~flat       |
| branch-miss rate  | 0.07 %   | 0.06 %  | **flat (even lower)** |

Throughput, cycles/query and **instructions/query all move together by ~19–20%** while
**branch-misprediction stays flat at 0.06–0.07%** and IPC is unchanged. So:

- **The indirect-branch-mispredict / VM-artifact hypothesis is refuted.** The BTB predicts
  the coroutine resumes perfectly; misprediction is not the cost. It will *not* disappear
  on better hardware.
- **The overhead is pure extra work: ~20% more instructions retired per pull**, at
  unchanged IPC. The coroutine path simply *does more*.

Attribution from the flag-ON sampled profile (all entries below are **new** vs flag-OFF):
`InitEdgesCo [.resume]` **6.84%** (a one-shot helper coroutine, frame-allocated *per call*),
`Expand::DoPull [.resume]` **6.77%**, plus `Filter/ScanAll::DoPull [.resume]`, `PullCo`,
`PullAwaitable::ResumeAwaitable::await_resume`; and elevated allocator traffic
(`je_sdallocx` 6.75→8.18%, extra `malloc`/`mallocx`/`sdallocx`) from heap-allocated
coroutine frames. This is exactly the **per-helper-call allocation** component the original
investigation flagged as real and arch-independent — now confirmed as a *leading* cost, not
a footnote.

### Bottleneck attribution + fix (P3.3, 2026-06-24)

A call-frequency isolation (same 100k edges emitted, but vary how often the helper is called)
pinned the dominant cost exactly. Chain (1 edge/source → `InitEdgesCo` ~once/edge) showed **502
extra instr/edge**; fanout (100 edges/source → `InitEdgesCo` ~once/100 edges) showed **100 extra
instr/edge**. The difference attributes **~405 instr to each per-call `InitEdgesCo` coroutine
frame** (alloc + symmetric-transfer + state machine + destroy) = **~80% of the chain-expand
overhead**; the remaining ~92 instr/pull is the fundamental persistent-`gen_` resume floor.

**Fix landed (this branch):** inline `Expand::ExpandCursor::InitEdgesCo`'s body into the
persistent `DoPull` `gen_` — the child pull stays `co_await PullChild` (child's persistent
`gen_`, no allocation), removing the per-call helper frame. Verified by `cursor_parity`
(Corpus + MutationCorpus, flag-OFF ≡ flag-ON). Re-measured on the same bare-metal box (HW
counters, instr/query):

| query (pull-dominated) | before fix | helper inline | + lean await_resume |
|------------------------|-----------:|--------------:|--------------------:|
| expand_count (1-hop)   | +20.0%     | +10.0%        | **+8.5%**           |
| expand2_count (2-hop)  | +34.2%     | +13.7%        | (~12%)              |
| project / sum-agg      | +3–10%     | +0.3–3.5%     | ~same               |

Two landed reductions: (1) inline `InitEdgesCo`/`PullInputCo` (kill per-call helper frames), (2) a
safe lean of `ResumeAwaitable::await_resume` (drop a redundant `done()` branch). A full
microarchitectural study (`tests/mgbench/coroutine_perf/INVESTIGATION.md`, EXP-5…11) established the
residual root cause: a **per-cursor-boundary crossing costs ~60 instr (~65% L1-hot loads/stores)** —
the stackless resumability tax (frame state save/restore + await protocol), NOT misprediction
(branch-miss ~0%), NOT cache misses, NOT the yield check (free). It is linear in plan depth. Levers:
**fewer crossings** (a hybrid that confines coroutines to pipeline breakers + on-disk/blocking leaves,
recovering ~flag-OFF for the regular pipeline — no planner change, reuses the dual-path seam) or
**stackful fibers**; further framework leaning is load-bearing-bounded. Crucially, EXP-7 showed the
dispatch tax is **moot for the on-disk direction** (I/O-bound: +0.3–3.6% at NVMe/SSD latencies), so
the stackless design is already correct there.

The remaining ~10% on `count(*)`-over-expansion is the **structural per-pull resume floor**
(intrinsic to the per-cursor coroutine model); its impact is inversely proportional to per-row
real work, so it is near-zero on realistic queries (`sum_agg` +0.3%, `project` +3.5%) and only
bites the most pull-dense, least-work shapes.

Full investigation log: `tests/mgbench/coroutine_perf/INVESTIGATION.md`.

### Remaining reduction targets (gate still open for pull-dense expansion)

1. **Done — inline `InitEdgesCo`** (above). Follow-up cleanup: delete the now-dead `InitEdgesCo`
   (def + decl); inline `ExpandVariable::PullInputCo` too (same pattern, but called per *input
   vertex* not per row → low frequency / low payoff, for consistency).
2. **Structural per-pull resume floor (~92 instr/pull):** only reducible by cursor fusion /
   fewer suspension points (the original big-bang shape) — a separate, larger change. Needed
   only if pull-dense `count(*)`-over-expansion must also clear 5%; realistic queries already do.
3. A pooled/freelist coroutine-frame allocator is **no longer indicated** — with the per-call
   helper frame gone, the persistent `gen_` frames allocate once per cursor per query (negligible).

Tooling note: DWARF call-graph unwinding remains unusable for coroutines (the
symmetric-transfer chain collapses into a recursive `SessionHL::Pull`); flat self-time
sampling + `perf stat` counters are the reliable attribution path.

## Status of remaining work

- **P3.3** — performance gate: bottleneck found (per-call `InitEdgesCo` helper frame, ~80% of
  expand overhead) and **fixed by inlining → expand +20%→+10%, expand2 +34%→+14%** (parity
  preserved). Realistic queries are within budget; only pull-dense `count(*)`-over-expansion
  still exceeds 5%, due to the structural per-pull resume floor. Gate effectively **passes for
  realistic workloads**; clearing the pathological pull-dense case needs the structural change
  in target 2 above.
- **P3.4** — flip `COROUTINE_CURSORS` default ON, delete `PullLegacy`/router, make
  `Pull` non-virtual, remove the throwaway gql_behave coroutine arm. Gated on P3.3.
- **Follow-up PR** — Approach B (uniform-coordinator-park) parallel-cursor redesign, a
  throughput optimization with a pre-verified design/risk register.

## Risks and trade-offs

- **Performance** is the gating risk; see P3.3. The dual path itself disappears at P3.4,
  so any router/seam overhead is transitional.
- **Coroutine lifetime / teardown.** Suspended child frames are destroyed by the owning
  `Awaiter`/`PullAwaitable` (coroutine_handle::destroy is not recursive). The yield slot
  (`suspended_task_handle_ptr`) is a non-owning observer to avoid double-free.
- **Profiling fidelity.** `PROFILE` queries rely on per-`Pull` `SCOPED_PROFILE` scoping;
  hoisting it in a coroutine would change attribution across suspends. Any setup-hoisting
  optimization must preserve the `!is_profile_query` gating.
- **Concurrency.** The parallel path required the C0 atomic fix and, in C3, per-branch
  yield-suppression to give each concurrent branch its own handle slot. The full park
  redesign (Approach B) has its own HIGH-severity concurrency requirements (R1/R2)
  recorded for the follow-up PR.

## References

- Branch `pr/coroutine-cursors-p3-unify` (off master); commits C0 `2d8f63e40`, C1
  `8539b17dd`, C3 `ddfc95def`.
- Original prototype: branch `coroutine_cursors`.
- Coroutine machinery: `src/query/plan/cursor_awaitable_core.hpp`,
  `src/query/plan/cursor_awaitable.hpp`; cursors in `src/query/plan/operator.{hpp,cpp}`;
  parallel planner rewrite in `src/query/plan/rewrite/parallel_rewrite.hpp`.
- Tests: `tests/unit/cursor_parity.cpp`, `cursor_yield_real.cpp`,
  `cursor_yield_interpreter.cpp`, `cursor_yield_pool.cpp`;
  `tests/e2e/parallel/test_parallel_correctness.py` (incl. `TestSubqueryArmParallelism`);
  `tests/gql_behave` (`--coroutine-cursors` arm).
- Performance reproduction (P3.3): `tests/mgbench/coroutine_perf/` — `run_ab.sh` (flag-OFF
  vs flag-ON microbench A/B), `profile_perf.sh` (HW-counter profiling for the bare-metal
  box), `p33_microbench.py` (chain), `p33_fanout.py` (fan-out component isolation),
  `hammer.py`, and a README with the VM baseline numbers and the bare-metal decision
  criteria.
