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

### Conclusion and hypothesis

The dominant ~12–15% is the **per-pull coroutine resume/suspend dispatch itself** — not
setup, not the yield point, not allocation. A coroutine resume is an **indirect branch**
(jump to the stored resume point) and symmetric transfer adds more indirect jumps. The
strong hypothesis is that this is **largely an artifact of the measurement environment**:
the box is a nested aarch64 VM (Lima on Apple Silicon) with poor indirect-branch
prediction (no real BTB), so each resume mispredicts — a cost that is near-free on bare
metal. This would reconcile the original-branch ~0 (real hardware) with the local ~12–15%
(nested VM).

### Tooling limitation and next step

Hardware PMU counters are **unavailable** on this VM (`perf stat -e cycles,instructions`
= `<not supported>`); only time-based software sampling works (after lowering
`perf_event_paranoid`), and DWARF call-graph unwinding is unusable for coroutines (the
symmetric-transfer chain collapses into a recursive `SessionHL::Pull`). The local
environment is therefore exhausted for attribution.

**Next: a bare-metal Linux box** to (1) measure the *true* production overhead and (2) use
hardware counters (`branch-misses`, IPC, `cycles`) on flag-ON vs flag-OFF expand/scan to
confirm whether the per-result dispatch base is indirect-branch mispredict (VM artifact →
P3.4 default-flip is fine) or real (then target per-pull dispatch). Independently, the
~5–7% helper-allocation component is real and worth eliminating by inlining the one-shot
helper coroutines into the persistent generator body, regardless of the box.

## Status of remaining work

- **P3.3** — performance gate: open, pending bare-metal measurement (above).
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
