# Coroutine Cursors — ground-up implementation plan (PR stack)

**Status** — PLAN (2026-06-24). Supersedes the 3-branch migration stack (`pr/coroutine-cursors-p3-*`).
Design: [`008_coroutine_cursors.md`](008_coroutine_cursors.md) + its Addendum. Perf learnings:
[`tests/mgbench/coroutine_perf/INVESTIGATION.md`](../tests/mgbench/coroutine_perf/INVESTIGATION.md) (EXP-0…13).

## Why restart

The existing stack is scaffolded around a **migration**: a `COROUTINE_CURSORS` experimental flag, a
`use_coroutine_` router in `Cursor::Pull`, "`PullLegacy` byte-identical to master", parity as a
migration gate, and a P3.4 step that flips the default and *deletes* `PullLegacy`. The end-state we
settled on keeps **both** pull paths permanently with a **per-cursor runtime mode** — which dismantles
all of that scaffolding. Building the end-state directly is cleaner than stacking more PRs on a frame
we will tear down.

## Principles (constraints on the stack)

1. **Default = all-Sync == master.** Every cursor is constructed in `Sync` mode by default and runs its
   ordinary synchronous `Pull()` — byte-for-byte master behavior, zero coroutine tax, zero risk.
   Coroutine mode is **opt-in per cursor** via a runtime knob / `MakeCursor` selection.
2. **No experimental flag, no router-on-flag.** Mode is a per-cursor property set at construction.
3. **Every PR is a logical unit < 500 lines, non-breaking, independently useful, and ignorable.** Because
   the default is all-Sync, the whole coroutine layer lands as *dormant* code that changes nothing until
   the knob enables it. If the stack stops at any PR, the engine still behaves as master.
4. **Reuse the parity-proven assets**; rewrite only the framing.

## Inventory — what we keep / fixed / change / build new

### Already good — SALVAGE as-is
- **Coroutine runtime headers** `src/query/plan/cursor_awaitable_core.hpp`, `cursor_awaitable.hpp`:
  awaitables, symmetric transfer, persistent-`gen_`, immediate-mode `ResumeAwaitable`, `ResumePullStep`,
  `PullDriverScope`, `YieldPointAwaitable`. Sound, mode-agnostic.
- **The 50 `DoPull` coroutine bodies** in `operator.cpp` — parity-proven; become the coroutine
  instantiation of each cursor (and later the input to single-source).
- **Parity harness** `tests/unit/cursor_parity.cpp` — repurposed: validates each cursor's Coro body ≡ its
  Sync body (run with coro forced on), so enabling the knob is provably safe.
- **Phase-2 scheduling primitives** `WorkerYieldRegistry`, the uniform priority pool, task states — reused
  by the scheduler/waker layer.
- **Parallel execution model** (Approach A) — works today; evolved to `BranchJoin` park later.
- **Perf harness** `tests/mgbench/coroutine_perf/` (`cursor_models.cpp`, `run_ab.sh`, …) — A/B any
  regular/coroutine mix and any kernel change on the perf box.

### Fixed / improved during the P3.3 perf debugging — KEEP
- **Inlined `Expand::InitEdgesCo` + `ExpandVariable::PullInputCo`** (killed per-call helper frames): chain
  expand +20.0% → +10.0% (the real bug; ~80% of the overhead). `05171a8ce`, `34d832698`.
- **Lean `ResumeAwaitable::await_resume`** (dropped a redundant `done()` branch): +10.0% → +8.5%,
  parity-green. `27985429c`.
- Removed dead `produced` vars in Expand/ExpandVariable `DoPull`.
- **The entire perf model + decision** (EXP-0…13, the ADR addendum): per-crossing cost is ~60 L1-hot
  loads/stores (resumability tax), linear in depth, moot on-disk; fused beats legacy; hybrid feasible on
  one body/cursor. This is the design justification — keep it referenced.

### Needs complete change — the FRAMING
- Delete `COROUTINE_CURSORS` flag, the `use_coroutine_` router in `Cursor::Pull`, the `gql_behave`
  flag arm, the "P3.4 flip + delete PullLegacy" step, and the "PullLegacy == master twin" narrative.
- `Cursor::Pull` becomes: Sync mode → run the cursor's sync body; Coro mode → drive `gen_` one step.
- Cursor SELECTION: was a global flag → becomes **bottom-up at `MakeCursor`** + a runtime knob.

### New — did not exist
- Per-cursor `CursorMode` + bottom-up selection (`coro = needs_midpull_yield(op,storage) || any_child_coro`).
- Runtime knob to bias/force mode (default all-Sync).
- Suspension family beyond `Yield`: `IoWait` (on-disk), `BranchJoin` (parallel coordinator park).
- Scheduler waker integration (park off-queue, wake re-queues).
- Parallel coordinator **park** (Approach B) replacing Approach A blocking.
- (Optional, later) single-source body generation (macro/`.inc` or codegen) to collapse the two bodies.

## Independent bug fixes — land FIRST, off master, regardless of this effort

These are not coroutine-coupled; they fix real issues in code that ships today.
- **PR-0a — `CollectionScheduler::pool_` atomic** (TSan data race). Salvage `2d8f63e40`. ~30 lines.
- **PR-0b — forbid parallel sections inside subquery arms** + `Union` in `ConflictingOperators`
  (fixes silent data loss in parallel `Aggregate`/`OrderBy` under `Apply`/`CALL{}`). Salvage `8539b17dd`
  + its regression test `TestSubqueryArmParallelism`. ~150 lines.

## The PR stack (each < 500 lines, non-breaking under all-Sync default)

**Foundation**
- **PR-1 — Coroutine pull runtime (dormant).** Land `cursor_awaitable_core.hpp` + `cursor_awaitable.hpp`
  (with the lean `await_resume`) and isolated unit tests for the awaitable/driver mechanics. Not wired into
  any cursor. *Useful:* the tested runtime library. *Non-breaking:* unused. ~450 lines incl. tests.
- **PR-2 — `Cursor` base: per-cursor mode seam, default Sync.** Add `enum class CursorMode {Sync,Coro}`
  `mode_{Sync}`, the `gen_` member, `Reset()`, and `PullCo()` default = `Immediate(Pull())`; `Pull()` for
  Coro mode drives `gen_` via `ResumePullStep`. NO flag, NO router-on-flag. With every cursor Sync, the whole
  tree runs synchronously == master. *Useful:* the architectural seam; makes the runtime reachable.
  *Non-breaking:* all-Sync. ~250 lines.

**Per-cursor coroutine bodies (salvage `DoPull`, grouped; each group parity-checked with coro forced ON)**
Each PR adds `DoPull` (+ `PullCo` override) for a group; cursors stay Sync by default; the group's coro body
is validated ≡ its sync body by `cursor_parity`. *Useful:* those cursors become coro-capable. *Non-breaking:*
dormant. Suggested grouping to stay < 500 lines:
- **PR-3 — leaf/simple streaming:** Once, Produce, Filter, Limit, Skip, EvaluatePatternFilter.
- **PR-4 — scan/expand:** ScanAll(+ByEdge), Expand (helper-inlined), ExpandVariable (helper-inlined),
  EdgeUniquenessFilter, ConstructNamedPath.
- **PR-5 — writers:** CreateNode, CreateExpand, Set*/Remove* family, Delete.
- **PR-6 — multi-child:** Apply, Merge, Optional, Union, IndexedJoin, Foreach, RollUpApply.
- **PR-7 — breakers (coroutine-only twins):** Aggregate, OrderBy, Accumulate, Cartesian, HashJoin,
  EmptyResult, Distinct, Unwind, OutputTable(+Stream). (These need the mid-drain `co_await`; keep their
  helper-coroutine where it runs once/query.)
- **PR-8 — shortest paths + misc:** STShortestPath, SingleSource, WeightedSP, AllSP (structural — keep its
  helper coroutine), KShortestPaths, LoadCsv/Parquet/Jsonl, CallProcedure, PeriodicCommit/Subquery.

**Enablement**
- **PR-9 — mode selection + knob.** `MakeCursor` computes `mode_` bottom-up
  (`needs_midpull_yield(op,storage) || any_child_coro`); a runtime knob (defaults to all-Sync). *Useful:*
  coro can now be enabled per-cursor for tests/features. *Non-breaking:* knob defaults off.
- **PR-10 — Yield + scheduler integration.** Wire `YieldPointAwaitable` + `WorkerYieldRegistry` + the pool
  so a Coro cursor can cooperatively yield for fairness; the driver parks the task off-queue and the
  scheduler re-queues it. *Useful:* fairness preemption for long queries (when coro on). Gated.
- **PR-11 — `BranchJoin` park + parallel coordinator (Approach B).** Salvage the parallel cursors; the
  coordinator `co_await`-parks on branch completion instead of blocking, freeing its worker. *Useful:*
  parallel throughput. Carries the R1/R2 concurrency risk register from the ADR.
- **PR-12 (future) — `IoWait`** for on-disk: leaf parks on a cold-page read, woken by I/O completion.
  Lands with the on-disk scope.

**Optional refactor (defer; reduces maintenance, not behavior)**
- **PR-S* — single-source bodies.** Convert cursor groups from two hand-bodies to one source + boilerplate
  (macro/`.inc` or codegen; validated in `single_source_poc.cpp`). Each PR converts a group, parity-proven,
  net-negative lines. Sequenced after the stack is functional; mechanism (macro vs codegen) decided then.

## Decisions still open
- **Single-source mechanism & timing:** salvage the two parity-proven bodies now; convert to single-source
  later (PR-S*). Macro/`.inc` vs codegen decided when we get there (lean toward a small codegen if the macro
  form hurts readability on the hot path).
- **Knob shape:** global default (Sync) + per-operator/per-storage override; final surface TBD in PR-9.

## Net
Land PR-0a/0b immediately (independent fixes). Then PR-1…PR-9 deliver a complete, dormant, all-Sync hybrid
that is identical to master in behavior but fully coro-capable per cursor; PR-10/11 light up fairness-yield
and parallel-park; PR-12 lights up on-disk. Each PR is reviewable (< 500 lines), useful on its own, and
ignorable — stop anywhere and the engine is still master.
