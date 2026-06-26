# Coroutine cursors v2 — project state (handoff snapshot)

Single-page snapshot of where the coroutine-cursor work stands. Companion docs:
[`PERF_GATE.md`](./PERF_GATE.md) (gate plan), [`RESULTS.md`](./RESULTS.md) (bare-metal verdict),
[`FOLLOWUPS.md`](./FOLLOWUPS.md) (deferred work). Nothing below is pushed or merged to master.

## Branches

| Branch | Tip | Contents |
|--------|-----|----------|
| `pr/coroutine-cursors-v2-01-runtime` | `d228c3c02` | PR-1…14: the cursor conversion stack (dormant seam). |
| `perf/coroutine-cursors-v2-gate` | `d38530413` | off `d228c3c02`: perf-gate docs + bare-metal RESULTS, flip-default, broadened split set, observability metrics, follow-ups doc. |
| `feat/coroutine-parallel-park-approach-b` | (this work) | off the gate branch: approach-B parallel self-park scheduling. |

## What is DONE (and verified)

- **Dual-path seam (PR-1…14).** Every cursor keeps its synchronous `Pull()`; a virtual `PullCo()`
  (default `Immediate(Pull())`) lets a converted cursor run as a coroutine when its `mode()` is
  `Coro`. Per-cursor `CursorMode` selected at construction.
- **Split-point policy + knob.** `--query-coroutine-yield-ops` (default = blocking ops
  `Aggregate,OrderBy,Accumulate,Distinct,HashJoin`; `All`/`*` = whole plan; empty = kill switch).
  Coroutine region = root → lowest blocking operator; synchronous below. Default flipped ON.
- **Enterprise parallel cursors = approach A.** The 6 parallel cursors stay synchronous and ride the
  base `Immediate(Pull())`; `ExecuteBranchesInParallel` drives branch sub-cursors via plain `Pull()`
  and the coordinator **blocks** on `collection_scheduler_->WaitOrSteal()` (work-stealing). They must
  never get the `gen_` macro (shared producers can't share a coroutine frame).
- **Perf gate (bare metal).** SPLIT ≈ +1% instructions/query vs ALL ≈ +10%; parallel knob-invariant;
  pokec macro within noise. Green-lit the flip.
- **Observability.** Prometheus `query_coroutine_driven_total` / `query_sync_driven_total` /
  `coroutine_region_cursors_total` (÷driven = avg region size); PROFILE per-operator `[coro]` suffix
  + JSON `coro` field.
- **Tests (targeted, local).** `cursor_parity` 2/2 (Debug), `cursor_knob` 5/5, `cursor_seam` 5/5,
  `cursor_yield` 12/12, `query_profile` 2/2, `interpreter` 97/97.

## The yield protocol that EXISTS but is DORMANT

`YieldPointAwaitable` + `PullDriverScope` + `ResumePullStep`'s `Yielded` detection are all present
(`src/query/plan/cursor_awaitable.hpp`). But **nothing yields in production**:

- The production drive (`PullPlan::Pull`) runs `PullCo + ResumePullStep` with **no
  `PullDriverScope(Enabled)`**, so `ctx.suspended_task_handle_ptr` is nullptr and
  `YieldPointAwaitable::await_ready` always short-circuits to "don't suspend".
- Nothing sets `ctx.stopping_context.yield_requested` — there is **no `WorkerYieldRegistry`** on this
  branch (only referenced in comments) and **no park/unpark primitive** in `PriorityThreadPool`.

Consequence: coroutine pull is ON by default but only *capable* of yielding — it costs ~1% for no
functional gain yet. The kill switch (empty knob) reverts to master-identical behaviour.

## What is LEFT

1. **Approach B — parallel self-park (this branch).** Make the parallel coordinator park its worker
   instead of blocking in `WaitOrSteal`, so the pool thread is freed while branches run. This is the
   first real use of the yield/park machinery. Design: [`APPROACH_B.md`](./APPROACH_B.md).
2. **Re-validate the broadened split default** on the perf box (FOLLOWUPS §2).
3. **`EvaluatePatternFilter` (EXISTS)** coroutine wiring — still a synchronous island.
4. **Query split HINT** — designed, deferred (FOLLOWUPS §1).
5. **Ship path** — full unit + e2e + **TSan** sweep, PR review, merge to master. Not started.

## NOT a todo

- **Deleting the dual path** is retracted: the dual path *is* the permanent split mechanism (sync
  below the split, coro above). Forcing all-coro = the measured +10% regression.
