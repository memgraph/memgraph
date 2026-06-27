# Cursor coro-port audit + yield-wiring open decisions (pick up here)

Snapshot of the 2026-06-27 audit and the decisions to settle before continuing the scheduler work.
Branch: `feat/coroutine-scheduler-redesign` (tip after S1 = `93373f441`). Nothing pushed.

## 1. Cursor coro-port audit — COMPLETE and EQUAL

Method: ran `cursor_parity` under all-coro (forces every cursor Coro) with a `COROHIT` typeid probe in
the `MG_COROUTINE_CURSOR_PULLCO` macro (ungate-in-RelWithDebInfo recipe), at the current branch tip.

Result: **parity 2/2**; **45 distinct cursor `DoPull` bodies fire and are parity-equal**. Every serial
cursor is converted (verified each class has `macro + DoPull`, including all 5 shortest-paths and
CallProcedure — an earlier crude 150-line window scan gave false negatives on those huge classes).

Cursors NOT firing under the corpus — all expected, none a surprise:

| Cursor | Why not fired | Equal? |
|---|---|---|
| OutputTable / OutputTableStream | mgp-procedure outputs, not reachable in the read faker | equal by construction (converted, twin of verified) |
| LoadParquet | needs a binary fixture | equal by construction (twin of LoadCsv/Jsonl, which ARE verified) |
| PeriodicCommit / PeriodicSubquery | IN TRANSACTIONS / managed mode, not runnable in the faker | equal by construction |
| EvaluatePatternFilter | structural sync island — see §2 | equal (its Sync `Pull` runs in both modes) |
| 6 parallel cursors | intentionally `Immediate(Pull())` (approach-A, PR-14) | equal by base default |

**Conclusion: the coro port is complete. Every cursor is equal between coro and regular** — 45 proven
by the harness, the rest equal by construction / by base default. No overlooked cursor.

## 2. EvaluatePatternFilter (the EXISTS island) — recommend LEAVE as intentional Sync

It has a real `DoPull`, but `FilterCursor` pulls its EXISTS sub-plan **synchronously** from inside
expression evaluation: the cursor writes a `std::function<void(TypedValue*)>` into the frame; when the
`exists(...)` predicate is evaluated, that callback runs `input_cursor->Pull()` and must return a bool
right there. So the EXISTS subtree always runs Sync, regardless of policy.

**Recommendation: do NOT convert it.** To make it coro you'd drive the sub-pull via `PullCo` under a
**Suppressed** scope (it cannot yield — it's mid-expression-eval and must return synchronously). That
is coro-for-completeness with per-boundary cost and **zero benefit** (no yield possible). It is exactly
a bounded "sync region below an effective split point" — consistent with the split philosophy, not a
violation. It IS equal between modes (the Sync `Pull` runs in both). Action when resumed: add a
one-line code comment marking it an intentional sync island (and drop it from the "remaining gaps"
list). Only convert if literal 100% coro coverage is required, accepting the overhead.

## 3. Yield wiring — open decisions (the thing to discuss before coding)

S1 already wired the **drive** side (committed `93373f441`): `PullDriverScope(Enabled)` + the `Yielded`
re-resume loop are live in `PullPlan::Pull`. Production is unchanged because nothing sets
`yield_requested`. Two pieces remain to make yield *do something*, neither needing the thread layout to
change (HP pool + monitor stay):

**Decision A — the trigger (who sets `yield_requested`):**
- (A1) Reuse the existing 100 ms `sched_mon` monitor: set `yield_requested` on a worker whose running
  coro task has work queued behind it (demand-based). Minimal; threads untouched.
- (A2) A simple throttled/always-on yield for bring-up (every Nth checkpoint), then refine. Easier to
  test, less "smart".

**Decision B — park-on-yield (what the drive does on `Yielded`):**
- (B-i) Re-resume immediately on the same thread (current S1 behavior). Safe, but yield then frees no
  worker — **functionally pointless**. Only useful as a "connected but inert" placeholder.
- (B-ii) **Actually park** the task: return the worker to the pool, resume the continuation later. This
  is the real throughput win and the only version where yield has value — but it's the genuinely
  concurrency-critical primitive (carry the verified approach-B constraints: R1 2-phase `in_flight`
  resume gate, R2 single-waiter, atomic `pool_`). Needs TSan from the start. See `APPROACH_B.md` +
  `SCHEDULER_REDESIGN.md` S2.

**Proposed close-out (pending the A/B choice):** mark EvaluatePatternFilter intentional-sync (tiny
commit), then implement **B-ii minimal park-on-yield** keyed to the existing pool with the **A1**
monitor trigger — HP threads and the monitor stay in place. Smallest change that makes yield real;
TSan-gated. If a placeholder is preferred first, do **B-i + A2** to connect the path, defer the park.

## 4. Branch state for pickup
- `feat/coroutine-scheduler-redesign` @ `93373f441` (S1 done: yield fires end-to-end on the serial
  drive, proven non-vacuous — 11 yields, results identical).
- Stacked under it: `feat/coroutine-parallel-park-approach-b` (APPROACH_B.md), `perf/...-gate`
  (flip + broaden + observability + perf RESULTS), `pr/...-01-runtime` (PR-1…14). Nothing pushed.
- Next concrete step once A/B is chosen: S2 park primitive (the one new concurrency primitive).
