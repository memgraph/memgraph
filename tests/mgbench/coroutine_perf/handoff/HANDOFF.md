# Coroutine Scheduler Redesign — Parallel Uniform Branches: Handoff

**Branch:** `handoff/coroutine-scheduler-perf-gate` (forked from `feat/coroutine-scheduler-redesign` @ `8a9e25c40`).
**Status:** core work **done and verified on a dev box**; the remaining gate (**c3.4**) needs a
**faster / bare-metal, PMU-capable machine** — this is the reason for the handoff.
**Nothing is pushed.** All work is local commits on this branch.

> This document is the single coherent entry point. Background design docs live next to it in
> `tests/mgbench/coroutine_perf/` — read them if you want the full "why":
> - [`SCHEDULER_REDESIGN.md`](../SCHEDULER_REDESIGN.md) — why redesign the scheduler around coroutines.
> - [`APPROACH_B.md`](../APPROACH_B.md) — the self-park ("approach B") scheduling model and its
>   throughput hypothesis (relevant to the c3.4 throughput gate below).
> - [`C3_PARALLEL_PARK_DESIGN.md`](../C3_PARALLEL_PARK_DESIGN.md) — the c3 park design + the
>   verification-cleared revisions that shaped the implementation.
> - [`PERF_GATE.md`](../PERF_GATE.md) / [`STATE.md`](../STATE.md) / [`RESULTS.md`](../RESULTS.md) —
>   the **coroutine-cursors-v2** perf gate. That is a *related but separate* effort (it gates the
>   per-cursor coroutine conversion). **This** handoff gates the *parallel scheduler* redesign.

---

## 1. What this branch delivers (the goal)

Enterprise parallel query execution (`USING PARALLEL EXECUTION N ...`) used to run **branch-0 inline**
on the coordinator thread while the coordinator **blocked** in `WaitOrSteal` (work-stealing). That
pinned one branch to the coordinator's worker and kept the coordinator's worker busy for the whole
query.

This branch makes parallel execution **uniform and scheduler-driven**:

- **All N branches are equal pool tasks.** There is no privileged inline branch-0.
- **The coordinator parks** (`co_await WaitForProgressAwaitable`) after scheduling, which **frees its
  worker** back to the pool while branches run. The scheduler decides what executes when.
- Branch completion re-wakes the coordinator (pinned) via `NotifyProgress`; it merges after a
  two-phase barrier.

This is the "point 7" end state: *uniform parallel branches, scheduler decides execution.*

---

## 2. Commit stack (on top of prior scheduler work)

| Commit | Stage | What | Files |
|--------|-------|------|-------|
| `a573e612a` | **c3.1** | Coordinator coro **park** (event-kind). Coordinator `co_await`s `WaitForProgressAwaitable`, frees its worker; two-phase resume barrier `Finished() && InFlightZero()`. | `operator.cpp` |
| `0ad83aa93` | **c3.2a** | Pool **exactly-once `RUNNING` state** (the enabler for uniform branches). | `priority_thread_pool.{hpp,cpp}` |
| `8a9e25c40` | **c3.2b** | **Uniform branches**: all N as pool tasks in `ScheduleBranchesAndRunMain`; inline branch-0 removed. | `operator.cpp` |

Ancestors already on `feat/coroutine-scheduler-redesign` (context, not part of this handoff's changes):
`c3.0` `ad025181f` (EventParked propagation primitive), `c3.R6` `d50ba310b` (planner nested-parallel guard).

Total delta of the three files vs c3.1 base is small: ~55/49/5 lines. It is a **surgical scheduler
change**, not a rewrite; the parallel-cursor *merge* logic is unchanged.

---

## 3. Architecture after this branch (how a parallel query runs)

The two enterprise parallel coordinators are `AggregateParallelCursor` and `OrderByParallelCursor`
(both subclasses of `ParallelBranchCursor`, in `src/query/plan/operator.cpp`). They share one
scheduling function, `ParallelBranchCursor::ScheduleBranchesAndRunMain`.

1. **Build N branch cursors** — each an independent sub-cursor tree (e.g. `ScanAll → Aggregate`),
   all allocated from the *same* query memory resource. For a parallel query that resource is a
   **thread-safe** pool (`QueryExecution::CreateThreadSafe` → `PoolResource<ThreadSafePool>` over
   `ThreadSafeMonotonicBufferResource`), so concurrent branch allocation is safe.
2. **Schedule all N as pool tasks** — `ScheduleBranchesAndRunMain` builds a `TaskCollection` of N
   tasks (loop `i = 0 … N-1`), `SetCollection` + `SetPool`, then (coro path) `Trigger()` dispatches
   every task to the pool. Each branch task captures `context` **by value** (its own copy) and uses a
   task-local `Frame`. Per-branch accumulator counters are reset so `UnifyContexts` sums only each
   branch's delta into the coordinator's baseline.
3. **Coordinator parks** — the coordinator's `DoPull` coroutine does:
   ```cpp
   while (!(collection_scheduler_->Finished() && collection_scheduler_->InFlightZero()))
     co_await collection_scheduler_->WaitForProgressAwaitable(&context.event_parked);
   ```
   `WaitForProgressAwaitable::await_suspend` → `RegisterProgressWaiter` registers the coordinator's
   resume closure and **suspends the coroutine, freeing the worker**. (The sync/kill-switch path
   instead calls `WaitOrSteal`, which steals branch tasks inline on the coordinator thread.)
4. **Branch finishes → wakes coordinator** — a branch task, on completion, stores `FINISHED` and calls
   `NotifyProgress`, which re-enqueues the coordinator's resume closure **pinned** to a worker. The
   coordinator resumes, re-checks the barrier, and re-parks if branches remain.
5. **Two-phase barrier** — the coordinator only proceeds to merge when `Finished()` (all tasks
   `FINISHED`, acquire-loads that synchronize-with each branch's `FINISHED` release store, which
   happens-after that branch's map writes) **AND** `InFlightZero()` (no worker still physically inside
   `WrapTask`, so none can touch `progress_cv_`/collection after teardown).
6. **Merge (unchanged)** — `AggregateParallelCursor` uses the mutex-guarded tournament merge;
   `OrderByParallelCursor` builds a min-heap over each branch's sorted cache. Both run **after** the
   join. These were *not* the source of any bug and were left byte-identical.

### Pool task state machine (the c3.2a fix)

`TaskCollection::Task::State` in `src/utils/priority_thread_pool.{hpp,cpp}`:

```
IDLE ──claim──▶ RUNNING ──yield──▶ SCHEDULED ──resume──▶ RUNNING ──done──▶ FINISHED
                  │  └─park (RegisterProgressWaiter stores PARKED)─▶ PARKED ──event resume──▶ RUNNING
                  └─ (concurrent duplicate dispatch sees RUNNING → SKIP)   STOLEN = claimed by WaitOrSteal
```

`WrapTask` claims exclusively: `IDLE→RUNNING`, or `SCHEDULED→RUNNING` (resume-after-yield, which is
**sequential** — the prior `WrapTask` returned before the reschedule re-enqueued the closure), or
`PARKED→RUNNING` (event resume). **A duplicate dispatch that observes `RUNNING`/`STOLEN`/`FINISHED`
skips** — this is the exactly-once guard.

---

## 4. The hard bug that c3.2a fixes (context for reviewers)

Before c3.2a, `SCHEDULED` was **overloaded** — it meant both "actively running" *and* "suspended
between yield-resume cycles." `WrapTask` claimed `IDLE→SCHEDULED`, so a **concurrent duplicate
dispatch** of the same branch task closure observed `SCHEDULED` and fell through the
"resume-after-yield" branch — **running the task body a second time on another worker**. Two workers
then executed the *same* branch cursor's `Pull`, mutating one `aggregation_` hash map concurrently →
heap corruption → `SEGV` in the post-join merge.

- This only surfaced with the **uniform** pattern: the coordinator parks *immediately*, freeing its
  worker into the same pool that runs the branches, which opens the duplicate-dispatch window.
  Branch-0-inline (the old model) kept the coordinator busy and masked it.
- **Heisenbug warning for whoever debugs this area:** heavy `fflush` logging on the pool dispatch hot
  path *serializes workers enough to close the race window* — the crash disappears under logging and
  reappears in a clean build. Any instrumentation here must be validated against un-instrumented
  behaviour. The bug was ultimately pinned by observing the *same branch cursor* logging completion
  from *two different thread-ids*.

---

## 5. Verification already done (on the dev box)

| Gate | Result |
|------|--------|
| ASan parallel hammer (`aggregate` / grouped / `ORDER BY`, `USING PARALLEL EXECUTION 4`) | **~930 queries, 0 crashes, 0 ASan reports, results == serial.** Includes a 600-run loop of the exact query that previously crashed by iteration ~4. |
| Unit regressions (ASan) | `utils_priority_thread_pool` **41/41** (pool state machine), `cursor_yield` **12/12** (serial-yield resume path), `cursor_knob` **5/5**, `cursor_coro_driver` clean. |
| **TSan — pool state machine** | `utils_priority_thread_pool` **41/41, 0 data races**. This is the authoritative concurrency test for the `RUNNING` change (it exercises concurrent claim / resume / park / steal / exactly-once-skip with real pool threads). |
| Shutdown / disconnect-while-parked (c3.3) | Abort-mid-parked-query verified clean: 5/5 `TERMINATE`-mid-flight → clean unwind, 0 hang, 0 crash, 0 ASan reports, server healthy after each. See §7. |
| **TSan — end-to-end query hammer** | ⏳ **environment-blocked on the dev box** (see §6). |
| **Throughput-under-concurrency** | ⏳ **needs the perf box** (see §6). |

Build was clean (no diagnostic instrumentation) for all of the above.

---

## 6. Remaining work — **c3.4** (this is the perf-box job)

Two gates could not be completed on the dev box. Both are in [`c34-perf-box-runbook.md`](./c34-perf-box-runbook.md)
with exact commands; summary and **pass/fail criteria** here.

### 6.1 Full end-to-end TSan campaign
- **Why the perf box:** `memgraph` built under TSan on the dev box cannot complete a Bolt handshake
  within a 45–180 s client deadline (TSan instrumentation overhead on the whole server, on a
  constrained/virtualized box). Every partial run that *did* connect showed **0 races / 0 crashes**,
  and the **pool state-machine TSan test is already clean (41/41)** — so this is a *confirmation*
  gate on realistic query load, not an open question.
- **What to run:** build memgraph + `utils_priority_thread_pool` under TSan, then drive the parallel
  hammer (aggregate / grouped / order-by / hops-bearing expansion) under TSan with **`num_threads` =
  2 and 4** and with **several concurrent clients**. See runbook §2.
- **Pass criteria:** 0 `WARNING: ThreadSanitizer` across the campaign; results still == serial.

### 6.2 Throughput-under-concurrency gate (the real payoff measurement)
- **Hypothesis (from [`APPROACH_B.md`](../APPROACH_B.md)):** because the coordinator now **parks and
  frees its worker** instead of blocking in `WaitOrSteal`, aggregate throughput should **improve (or at
  least not regress)** when *many* parallel queries run concurrently (workers are shared better),
  **with no single-query latency regression**.
- **A/B:** compare the **uniform-park** build (this branch) against the **blocking baseline**. The
  baseline is reachable *in the same binary* via the kill-switch knob that forces the synchronous
  `WaitOrSteal` path — see the runbook for exactly which knob and value. (No separate build needed.)
- **Measure:**
  1. **Single-query latency** for a representative parallel query (agg / order-by / hops expansion),
     park vs blocking — **must not regress** beyond noise.
  2. **Throughput under concurrency**: fixed pool size, ramp concurrent parallel-query clients
     (e.g. 1, 2, 4, 8, 16), measure completed-queries/sec, park vs blocking — **park should win or tie**.
  3. PMU counters (instructions/query, IPC) for the same runs — **needs bare metal** (PMU unavailable
     in the dev VM).
- **Decision:** if park holds single-query latency and improves/ties throughput-under-concurrency,
  the redesign is justified end-to-end. Record the numbers in a `RESULTS`-style doc (mirror the
  existing [`RESULTS.md`](../RESULTS.md) format).
- **Caveat carried from approach-B analysis:** self-park preempts no better than root-only for heavy
  work *below* the split point; revisit split-point placement if the throughput numbers disappoint.

### 6.3 (Optional) shutdown-while-parked CI regression test
A CI test for abort-mid-parallel-query (the behaviour verified in §7). It is timing-sensitive but
reliable because a parallel coordinator stays parked for the whole query window, so a `TERMINATE`
issued from a second connection deterministically hits the parked state. Skeleton in the runbook.

---

## 7. c3.3 finding: shutdown/disconnect-while-parked needs **no code fix**

Investigated and empirically verified (not just reasoned):

- A parked coordinator + `TERMINATE TRANSACTIONS` mid-flight → each branch observes the abort in its
  per-row `AbortCheck` (`MustAbort()` sees `TERMINATED`) → finishes → `NotifyProgress` → coordinator
  wakes → `FinalizeBranches` rethrows → **clean unwind.** 5/5 trials: 0 hang, 0 crash, 0 ASan report,
  server healthy, post-abort parallel queries work.
- `RegisterProgressWaiter` already **refuses to park into a shutting-down pool** (returns `false`, the
  caller busy-spins and observes shutdown).
- Graceful process shutdown (SIGTERM) aborts in-flight queries **first** (same abort path) before
  stopping the pool.
- **Only theoretical residual:** an *abrupt* pool stop with branch tasks still **queued-not-started**
  (the worker loop abandons queued tasks when `run_` goes false). This is a benign process-exit leak
  that graceful shutdown avoids. Not worth a fix; noted for completeness.

Conclusion: the "NotifyAll cancellation" originally sketched for c3.3 is **not required**.

---

## 8. Building (perf box)

Standard Memgraph toolchain build. Key gotchas (learned the hard way — see `../STATE.md` too):

- **Activate the toolchain first**, every shell: `source /opt/toolchain-v7/activate` (adjust the
  version to whatever the repo pins). Without it, the conan dependency builds fail confusingly with
  `clang: No such file or directory` even though the toolchain clang exists.
- **ASan:** `./build.sh --build-type RelWithDebInfo -DASAN=ON --reserve-cores <N> --skip-os-deps --keep-build --target memgraph`.
  `-DASAN=ON` goes through `build.sh` so the conan deps are rebuilt under the `add_asan` host profile
  (ABI-consistent). **Do not** `cmake -DASAN=ON` on an existing non-ASan build dir — you get abseil
  startup SEGV / `__tsan` link errors from ABI mismatch. Build cleanly when changing the sanitizer.
- **TSan:** same pattern with the TSan profile / a dedicated TSan build dir (deps built under the
  TSan host profile). Do not mix ASan and TSan in one build dir.
- **Unit tests:** they need a temp dir — `mkdir -p build/tmp` and run with `TMPDIR=build/tmp
  ./build/tests/unit/<target>`. A fresh reconfigure drops `build/tmp`.
- **Python venv for the driver-based tests:** `source tests/ve3/bin/activate` (neo4j driver).
- **Shell footgun in the helper scripts:** never `pkill -f "build/memgraph --data-directory"` *inline*
  in the same shell that also contains that string — it self-matches and kills your shell. The
  provided scripts run `pkill` from inside a script file (whose own command line is just
  `bash script.sh`) to avoid this.

---

## 9. Running the gates (scripts in [`scripts/`](./scripts/))

All scripts read the enterprise license from the environment (see §10) and take the bolt port /
data dir as arguments — no hardcoded paths or secrets.

| Script | Purpose |
|--------|---------|
| [`scripts/run_asan_hammer.sh`](./scripts/run_asan_hammer.sh) | Re-verify the double-execution fix: launch an ASan build, run the parallel hammer, report crashes / ASan reports / parity. |
| [`scripts/parallel_hammer.py`](./scripts/parallel_hammer.py) | The query hammer itself (sequential + concurrent `USING PARALLEL EXECUTION` agg/grouped/order-by; checks results == serial ground truth). |
| [`scripts/abort_mid_park_test.py`](./scripts/abort_mid_park_test.py) | c3.3 verification: `TERMINATE` a parallel query mid-park, assert clean unwind + healthy server. |
| [`scripts/run_tsan_gate.sh`](./scripts/run_tsan_gate.sh) | c3.4 §6.1: run the pool unit test + the parallel hammer under a TSan build; report data races. |
| [`scripts/throughput_gate.py`](./scripts/throughput_gate.py) | c3.4 §6.2 skeleton: A/B single-query latency + throughput-under-concurrency, park vs blocking (kill-switch) knob. |

---

## 10. Secrets / environment needed on the perf box (NOT included here)

- `MEMGRAPH_ORGANIZATION_NAME` and `MEMGRAPH_ENTERPRISE_LICENSE` — **required**: `USING PARALLEL
  EXECUTION` is an enterprise feature; without a valid license the parallel plan is not produced and
  the gates test nothing. These are **intentionally not in this branch**. Obtain them from the team
  and `export` both before launching `memgraph`. The scripts fail fast with a clear message if unset.
- No other secrets, keys, passwords, or personal data are needed or included.

---

## 11. Git state / how to pick up

- Branch `handoff/coroutine-scheduler-perf-gate` @ `8a9e25c40` + these handoff docs (this commit).
- Base line: `… → c3.0 ad025181f → c3.R6 d50ba310b → c3.1 a573e612a → c3.2a 0ad83aa93 → c3.2b 8a9e25c40`.
- **Nothing is pushed.** Merge/push per your team's process once the c3.4 gates pass.
- After c3.4: if the throughput gate is favourable, the redesign is complete end-to-end; if not,
  revisit the split-point placement (see §6.2 caveat and `APPROACH_B.md`).
