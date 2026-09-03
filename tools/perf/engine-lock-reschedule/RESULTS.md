# S2e engine-lock reschedule — perf results

Validation of the **never-block + park** admission work (S2e) against master and the shipped block-vs-try
reschedule (S2b / #4706). All numbers from the netem harness in this kit; medians of 3 reps unless noted.

## Bundles

| label | branch | commit | what |
| --- | --- | --- | --- |
| `master` | origin/master | `254c91287` | baseline |
| `s2b` | `feat/adaptive-engine-lock-reschedule` | `dbf948368` | #4706 bounded-try + reschedule (no park) |
| `s2e` | `feat/adaptive-engine-lock-neverblock` | `95bb7c932` | never-block + park |
| `s2e_fix` | `perf/s2e-wake-all-commits` (on s2e) | — | s2e + wake-trigger fix + admission knob flags |

## Method — what makes the win visible

- **netem is required.** The engine_lock hold that blocks admission is the SYNC-replica COMMIT wait; only a
  netem-delayed replication link reproduces it. Localhost `SIGSTOP` on a replica does **not** create the hold
  (a frozen-but-connected SYNC/STRICT_SYNC replica lets a write return in ~0.01s), so localhost can't measure
  the stall scenarios — use `run_s2e_netem.sh` / `sweep_s2e.sh`.
- **The park gate only opens with independent productive work.** `ShouldParkAdmission = productive_pending_ > 0
  && !AnySet()`; admission retries are `productive=false`, so a pure-admission storm never opens it. The
  decisive workload (`SCHED`) is **two populations**: streamers (explicit txns past admission, streaming
  productive pool tasks) + an admission storm + a slow writer. A pure-contention benchmark cannot show the win.

## The win — SCHED family (KPI = STREAM q/s, productive work flowing under contention)

Baseline (4 streamers + 16 storm + 1 slow writer, REPL_MS=500):

| | STREAM q/s | STREAM p99 | cpu |
| --- | --- | --- | --- |
| master | 2.0 | 2012 ms | 690% |
| s2b | ~90 | ~1600 ms | 678% |
| **s2e** | **~505** | **9.6 ms** | **426%** |

- **~250× master**, at p99 9.6 ms vs 2012 ms, and **lower CPU** (parked admissions stop spinning).
- **Insensitive to hold length** — REPL_MS 50→1000: master collapses 34→1.2, s2b 256→50, **s2e stays
  ~540→481**; s2e cpu falls with longer holds (more parking). Park fully decouples productive work from the stall.
- **Scales linearly with productive work** — NSTREAM 1/2/4/8: s2e 132/263/507/957 (~full throughput per
  streamer); master flat ~0.5–4.5.
- **Robust** to storm size (8→64: s2e 511→437 vs s2b 145→36), writers (1→4: 504→403), pool size (4/8/16:
  494/511/468), and mode (STRICT_SYNC ~436).
- **park (s2e) ≫ reschedule (s2b)** in every cell — park is the differentiator, not reschedule alone.
- **Nuance (SNQ):** the win needs productive work to stay *past admission*. Short streamer txns that re-BEGIN
  often (SNQ=100) drop s2e to 189 (still 95× master); SNQ≥1000 → ~500.

## Do-no-harm and regressions (pure-contention scenarios, no productive work)

READ q/s, master / s2e / s2e_fix:

| scenario | master | s2e | s2e_fix | note |
| --- | --- | --- | --- | --- |
| RO | 122 | 127 | 127 | parity (cpu identical) |
| RW-slow | 4.8 | 7.4 | 7.3 | s2e better |
| **RW-fast** | 114 | 91 | 92 | **−19%** — engine_lock at fast commit |
| **UNIQ_RO** | 113 | 98 | 100 | **−11%** — engine_lock from DDL commits |
| **UNIQ_UQ** | 114 | 77 | 78 | **−31%** — UNIQUE excludes READ |

All three regressions: **pure-contention, zero productive work**, **lower CPU** (trading throughput for less
spin buys nothing here), and **inherited from s2e** (s2e_fix ≈ s2e). Root cause: s2e replaces master's
**blocking condvar wait** on the admission lock with **bounded-try(2µs)→reschedule(LOW priority)** — which,
with nothing to yield to, (a) misses the release moment (2µs window), (b) LOW-priority re-queue loses the lock
race to the writer/DDL, (c) has no `notify_all`-style all-at-once flood. Severity tracks how excluded READ is:
engine-lock-only (RW-fast/UNIQ_RO) < UNIQUE-excludes-READ + writer-preference (UNIQ_UQ). Against each binary's
own RO baseline, master loses only 6% to writers in RW-fast; s2e loses 28% — it's the never-block *reaction*
to the ms-scale hold, not the hold length.

## UNIQ_UQ investigation (root cause corrected)

- **First diagnosis (wrong): wake asymmetry.** `WakeAllParked` fired only on a write-commit with deltas
  (`took_engine_lock`), so a UNIQUE-DDL release (empty `DROP ALL CONSTRAINTS`, no deltas) woke nobody; parked
  readers drained on the 100 ms monitor tick.
- **Fix built (kept — do-no-harm, but does NOT fix UNIQ_UQ):** wake parked admissions when a commit released
  `engine_lock` **or** a UNIQUE/READ_ONLY hold (`original_access_type()`), skip plain READ/WRITE commits (keeps
  read-heavy loads off `parked_mtx_`). RO cost from an earlier "wake-all-commits" cut (−4%) is gone; SCHED win
  preserved; **UNIQ_UQ unchanged (77→78)** → the wake timing was not the cause.
- **Re-diagnosis: reschedule fairness.** Readers reschedule at `Priority::LOW` and lose the main_lock
  re-acquire race to the DDL's tight UNIQUE loop, vs master's fair `notify_all`. Waking them just lets them
  lose again.
- **Decision:** UNIQ_UQ (back-to-back empty UNIQUE DDL) is not a realistic workload — accept the penalty.

## Knob sweep (see `knob_sweep.sh`; flags in `s2e_cfg`)

- **K1/K2 (try budget, 2→256µs): irrelevant to RW-fast/UNIQ_RO.** The hold is the ~40 ms replication RTT; a
  µs-scale bounded-try can't bridge a ms hold. Flat across values; SCHED win stable (~470) so the default
  2µs/64µs is fine. `idle=1024µs` slightly hurts PARK.
- **B1 (block-when-no-work): recovers RW-fast (−15%→−6%) but craters SCHED (469→289, cpu 435%→678%).** The
  `PoolHasPendingWork()` gate is instantaneous; in SCHED it misfires (streamers' work is intermittently
  queued) and blocks workers. **Keep B1 default OFF.**
- **Conclusion:** the RW-fast/UNIQ_RO cost is bound to the ms engine_lock hold, not to any knob here. A smarter
  block-gate (`!AnySet()` — block only when a core is genuinely idle, so it can't fire mid-SCHED) is the next
  candidate; otherwise the −6…−18% is the accepted never-block trade-off (independent of any hold-shortening
  work such as #4685, which is out of scope for this branch).

## Tunable knobs inventory (for future A/B)

| knob | location | default | exposed as flag? |
| --- | --- | --- | --- |
| K1 try budget (busy) | `SessionHL.hpp` `AdmissionTryBudget` | 2µs | ✅ `--admission-try-budget-busy-us` |
| K2 try budget (idle) | `SessionHL.hpp` `AdmissionTryBudget` | 64µs | ✅ `--admission-try-budget-idle-us` |
| B1 block-when-no-work | `SessionHL.hpp` `AdmissionEngineLockMode` | false | ✅ `--admission-block-when-no-work` |
| K3 tries before park | `session.hpp` `kAdmissionTriesBeforePark` | 4 | ⬜ (bolt layer) |
| K4 monitor tick | `priority_thread_pool.cpp` `SetInterval` | 100ms | ⬜ (utils layer) |
| K5 access timeout (park deadline / fail-fast) | `run_time_configurable.cpp` | 10s | ✅ `--storage-access-timeout-sec` (runtime-SET) |
| B2 park gate | `priority_thread_pool.cpp` `ShouldParkAdmission` | `productive>0 && !AnySet` | ⬜ (utils layer) |

## Code changes (branch `perf/s2e-wake-all-commits`, based on s2e `95bb7c932`)

- `src/query/interpreter.cpp` — smarter wake trigger in `Commit()` (wake parked admissions on engine_lock OR
  UNIQUE/READ_ONLY release; skip plain READ/WRITE).
- `src/glue/SessionHL.{hpp,cpp}` — K1/K2/B1 admission knobs as startup flags (defaults reproduce shipped s2e).

## Harness (this branch, `perf/engine-lock-reschedule-validation`)

- `phase3.py` — added DDL-contention population (`NDDL`/`DDLMODE` = readonly/unique) and productive streamer
  population (`NSTREAM`/`SQROWS`/`SNQ`).
- `phase3_run_ab.sh` — server-CPU sampling (`/proc`), N-way interleaving, `NWORKERS`, streamer/DDL passthrough,
  per-bundle server flags (`label:bin:flags`).
- `run_s2e_netem.sh` — RO/SCHED/PARK/UNIQ scenario driver (netem, sudo).
- `sweep_s2e.sh` — 31-cell broad sweep (scenarios + repl/stream/storm/writers/workers/mode/snq); auto-includes
  `s2e_fix` as a 4th bundle when built.
- `knob_sweep.sh` — K1/K2/B1 knob sweep across the anchor scenarios.
- `local_validate.sh` — localhost (no-sudo) validator for the no-hold scenarios (RO/UNIQ); cannot exercise the
  stall (see Method).
