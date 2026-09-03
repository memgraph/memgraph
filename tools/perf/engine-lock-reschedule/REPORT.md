# Perf-box validation — lock-park scheduling (commit-lock U4 + main-lock U3, on #4685)

**For:** perf engineer running throughput measurements on a stable, isolated box.
**Start at "## 0" below** — it names the current target (the `feat/adaptive-commit-lock-scheduling`
flag A/B). The engine-lock sections (§1–§3b) are the historical lineage that #4685 superseded.
**Author context:** the numbers below were gathered on a *non-perf-stable* dev VM and are
**preliminary/directional only** — the point of this task is to confirm (or refute) them on real
hardware. Everything needed (root cause, per-PR hypotheses, scripts, exact scenarios, expected
shape) is in this directory.

---

## 0. CURRENT TARGET — commit-lock (U4) + main-lock (U3) park, on #4685 (read this first)

This kit now validates the **commit-lock / main-lock park feature** on branch
**`feat/adaptive-commit-lock-scheduling`** (stacked on #4685 `experiment/commit-lock-narrowing`).
`build_binaries.sh` builds it by default as `bins/cls/memgraph`. The whole feature is gated behind one
server flag, so the clean A/B is the **same binary, flag ON vs OFF** — `phase3_run_ab.sh` takes an
optional 3rd colon field per bundle for exactly this:

```
BIN=bins/cls/memgraph
ON="on:$BIN:--experimental-enabled=lockfree-read-snapshot"   OFF="off:$BIN:"

# U4 — commit-lock (slow writes): writers (W>0) contend the NEW commit_mutex_ behind a slow SYNC commit.
DUR=20 REPS=6 COMBOS="16,2 16,4" NSTREAM=4 ./phase3_run_ab.sh "$ON" "$OFF"

# U3 — main-lock (DDL/schema): NDDL workers hold main_lock_ while R readers BEGIN. Keep R > NWORKERS(8).
#   DDLMODE=unique  -> DROP ALL CONSTRAINTS (a UNIQUE hold, excludes all)
#   DDLMODE=readonly-> CREATE/DROP INDEX     (a READ_ONLY hold, excludes WRITE only)
DUR=20 REPS=6 COMBOS="32,0 64,0" NDDL=1 DDLMODE=unique NSTREAM=4 ./phase3_run_ab.sh "$ON" "$OFF"
```

**Why `NSTREAM` matters:** parking only helps if freed workers have *other* productive work to run.
`NSTREAM>0` runs a long-lived producer (the "other work" the park yields to) — that is where the ON/OFF
gap appears. **Signal:** flag ON keeps READ `txn/s` + `STREAM q/s` up and bounds `q_p99` while the
contention holds; OFF floors them as the 8 workers fill with blocked BEGINs/commits. The `DDL op/s` and
write op/s columns should be ≈ equal A/B (the feature must not slow the contended DDL/writer itself).
**No-regression control:** `NDDL=0 NSTREAM=0 W=0` (no contention) must be ≈ equal A/B, and running the
same flag under both labels must report ≈0%.

> **The `engine-lock` sections below (§1–§3b) are the historical lineage.** §2's root cause (a COMMIT
> holding `engine_lock_` across the whole durability wait) is exactly what #4685 fixes at source —
> `engine_lock_` becomes mint-only/fast and the write stall moves to the new `commit_mutex_`. The S2e
> never-block+park work (§3b) parked on `engine_lock_`; this feature instead parks on the relocated
> `commit_mutex_` (U4) and on `main_lock_` (U3). Kept for context; §0 is what the box builds and runs.

## 1. The customer symptom

Fast, few-millisecond reads collapse in throughput when the number of concurrent clients exceeds the
number of Bolt workers, **amplified by explicit transactions and by slow commits** (SYNC /
STRICT_SYNC replication). Reads that should be unaffected by a slow writer instead stall behind it.

## 2. Root cause (verified in code + micro-repro)

A write **COMMIT** holds the storage `engine_lock_` (a `SpinLock`) across its **entire durability
phase** — WAL append *and* `HandleDurabilityAndReplicate`, which for a SYNC/STRICT_SYNC replica is a
network round-trip to the replica and back (`storage/v2/inmemory/storage.cpp`, commit path
~`:1079→:1185`).

Every new transaction's **admission** also takes that same `engine_lock_` to mint its
timestamp/transaction-id (`CreateTransaction`, ~`:2876`). So while a slow commit holds the lock, each
arriving BEGIN (explicit) or first-query admission (autocommit) **busy-spins a whole Bolt/pool
worker** waiting for it. With clients > workers, all workers end up spinning on admission behind one
slow commit → reads collapse. STRICT_SYNC (2-phase commit) holds the lock ~2× longer → worse.

**Non-fix (ruled out):** narrowing the `engine_lock_` critical section around the durability wait is
**not viable** — the WAL/commit-timestamp ordering under that lock is load-bearing for replication
correctness. The only safe lever is to **stop workers from spinning on admission**: attempt the lock
with a tiny bounded try, and on contention **reschedule the admission onto the pool** so the worker
is freed to do other (non-admission) work while the commit finishes.

## 3. The stack under test

| PR | Branch | What it does | Perf hypothesis to confirm |
|----|--------|--------------|-----------------------------|
| **#4669** | `perf/begin-engine-lock-tryresched` | Bounded-try + reschedule the **explicit `BEGIN`** admission. | Under concurrent slow commits, **explicit-transaction** read throughput improves substantially; **flat on autocommit**; no regression when there is no stall. |
| **#4684** | `perf/prepare-reschedule` (stacked on #4669) | Extends the same mechanism to the **autocommit `RUN`→PREPARE** admission (the majority path). | Same shape as #4669 but for **autocommit** workloads — the row where #4669 is flat. |
| **#4662** | `perf/query-timeout-deadline` | Replaces per-query `AsyncTimer` (POSIX timer + skiplist) with a `steady_clock` deadline read against a coarse 100 ms cached clock in `MustAbort()`. | Small **single-thread fast-op** gain (removes fixed per-query timer setup). No regression elsewhere. |
| **#4663** | `perf/adaptive-worker-spin` | Gates the worker idle-spin so the pool only spins when it is **not** oversubscribed (`HotMask::Count() < budget`). | Safety/scheduling; no throughput regression, possible small gain at moderate concurrency. |
| **#4668** | `perf/read-commit-high-priority` | Routes a **read-only** transaction's COMMIT to HIGH priority so read txns drain fast under a commit pile-up. | Helps read-txn drain latency when commits queue; no regression. |

Baseline: **master** (`origin/master`).

> The two reschedule PRs (#4669, #4684) are the ones that move the customer number. #4662/#4663/#4668
> are supporting/safety changes; validate them mainly for *no regression* plus their small local wins.

## 3b. S2e — the current fix under test (supersedes §3's #4669/#4684)

The #4669/#4684 reschedule stack was consolidated into **#4706** (`feat/adaptive-engine-lock-reschedule`,
"S2b": bounded-try admission that reschedules onto the pool, using a *block-vs-try* gate — TryBounded
when the pool has other work to yield to, Blocking fallback otherwise). **S2e**
(`feat/adaptive-engine-lock-neverblock`) then evolves S2b into **never-block + park**:

- admission **never** takes the Blocking fallback on the in-memory READ/WRITE path — it bounded-tries
  with an adaptive budget and, after a few tries (`kAdmissionTriesBeforePark`=4) **when the pool is
  under pressure (other productive work queued, no idle worker)**, **parks** the admission at ~0 CPU;
- a **write COMMIT full-drains** the parked set the instant it releases `engine_lock_`
  (`WakeAllParked` from `Interpreter::Commit`), a 100 ms monitor tick is the correctness backstop, and
  a parked admission that is never admitted **times out at `storage_access_timeout_sec` (default now
  10 s)** and fails fast instead of hanging.

**A/B set:** `master` (baseline) · `s2b` = #4706 · `s2e`. `build_binaries.sh` builds all three
(default). S2b is the intermediate so we can attribute deltas: master→s2b = the reschedule win,
s2b→s2e = the never-block+park delta.

**Win shape:** same as §3 — read throughput under concurrent slow writers, **explicit *and*
autocommit** (S2e never-blocks both paths, so unlike #4669-alone the autocommit rows also win),
larger under STRICT_SYNC (longer lock hold). `W=0` (no stall) must stay ≈ master for all three.

**What distinguishes s2e from s2b (measure these, not just throughput):**
1. **Server CPU during the stall.** S2b's Blocking fallback still parks a worker on the OS lock;
   master busy-spins the `SpinLock`. S2e **parks at ~0 CPU**. Throughput alone may show s2e ≈ s2b —
   the CPU axis is where S2e wins. Sample the main's CPU during a STALL cell (e.g. `pidstat -p <main>
   1`, or `thermal_watch.sh`, or add CPU sampling to `phase3.py` — a recommended follow-up).
2. **Long-stall behaviour.** Raise `REPL_MS` high (e.g. 500–2000 ms, or pause the replica) so the
   commit stalls > a few seconds: master/s2b keep a worker pinned per blocked admission; s2e keeps CPU
   low and, past 10 s, returns a storage-access-timeout to the client (fail-fast) rather than hanging.
3. **Post-stall drain.** After the slow commit finishes, s2e's `WakeAllParked` drains the backlog in a
   burst; watch read p99 recover faster.

> **Honest framing.** Because `engine_lock_` is held for the whole replication wait (§2, and narrowing
> it is ruled out), a *new* read admission still cannot mint a timestamp *during* a held-lock stall —
> S2e does not make new reads "flow during" the stall. Its wins are: near-zero CPU while stalled,
> freed workers so already-open read txns' PULLs and other pool work keep flowing, a burst drain when
> the stall clears, and fail-fast at the deadline. "Reads flow *during* the stall" would require
> releasing `engine_lock_` before replicating (commit-lock narrowing) — a separate, deeper change,
> complementary to S2e, not part of it.

## 4. What you need on the box

- Linux, **passwordless `sudo`** for `ip netns` / `tc` (the harness builds isolated network namespaces).
- **≥ 12 online cores.** The harness pins the server to cores `0-7` and the client load-gen to `8-11`
  (disjoint — server and client never share a core). Override with `SRV_CORES` / `CLI_CORES`.
- `python3` + the `neo4j` driver (`pip install neo4j`, 5.28.x used).
- Memgraph build toolchain (v8) at `/opt/toolchain-v8` (override `TOOLCHAIN=`). ccache recommended.
- `tc`, `ip`, `taskset`, `ping`, `bc`.

Ideally run on a **quiesced** box (no other load, fixed CPU governor = `performance`, turbo/SMT
settled) — the whole reason for this task is that the dev VM was too noisy to trust.

## 5. Setup / build

```bash
cd tools/perf/engine-lock-reschedule

# 5a. sanity-check the netns + pinning harness WITHOUT memgraph (proves the box is ready)
./scripts/dry_run.sh 10          # 10ms one-way -> ~20ms RTT; must exit 0

# 5b. build all six binaries into relocatable bundles under ./bins/<label>/
#     (label:branch pairs; branches are already pushed to origin)
REPO=/path/to/your/memgraph/checkout ./scripts/build_binaries.sh
#   -> bins/{master,p4662,p4663,p4668,p4669,p4684}/memgraph (+ src/query/*.so beside each)
```

The bundles are relocatable: each `bins/<label>/memgraph` RUNPATHs to `$ORIGIN/src/query`, so the
matching `src/query/*.so` sit beside it. `build_binaries.sh` verifies each relocated bundle runs
before trusting it.

## 6. The measurement — Phase-3 grid

The harness models the real topology: a **fast client link** and a **separate, slow replication
link**, so a SYNC/STRICT_SYNC writer's COMMIT blocks on the netem-delayed replica ack (= the "slow
commit") while reads over the fast link stay quick.

```
client netns (10.0.0.1) --fast veth/netem-- main netns (10.0.0.2) --SLOW veth/netem-- replica netns (10.0.1.3)
```

`phase3.py` runs `n_readers` readers + `n_writers` writers concurrently and reports **read q/s, read
txn/s, read latency p50/p99, and write op/s**. It is parameterized by env (see the header of
`scripts/phase3.py`):

- `RMODE` = `explicit` (BEGIN + `NQ` PULLs + COMMIT) or `auto` (per-query implicit txn)
- `RQROWS` = read cost (`~130000` fast, `~1000000` long)
- `NQ` = PULLs per explicit read txn (the engine-lock-free "other work" a freed worker can do)
- `WMODE` = writer txn mode; `WQROWS` = `0` short CREATE, `>0` long UNWIND-range CREATE
- writers' COMMIT blocks on the replica ack (`REPL_MS` netem delay) = the slow commit

### Run the full spectrum (8 scenarios × {SYNC, STRICT_SYNC} × reader/writer combos):

```bash
# ---- S2e validation (current) ----
# 0. build the three binaries (default BUILDS = master + s2b(#4706) + s2e):
REPO=/path/to/memgraph ./scripts/build_binaries.sh    # -> bins/{master,s2b,s2e}/memgraph

# 1. INTERLEAVED, counterbalanced A/B (kills the second-slot bias) — the preferred runner.
#    Attribute deltas in two hops: master->s2b (reschedule win), s2b->s2e (never-block+park delta).
MODE=SYNC REPL_MS=20 DUR=20 REPS=6 COMBOS="16,0 16,2 16,4" \
  ./scripts/phase3_run_ab.sh master:$PWD/bins/master/memgraph s2e:$PWD/bins/s2e/memgraph
MODE=SYNC        ./scripts/phase3_run_ab.sh s2b:$PWD/bins/s2b/memgraph s2e:$PWD/bins/s2e/memgraph
MODE=STRICT_SYNC ./scripts/phase3_run_ab.sh master:$PWD/bins/master/memgraph s2e:$PWD/bins/s2e/memgraph
# sanity: master-vs-master must report ~0%
  ./scripts/phase3_run_ab.sh a:$PWD/bins/master/memgraph b:$PWD/bins/master/memgraph
# long-stall (park CPU + 10s fail-fast): raise REPL_MS well past the 10s deadline's reach
MODE=SYNC REPL_MS=2000 DUR=30 ./scripts/phase3_run_ab.sh master:$PWD/bins/master/memgraph s2e:$PWD/bins/s2e/memgraph
#   (sample the main's CPU during this — pidstat/thermal_watch.sh — to see park≈0 vs master spinning)

# ---- old per-PR grid (superseded; kept for reference) ----
GRID_OUT=grid_master_vs_4669.txt \
  BINS="master:$PWD/bins/master/memgraph p4669:$PWD/bins/p4669/memgraph" \
  REPL_MS=20 DUR=15 REPS=3 \
  ./scripts/phase3_grid.sh
```

Run the grid **once per binary you care about**, pointing `BINS` at the pair (baseline + candidate),
e.g. `master` vs `p4684`, `master` vs `p4662`, etc. Or list all six in `BINS` for a single sweep
(longer). The eight scenarios already cover fast/long reads × short/long writes × explicit/auto.

**Key knobs for the perf box:** raise `DUR` (15–30 s) and `REPS` (3–5) for stable medians; `REPL_MS`
sets how slow the commit is (20 ms is a moderate WAN; try 5/20/50 to sweep commit slowness);
`SRV_CORES`/`CLI_CORES` to match the box.

### The rows that matter most

- **#4669 win** shows in `longread-expl` and `longR-longW-expl` at `W=2`/`W=4` (readers stalled by
  slow writers), **explicit** reader mode.
- **#4684 win** shows in the **`*-auto`** scenarios (`longread-auto`, `longR-longW-auto`) at
  `W=2`/`W=4` — the autocommit rows where #4669 alone is flat.
- **No-regression** check: the `W=0` column (no writer = no stall) must be ≈ master for every binary.

## 7. Expected results (preliminary — from the noisy dev VM; CONFIRM these)

Directional shape observed on the dev VM (single runs, ±10–15% CI-grade noise — treat as hypotheses):

| Scenario (readers stalled by slow writers) | master | #4669 | #4684 |
|---|---|---|---|
| **explicit** long read, SYNC, W=4 | baseline | **+14 – +22%** | ≈ #4669 (inherits it) |
| **explicit** long read + long write, STRICT_SYNC, W=4 | baseline | **+28 – +50%** | ≈ #4669 |
| **autocommit** long read, SYNC/STRICT, W=4 | baseline | ≈ master (flat) | **expected win (unmeasured)** |
| any scenario, **W=0** (no stall) | baseline | ≈ master | ≈ master |

- **#4669**: consistent large positive under stall, **explicit-txn only**; robust across SYNC and
  STRICT_SYNC; STRICT_SYNC shows the bigger gain (longer lock hold). Flat on autocommit by design.
- **#4684**: **not yet benchmarked** — only unit-tested and CI-green. The hypothesis is that it turns
  the flat autocommit rows into wins of a similar shape, largest under STRICT_SYNC.
- **#4662 / #4663**: CI mgbench (isolated single-query) showed **+6–9% on fast point read/write** for
  #4662/#4663 vs master — plausibly real (fixed per-op overhead removed) but confounded by CI noise
  and a slightly older master baseline; confirm in isolation.
- **#4668**: expect improved read-txn drain latency (lower read p99) when commits pile up; no
  throughput regression.

### Success criteria (what to report back)

1. **#4669**: explicit-BEGIN read throughput under concurrent slow writers **≥ +10% vs master** at
   `W≥2` (SYNC), larger under STRICT_SYNC; **no regression at `W=0`**.
2. **#4684**: the same win shape on the **autocommit** (`*-auto`) rows where #4669 is flat.
3. **All PRs**: **no regression** on any `W=0` (no-stall) cell, and no regression on the fast-read
   `fastread-*` scenarios.
4. #4662/#4663: confirm or deny the small single-thread fast-op gain.

## 8. Caveats & known residuals

- **In-memory only.** `#4669`/`#4684` reschedule only for READ/WRITE accessor acquisition on
  **in-memory** storage. **On-disk** storage has no non-blocking probe, so an on-disk BEGIN/PREPARE
  reschedules up to the cap (32×) then blocks — correct but not optimized (a tracked one-hop
  follow-up). Benchmark **in-memory transactional** mode.
- **UNIQUE / READ_ONLY** acquires always block (never reschedule) — DDL/schema-assert queries are out
  of scope; they won't show a win.
- **CI mgbench does NOT exercise this.** The GitHub `Release / Benchmark` job is *isolated
  single-query* — no concurrent slow commit — so it correctly shows #4669/#4684 ≈ master. The win
  only appears under the concurrent-slow-commit workload this harness creates.
- The harness is a **single-box netns simulation** with disjoint CPU pinning. It is a faithful
  stand-in for a fast-client / slow-replica topology, but if you have two real machines + a real
  replica link, that is even better — the same `phase3.py` driver works against any `bolt://` URI.
- **Correctness is already covered** (unit tests, concurrency-model audit, CI matrix green). This task
  is purely throughput confirmation.

## 9. Files in this directory

- `REPORT.md` — this file.
- `scripts/dry_run.sh` — verify netns + pinning before spending a build/bench.
- `scripts/setup_repl.sh` — build the 3-netns fast-client / slow-replica topology.
- `scripts/phase3.py` — the reader+writer load driver (env-parameterized; see its header).
- `scripts/phase3_run.sh` — start main + SYNC/STRICT replica (WAL on), register, run one binary set.
- `scripts/phase3_grid.sh` — the full 8-scenario × {SYNC,STRICT_SYNC} sweep.
- `scripts/build_binaries.sh` — build all six branches into relocatable `bins/<label>/` bundles.

Teardown is automatic; stray namespaces: `sudo ip netns del mgcli mgsrv mgrepl`.
