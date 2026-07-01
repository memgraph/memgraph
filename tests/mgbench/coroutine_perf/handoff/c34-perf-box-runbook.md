# c3.4 Perf-Box Runbook — exact steps

Companion to [`HANDOFF.md`](./HANDOFF.md). This is the *do-this* checklist for the bare-metal /
PMU-capable machine. All commands assume repo root and the toolchain activated.

Two independent env vars must be exported first (see `HANDOFF.md` §10 — obtain from the team):

```bash
export MEMGRAPH_ORGANIZATION_NAME='<licensee-org-name>'
export MEMGRAPH_ENTERPRISE_LICENSE='<enterprise-license-key>'
```

Without them, `USING PARALLEL EXECUTION` produces a non-parallel plan and every gate below tests
nothing. The scripts abort with a clear message if either is unset.

The **A/B knob** for park-vs-blocking (same binary, no rebuild):

| Mode | `--query-coroutine-yield-ops` | Parallel coordinator behaviour |
|------|-------------------------------|--------------------------------|
| **Park** (this branch, default) | `Aggregate,OrderBy,Accumulate,Distinct,HashJoin` (the default) | coroutine coordinator **parks**, frees its worker |
| **Block** (baseline) | `` (empty string) | Sync coordinator **blocks** in `WaitOrSteal` (work-stealing) |

---

## 0. Sanity: reproduce the green ASan gate (fast, optional)

Confirms the branch behaves as verified on the dev box.

```bash
source /opt/toolchain-v7/activate                        # adjust toolchain version to repo pin
./build.sh --build-type RelWithDebInfo -DASAN=ON --reserve-cores 8 --skip-os-deps --keep-build --target memgraph
tests/mgbench/coroutine_perf/handoff/scripts/run_asan_hammer.sh 7699
```
Expected: `crash=0`, `asan_reports=0`, `seq_bad=0`, `con_bad=0`, results == serial.

---

## 1. Build for TSan (§6.1 of HANDOFF)

Use a **dedicated TSan build dir** (do not reuse the ASan dir). Deps must be built under the TSan
host profile — go through `build.sh` so that happens ABI-consistently.

```bash
source /opt/toolchain-v7/activate
# Whatever the repo's canonical TSan invocation is; typically:
./build.sh --build-type RelWithDebInfo -DTSAN=ON --reserve-cores 8 --skip-os-deps --keep-build \
  --target memgraph
cmake --build build --target memgraph__unit__utils_priority_thread_pool -j"$(nproc)"
```

## 2. TSan campaign (§6.1)

```bash
mkdir -p build/tmp
# 2a. Pool state machine (this is the core; already 0-race on the dev box — reconfirm here):
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1" \
  TMPDIR=build/tmp ./build/tests/unit/utils_priority_thread_pool

# 2b. End-to-end query hammer under TSan, workers=2 and 4, concurrent clients:
tests/mgbench/coroutine_perf/handoff/scripts/run_tsan_gate.sh 7710
```
`run_tsan_gate.sh` launches the TSan `memgraph`, runs `parallel_hammer.py` (agg/grouped/order-by +
a hops-bearing expansion) at `USING PARALLEL EXECUTION 2` and `4`, then greps the server log for
`WARNING: ThreadSanitizer`.

**Pass:** `0` ThreadSanitizer warnings across 2a + 2b; hammer parity clean.
**Note:** on a slow/virtualized box the TSan server may not complete the Bolt handshake in time —
that's the exact reason this must run on real hardware. If it still starves, raise the driver
`connection_timeout` in the script and reduce the dataset size (documented inline).

## 3. Throughput-under-concurrency gate (§6.2 — the payoff measurement)

Build a normal optimized binary (no sanitizer) for perf:

```bash
source /opt/toolchain-v7/activate
./build.sh --build-type Release --reserve-cores 8 --skip-os-deps --keep-build --target memgraph
```

Run the A/B (the script launches the server twice — once per knob value — with a fixed worker pool
and drives concurrent parallel-query clients):

```bash
tests/mgbench/coroutine_perf/handoff/scripts/throughput_gate.py \
  --port 7712 --workers 8 --clients 1,2,4,8,16 --dataset 400000 --repeats 5
```

It reports, for **park** vs **block**:
1. **Single-query latency** (p50/p95) for each query shape — *park must not regress vs block beyond noise.*
2. **Throughput** (completed parallel queries/sec) at each client concurrency level — *park should win or tie, especially as concurrency rises.*

For PMU (instructions/query, IPC) on the same runs, wrap the client-load window with `perf stat`
(bare metal only) — see [`../perf_pmu.sh`](../perf_pmu.sh) for the existing pattern used by the
coroutine-cursors-v2 gate; the same approach applies here.

**Decision criteria:**
- ✅ Ship the redesign as-is if: single-query latency within noise, and throughput-under-concurrency
  improves or ties for park.
- ⚠️ If park *regresses* throughput or single-query latency: the coroutine region likely extends over
  heavy work below the split point. Revisit split-point placement (see `APPROACH_B.md` — self-park
  preempts no better than root-only below the split) before shipping. Record findings.

Record the numbers in a new `RESULTS`-style doc next to this one (mirror [`../RESULTS.md`](../RESULTS.md)).

## 4. (Optional) shutdown-while-parked CI regression test (§6.3)

`abort_mid_park_test.py` is the ready-made behavioural check. To make it a CI unit/e2e test, port its
logic (start a long parallel query on one connection; from a second connection poll `SHOW
TRANSACTIONS`, `TERMINATE` the parallel txn; assert the first connection gets an abort error, the
server stays healthy, and a subsequent parallel query succeeds). It is deterministic because a
parallel coordinator stays parked for the whole query window.

---

## Gotchas (carried from the dev-box work)

- **Toolchain activation is mandatory** in every shell before any build (`clang: No such file` if not).
- **Never mix sanitizers in one build dir**; always `build.sh` for the sanitizer so deps match (a
  hand `cmake -DASAN=ON` on a non-ASan dir gives abseil startup SEGV / `__tsan` link errors).
- **Unit tests need** `mkdir -p build/tmp` + `TMPDIR=build/tmp`.
- **Driver tests need** `source tests/ve3/bin/activate`.
- **Do not** `pkill -f "build/memgraph …"` *inline* in a shell whose command line contains that
  pattern (self-match kills the shell). The provided scripts run `pkill` from inside a script file.
- The Bash "nonzero exit hides stdout" trap: capture to a file and read it separately, or append
  `; echo done`.
