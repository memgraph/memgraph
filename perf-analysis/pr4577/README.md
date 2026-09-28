# PR #4577 point-query regression — analysis handoff

Scratch branch for perf-box investigation. **Not for merge.** Base: master `8902f6768`.

## TL;DR

- The nightly Daily Benchmark shows a **~2–3% throughput drop on cheap single-node mgbench point
  queries** starting with the 2026-09-24 nightly (`c67f6b98d`). Heavy traversals, macro, planner,
  vector, text and parquet suites are flat.
- CI benchmark-only runs pin it to **#4577 `1233eabb6`** (SHOW/TERMINATE SESSIONS):

  | comparison (signal median − control median) | machines | S − C |
  |---|---|---:|
  | #4577 vs its parent `88c90e77c` | doctor-doom / doctor-doom | **−3.1%** |
  | master `8902f6768` vs `88c90e77c` | doctor-doom / doctor-doom | **−3.8%** |
  | master + full revert of #4577 vs master | firebird / doctor-doom | **+4.5%** |
  | master + E2 DoRead probe vs master | firebird / doctor-doom | +1.8% |
  | master + E2 DoRead probe vs master + full revert | firebird / firebird | −2.1% |
  | **master + full revert vs master (repeat)** | **firebird / firebird** | **+4.4%** |
  | **master + E2 DoRead probe vs master (repeat)** | **firebird / firebird** | **+2.0%** |

- The only per-request change in #4577 is **`Session::DoRead()` → `dispatch(strand_)` → `ArmRead_`**,
  which adds a worker→io-thread hop per request. The E2 probe that removes it recovers **only
  about half** of what the full revert recovers, **twice** (+1.8% cross-machine, +2.0% same machine).
  So the hop is likely ~half the cost and **something else in #4577 is the other half**. That split is
  **the open question for the perf box**.
- **Local aarch64 A/B (12 vCPU VM, 4 clients) showed #4577 5–9% *faster*.** Local A/B on a small
  box is misleading for this change. See "Topology" below for the likely reason.

Full per-query numbers: [`raw_results.md`](raw_results.md). Raw JSON: [`data/`](data/).

## Branches

| branch | commit | content |
|---|---|---|
| `tmp/bench-pre-4577` | `88c90e77c` | parent of #4577 (includes #4854, #4865) |
| `tmp/bench-4577` | `1233eabb6` | #4577 squash |
| `tmp/perf4577-e0-master` | `8902f6768` | master baseline |
| `tmp/perf4577-e1-revert` | `f9361d25c` | master + `git revert 1233eabb6` (applies cleanly) |
| `tmp/perf4577-e2-doread-direct` | `e359aa182` | master + worker arms `async_read_some` directly again (**probe, reintroduces the lost-termination window below — do not merge**) |
| `tmp/perf4577-analysis` | this branch | this doc + data + scripts |

E0 was run twice: run 1 `36396283130` on doctor-doom, run 2 `36409024767` on firebird. E1 and E2 ran on firebird, so the run-2 comparisons are same-machine.

## Workload that shows it

CI invocation (`release/package/mgbuild.sh:2165`):
`./benchmark.py --installation-type native --num-workers-for-benchmark 6 --no-authorization pokec/<size>/*/*`
so **6 client connections, `bolt_num_workers=6`**, default `--scheduler=priority_queue`.

Signal set (moved cleanly at 09-24 in the nightlies, single-run noise ≤1.6%):
`arango/{expansion_1_with_filter,single_vertex_read,single_edge_write,shortest_path,single_vertex_write}`,
`match/{vertex_on_label_property_index,vertex_on_property (25k QPS),pattern_short,pattern_long}`,
`create/{vertex,pattern,vertex_big}`. These run at ~20–30k QPS, i.e. ~40 µs/query.

Control set (stable, didn't move): `arango/{expansion_2,expansion_2_with_filter,expansion_3,neighbours_2,
neighbours_2_with_filter,neighbours_2_with_data_and_filter,shortest_path_with_filter,allshortest_paths}`.

Too noisy for single runs: `aggregation/count` (±18%), `update/vertex_on_property` (195 QPS, ±13%),
`aggregation/min_max_avg`, `arango/unwind_range_vertex_write`, `arango/aggregate*`,
`match/vertex_on_label_property` (123 QPS).

## Topology (why only a busy box sees it)

- `src/memgraph.cpp:939–957`: with `--scheduler=priority_queue` (the default), queries run on a
  `PriorityThreadPool` of `bolt_num_workers` threads, and **`io_n_threads = 1`**. A *single* asio io
  thread serves every Bolt session.
- Before #4577 that io thread ran **one handler per request** (`OnRead` → hand off to a worker).
- After #4577 it runs **two**: `OnRead`, plus the `ArmRead_` lambda that the worker `dispatch`es back
  to arm the next read.
- With 6 clients at 25k QPS the single io thread's share per request grows, and each request waits
  for it twice. With 4 clients on a lightly loaded VM the io thread is idle enough that handing the
  read-arm off frees the worker sooner, which is net positive there.
- **Hypothesis to verify on the perf box:** the regression scales with client count / QPS and is
  bounded by single-io-thread utilization. Measure the io thread's CPU% and run-queue latency,
  before and after.

## Code analysis — network layer (`src/communication/v2/session.hpp`)

Line numbers are on master `8902f6768`.

### One RUN+PULL cycle, BEFORE (`88c90e77c`)

```
IO THREAD (single, asio, epoll_wait)        PRIORITY WORKER THREAD
────────────────────────────────────        ──────────────────────────────────────────
epoll_wait wakes: client sent RUN+PULL
OnRead(ec, n)            [on strand_]
  input_buffer_.Written(n)
  DoWork()
    session_context_->AddTask(               ── mutex + cv.notify_one ──►  lambda runs (off-strand)
        [shared_this = shared_from_this()])                                   session_.Execute()  (query)
back to epoll_wait                                                             Write() → socket send (sync)
                                                                               session_.Execute() → false
                                                                               DoRead():
                                                                                 IsConnected()
                                                                                 socket.async_read_some(
                                                                                   bind_executor(strand_,
                                                                                     bind_front(OnRead, shared_from_this())))
                                                                                 → epoll_ctl; no asio queue push, no wake
epoll_wait wakes on next request → OnRead …
```
Thread hops per request: **1** (io → worker).

### One RUN+PULL cycle, AFTER (`1233eabb6`)

```
IO THREAD (single)                          PRIORITY WORKER THREAD
────────────────────────────────────        ──────────────────────────────────────────
OnRead(ec, n)            [on strand_]
  read_armed_ = false
  DoWork() → AddTask(...)                  ── cv.notify_one ──────────►  Execute() / Write() as before
back to epoll_wait                                                         DoRead():                       (session.hpp:303)
                                                                             dispatch(strand_,
                                                                               [self = shared_from_this()] {...})
                                                                             running_in_this_thread()==false
                                                                             ⇒ QUEUED on io_context (+ eventfd wake)
epoll_wait/eventfd wakes  ◄──────────────────────────────────────────────  (worker returns to pool)
[dispatch lambda, on strand_]
  ArmRead_(bind_front(OnRead, self))       (session.hpp:287)
    IsConnected()
    terminate_requested_.load(acquire)
    read_armed_ = true
    socket.async_read_some(bind_executor(strand_, on_read))
  ~self (refcount--)
back to epoll_wait … next request → OnRead
```
Thread hops per request: **2** (io → worker, worker → io).

`boost::asio::dispatch(ex, f)` runs `f` inline only when the caller is already running inside that
strand (`strand::running_in_this_thread()`). Otherwise it is queued exactly like `post`. `DoRead` is
always called from the worker pool, so the dispatch is always queued. `DoFirstRead` and `DoReadAsio`
(ASIO scheduler) are already on the strand, so their dispatch runs inline and costs nothing.

### Per-request delta table

| # | change | where | per-request cost | verdict |
|---|---|---|---|---|
| 1 | `DoRead` → `dispatch(strand_)` → `ArmRead_` instead of direct `async_read_some` | `session.hpp:303` → `:287` | +1 io_context queue push + possible eventfd wake + one more handler on the **single** io thread + a context switch | **prime suspect** (E2) |
| 2 | extra `shared_from_this()` captured by the dispatch lambda | `session.hpp:303–306` | +1 atomic inc/dec; cache line bounces worker→io core | minor, removed by E2 too |
| 3 | `terminate_requested_.load(acquire)` in `ArmRead_` | `:287ff`, decl `:584` | plain `mov` on x86 | negligible |
| 4 | `read_armed_` bool writes (strand-confined) | `:262, :334, :384`, `ArmRead_` | L1 store | negligible |
| 5 | `SessionRegistry::Register/Deregister` (global mutex + map) | `:136`, `:117` | per **connection**, not per request | not a cause |
| 6 | `Session` gains `TerminableSession` base (vptr) + 2 fields | `session.hpp` | none on the hot path | negligible |

### Why `ArmRead_` exists (the constraint any fix must keep)

`TERMINATE SESSIONS` lets a foreign thread close a session's socket. `RequestTermination()`
(`:208`) sets `terminate_requested_` and **posts** `TerminateIfIdle_` (`:461`) onto the strand.
`read_armed_` (plain bool, **strand-confined**, `:584–589`) says who owns the socket:
- `true`: a read is pending, and the strand alone owns the socket. `TerminateIfIdle_` closes it now.
- `false`: a worker may be inside `Execute()`/`Write()`. `TerminateIfIdle_` defers, and `ArmRead_`
  shuts down instead of arming on the next read-arm.

Because `ArmRead_` and `TerminateIfIdle_` both run on the strand, "check flag → arm → mark armed"
cannot interleave with the termination check. If the worker arms the read directly (pre-#4577,
and the E2 probe), this interleaving is possible:
worker checks flag (false) → `TerminateIfIdle_` sees `read_armed_==false`, defers → worker arms read
→ session idles, **termination is lost until the client sends another request** (never, for an idle
pooled connection — the case this feature exists for).

Fix direction (not written yet): keep the handshake race-free **without** the hop. For example, the
worker arms the read, publishes "armed" through an atomic (seq_cst), then re-reads
`terminate_requested_`. `RequestTermination` sets its flag (seq_cst) and then reads "armed". At least
one side always observes the other. Needs concurrency review.

## Code analysis — query layer (not per request)

| # | change | where | when it runs | why it's not the cause |
|---|---|---|---|---|
| 7 | `foreign_user_view_.store()` in `SetUser` | `interpreter.cpp:11997, 12017` | login / impersonation / changed extras | `RuntimeConfig::Configure` early-returns for repeated auto-commit RUNs with unchanged extras (`SessionHL.cpp:790`) |
| 8 | `make_shared<SessionInfo>` + `foreign_session_view_.store()` | `interpreter.cpp:12028` | login | once per connection |
| 9 | `ResetUser` nulls both views | `interpreter.cpp:12033–12035` | logout / dtor | once per connection |
| 10 | `SessionQuery` branches (`Prepare`, `ApproximateQueryPriority`, visitor) | `interpreter.cpp`, `SessionHL.cpp` | SHOW/TERMINATE SESSIONS only | Cypher matches earlier branches |
| 11 | SHOW TRANSACTIONS reads `foreign_user_view_.load()` | `interpreter.cpp:7420, 7461–7463` | admin query | not in the benchmark |

`std::atomic<std::shared_ptr<T>>` on the toolchain's libstdc++ 16.2 (`bits/shared_ptr_atomic.h`) is
**not lock-free**: it uses a per-object spin bit in the control-block word. There's no global lock
pool, and in any case it's off the hot path.

### `Interpreter` layout (measured: `gdb -batch -ex 'ptype /o memgraph::query::Interpreter'`, aarch64 RelWithDebInfo)

| member | before | after |
|---|---:|---:|
| `foreign_user_view_`, `foreign_session_view_` | — | 216, 232 (16 B each) |
| `in_explicit_transaction_` | 216 | 248 |
| `current_db_` (`db_acc_` first, 16 B) | 224 | 256 |
| `transaction_status_` | 736 | 768 |
| `sizeof(Interpreter)` | 1384 | 1416 |

No hot member straddles a cache line before or after. Worth re-measuring on the x86 perf-box build.

## Suggested perf-box plan

1. **Reproduce first**: `88c90e77c` vs `1233eabb6` (or master vs E1), pokec point queries,
   `--num-workers-for-benchmark 6` (CI setting) **and** a sweep 1/2/4/6/12/24 clients. Expect the
   regression to appear or grow with client count.
2. **Attribute**:
   - `perf stat -e context-switches,cpu-migrations` per query, before/after (expect ≈ +1 cs/request).
   - `perf sched latency` / `offcputime` on the single io thread (the thread named from
     `IOContextThreadPool`), plus its CPU%. Is it saturating after #4577?
   - `perf trace -s` / `strace -c -f`: count `eventfd`/`write`/`epoll_wait` syscalls per request.
3. **Split the remaining gap** (E2 recovered ~half on CI):
   - E2 vs E1 on the same box, many iterations. CI says E2 ≈ halfway (twice), so expect a gap.
   - Then build "#4577 reverted except `session.hpp`" to split network vs everything else, then
     move the `foreign_*` members to the end of `Interpreter` (layout).
4. **Candidate knobs to sanity-check the hypothesis** (not fixes): `--scheduler=asio` (DoReadAsio
   path, no cross-pool hop); a patched build with `io_n_threads > 1`.

## Tools in this directory

- `ci/ci_bench.sh <branch>`: dispatches a benchmark-only Diff run (`release_benchmark=true`, all
  else false) on a pushed branch, cancels it after the `Run mgbench` step, downloads the job log, and
  writes `<branch>.json`.
- `ci/ci_cmp.py <base.json> <exp.json>...`: signal/control medians + per-query table.
- `ci/gen_raw.py > raw_results.md`: regenerate the per-query table and pairwise summary from `data/` (add new runs to `RUNS`/`PAIRS`).
- `ci/bench_compare.py`: PR-vs-nightly comparator (same as `~/.claude/skills/ship-pr/bench_compare.py`).
- `data/nightly_0917-0927_mgbench.json`: per-night, per-query iteration lists parsed from the
  Daily Benchmark logs (`{night: {suite: {query: [qps…]}}}`).
- `2026-09-28--investigation.html`: running investigation log (decision log, experiment ladder).
