# lockfree-read-snapshot A/B benchmark

Self-contained A/B benchmark for the experimental `lockfree-read-snapshot`
commit flag. It launches a local `build/memgraph` twice per scenario -- once
**OFF** (plain) and once **ON** (`--experimental-enabled=lockfree-read-snapshot`)
-- and compares the results.

## What the flag does

With the flag **ON**, a write committer releases its internal engine lock right
after minting the commit timestamp, then runs WAL + SYNC replication under a
separate mutex. A new transaction `BEGIN` no longer stalls behind another
transaction's slow commit.

With the flag **OFF**, the engine lock is held across the entire commit,
*including the SYNC-replica ACK wait*, so every `BEGIN` serializes behind the
slowest in-flight commit.

The win is therefore **concurrent reads not stalling behind slow commits**.

## Scenarios

| Scenario  | Setup | Metric | Expectation |
|-----------|-------|--------|-------------|
| **GOOD**    | MAIN + a genuinely slow SYNC replica; writers commit **large CREATE batches**, readers point-read | reader throughput, read p50/p99 | ON **higher** throughput / **lower** p99 |
| **BAD**     | single MAIN, no replica, one writer, no read contention | write throughput | ON **slightly lower** (mutex + snapshot-ring are pure overhead) |
| **NEUTRAL** | single MAIN, read-only, no writers | read throughput | ON **~equal** (one extra atomic load at BEGIN) |

Each scenario runs against a freshly launched memgraph with a fresh temp
data-directory; instances are always torn down (terminate -> wait -> kill) and
their temp dirs removed, even on error / Ctrl-C.

The GOOD scenario needs each write to hold `engine_lock` for a **meaningful**
amount of time -- otherwise there is no reader stall to relieve and only the ON
overhead shows. On a single machine we cannot inject network latency, so instead
each writer commits **one large CREATE-only transaction**
(`UNWIND range(0, $batch-1) AS i CREATE (:Churn {w:$w, i:i})`, `--write-batch`
vertices, default 2000). A big commit means a big WAL write plus the replica
applying every delta, which is what holds `engine_lock` in the OFF path. Using
**fresh CREATE nodes (no shared ids)** also eliminates write-write conflicts, so
there are no `TransientError` retries.

The replica's ACK is made genuinely slow **with flags only**: it is launched
with `--storage-wal-enabled=true` and `--storage-wal-file-flush-every-n-tx=1`,
so it fsyncs the WAL on every replicated commit. Replication is wired exactly
like `tests/e2e/replication/workloads.yaml`: `SET REPLICATION ROLE TO REPLICA
WITH PORT 10000` on the replica, `REGISTER REPLICA r1 SYNC TO '127.0.0.1:10000'`
on the main. Only the MAIN's flag is toggled OFF/ON; the replica is always plain.

### Reader mode: thread vs process

`--reader-mode` selects how readers run (default `process`; applies to
GOOD/NEUTRAL, BAD has no readers):

* `thread` -- reader threads in one process. The CPython **GIL caps total read
  throughput at ~6000 reads/s regardless of reader count** (measured: 4/8/16/24
  reader threads all land ~5800-6300 reads/s). Reads are **client-bound**, not
  `engine_lock`-bound, so the server is never pushed hard enough for the
  `BEGIN`-vs-commit contention the flag targets to show up.
* `process` -- each reader is its own OS process with its own driver
  (GIL-free). Read throughput now **scales with cores** (measured: 4/8/16/24
  reader processes -> ~24k / 29k / 35k / 34k reads/s on a 12-core box), i.e. the
  **server** becomes the bottleneck. Per-process window latencies are shipped
  back over a `multiprocessing.Queue` and merged exactly like the thread path;
  start/window alignment uses an absolute `CLOCK_MONOTONIC` `t0` (system-wide on
  Linux, so comparable across processes) instead of a barrier.

### Honest single-machine caveat

Even with a saturating multi-process client, this benchmark does **not** produce
a clear, robust GOOD win on a single machine, because a localhost SYNC commit
holds `engine_lock` for only ~sub-millisecond (microsecond loopback RTT + fast
local WAL/replica-apply). Measured, process-mode, 16 readers / 2 writers:

* `--write-batch` 2000-5000: ON is **worse by ~5-13%** -- once the CPU is
  saturated, the flag's write-path overhead (commit-serializer mutex +
  snapshot-ring writes) costs more than the tiny `engine_lock`-stall it relieves.
* `--write-batch` 10000 (very large commits): the hold finally grows enough that
  ON edges ahead, but only marginally (~+2-3% reader throughput at reps=3, p99
  roughly flat).

In other words the mechanism is real and correctly exercised, but on one machine
the effect is at the noise floor and can even go negative. A clear, materially
positive win needs genuine commit-hold latency -- real network RTT between main
and replica (SYNC ACK wait), which we deliberately do **not** inject (no OS /
network manipulation). Numbers are reported as measured; nothing is forced.

## Requirements

* Python 3 with the `neo4j` driver (`pip install neo4j==5.28.3`).
* A runnable `build/memgraph` that understands
  `--experimental-enabled=lockfree-read-snapshot`.
* **GOOD only:** an enterprise license for `REGISTER REPLICA`. Pass it via the
  environment (`MEMGRAPH_ORGANIZATION_NAME`, `MEMGRAPH_ENTERPRISE_LICENSE`) or
  `--organization-name` / `--license-key`. If it is missing, GOOD is skipped
  gracefully (with a clear message) and BAD/NEUTRAL still run.

No Docker, no netem/netns/iptables -- only localhost memgraph processes plus
memgraph flags and Cypher.

## Usage

```bash
# All three scenarios with defaults (15s each, 5000 vertices, 8 readers, 2 writers).
python3 tests/benchmark_lockfree_read_snapshot/ab_bench.py

# A quick smoke run.
python3 tests/benchmark_lockfree_read_snapshot/ab_bench.py \
    --scenario all --duration 3 --vertices 500 --readers 4 --writers 1

# One scenario against an explicit binary.
python3 tests/benchmark_lockfree_read_snapshot/ab_bench.py \
    --scenario good --memgraph-binary ./build/memgraph
```

### Options

| Option | Default | Meaning |
|--------|---------|---------|
| `--memgraph-binary` | `./build/memgraph` | binary to launch |
| `--scenario` | `all` | `good` / `bad` / `neutral` / `all` |
| `--duration` | `15` | measurement window (seconds) |
| `--warmup` | `2` | warmup seconds excluded from measurement |
| `--readers` | `8` | reader threads |
| `--writers` | `2` | writer threads (GOOD/BAD) |
| `--write-batch` | `2000` | GOOD: vertices CREATEd per writer commit (bigger => longer engine_lock hold in OFF) |
| `--reader-mode` | `process` | readers as `thread` (GIL-bound) or `process` (GIL-free, saturates the server); GOOD/NEUTRAL only |
| `--vertices` | `5000` | `:Node {id,v}` vertices seeded |
| `--reps` | `1` | repeat each A/B, take the median |
| `--ready-timeout` | `30` | Bolt-readiness poll timeout (seconds) |
| `--seed` | `1234` | fixed RNG seed |
| `--organization-name` / `--license-key` | env | enterprise license override |
| `--experimental-flag` | `lockfree-read-snapshot` | experimental value toggled ON |

## Output

For each scenario a table is printed with `metric | OFF | ON | delta% | verdict`,
where the verdict checks the *expected* direction (GOOD: reader throughput
ON > OFF; BAD: write throughput ON not much below OFF; NEUTRAL: within a few
percent), followed by a one-line `SUMMARY [scenario] ...`. An `errors` row
reports query errors seen during the window (should be 0). Numbers are reported
as measured -- no favorable result is hard-coded.

Worker threads never propagate an exception: a failed op is counted in `errors`
and the loop continues after refreshing the session, and `TransientError` is
retried a few times before being counted -- one bad op can never crash a thread
or corrupt a measurement.
