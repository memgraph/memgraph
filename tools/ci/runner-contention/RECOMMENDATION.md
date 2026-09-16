# RESOURCE_GROUPS, --resource-spec-file and PROCESSORS for the unit suite

## 1. The mechanics, verified by experiment

A throwaway CMake project with sleep-based tests, run under ctest, establishes
what the prose docs leave ambiguous. Every row below was measured, not read.

| behaviour | result |
|---|---|
| `RESOURCE_GROUPS "disk:1"`, spec declares 2 slots, `ctest -j8` | peak concurrency exactly 2 |
| the test never reads `CTEST_RESOURCE_GROUP_*` | the cap still applies |
| `PROCESSORS 4`, `ctest -j8` | peak concurrency 2 |
| `PROCESSORS 64`, `ctest -j8` | still runs and passes |
| `RESOURCE_GROUPS "disk:99"` against a 2-slot spec | `***Not Run`, ctest exits 8 |
| **`RESOURCE_GROUPS` with no `--resource-spec-file`** | **no cap at all, ctest exits 0** |

The last row is the one to design around. Forgetting the flag does not fail; it
silently removes the protection while the suite stays green. Over-requesting
slots, by contrast, is loud.

So: `RESOURCE_GROUPS` works purely as a scheduler cap, which is what we want,
because the tests need no modification to honour it.

Working spec file:

```json
{
  "version": { "major": 1, "minor": 0 },
  "local": [ { "disk": [ { "id": "d0", "slots": 4 } ] } ]
}
```

`local` is an array (normally one entry) of objects keyed by resource type; each
type holds instances with an `id` and a slot count. A `"disk:1"` requirement
takes one slot from a single instance, so one instance with N slots gives a
clean "at most N concurrent" cap.

## 2. What to classify on: sync count, not bytes

The suite writes 12.2 GB across 227 tests, and 134 of those write nothing. The
obvious move is to charge a disk slot to the big writers. **That is wrong.**

Under an identical competing IO load, the time a test gains tracks the number of
synchronous IO calls it makes, not the bytes it moves:

| test | writes | syncs | added under load | ms per sync |
|---|---|---|---|---|
| `query_expression_evaluator` | 774 MB | 20,972 | +394s | 18.8 |
| `interpreter` | 549 MB | 14,442 | +274s | 19.0 |
| `storage_v2_indices` | 381 MB | 8,300 | +152s | 18.3 |
| `storage_v2_constraints` | 472 MB | 7,798 | +137s | 17.6 |
| `query_dump` | 213 MB | 5,627 | +50s | 8.9 |
| `storage_v2` | 466 MB | 3,177 | +23s | 7.1 |
| `storage_v2_wal_file` | 92 MB | 2,988 | +30s | 10.0 |
| `rpc_file_framing` | 91 MB | 1,616 | +33s | 20.1 |
| `utils_scheduler` (control) | 0 MB | 36 | +2s | - |

`storage_v2` writes five times what `rpc_file_framing` writes and gains *less*
time. A queued device punishes round trips, not volume.

Classify with `measure_fsync.sh`, which counts `fsync`, `fdatasync` and
`renameat` per test. `measure_io.sh` (bytes) is kept because it cheaply
identifies the 134 tests that touch nothing and can be excluded outright, but it
must not be used to rank the rest.

## 3. Proposed properties

Charge a disk slot to every test with a material sync count, and leave the rest
uncapped so they still use the full `-j`:

```cmake
# In the add_unit_test wrapper, for tests measured as sync-heavy:
set_tests_properties(<test> PROPERTIES RESOURCE_GROUPS "disk:1")
```

and run CI with:

```
ctest ... --resource-spec-file tests/unit/resources.json
```

**Guard the flag.** Because omitting it is silent, the CI step should assert the
file was passed, for example by failing the job if `--resource-spec-file` is
absent from the command it is about to run.

`PROCESSORS` is the wrong instrument here and should not be used for this
purpose: it bounds CPU slots, and the contended resource is the device. Reserve
it for genuinely multi-threaded tests, where it describes a real CPU demand.

## 4. The slot count: not established

I could not size this from measurement, and would rather say so than publish a
number the data does not support.

- On a fast, idle NVMe there is no knee through `-j10`: wall time falls
  monotonically (75s to 25s) while per-test time inflates. More concurrency
  always wins.
- Under an external competing load, capping our own concurrency helps much less
  than expected, because the latency is imposed from outside. At `-j1` a single
  test was still slowed 13.8x by an external load, and `-j1` versus `-j5` traded
  a 20% improvement in the slowest test for nearly double the wall time.
- Attempts to isolate self-inflicted contention on this box did not converge:
  some tests reported shorter durations at `-j4` than at a warm `-j1`, which no
  contention model explains, so that arm is unreliable rather than informative.

A slot limit only recovers the self-inflicted share. On the CI fleet that share
looks real but bounded: the slow class was already 1.94x behind the fast class
at `-j2` and 4.12x at `-j16`, so roughly half the gap is baseline device deficit
that scheduling cannot fix.

**Suggested starting point, to be tuned on the runners themselves rather than
trusted from here:** declare `slots` at about half the runner's `nproc` (8 on
these 16-thread machines) and watch the slowest per-test time and the job wall
time. The data supports the shape of the fix, not a specific constant, and the
constant is cheap to tune in CI where the real device lives.

## 5. What would settle it

Run the real suite on one slow-class runner at a fixed `-j16` with a spec file
declaring 2, 4, 8 and 16 disk slots, and record per-test durations and job wall
time at each. That is four jobs, needs no code change beyond the properties, and
measures the actual device instead of an emulation of it.
