# Replication benchmarks — design

**Status**: Draft. Design complete. Slices 01 and 02 implemented on `testing/repl-benchmarks`,
neither yet run against a built binary. **Designed**: 2026-08-18.

Benchmarks the cost of synchronous replication by running the existing `tests/mgbench` suite
against a coordinator-managed HA cluster instead of a single instance, so that the resulting
throughput is directly comparable to the standalone numbers already collected.

## Goal

Produce **the same number the standalone mgbench suite produces** — `throughput` in queries per
second as computed by `tests/mgbench/client.cpp` — with a main and two SYNC replicas behind the
Bolt port. The replication tax is then the ratio between two series that differ only in what is
running behind that port.

The consequence that shapes the whole design: the measurement path is reused *verbatim*. The
same client binary, the same query-count calibration, the same phase structure, the same export
format. Nothing about how throughput is computed is re-implemented, because a re-implementation
could not be trusted to be comparable.

## Non-goals

- **Routing.** Benchmarking through a routing connection to the coordinators is deferred until
  `src/communication/bolt/client.hpp` supports routing. Comparing a Python routing driver against
  the C++ direct client would measure the driver, not the cluster.
- **Network latency injection.** `tc`/netem shaping is deferred. Nothing in this design needs to
  change to accommodate it later; see [Future work](#future-work).
- **Other replication modes and topologies.** ASYNC and STRICT_SYNC, more replicas, and multiple
  data centres are out of scope for the first cut. They are YAML edits, not code changes.
- **Replica-side memory.** Only main is measured; see decision 1.

## Cluster topology

Six processes on localhost, distinguished by port: three coordinators, one main, two SYNC
replicas. Main is `instance_1` on Bolt port 7687, matching the default the benchmark client
falls back to.

| Instance | Bolt | Management | Replication / Coordinator |
|---|---|---|---|
| `instance_1` (main) | 7687 | 10011 | replication 10001 |
| `instance_2` (replica) | 7688 | 10012 | replication 10002 |
| `instance_3` (replica) | 7689 | 10013 | replication 10003 |
| `coordinator_1` | 7690 | 10121 | coordinator 10111 |
| `coordinator_2` | 7691 | 10122 | coordinator 10112 |
| `coordinator_3` | 7692 | 10123 | coordinator 10113 |

`REGISTER INSTANCE … WITH CONFIG` without a mode qualifier registers a SYNC replica, so the
topology needs no special syntax; `AS ASYNC` / `AS STRICT_SYNC` is what a future mode sweep would
add.

## Decisions taken

| # | Decision | Choice |
|---|---|---|
| 1 | What is measured | `throughput` from `client.cpp`, unchanged, plus the `latency_stats`, `count` and `retries` it already emits, plus `database: {cpu, memory}` taken from **main only** (peak `VmHWM` at `stop_db`, as today). Main's peak RSS *is* the replication tax — queues, buffers, in-flight deltas — whereas replicas largely mirror main's dataset size |
| 2 | Where the cluster plugs in | A new `MemgraphHA(BaseRunner)` in `tests/mgbench/runners.py`. `BaseRunner` — five lifecycle methods plus `get_database_port` — is the only seam needed, so `benchmark.py`, `client.cpp`, every `Workload` and the export format stay untouched |
| 3 | How it is selected | A new `BenchmarkInstallationType.HA = "ha"`, i.e. `--installation-type ha`, keeping `--vendor-name memgraph`. `BaseRunner.create` builds the registry key as `f"{vendor_name}{installation_type}"` for non-native types, so `MemgraphHA` registers automatically. **A new vendor name would be wrong**: every workload selects its queries behind `match self._vendor: case GraphVendors.MEMGRAPH`, so a new vendor would silently yield no queries. `--installation-type ha` also keeps `get_runner_client` returning `BoltClient`, which only special-cases Docker |
| 4 | Where per-instance flags live | **One cluster-description YAML**, in the shape `interactive_mg_runner` already accepts via `--context-yaml`: top-level keys are instance names, each with `args`, `log_file`, `setup_queries`. This file is the single source of truth for both flags and topology, so changing replication mode or adding replicas later is an edit here rather than in Python. **Amended during implementation**: this originally suggested YAML anchors for shared flag blocks, which does not work here — a top-level anchor key would be read as a seventh instance, so each instance spells its flags out |
| 5 | Data directories | **Runner-injected**, overriding whatever the YAML says. They must be pinned *within* an invocation (the dataset has to survive the phase restarts) and fresh *across* invocations (otherwise the previous run's dataset is still present and the import phase silently benchmarks doubled data). `interactive_mg_runner.start` otherwise defaults each start to `secrets.token_hex(4)`, a fresh random directory, which would lose the dataset between phases |
| 6 | Coordinator state across restarts | **Durable.** All six instances keep their directories for the whole invocation; instances are registered exactly once and never re-registered. `start_all(..., ignore_setup_failures=True)` turns the repeated setup queries into no-ops. The rejected alternative — wiping coordinator state each phase and re-running registration — would call `REGISTER INSTANCE` on a data instance already holding a full dataset, driving it through a role transition whose effect on that data is exactly the kind of thing that would corrupt results rather than fail them |
| 7 | `start_db` readiness contract | Returns only once: a coordinator leader exists, all three data instances are registered and healthy in `SHOW INSTANCES`, a main exists, `SHOW REPLICAS` on main lists both replicas as SYNC and caught up, and main accepts a write. Built from the existing `tests/e2e/high_availability/common.py` helpers (`has_leader`, `has_main`, `show_instances`, `show_replicas`, `wait_until_main_writeable`) wrapped in `mg_utils.mg_sleep_and_assert` |
| 8 | Main identity after a restart | Main **may drift** off `instance_1`. Whichever instance `SHOW INSTANCES` reports as main is used as-is. No attempt is made to force main back with `SET INSTANCE … TO MAIN`, which would have the runner fighting the cluster's own recovery decision and would reintroduce the role-transition risk of decision 6 |
| 9 | How the client follows main | `BoltClient._bolt_port` becomes **dynamic**, resolved from `runner.get_database_port()` when a runner is supplied and falling back to today's `vendor_args["bolt-port"]` otherwise. `MemgraphHA.get_database_port()` returns the Bolt port of whichever instance `SHOW INSTANCES` reports as main. This is required, not cosmetic: `BoltClient` currently caches the port in `__init__` and is constructed once per run, so after drift it would keep writing to a replica and every write would fail. The fallback keeps the standalone path byte-identical in the flags it passes. **Amended during implementation**: `PythonClient` was described here as already resolving its port this way, which was only half true — it takes the runner's port once at construction and caches it, so it had the same staleness. Both clients now resolve per execution. The runner is supplied only for the HA installation type, because `ExternalVendor.get_database_port()` returns a hardcoded 7687 and ignores `vendor_args`, so consulting it would have regressed external vendors |
| 10 | Workloads | `pokec/medium/create/*` and `pokec/medium/update/*` for writes; `pokec/medium/match/*` as a **control**. No new workload code. Each pokec write query is one autocommit transaction, so each pays exactly one SYNC round trip to both replicas — the unit of cost being measured |
| 11 | Workloads excluded | `high_write_set_property` is dropped. Its query is a single `MATCH (n:Node) SET …` over 100 000 nodes — *one* transaction — so it measures the cost of shipping one enormous delta batch, not commit rate. A genuinely different regime, and reporting it alongside per-commit numbers would mislead. The `basic` group is also avoided as a primary target because it mixes reads, writes and aggregations under one group name and so cannot be split by group filtering |
| 12 | Dataset size | `medium` (100 000 vertices / 1 768 515 edges). `small` at 10 000 vertices risks the write queries finishing too fast for stable timing; `large` pays the replication tax on every commit of a 30M-edge import |
| 13 | Import path | Imports through the **fully attached** SYNC cluster — both replicas are present from the first start, so the import itself is replicated. This is the honest configuration, and the alternative (import to a detached main, attach replicas afterwards) contradicts decision 6. `import_results` is already captured and exported, so "import throughput under SYNC replication" comes for free |
| 14 | `clean_db` scope | Clears the three data instances' snapshots only — mirroring today's `rm -Rf memgraph/snapshots/*` — and **never** coordinator state. `save_memory_usage_of_empty_db` calls `clean_db` mid-run; wiping coordinator directories there would destroy the Raft state that makes registration durable and force the re-registration path decision 6 avoids |
| 15 | Worker count | `--num-workers-for-benchmark 6`, matching what `mgbuild.sh` passes for the standalone `mgbench` suite. Comparability requires it |
| 16 | Log level | Data instances and coordinators run at `WARNING`, **not** the `TRACE` used by the e2e HA tests. Trace-level logging on the commit path would dominate the measurement |
| 17 | CI trigger | The existing benchmark gate, no new label: `CI -build=release -test=benchmark` → `run_release_benchmark` → `diff_release.yaml`'s `inputs.run_benchmark`. HA steps are added inside that already-gated job, and a matching step goes into `daily_benchmark.yaml` |
| 18 | Results series | A **separate** bench-graph series and results file: `--benchmark-name "mgbench-replication"` from `benchmark_result_replication.json`, following the `mgbench-planner-optimizations` / `benchmark_result_extended.json` precedent. Sharing either would clobber the file within the job and interleave HA and standalone throughput into one series — and since `pokec/medium/create/vertex` is a valid test name in both legs, the existing standalone history would appear to regress |
| 19 | HA-vs-standalone comparison | Two ordinary invocations plus the existing `compare_results.py`, which already diffs two result JSONs field by field. The ratio is derived downstream, in the dashboard or locally — the harness does not compute it |

## Cluster description

`tests/mgbench/ha_cluster.yaml`, consumed directly by `MemgraphHA`. Cluster-level setup queries
attach to one coordinator; `start_all_keep_others` runs each instance's setup queries in context
order once every process is up.

```yaml
data_defaults: &data_defaults
  - "--log-level=WARNING"

instance_1:
  args: [*data_defaults, "--bolt-port", "7687", "--management-port", "10011"]
  log_file: "instance_1.log"
  setup_queries: []

# instance_2, instance_3, coordinator_1, coordinator_2 likewise

coordinator_3:
  args:
    - "--log-level=WARNING"
    - "--bolt-port=7692"
    - "--coordinator-id=3"
    - "--coordinator-port=10113"
    - "--management-port=10123"
    - "--coordinator-hostname"
    - "localhost"
  log_file: "coordinator_3.log"
  setup_queries:
    - "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}"
    - "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}"
    - "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}"
    - "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'}"
    - "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'}"
    - "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'}"
    - "SET INSTANCE instance_1 TO MAIN"
```

`data_directory` is deliberately absent — decision 5 has the runner inject it.

## Behaviour a user or operator can observe

Running the suite:

```
cd tests/mgbench
./benchmark.py --installation-type ha --num-workers-for-benchmark 6 \
  --export-results benchmark_result_replication.json \
  'pokec/medium/create/*' 'pokec/medium/update/*' 'pokec/medium/match/*'
```

Requires `MEMGRAPH_ENTERPRISE_LICENSE` and `MEMGRAPH_ORGANIZATION_NAME` in the environment — HA
is enterprise-gated (`src/query/interpreter.cpp` rejects `high availability` without a licence),
and `tests/e2e/memgraph.py` starts instances with a bare `Popen`, so the environment is inherited
rather than plumbed. The runner fails fast with a clear message when either is absent, rather
than letting cluster setup fail confusingly.

Output is a `benchmark_result_replication.json` in the same schema as every other mgbench run, so
dashboards, `compare_results.py` and the bench-graph upload need no knowledge that HA was involved.

## Work to do

1. `tests/mgbench/constants.py` — add `HA = "ha"` to `BenchmarkInstallationType` and to
   `get_all_installation_types()`; `--installation-type`'s argparse `choices` derive from it, so
   the flag becomes valid automatically.
2. `tests/mgbench/runners.py` — add `MemgraphHA(BaseRunner)`: load the YAML, inject data
   directories under a per-invocation temp directory, start and stop the cluster through
   `interactive_mg_runner`, implement the decision-7 readiness contract, resolve main via
   `SHOW INSTANCES` for `get_database_port`, return main's `_get_usage` from `stop_db`, and scope
   `clean_db` to the data instances. Importing `interactive_mg_runner` from `tests/mgbench` needs
   a `sys.path` insert of `tests/e2e`, as `tests/e2e/conftest.py` does, because it does
   `from memgraph import *`.
3. `tests/mgbench/runners.py` — make `BoltClient`'s port dynamic per decision 9. `_bolt_port` is
   read only inside `execute()`, so a property backed by the runner needs no call-site changes.
   `PythonClient._database_port` gets the same treatment. `get_runner_client` decides which runners
   are allowed to resolve the port, and hands one over only for the HA installation type.
4. `tests/mgbench/ha_cluster.yaml` — new, as above.
5. `release/package/mgbuild.sh` — a `mgbench-replication` case mirroring the existing `mgbench`
   one, with `--installation-type ha` and the decision-10 target set.
6. `.github/workflows/diff_release.yaml` — run and upload steps inside the existing
   `run_benchmark`-gated job, using the decision-18 name and file.
7. `.github/workflows/daily_benchmark.yaml` — the matching step, and a runtime note in the
   per-suite comment block at the top.
8. `tests/mgbench/README.md` — document the mode, the licence requirement and the YAML.

## Limitations

Stated plainly, because each one bounds how far the resulting numbers can be pushed:

- **Coordinator chatter is inside the measurement.** Coordinators health-check every data
  instance continuously and run Raft among themselves. None of that is on the write path, but it
  is not excluded from the throughput number either. The `match` control group exists to detect
  this: reads on main should show a near-zero delta against standalone, and if they do not, the
  perturbation is the harness or the coordinators rather than replication.
- **The cluster restarts five-plus times per run.** `benchmark.py` brackets every phase — import,
  query-count calibration, authorization setup, each workload group — with `start_db`/`stop_db`.
  Each becomes a six-process restart plus a wait for Raft convergence. This is the dominant added
  wall-clock cost and the most likely source of flakiness.
- **The commit-size axis is sampled, not swept.** pokec's write queries were designed to stress
  the storage engine; none produces a transaction of deliberately controlled delta size. The
  small-commit regime is covered; the large-commit regime is not covered at all now that
  `high_write_set_property` is excluded.
- **Memory is main-only.** Replica memory under replication load is not reported, and adding it is
  a schema change rather than a flag, because `database: {cpu, memory}` holds exactly one value.
- **Import is slower than standalone by construction** (decision 13), on every run.

## Future work

- **Routing leg**, once `communication/bolt/client.hpp` speaks routing: a second benchmark type
  connecting to the coordinators rather than directly to main.
- **`tc`/netem latency injection.** A `NetworkShaper` seam with a no-op default, implemented as a
  `prio` qdisc on `lo` with `u32` filters. Because replicas 2 and 3 already listen on distinct
  replication ports (10002, 10003), per-replica asymmetric latency needs no addressing change —
  the localhost-plus-distinct-ports topology of this design is sufficient. Requires `sudo`.
- **Other replication modes and larger topologies** — ASYNC, STRICT_SYNC, more replicas — as
  additional YAML files rather than code.
- **A purpose-built workload with tunable commit size**, to sweep rather than sample the axis
  between RTT-bound and delta-volume-bound replication cost.
