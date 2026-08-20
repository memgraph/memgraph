# Replication benchmarks — design

**Status**: Draft. Design complete. Slices 01 and 02 implemented on `testing/repl-benchmarks`,
neither yet run against a built binary. **Designed**: 2026-08-18.

Benchmarks the cost of synchronous replication by running the existing `tests/mgbench` suite
against a coordinator-managed HA cluster instead of a single instance, so that the resulting
throughput is directly comparable to the standalone numbers already collected.

## Goal

Produce **the same number the standalone mgbench suite produces** — `throughput` in queries per
second as computed by `tests/mgbench/client.cpp` — with a main and one SYNC replica behind the
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

Five processes on localhost, distinguished by port: three coordinators, one main, one SYNC
replica. Main is `instance_1` on Bolt port 7687, matching the default the benchmark client
falls back to.

**Amended during implementation**: the topology started as main plus *two* SYNC replicas and was
reduced to one. Nothing in the runner encodes the count — the readiness contract derives the
expected replica count from the description — so this is a YAML edit, which is what decision 4 was
for.

| Instance | Bolt | Management | Replication / Coordinator |
|---|---|---|---|
| `instance_1` (main) | 7687 | 10011 | replication 10001 |
| `instance_2` (replica) | 7688 | 10012 | replication 10002 |
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
| 3 | How it is selected | A new `BenchmarkInstallationType.HA = "ha"`, keeping `--vendor-name memgraph`. **Amended during implementation**: `ha` is no longer offered as an `--installation-type` value. High availability is orthogonal to how a single Memgraph is installed — a cluster is a topology, not an installation — so it is selected by `--ha-only` for a cluster-only run or `--run-ha` to measure a single instance and a cluster in one invocation, and neither combines with `docker` or `external`. The constant stays, because it is what `BaseRunner.create` keys the cluster runner on. `BaseRunner.create` builds the registry key as `f"{vendor_name}{installation_type}"` for non-native types, so `MemgraphHA` registers automatically. **A new vendor name would be wrong**: every workload selects its queries behind `match self._vendor: case GraphVendors.MEMGRAPH`, so a new vendor would silently yield no queries. `--installation-type ha` also keeps `get_runner_client` returning `BoltClient`, which only special-cases Docker. **Amended during implementation**: this was not sufficient on its own. `sanitize_args` and the benchmark-context construction gated `--vendor-binary` on `native` and `--client-binary` on `native`/`external`, so an HA run received `client_binary=None` and could not execute a single query. Both gates now include HA, via `BenchmarkInstallationType.get_local_binary_installation_types()`, which also gives HA the binary auto-detection and path validation that native already had |
| 4 | Where per-instance flags live | **One cluster-description YAML**, in the shape `interactive_mg_runner` already accepts via `--context-yaml`: top-level keys are instance names, each with `args`, `log_file`, `setup_queries`. This file is the single source of truth for both flags and topology, so changing replication mode or adding replicas later is an edit here rather than in Python. **Amended during implementation**: this originally suggested YAML anchors for shared flag blocks, which does not work here — a top-level anchor key would be read as a seventh instance, so each instance spells its flags out |
| 5 | Data directories | **Runner-injected**, overriding whatever the YAML says. They must be pinned *within* an invocation (the dataset has to survive the phase restarts) and fresh *across* invocations (otherwise the previous run's dataset is still present and the import phase silently benchmarks doubled data). `interactive_mg_runner.start` otherwise defaults each start to `secrets.token_hex(4)`, a fresh random directory, which would lose the dataset between phases |
| 6 | Coordinator state across restarts | **Durable.** Every instance keeps its directory for the whole invocation; instances are registered exactly once and never re-registered. `start_all(..., ignore_setup_failures=True)` turns the repeated setup queries into no-ops. The rejected alternative — wiping coordinator state each phase and re-running registration — would call `REGISTER INSTANCE` on a data instance already holding a full dataset, driving it through a role transition whose effect on that data is exactly the kind of thing that would corrupt results rather than fail them |
| 7 | `start_db` readiness contract | Returns only once: a coordinator leader exists, every data instance is registered and healthy in `SHOW INSTANCES`, a main exists, `SHOW REPLICAS` on main lists every replica as SYNC and caught up, and main accepts a write. The expected replica count comes from the cluster description rather than being hardcoded, so changing the topology needs no code change. Built from the existing `tests/e2e/high_availability/common.py` helpers (`has_leader`, `has_main`, `show_instances`, `show_replicas`, `wait_until_main_writeable`) wrapped in `mg_utils.mg_sleep_and_assert` |
| 8 | Main identity after a restart | Main **may drift** off `instance_1`. Whichever instance `SHOW INSTANCES` reports as main is used as-is. No attempt is made to force main back with `SET INSTANCE … TO MAIN`, which would have the runner fighting the cluster's own recovery decision and would reintroduce the role-transition risk of decision 6 |
| 9 | How the client follows main | `BoltClient._bolt_port` becomes **dynamic**, resolved from `runner.get_database_port()` when a runner is supplied and falling back to today's `vendor_args["bolt-port"]` otherwise. `MemgraphHA.get_database_port()` returns the Bolt port of whichever instance `SHOW INSTANCES` reports as main. This is required, not cosmetic: `BoltClient` currently caches the port in `__init__` and is constructed once per run, so after drift it would keep writing to a replica and every write would fail. The fallback keeps the standalone path byte-identical in the flags it passes. **Amended during implementation**: `PythonClient` was described here as already resolving its port this way, which was only half true — it takes the runner's port once at construction and caches it, so it had the same staleness. Both clients now resolve per execution. The runner is supplied only for the HA installation type, because `ExternalVendor.get_database_port()` returns a hardcoded 7687 and ignores `vendor_args`, so consulting it would have regressed external vendors |
| 10 | Workloads | `pokec/medium`, targeting **all 8 distinct write shapes plus 2 reads as a control**: `create/*` (4 queries), `arango/single_vertex_write`, `arango/single_edge_write`, `arango/unwind_range_vertex_write`, `basic/single_vertex_property_update_update`, and `arango/single_vertex_read` + `arango/aggregate` as the control. 10 queries, no new workload code. Each write is one autocommit transaction, so each pays exactly one SYNC round trip to every replica — the unit of cost being measured. `unwind_range_vertex_write` is the deliberate exception at 100 nodes per commit, the only point where the delta batch is large enough to show bandwidth cost rather than round-trip cost. **Amended during implementation**: this was originally `create/*` + `update/*` for writes with all of `match/*` as control. Enumerating the queries showed that pokec contains exactly 11 writes, that the published dashboard series come from the `arango`, `create` and `match` groups, and that group-level filtering therefore both missed arango's three writes and dragged in 25 reads. Reads are held to two on purpose: they are only a harness sanity check, and every query costs its own cluster restart |
| 11 | Workloads excluded | `high_write_set_property` is dropped. Its query is a single `MATCH (n:Node) SET …` over 100 000 nodes — *one* transaction — so it measures the cost of shipping one enormous delta batch, not commit rate. A genuinely different regime, and reporting it alongside per-commit numbers would mislead. The `basic` group is not used as a group target because it mixes reads, writes and aggregations under one name; one query is named individually instead. **Amended during implementation**: two of the three writes in `basic` are byte-identical Cypher to `arango/single_vertex_write` and `arango/single_edge_write`, so only `single_vertex_property_update_update` is taken from it. `update/vertex_on_property` is dropped as well: it matches on `(n {id: $id})` with no label, so it scans rather than seeks and the commit it is meant to measure is lost in the scan time — `basic/single_vertex_property_update_update` is the same write against an index |
| 12 | Dataset size | `medium` (100 000 vertices / 1 768 515 edges). `small` at 10 000 vertices risks the write queries finishing too fast for stable timing; `large` pays the replication tax on every commit of a 30M-edge import |
| 13 | Import path | Imports through the **fully attached** SYNC cluster — every replica is present from the first start, so the import itself is replicated. This is the honest configuration, and the alternative (import to a detached main, attach replicas afterwards) contradicts decision 6. `import_results` is already captured and exported, so "import throughput under SYNC replication" comes for free |
| 14 | `clean_db` scope | Clears the three data instances' snapshots only — mirroring today's `rm -Rf memgraph/snapshots/*` — and **never** coordinator state. `save_memory_usage_of_empty_db` calls `clean_db` mid-run; wiping coordinator directories there would destroy the Raft state that makes registration durable and force the re-registration path decision 6 avoids |
| 15 | Worker count | `--num-workers-for-benchmark 6`, matching what `mgbuild.sh` passes for the standalone `mgbench` suite. Comparability requires it |
| 16 | Log level | Data instances and coordinators run at `WARNING`, **not** the `TRACE` used by the e2e HA tests. Trace-level logging on the commit path would dominate the measurement |
| 17 | CI trigger | **Amended during implementation**: the cluster benchmark no longer runs in the diff workflow. It runs as `mgbench-ha`, its own nightly suite and its own bench-graph series, placed first in the nightly job with every other benchmark step temporarily disabled while it is brought up. Run 32227509791 is why: folded into the diff workflow's `mgbench` step it pushed that step past 58 minutes against a 100 minute job budget shared with a dozen other steps, and PR feedback is the wrong place to absorb that. The combined single-invocation form stays supported through `--run-ha`, and with it the `ha_results` upload field, but CI does not use it today. Originally: the existing benchmark gate, no new label: `CI -build=release -test=benchmark` → `run_release_benchmark` → `diff_release.yaml`'s `inputs.run_benchmark`. HA steps are added inside that already-gated job, and a matching step goes into `daily_benchmark.yaml` |
| 18 | Results series | **Amended during implementation**: this originally shipped a separate bench-graph series, `mgbench-replication` from its own results file, on the grounds that both legs publish the same test names. That is superseded. The two legs now run in **one** `benchmark.py` invocation and upload as **one** measurement, the single instance leg in `results` and the cluster leg in a new `ha_results` field, which needs a column on the bench-graph server. One invocation buys two things a second series could not: the query count cache is a per-process dict, so both legs execute the same number of queries by construction rather than by depending on which one calibrated first — the condition `compare_results.py` refuses to work without — and with both legs in one row the replication cost is arithmetic between two fields rather than a join across series. The legs keep separate target sets, since every query measured against a cluster restarts every instance in it |
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

# instance_2, coordinator_1, coordinator_2 likewise

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
    - "SET INSTANCE instance_1 TO MAIN"
```

`data_directory` is deliberately absent — decision 5 has the runner inject it.

## Behaviour a user or operator can observe

Running the suite:

```
cd tests/mgbench
./benchmark.py --installation-type ha --num-workers-for-benchmark 6 \
  --export-results benchmark_result_replication.json \
  'pokec/medium/create/*' \
  pokec/medium/arango/single_vertex_write \
  pokec/medium/arango/single_edge_write \
  pokec/medium/arango/unwind_range_vertex_write \
  pokec/medium/basic/single_vertex_property_update_update \
  pokec/medium/arango/single_vertex_read \
  pokec/medium/arango/aggregate
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
  is not excluded from the throughput number either. The two control reads exist to detect this:
  reads on main should show a near-zero delta against standalone, and if they do not, the
  perturbation is the harness or the coordinators rather than replication.
- **The cluster restarts once per query, not once per phase.** `benchmark.py` brackets each
  query's measured run with `start_db`/`stop_db`, and its calibration too when the count is not
  cached, on top of the empty-database measurement and the import. Every one of those is a
  five-process restart plus a wait for Raft convergence, so wall-clock scales with the size of the
  target set — about a dozen restarts for the 10 queries of decision 10. This is the dominant added
  cost and the most likely source of flakiness.
- **The commit-size axis is sampled, not swept.** pokec's write queries were designed to stress
  the storage engine; none produces a transaction of deliberately controlled delta size. Seven of
  the eight writes are single-entity commits and `unwind_range_vertex_write` adds one mid-size point
  at 100 nodes, so the axis has two samples rather than a sweep.
- **Memory is main-only.** Replica memory under replication load is not reported, and adding it is
  a schema change rather than a flag, because `database: {cpu, memory}` holds exactly one value.
- **Import is slower than standalone by construction** (decision 13), on every run.
- **The cluster leg needs the e2e virtualenv.** Driving `interactive_mg_runner` pulls in `mgclient`
  through its star import of the e2e `memgraph` module, so the benchmark has to run from `tests/ve3`,
  built from `tests/requirements.txt`, which is what the e2e and stress suites already activate. This
  surfaced only in CI and only once the cluster leg existed, because the single-instance path never
  imports the runner. The import is now wrapped so the failure names the virtualenv rather than
  reporting a missing module several imports deep.
- **The fine-grained authorization pass doubles a leg's runtime, so the cluster leg skips it.**
  On run 32227509791 the mgbench step ran past 58 minutes with the pass enabled on both legs, against
  a 100 minute job budget shared with a dozen other benchmark steps. It is now off for the cluster leg
  added by `--run-ha` and available through `--ha-authorization`, and the diff and nightly job timeouts
  were raised to 130 and 900 minutes. Those figures are extrapolations from one over-budget run rather
  than measurements.
- **Authorization on a cluster works, it is just not on by default.** It forced a change in the
  runner, because it is the one thing in a run that makes the cluster stop accepting anonymous
  connections: it creates a user partway through and tells
  only the benchmark client the credentials, while the runner still needs to connect on its own
  account to check readiness. The probe therefore tries the credentials from the cluster description
  first and the benchmark's own second, and both states genuinely occur in one run, since the pass
  drops the user again when it finishes. An earlier revision of this design refused to run the pass
  at all and skipped it via `--no-authorization`; that was wrong — authorization overhead under
  replication is a legitimate thing to measure, and the harness should not decide otherwise.
- **The query-count cache is shared between the two legs.** `get_query_cache_count` keys its cache
  on `workload/variant/group/query` only — not on vendor or installation type — and persists it to
  `tests/mgbench/.cache/config.json`. Sharing it is a requirement, not an accident:
  `compare_results.py` refuses to diff two runs whose `count` or `num_workers` differ, raising
  `Incompatible results!`, so two independently calibrated legs could not be compared at all. The
  exposure is that whichever leg calibrates a query first authors its count, and the HA leg is slower
  per commit, so it would author a smaller count for the same single-threaded target. **Ordering is
  the guard**: the standalone suite runs first, calibrates all 60 pokec queries against this suite's
  10 — a strict superset — and an existing entry is never rewritten, since `set_value` is reached
  only on a cache miss. CI gets this for free, because `actions/checkout` cleans ignored files so
  every job starts with no `config.json` and the standalone step is the first to run. An earlier
  version of this design also passed `--no-save-query-counts` to the HA leg as a belt-and-braces
  guard for a cold cache; it was dropped. It is a no-op whenever ordering holds, it makes the HA
  invocation diverge from the standalone one for no gain in a harness whose whole claim is an
  identical measurement path, and because counts would then never persist it would charge anyone
  iterating on the HA suite alone ten extra five-process cluster restarts on every run. Resetting
  calibration is `rm tests/mgbench/.cache/config.json`, which leaves the cached datasets beside it
  untouched.

## Future work

- **Routing leg**, once `communication/bolt/client.hpp` speaks routing: a second benchmark type
  connecting to the coordinators rather than directly to main.
- **`tc`/netem latency injection.** A `NetworkShaper` seam with a no-op default, implemented as a
  `prio` qdisc on `lo` with `u32` filters matched on the replication port, so no addressing change
  is needed — the localhost-plus-distinct-ports topology of this design is sufficient. Should the
  topology grow back to several replicas, each listens on its own replication port, so asymmetric
  per-replica latency stays reachable the same way. Requires `sudo`.
- **Other replication modes and larger topologies** — ASYNC, STRICT_SYNC, more replicas — as
  additional YAML files rather than code.
- **A purpose-built workload with tunable commit size**, to sweep rather than sample the axis
  between RTT-bound and delta-volume-bound replication cost.
