## Parent

`specs/replication-benchmarks.md`

**Type**: AFK

## What to build

The end-to-end path that turns a single mgbench invocation into a benchmark number measured
against a coordinator-managed HA cluster.

A new installation type selects an HA runner while the vendor stays `memgraph`, so every existing
workload continues to resolve its queries. The runner reads a cluster-description YAML, boots
three coordinators plus a main and one SYNC replica, waits for the cluster to become genuinely
usable, hands the benchmark client main's Bolt port, and reports main's CPU and peak memory when
the cluster stops. The dataset survives the repeated cluster restarts that a benchmark run
performs between its phases.

This slice deliberately runs only one small query group on the smallest pokec variant. The point
is to prove the whole path works, not to produce publishable numbers.

It cannot be split further: a benchmark run unconditionally performs an empty-database
measurement, a database clean, an import, a query-count calibration pass and the benchmark
itself, each bracketed by a cluster start and stop. Any run at all therefore exercises the full
restart cycle, and anything less than a run produces no verifiable number.

## Acceptance criteria

- [ ] A new installation-type value selects the HA runner, with the vendor name left as `memgraph` so workload query selection is unaffected
- [ ] The cluster — three coordinators, one main, one SYNC replica, all on localhost distinguished by port — comes up from the description YAML
- [ ] Starting the cluster returns only after a coordinator leader exists, all data instances are registered and healthy, a main exists and accepts a write, and every replica reports as SYNC and caught up
- [ ] Instances run at `WARNING` log level, not `TRACE`
- [ ] Data directories are pinned for the whole invocation and fresh at the start of each new invocation
- [ ] Repeated start/stop cycles within one run succeed, and the imported dataset survives them
- [ ] Cleaning the database clears the data instances only; coordinator state survives, and no instance is ever re-registered
- [ ] Reported CPU and peak memory come from main alone
- [ ] A missing enterprise licence or organization name fails immediately with a message naming what is absent, rather than failing during cluster setup
- [ ] One small query group on the smallest pokec variant produces a result JSON with non-zero throughput, in the same schema as a standalone run

## Blocked by

None - can start immediately
