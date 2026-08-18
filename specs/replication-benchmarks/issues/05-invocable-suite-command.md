## Parent

`specs/replication-benchmarks.md`

**Type**: AFK

## What to build

A named suite command so the benchmark is invoked identically by a person and by CI.

The build-and-test entry point gains a replication-benchmark command that mirrors the existing
mgbench one but selects the HA installation type and the replication target set, and writes to its
own results file rather than the shared one. Documentation lands with it: how to run the mode, that
it needs an enterprise licence in the environment, and what the cluster-description YAML controls.

The separate results file matters — sharing the standalone one would have whichever suite runs
second clobber the first inside the same job.

## Acceptance criteria

- [ ] A replication-benchmark suite command runs the target set with the HA installation type and the standalone worker count
- [ ] It writes to a results file distinct from the standalone suite's
- [ ] The same command works locally and is what CI will call, with no divergent invocation
- [ ] Documentation covers the mode, the enterprise licence requirement, and the cluster-description YAML
- [ ] The existing standalone suite command is unchanged

## Blocked by

- `03-full-target-set-medium.md`
