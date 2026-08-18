## Parent

`specs/replication-benchmarks.md`

**Type**: AFK

## What to build

Make the benchmark client follow main rather than assume where it is.

The client currently fixes main's Bolt port when it is constructed, which happens once per run.
Because the cluster restarts between phases and the coordinators may promote a different data
instance to main on recovery, a fixed port can end up addressing a replica — where every write
fails. The client instead resolves main's port from the runner each time it executes, and the
runner answers from whichever instance the cluster reports as main. When no runner is involved,
the client falls back to the previously configured port, leaving the standalone benchmark path
untouched.

Main drift is accepted rather than corrected: whichever instance holds the role is used as-is,
and no attempt is made to move the role back.

## Acceptance criteria

- [x] The client resolves main's Bolt port at execution time rather than caching it at construction
- [x] The runner derives main from the cluster's own view of instance roles
- [x] With no runner supplied, the client passes exactly the same connection flags it passes today, so standalone runs are unchanged
- [ ] A run completes successfully when main is an instance other than the one on the default port, verified by pointing the description YAML's main election at a different instance (needs a built binary)
- [ ] An existing standalone benchmark run still completes and reports throughput (needs a built binary)

## Blocked by

- `01-ha-runner-tracer-bullet.md`
