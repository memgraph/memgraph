## Parent

`specs/replication-benchmarks.md`

**Type**: HITL

## What to build

Nothing is built here. This is the judgment call the design deliberately left to a human.

With the full target set running, someone has to read the first results and decide whether the
measurement is trustworthy enough to publish. Two known sources of doubt are inside the number by
construction: the coordinators health-check every data instance continuously and run Raft among
themselves, and the cluster restarts several times per run while waiting for convergence each
time. Neither is on the write path, and neither is excluded from the throughput figure.

The read control group is the instrument for this. Reads on main should be essentially unaffected
by synchronous replication, so a near-zero delta against the standalone series means the harness
is quiet; a visible delta means something other than replication is being measured.

Findings that change the agreed design belong in an amendment to the parent spec, noting what
changed and why.

## Acceptance criteria

- [ ] The read control delta against the standalone series has been reviewed and judged acceptable or not
- [ ] Run-to-run variance across several local runs has been eyeballed, so it is known whether the write numbers are stable enough to trend
- [ ] A decision is recorded on whether the medium dataset size stays
- [ ] Any conclusion that contradicts the parent spec is written up as an amendment to it

## Blocked by

- `03-full-target-set-medium.md`
