## Parent

`specs/replication-benchmarks.md`

**Type**: AFK

## What to build

Widen the smoke test into the real measurement.

The suite runs every distinct write shape pokec contains — eight queries covering a bare node
create, a labelled node create, a pattern create, a wide node create, two indexed edge creates, an
indexed property update, and one create of 100 nodes in a single commit — at the medium dataset size
with the same worker count the standalone suite uses, so the throughput is directly comparable.

Two reads come along as a control: one indexed vertex read and one aggregate. Reads on main are not
affected by synchronous replication, so a near-zero delta against the standalone series is evidence
the harness is not perturbing the measurement, and a visible delta is evidence that it is. The
control is deliberately small, because every query costs its own cluster restart and so reads are
priced the same as writes here.

The dataset is imported through the fully attached cluster, so the import is itself replicated and
its throughput is captured as a byproduct.

## Acceptance criteria

- [x] All eight distinct pokec write shapes and the two control reads run at the medium dataset size
- [x] The worker count matches what the standalone suite uses
- [ ] The result JSON contains entries for every targeted group, including the control (needs a built binary)
- [ ] Import throughput through the attached cluster is captured in the results (needs a built binary)
- [ ] The read control delta against an equivalent standalone run is measured and recorded for review (needs a built binary)

## Blocked by

- `01-ha-runner-tracer-bullet.md`
