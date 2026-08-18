## Parent

`specs/replication-benchmarks.md`

**Type**: AFK

## What to build

Widen the smoke test into the real measurement.

The suite runs the pokec write groups — node creation and property updates — at the medium
dataset size with the same worker count the standalone suite uses, so the resulting throughput is
directly comparable. Alongside them it runs a read group as a control: reads on main are not
affected by synchronous replication, so a near-zero delta against the standalone series is
evidence the harness itself is not perturbing the measurement, and a visible delta is evidence
that it is.

The dataset is imported through the fully attached cluster, so the import is itself replicated and
its throughput is captured as a byproduct.

## Acceptance criteria

- [x] Write groups and the read control group all run at the medium dataset size
- [x] The worker count matches what the standalone suite uses
- [ ] The result JSON contains entries for every targeted group, including the control (needs a built binary)
- [ ] Import throughput through the attached cluster is captured in the results (needs a built binary)
- [ ] The read control delta against an equivalent standalone run is measured and recorded for review (needs a built binary)

## Blocked by

- `01-ha-runner-tracer-bullet.md`
