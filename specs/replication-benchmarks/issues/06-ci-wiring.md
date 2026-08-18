## Parent

`specs/replication-benchmarks.md`

**Type**: AFK

## What to build

Run the replication benchmark wherever the standalone benchmark already runs, with no new trigger.

The existing benchmark label and its workflow gate stay exactly as they are; the replication suite
becomes additional run-and-upload steps inside that already-gated job, and a matching step in the
nightly benchmark workflow. Results upload to their own dashboard series.

The separate series is the load-bearing detail. The same test names appear in both the standalone
and HA legs, so uploading HA throughput into the standalone series would interleave two
populations and make the existing history look as though it had suddenly regressed.

## Acceptance criteria

- [x] The replication suite runs whenever the standalone benchmark suite runs, driven by the existing benchmark label and workflow gate
- [x] No new label, input or gate is introduced
- [ ] A step is added to the nightly benchmark workflow, with its expected runtime noted alongside the other suites (step added; the 10-minute figure is an unmeasured estimate and the job timeout was raised from 550 to 660 on that basis)
- [x] Results upload under a dashboard series distinct from the standalone one, from the distinct results file
- [x] The standalone series is unaffected, verified by confirming its name and results path are unchanged

## Blocked by

- `05-invocable-suite-command.md`
