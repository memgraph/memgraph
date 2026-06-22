# Enforce fine-grained authorization on a derive() overlay read-through

Status: not started

## Parent

`.scratch/projections/PRD.md` (ADR 0005)

## What to build

A `derive()` overlay node stores only the properties it overrides and reads
every other property through to its origin real vertex. Separately, a scanned
projection node is treated as always visible, because a projection mints no
real-graph privileges of its own. Together these may let a user read origin
properties or labels that fine-grained authorization would deny on the real
vertex: the overlay read-through goes straight to the origin accessor, and the
scan visibility check waves the node through.

Verify whether fine-grained authorization is enforced on the overlay
read-through path, and close the gap if it is not. The read-through must apply
the same fine-grained property and label checks the user would face reading the
origin vertex directly, and a scanned overlay node carrying an origin must not
bypass the vertex-level visibility check that a real vertex receives.

This is a check-and-fix: the test is the verification. If the read-through
already enforces the check, the test passes and stays as a regression guard.
Enterprise only, since fine-grained authorization is an enterprise feature.

## Acceptance criteria

- [ ] A test denies a property (and a label) via fine-grained authorization,
      derives an overlay over that node, and reads the unmodified property/label
      through a `USE` scope; the denied value is not exposed.
- [ ] A test confirms a scanned overlay node with an origin is subject to the
      same vertex visibility decision as its origin under fine-grained auth.
- [ ] If a gap is found, the read-through and scan-visibility paths enforce the
      fine-grained checks; if no gap is found, the tests remain as guards and the
      issue records that the path was already safe.
- [ ] Tests are enterprise-gated.

## Blocked by

- None - can start immediately
