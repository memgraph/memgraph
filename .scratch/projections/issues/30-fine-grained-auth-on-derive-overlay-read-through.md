# Enforce fine-grained authorization on a derive() overlay read-through

Status: done

## Resolution

A gap was found and closed at the scan-visibility seam.

Memgraph's fine-grained authorization is label-based (and edge-type-based) for
reads; there is no per-property read permission. So the protection that applies
to an overlay read-through is the label-based vertex-visibility decision its
origin would receive: if a user may not read the origin vertex (a denied label),
they must not read it through an overlay either. The read-through reaches origin
properties only after the overlay node is produced by a scan, so enforcing
visibility at the scan covers the read-through: a node you cannot scan, you
cannot read properties or labels from.

`ScannedVertexVisible` waved every `VirtualNode` through unconditionally. It now
gives an overlay node (one carrying an origin) the same READ visibility decision
as its origin vertex; a synthetic node (no origin, no real-graph data) stays
always-visible. A scanned overlay over a denied origin is filtered exactly as
the origin would be.

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

- [x] A label is denied via fine-grained authorization, an overlay carries that
      origin, and a scan over the projection does not expose it. Property-level
      denial is not a Memgraph feature (auth is label/edge-type only), so the
      label-gated vertex visibility is the applicable check; filtering the node
      at scan keeps both its labels and its read-through properties unexposed.
      Covered by `query_graph_view.ScanFiltersOverlayWhoseOriginIsDenied`.
- [x] A scanned overlay node with an origin gets the same READ visibility
      decision as its origin: the test grants one origin's label and denies
      another, and only the granted overlay (plus the origin-less synthetic node)
      survives the scan.
- [x] The gap was real and is closed in `ScannedVertexVisible`: an overlay's
      origin is checked, a synthetic node stays visible. The test fails without
      the change.
- [x] The test is enterprise-gated (`MG_ENTERPRISE`).

## Blocked by

- None - can start immediately
