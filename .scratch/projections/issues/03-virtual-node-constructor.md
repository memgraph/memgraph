# virtualNode() constructor

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md`

## What to build

A `virtualNode(gid, labels, props)` query function that constructs a **synthetic
node** - a node with no origin, holding an overlay property store only. It
returns the existing `VirtualNode` TypedValue, renders in Lab via the existing
Bolt path, and supports `SET` mutating its overlay (never errors, since the
overlay is its only store). Accept a single label or a list; settle null-vs-
missing argument handling.

## Acceptance criteria

- [ ] `RETURN virtualNode(1, ["A","B"], {x: 1})` returns a node with those labels and properties
- [ ] `RETURN virtualNode(1, "A", {x: 1})` (single label form) works
- [ ] The node carries a synthetic GID (counted down from `UINT64_MAX`), distinct from real GIDs
- [ ] `SET` on a synthetic node mutates its overlay and does not error
- [ ] The node serializes over Bolt and renders in Lab as an ordinary node
- [ ] e2e test asserting construction, properties, and `SET` behavior

## Blocked by

None - can start immediately (parallel track).
