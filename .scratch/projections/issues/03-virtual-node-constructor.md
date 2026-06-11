# virtualNode() constructor

Status: done

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

## Comments

Implemented as a builtin function `virtualNode(handle, labels, props)` returning
a `VirtualNode` TypedValue. Labels accept a single string or a list; the node
takes a fresh synthetic gid and serializes over Bolt as an ordinary node. `SET`
on a synthetic node (per-property, `+=`, and `=`) mutates its overlay; label
writes (`SET n:Label`) are not yet handled.

Design decision (the `gid` argument): the node's identity is a synthetic gid,
not the user-supplied `gid`. The `gid` is a logical **handle** used to wire
virtual edges by reference when a projection is assembled. `virtualNode()`
accepts and validates the handle but does not yet persist it on the node;
handle storage and resolution land in `04-virtual-edge-constructor`, its first
consumer.
