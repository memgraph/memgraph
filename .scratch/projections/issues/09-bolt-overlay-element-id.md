# Bolt: overlay node serializes at its origin element_id

Status: done

## Parent

`.scratch/projections/PRD.md`

## What to build

An overlay node serializes over Bolt at its **origin's `element_id`** (and GID),
with a merged property map (overlay shadows origin), so a click, expand, or edit
in Lab maps back to the real node. A synthetic node keeps its synthetic id. No
change to the Bolt `Node` structure shape (fixed-arity); this is purely which id
and which properties are written. Follow-on to the overlay model - not
MVP-blocking.

## Acceptance criteria

- [ ] An overlay node's serialized `element_id`/GID equals its origin's
- [ ] An overlay node's serialized properties are the merged view (overlay values shadow origin)
- [ ] A synthetic node still serializes with its synthetic id
- [ ] No new field added to the Bolt `Node` structure (existing drivers unaffected)
- [ ] Test at the glue/communication seam asserting the serialized id and property map

## Blocked by

- `06-derive-overlay-read-through`

## Comments

`ToBoltVertex(const query::VirtualNode &)` in `src/glue/communication.cpp` now picks the
origin's gid for the Bolt `id`/`element_id` when the node has an origin, and the synthetic
gid otherwise. One line; the property map was already the node's merged view (overlay
shadows origin, hidden omitted), so it needed no change. The Bolt `Node` structure shape
is untouched, so existing drivers are unaffected.

The Cypher `id(n)` function still returns the synthetic `CypherId()` for an overlay node;
this slice changes only the serialized Node identity, which is the channel a client uses
to resolve a node back to the real store. The two are deliberately separate.

e2e (`tests/e2e/write_procedures/virtual_graph.py`, `TestBoltOverlaySerialization`) asserts
the decoded `.id` equals the origin's for an overlay node, the merged/shadowed property
map, and the negative synthetic `.id` for a node with no origin - exercised through the
full glue path, the seam the PRD endorses.

The per-node provenance tag and the RUN-header schema table are `10-bolt-provenance-channel`.
