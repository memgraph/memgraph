# Bolt: overlay node serializes at its origin element_id

Status: ready-for-agent

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
