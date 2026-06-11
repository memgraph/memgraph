# derive() overlay projection - origin reference + lazy read-through

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md`

## What to build

A `derive(path, config)` query function that yields an **overlay projection**:
each projected node is an **overlay node** holding a reference to its origin
real vertex plus its own overlay store. Reads fall through to the origin lazily
- `origin.GetProperty(p)` every time, never cached - so origin properties are
not copied into compute. This is the read model only; per-property binding and
write-back are separate slices. The bare `derive(p, {})` form is supported.

The crucial behavior is the memory property: constructing an overlay projection
over nodes with large properties (e.g. vector embeddings) must not duplicate
those properties unless they are read.

## Acceptance criteria

- [ ] `WITH derive(p, {}) AS projection` yields overlay nodes referencing their origin vertices
- [ ] Reading an origin property through an overlay node returns the origin value
- [ ] A `derive()` over nodes with a large property does not grow memory by that property's size unless it is read
- [ ] Origin property reads reflect the transaction's view (no stale cache)
- [ ] e2e test asserting read-through values and the no-copy memory behavior

## Blocked by

- `01-project-subgraph-constructor`
