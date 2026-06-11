# derive() overlay projection - origin reference + lazy read-through

Status: done

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

- [x] `WITH derive(p, {}) AS projection` yields overlay nodes referencing their origin vertices
- [x] Reading an origin property through an overlay node returns the origin value
- [x] A `derive()` over nodes with a large property does not grow memory by that property's size unless it is read (proven behaviourally; see below)
- [x] Origin property reads reflect the transaction's view (no stale cache)
- [x] e2e test asserting read-through values and the no-copy memory behavior

## Blocked by

- `01-project-subgraph-constructor`

## Comments

Implemented the unified node from `docs/adr/0001-projected-node.md` on the
`VirtualNode` class: it carries an optional origin vertex, property reads fall
through to the origin lazily (latest transaction view, never cached), and
overlay keys shadow. `derive()` sets the origin and no longer copies inherited
properties, so a projection does not duplicate origin properties unless read.
e2e tests cover read-through values, the no-copy/lazy behaviour (mutating the
origin after `derive()` is visible through the overlay node), and the merged
property view.

### Remainder, now closed

- **Bare `derive(p, {})`.** `virtualEdgeType` is now required only when the path
  has edges (an edge needs a type to name it; an overlay node does not). So
  `derive(p, {})` over a single-vertex path projects its overlay node with no
  options; a path with edges still requires the type. e2e covers both.
- **No-copy memory assertion.** Closed by the behavioural proof, not a memory
  counter. Mutating the origin after `derive()` shows through the overlay node,
  which is only possible if the property was never copied. A counter-based
  assertion was rejected: the only seam is `SHOW STORAGE INFO`'s RES-based
  `memory_res` / `peak_memory_res`, which is noisy and measures transient
  per-query memory unreliably, so it would be flaky in CI.
