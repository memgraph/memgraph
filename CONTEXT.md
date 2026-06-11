# CONTEXT

Domain glossary for Memgraph. Add terms as decisions resolve; use these exact
words in issues, tests, and proposals rather than drifting to synonyms.

## Projections and derived views

- **Subgraph** - a derived view whose nodes and edges are **real accessors**,
  produced by `project(...)`. A write to a node in a subgraph persists to the
  real store. Membership is checked on mutation. Backed by `SubgraphDbAccessor`
  / `SubgraphVertexAccessor`.

- **Projection** - a derived view built from **overlay** and/or **synthetic**
  nodes, produced by `derive(path, config)`. Lazy and writable per a static
  per-property binding.

- **Projected node** - the single value type for every node in a derived view
  (see `docs/adr/0001-projected-node.md`). Holds an **optional origin** real
  vertex plus its own **overlay property store**. "Overlay node" and "synthetic
  node" are its two configurations, not separate types.

- **Overlay node** - a projected node **with** an origin. Reads fall through to
  the origin lazily (origin properties are never copied unless read); declared
  overlay keys shadow the origin on read. Writes target the origin (persist) or a
  declared overlay key (compute), never both for the same key.

- **Synthetic node** - a projected node with **no origin**: an overlay store
  only. Produced by `virtualNode(...)` or imported from external data. Reads and
  writes both hit its overlay. `SET` on a synthetic node never errors.

- **Handle** (import key) - the integer a user passes to `virtualNode(gid, ...)`.
  It is **not** the node's identity (that is a synthetic gid) and is **not**
  stored on the node or on an edge. It is a key used once, at list assembly, to
  wire `virtualEdge(type, from_gid, to_gid)` endpoints to nodes by their declared
  gid.

- **Binding** - the store a single property is fixed to: `origin` (read-through
  and write-through, persisted), `overlay` (read and write hit the overlay,
  compute-only), or `hidden` (not visible). Read source and write target are
  **coupled** to one store per property - they cannot be chosen independently,
  or a read-after-write returns the stale shadowed value.

- **Static schema** - the set of overlay/hidden keys is fixed from the
  `derive()` config at construction. A write to an **undeclared** key goes to
  the origin; overlay keys must be declared. This is what lets the projection
  schema ship in the pre-stream Bolt header.

- **Projection tag** / **projection-schema table** - the non-breaking Bolt
  provenance channel. Each projected node carries a small reserved property (the
  tag) referencing a schema table sent once in the RUN header (not the trailing
  summary, so streaming clients resolve provenance as nodes arrive). An overlay
  node serializes at its **origin's `element_id`**; a synthetic node keeps its
  synthetic id. Property values are never wrapped - provenance is schema, not
  per-value tags.

## Identity

- **Synthetic GID** - identity for virtual/overlay elements, counted **down from
  `UINT64_MAX`**, collision-free against real GIDs (which count up from 0).
