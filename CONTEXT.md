# CONTEXT

Domain glossary for Memgraph. Add terms as decisions resolve; use these exact
words in issues, tests, and proposals rather than drifting to synonyms.

## Projections and derived views

- **Subgraph** - a derived view whose nodes and edges are **real accessors**,
  produced by `project(...)`. A write to a node in a subgraph persists to the
  real store. Membership is checked on mutation. Backed by `SubgraphDbAccessor`
  / `SubgraphVertexAccessor`.

- **Projection** - a derived view built from **overlay** and/or **synthetic**
  nodes. Produced by `derive(path, config)` (lazy and writable per a static
  per-property binding) or assembled by `virtualGraph(nodes, edges, config)` from
  lists of synthetic elements to import an external graph in one query.

- **Projected node** - the single value type for every node in a derived view
  (see `docs/adr/0001-projected-node.md`), realized by the C++ `VirtualNode`
  class. Holds an **optional origin** real vertex plus its own **overlay property
  store**. "Overlay node" and "synthetic node" are its two configurations, not
  separate types.

- **Overlay node** - a projected node **with** an origin. Reads fall through to
  the origin lazily (origin properties are never copied unless read); declared
  overlay keys shadow the origin on read. Writes target the origin (persist) or a
  declared overlay key (compute), never both for the same key.

- **Synthetic node** - a projected node with **no origin**: an overlay store
  only. Produced by `virtualNode(...)` or imported from external data. Reads and
  writes both hit its overlay. `SET` on a synthetic node never errors.

- **Synthetic edge** - a typed edge in a derived view with its own synthetic
  gid, produced by `virtualEdge(type, from, to)`. Each endpoint is either a
  **virtual node** (a resolved endpoint) or a **handle** (an unresolved
  endpoint), settled per endpoint, so the two may be mixed.

- **Handle** (import key) - the integer a user passes to `virtualNode(gid, ...)`
  or as a `virtualEdge` endpoint. It is **not** a node's identity (that is a
  synthetic gid); it is an import key **stored on the node** so that, at list
  assembly, a synthetic edge's **unresolved (handle) endpoints** are bound to
  nodes by matching handles. A node built by `derive()` carries no handle.

- **Unresolved endpoint** - a synthetic edge endpoint holding a handle rather
  than a node, awaiting binding at list assembly. A standalone edge may carry
  unresolved endpoints; a real vertex is never an endpoint (wire it by its
  `id()` as a handle).

- **Dangling edge** - at `virtualGraph` assembly, an edge whose `from` or `to`
  endpoint resolves to no node in the node list (a handle with no matching node,
  or a node not present in the list). The `onDanglingEdge` config decides whether
  one aborts the construction (`error`) or is silently omitted (`drop`). Distinct
  from a duplicate import handle among nodes (an ambiguity, always an error) and
  from an isolated node (no edges, always fine).

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
