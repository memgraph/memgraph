# PRD: Cypher execution and write-back on subgraphs and projections

Status: ready-for-agent

Source: `projections .html` (Notion spec) + design grilling session.

## Problem Statement

Users run graph algorithms over a derived view of their graph (PageRank over a
BFS reachability projection, community detection over a filtered subgraph) and
then want to use the result. Today that breaks in three ways:

- **They can't write the result back.** An algorithm run over a `derive()`
  projection yields virtual nodes that have no link to the real node they came
  from, so `SET node.rank = rank` has nowhere to land. The headline use case -
  compute a feature, persist it on the real node for an ML pipeline - is
  impossible.
- **Projections are memory-hostile.** Constructing a projection copies every
  property of every node into compute. Over nodes carrying vector embeddings,
  that duplicates the embeddings in memory and users hit allocation limits on
  graphs that would otherwise fit.
- **They can't bring an external graph in, or shape an abstraction to run
  Cypher over.** There is no way to hand-build a graph from external table data
  in one query, and no way to run declarative Cypher against a projected view.

## Solution

Two node kinds and a small set of query functions that make derived views
first-class, writable, and cheap.

- **Overlay node** - a node that holds a reference to its origin real vertex
  plus its own overlay property store. Reads fall through to the origin lazily
  (the embedding is never copied unless read); declared overlay properties
  shadow the origin. Writes go to the origin (persist) or to a declared overlay
  key (compute), per a static per-property binding.
- **Synthetic node** - a node with no origin: an overlay store only. Produced
  by `virtualNode()`/`virtualEdge()` or imported from external data. Reads and
  writes both hit its overlay.

On top of these:

- `project(...)` yields a subgraph of real accessors; algorithm output written
  to a yielded node persists to the real store.
- `derive(path, config)` yields an overlay projection: lazy read-through, a
  declared per-property binding (`origin` / `overlay` / `hidden`), and a static
  schema fixed at construction.
- `virtualNode(...)` / `virtualEdge(...)` construct synthetic elements; a
  projection can be assembled from lists of them to import an external graph in
  one query.
- Overlay/synthetic nodes serialize over Bolt without breaking existing clients,
  carrying provenance through a per-node reference plus a once-per-result schema
  table so Lab can render and style projected graphs.

Declarative Cypher execution over a projection (`CALL { USE projection ... }`)
is the eventual capability this enables but is **out of scope for this PRD** -
see Out of Scope and the `ready-for-human` issue.

## User Stories

1. As a data scientist, I want to run PageRank over a `project()` subgraph and
   `SET node.rank = rank`, so that the score persists on the real node as an ML
   feature.
2. As a data scientist, I want to run an algorithm over a `derive()` projection
   and write the result back to the origin nodes, so that I can compute over an
   abstracted view but persist onto the underlying graph.
3. As a user with embedding-heavy nodes, I want a projection to reference origin
   properties lazily, so that constructing it does not duplicate my vectors in
   memory.
4. As a user, I want to declare which origin properties a projection exposes,
   hides, or overlays, so that I can shape a minimal, cheap view.
5. As a user, I want to declare per property whether a write targets the origin
   (persist) or the overlay (compute), so that I can mix persisted features and
   scratch values in one query.
6. As a user, I want a write to an undeclared key to follow a single defined
   rule, so that I never silently mutate a value that gets discarded.
7. As a user, I want a conflicting projection config (a key both overlaid and
   bound to origin) to be rejected at query construction, so that ambiguous
   views fail loudly instead of resolving arbitrarily.
8. As a user, I want `RETURN virtualNode(gid, labels, props)` to produce a node,
   so that I can render an arbitrary graph in Lab without persisting it.
9. As a user, I want `virtualEdge(type, from, to)` accepting either gids or
   virtual nodes, so that I can wire up a synthetic graph.
10. As a user, I want to build a projection from a list of virtual nodes and a
    list of virtual edges, so that I can import external table data as a graph
    in one query.
11. As a user importing an external graph, I want an edge that references a node
    absent from the node list to either error or be silently dropped per config,
    so that I control validation strictness.
12. As a user, I want `SET` on a synthetic node to mutate its overlay and never
    error, so that synthetic graphs behave predictably.
13. As a Lab user, I want a projected graph to render as nodes and edges, so
    that I can see the abstraction I constructed.
14. As a Lab user, I want an overlay node to appear at its origin's identity, so
    that clicking, expanding, or editing it maps back to the real node.
15. As a Lab user, I want to know which properties on a node are computed
    overlays versus real, so that the UI can style them distinctly.
16. As an existing Bolt client author, I want overlay/synthetic nodes to decode
    with no driver changes, so that my application does not break.
17. As a user streaming a large result, I want provenance to be resolvable as
    each node arrives, so that I do not have to buffer the whole result first.
18. As a user, I want function calls (e.g. `degree(n)`) to work over virtual
    nodes and edges, so that I can compute over projections the same way I do
    over the real graph.
19. As an operator, I want Memgraph usable as a compute layer over an on-disk
    source of truth, so that I can scale compute on projections without the cost
    of persisting derived state.

## Implementation Decisions

### Data model: two node kinds

- **Overlay node** = `(origin reference, overlay property store)`. **Synthetic
  node** = overlay store only, no origin. These are the canonical terms (see
  `CONTEXT.md`). The branch already carries the `VirtualNode`/`VirtualEdge`/
  `VirtualGraph`/`Graph` TypedValue variants, Bolt serialization for them, and
  three-way `mgp_graph` dispatch (real / subgraph / virtual). The new work is
  the origin reference and binding rules, not the value plumbing.

### Property binding (the consistency core)

- Read source and write target for a given property are **coupled to one store**
  - they cannot be chosen independently, or a read-after-write returns the stale
  shadowed value. Each property has one **binding**: `origin` (read-through and
  write-through, persisted), `overlay` (read and write hit the overlay,
  compute-only), or `hidden` (not visible).
- Reads are **lazy read-through**: `origin.GetProperty(p)` every time, never
  cached in the overlay. A property is overlay-bound iff present in the overlay,
  else origin-bound. A projection over a node with a vector property must not
  materialize that vector unless it is explicitly read.
- The projection **schema is static** - the full set of overlay/hidden keys is
  fixed from the `derive()` config at construction. A `SET` to an **undeclared**
  key goes to **origin**; overlay keys must be declared. This keeps the schema
  shippable in the pre-stream Bolt header and forbids minting scratch keys
  mid-query.
- Conflicts between the per-property policy and the `sourceNodeProperties` /
  `targetNodeProperties` overrides are a **construction-time query error**, not
  a silently-resolved precedence.
- `SET` on a synthetic node mutates its overlay and never errors.

### `derive()` config surface

- `propertyPolicy` (per-property `origin`/`overlay`/`hidden` binding),
  `sourceNodeProperties` / `targetNodeProperties` (overlay overrides at
  construction), `sourceNodeLabels` / `targetNodeLabels`, `virtualEdgeType`, and
  a graph-validation mode for list construction (`error` vs `drop`).
- Read precedence: overlay shadows origin. Hidden keys are invisible to reads
  and to function calls over the node.

### Bolt marshalling (non-breaking)

- The Bolt `Node` structure is fixed-arity (3 fields on v4, 4 on v5); a new
  field would break drivers. Provenance therefore rides **inside the property
  map** and **the result header**, never as a new node field.
- An **overlay node serializes at its origin's `element_id`** with a merged
  property map (overlay shadows origin). A **synthetic node** keeps its
  synthetic id.
- Provenance is conveyed as a per-node **projection tag** (a small reserved
  property, e.g. `__mg_overlay_ref: 0`, a plain Int that generic clients ignore)
  plus a **projection-schema table** sent once in the **RUN header** alongside
  `fields` (`{0: {overlay: [...], hidden: [...], edgeType: "..."}}`). Header,
  not trailing summary, so streaming clients resolve provenance as each node
  arrives. The reserved-key convention mirrors the existing Enum marshalling
  (`__mg_*` keys).
- Property **values are never wrapped** (no `{__type, __value}` around a
  scalar) - that would change the shape of values clients consume directly and
  break application code. Wrapping is only acceptable for genuinely non-standard
  types like Enum.

### Delivery order (independently shippable slices)

1. `project()` + write-back to real nodes (MVP spine; establishes the write-back
   path slice 4 reuses).
2. `virtualNode()` / `virtualEdge()` constructors (independent; parallel track).
3. Projection from lists of virtual elements + graph validation (depends on 2).
4. `derive()` overlay projection: origin reference, lazy read-through,
   per-property binding, static schema (depends on slice 1's write-back path).
5. Wire provenance: per-node ref + RUN-header schema table (depends on 4).
6. Cypher-on-projection (`USE`) - **out of scope here**, `ready-for-human`.
7. Cross-tenant virtual nodes - nice-to-have.

Critical path is `1 -> 4 -> 5`; `2 -> 3` is a parallel track. The MVP is slice
1; slices 1+4 deliver the full "compute features without copying embeddings"
story via the procedure path, without needing Case 7.

## Testing Decisions

Good tests assert **external behavior** - the result of a Cypher query and
whether a write persisted - not internal accessor wiring.

- **Highest seam: e2e Cypher query tests.** Run a query string against a running
  Memgraph and assert returned rows; assert write-back by issuing a follow-up
  `MATCH` and checking the property persisted on the real node. Prior art:
  `tests/e2e/write_procedures/virtual_graph.py` already sketches the intended
  `project()`/`derive()` usage and is the natural home.
- **Memory behavior** is a behavioral assertion: a `derive()` over
  embedding-heavy nodes that does not read the embedding must not grow memory by
  the embedding size. Assert via the same e2e seam against memory counters.
- **Bolt marshalling** tested at the glue seam: run a query returning
  overlay/synthetic nodes and assert the decoded `element_id` (origin vs
  synthetic), the merged property map, the per-node ref, and the header schema
  table. Prior art: existing `src/glue/communication` and bolt encoder tests.
- **Procedure consumption** of projections (a node yielded by an algorithm is
  writable) at the unit seam. Prior art:
  `tests/unit/query_procedures_mgp_graph.cpp`.
- **Construction-time errors** (conflicting binding, dangling edge under
  `error` mode) asserted at the e2e query seam as query failures with clear
  messages.

Prefer the existing e2e and unit seams above; no new seam is proposed.

## Out of Scope

- **Cypher execution over a projection (`CALL { USE projection MATCH ... }`,
  Case 7).** This is a second execution target for the entire read path, not a
  function: no graph-view switch exists, projections have no indexes or
  statistics, write semantics inside the subquery multiply the case matrix, and
  nesting plus MVCC consistency raise subtle inconsistency risks. Captured as a
  `ready-for-human` issue to be designed separately.
- **Cross-tenant virtual nodes (Case 9).** Nice-to-have; depends on a cross-db
  Bolt call path.
- **Per-property provenance value wrapping on the wire.** Explicitly rejected;
  provenance is a schema table, not per-value wrappers.

## Further Notes

- Synthetic GIDs already count down from `UINT64_MAX`, collision-free against
  real GIDs - no new id scheme needed.
- "Memgraph as a compute layer without persistence" is a stated goal that the
  `overlay`-bound write target serves directly: chain one algorithm's output
  into the next without touching disk.
- Marketing framing from the source spec (breadth of data sources vs Neo4j,
  declarativeness) is context, not a deliverable.
