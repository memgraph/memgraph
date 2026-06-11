# ADR 0001: One projected-node type for derived views

Status: proposed (Decision point 3 and "The user-supplied gid is an import key,
not node state" are superseded by ADR 0002)

## Context

A derived view (from `derive()`, `virtualNode()`, or list import) is made of nodes
that are not real graph vertices. Today these are modelled as a single in-memory
value type, `VirtualNode`: a synthetic identity plus a property store, with no link
to any real vertex.

The feature work ahead needs two things this type does not have:

- an **overlay node** that references a real origin vertex and reads its properties
  lazily (issue 06), so a projection over embedding-heavy nodes does not copy the
  embeddings;
- a **per-property binding** that decides, per key, whether a read or write hits the
  origin (persist) or the overlay (compute), fixed at construction (issues 07, 08).

If we add overlay nodes as a second kind alongside synthetic nodes, every consumer
that already branches on node kind grows another branch: the built-in functions
(`IsVertex() || IsVirtualNode()` in roughly a dozen places), the `SET` operator, and
Bolt serialization. Each later slice multiplies the branching further.

The synthetic node and the overlay node are not two things. They are one thing with
an origin or without one. Modelling them as one type collapses the branching to a
single projected-node path and turns the remaining slices into method bodies rather
than new kinds.

## Decision

Introduce one value type, `ProjectedNode`, for every node in a derived view. It holds
an optional origin and its own overlay store:

```cpp
class ProjectedNode {
  storage::Gid                  gid_;      // synthetic identity, always; collision-free with real gids
  std::optional<VertexAccessor> origin_;   // present => overlay node; absent => synthetic node
  property_map                  overlay_;  // this node's own store
  std::shared_ptr<const ProjectionSchema> schema_;  // labels override + per-property binding; static, shared across a projection's nodes

 public:
  storage::Gid Gid() const;       // synthetic identity
  int64_t      CypherId() const;

  // labels: schema's declared labels if set, else the origin's labels.
  auto Labels(storage::View view) const;

  // read: schema hidden -> null; key present in overlay_ -> overlay value; else lazy
  // read-through to origin_ (never cached); else null.
  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const;

  // write: per the binding -> origin-bound persists to the real vertex; overlay-bound,
  // or undeclared with no origin, writes the overlay.
  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, storage::PropertyValue value);
};
```

The two domain roles map onto one type:

- **Synthetic node** = `ProjectedNode` with no origin and the default schema (every key
  overlay-bound). This is exactly today's `VirtualNode`.
- **Overlay node** = `ProjectedNode` with an origin; reads fall through to it, declared
  overlay keys shadow it, and writes route per the binding.

Three settled choices:

1. **Name.** The C++ type is `ProjectedNode`, a neutral umbrella; "synthetic node" and
   "overlay node" remain the domain terms for its two configurations. CONTEXT.md is
   updated to say so.
2. **Read signature.** `ProjectedNode` reads with `(storage::View) -> storage::Result<...>`,
   matching `VertexAccessor`. This lets a single call site eventually read either a real
   vertex or a projected node, which is the lever for folding the two together later
   (issue 16). Origin read-through forwards the view straight through; an overlay-only
   read cannot fail but still returns a `Result` for one uniform shape.
3. **Edges reference nodes, never ids.** `VirtualEdge` holds its two endpoint nodes, the
   same as a real edge. The node-reference form `virtualEdge("T", a, b)` is canonical.
   The gid form `virtualEdge("T", 1, 2)` references nodes that do not exist standalone;
   its gids are resolved against the node list at projection assembly (issue 05), not
   stored as unresolved endpoints on the edge and not materialized as placeholder nodes.

   **Superseded by ADR 0002.** Eager evaluation makes a standalone gid-form edge value
   exist before assembly, so its endpoints must be stored on the edge. The handle is now
   stored on the node and an edge endpoint may be an unresolved handle.

### The user-supplied gid is an import key, not node state

**Superseded by ADR 0002**: the import key (handle) is stored on the node.

A node's identity is its synthetic gid. The integer a user passes to
`virtualNode(gid, ...)` is a key used once, at list assembly, to wire edges to nodes by
their declared gid. It is not stored on the node and not stored on the edge. It lives in
the assembly step (issue 05) and nowhere else.

## Consequences

- The remaining slices become method bodies on one type instead of new kinds:
  overlay read-through (06) sets `origin_`; per-property binding (07) populates `schema_`;
  overlay write-back (08) is already the `SetProperty` routing; Bolt provenance (09)
  is one identity rule (origin's element id if it has an origin, else the synthetic gid).
- `virtualNode()` and `derive()` both produce `ProjectedNode`. The current `VirtualNode`
  is renamed and generalized; its synthetic-only behaviour is the no-origin case.
- `SET` keeps one projected-node branch (writes route by binding), distinct from the real
  vertex branch. Overlay-bound writes still mutate an in-memory store, so they target the
  frame-resident node, as the current synthetic `SET` already does.
- This unifies the **projected family** only. A real vertex is MVCC-backed storage, not a
  value, so `IsVertex() || IsProjectedNode()` branches remain (one branch, not three).
  Collapsing those is a separate, larger step (issue 16); matching `VertexAccessor`'s read
  signature here is what makes it reachable.

## Alternatives considered

- **Keep synthetic and overlay as separate kinds.** Rejected: every consumer that branches
  on node kind grows a third branch, and each later slice adds more. This is the churn the
  ADR exists to avoid.
- **Give `VirtualEdge` optional unresolved handle endpoints.** Rejected: it forces every
  `From()`/`FromGid()`/Bolt/`startNode` path to branch on resolved-vs-unresolved, and a
  standalone unresolved edge has no defined rendering.
- **Materialize placeholder nodes for gid endpoints.** Rejected: it creates a second node
  identity per handle that assembly then has to merge away.

## Implementation status

The unified node and overlay read-through landed on the existing `VirtualNode` class. Two
of the settled choices above are deferred to keep churn down until they pay off:

- **Name.** The class is still `VirtualNode`, not `ProjectedNode`. A rename touches the
  `TypedValue` type-enum and ~170 sites for no behavioural gain, and would leave
  `VirtualNode` renamed while `VirtualEdge`/`VirtualGraph` are not. `VirtualNode` is the
  realization of the projected-node concept for now; a coherent family-wide rename is
  optional later cleanup.
- **Read signature.** `VirtualNode` keeps its bare `GetProperty(key)` / `Properties()` and
  reads the origin internally with `storage::View::NEW` (the view `derive()` already used),
  so no read caller changes. The `VertexAccessor`-shaped `(View) -> Result<>` signature -
  the lever for folding real and projected reads together - is adopted if and when issue 16
  is taken on.

Landed: `VirtualNode` carries an optional origin; property reads fall through to it lazily
(overlay keys shadow); `derive()` sets the origin and no longer copies inherited properties.
Per-property binding and write-back (the origin write path) remain separate slices.
