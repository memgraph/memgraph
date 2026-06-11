# ADR 0005: GraphView - one seam for real, subgraph, and projected graphs

Status: accepted (target model; implementation staged, see Consequences)

## Context

Three graph kinds run through the read path: **real** (`DbAccessor` over
MVCC storage, indexed), **subgraph** (a real accessor plus a membership
predicate), and **projection** (a `VirtualGraph` of overlay/synthetic nodes,
ADR 0001). The branch-point inventory (`.scratch/projections/issue-16-branch-
inventory.md`) found one polymorphic seam already exists - `mgp_graph.impl` is a
`std::variant<DbAccessor *, SubgraphDbAccessor *, VirtualGraphDbAccessor *>` with
~139 `visit`/`get_if` sites - but it is confined to the `mgp` procedure boundary.
The Cypher operator layer binds a concrete `DbAccessor *` (`ExecutionContext::
db_accessor`), so `MATCH`/`Expand` cannot run over a subgraph or projection, and
the function layer hand-rolls `IsVertex() || IsVirtualNode()` per built-in.

ADR 0004 made a projection a bindable graph **scope** (`USE`) and named this seam
the keystone of the projections-query arc (issues 11, 14, 15, 16). This ADR fixes
the shape of that seam and where it lives, so the arc lands on one abstraction
rather than re-deriving it per issue.

## Decision

Introduce **`GraphView`**: the single abstraction the execution engine runs
against, answering only what an operator needs of "the graph" - scan vertices
(optionally by label/index), expand a vertex's edges, and map names to/from ids.

- **The real `DbAccessor` is the identity view.** Subgraph and projection are not
  sibling kinds; they are **views layered over a base view** - a subgraph
  *filters* a base view by membership, a projection *composes* its own topology
  and *delegates* origin reads to a base view (the node-level read-through of
  ADR 0001, lifted to the graph level). A projection over a projection is then
  just composition, which is what `USE`-nesting needs to not be a special case.

- **It lives at the `query/` accessor boundary**, beside `db_accessor.hpp` (e.g.
  `src/query/graph_view.hpp`) - not in storage (a projection is a compute-time
  concept; storage must not know about overlay bindings or synthetic gids) and not
  in the operators (placing it there re-forks every operator). This is exactly the
  line `ExecutionContext::db_accessor` already sits on: that field becomes the
  ambient `GraphView`, and **`USE` rebinds it** for a `CALL { ... }` scope.

- **Two cooperating seams, split by topology vs data.** Scan/expand are graph
  operations on `GraphView`. Property read/write are *element* operations that stay
  on the element (`VertexAccessor` / `VirtualNode`), which already encapsulates
  binding, read-through, and shadowing. The element half is ADR 0001's deferred
  lever - giving `VirtualNode` the `VertexAccessor`-shaped `(View) -> Result<>`
  read signature - now adopted as part of this seam.

- **The virtual boundary is coarse: `GraphView` returns ranges, not elements.**
  One virtual call yields a vertex range or a node's edge range; iteration is then
  over concrete elements with direct, devirtualized reads. The boundary is crossed
  once per scan, not once per row, so the real-graph hot path is not regressed.
  This is the deciding property over the alternatives below.

## Consequences

- Issues 11, 14, 15, 16 collapse to "operations go through the ambient
  `GraphView`": `USE` rebinds it (11/14), `ScanAll`/`Expand` take it instead of a
  concrete `DbAccessor *` (16), and topology functions resolve through it (15).
- Nesting is composition, not a case in a switch - a view over a view.
- Implementation is staged, not a big-bang rewrite:
  1. Give the projection view a real `Vertices()`/`Edges()` scan surface (today
     `VirtualGraphDbAccessor` has none). Smallest step that makes `ScanAll`
     runnable over a projection and proves the seam.
  2. Define `GraphView` and make the real `DbAccessor` and the subgraph/projection
     views implement it; switch `ExecutionContext` to hold a `GraphView`.
  3. Route the operator read path and the topology functions through it; retire the
     hand-rolled forks (inventory tiers 2-4).
  The `mgp` variant is a working **interim bridge** for the procedure path and can
  stay until the operator path is migrated.
- The coarse-boundary hot-path claim is a design constraint to be **validated by
  profiling** (issue 12), not assumed; if a per-scan virtual call ever shows up,
  the real view can be devirtualized at the call site since it is `final`.

## Alternatives considered

- **Lift the `mgp` `std::variant` + `std::visit` into the operators.** Rejected as
  the destination (kept as interim bridge): a variant is a **closed set** - a
  fourth kind touches every `visit` site - it **does not compose** (a view-over-a-
  view is awkward in a flat variant, bad for nesting), and it leaks the kind
  enumeration into every operator.
- **A shared interface with per-element virtual reads** (virtual `GetProperty`).
  Rejected: a vtable call per property read in a tight scan loop regresses the real
  hot path. The range-returning boundary keeps the interface while paying the
  virtual cost once per scan.
- **Templated operators on accessor type.** Rejected: monomorphization avoids the
  vtable but forbids runtime rebinding, which `USE` requires, and bloats compile
  time.
- **Put the seam in storage, or in the operators.** Storage is a layering
  violation (compute concepts leaking down); the operator layer re-forks the very
  branches this seam exists to remove.
