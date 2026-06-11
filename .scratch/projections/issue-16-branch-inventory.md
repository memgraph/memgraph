# Issue 16: node-kind branch-point inventory

Where the codebase forks on node/edge/graph kind today. Three kinds run through
the read path, functions, serialization, and `mgp` dispatch: **real**
(`VertexAccessor`/`EdgeAccessor`), **subgraph** (`SubgraphVertexAccessor` - a real
accessor plus membership), and **projected** (`VirtualNode`/`VirtualEdge`, overlay
or synthetic). This maps every fork so the issue-16 seam can target them.

Distinction used below: a **behavioural fork** special-cases logic by kind (the
ones the seam should collapse); an **enumeration fork** is a mechanical
tagged-union switch that lists every `TypedValue::Type` (inherent to the union,
low priority).

## Headline: the seam already exists, but only at the `mgp` boundary

The `mgp` C-API layer is already a polymorphic three-way accessor:

- `mgp_graph.impl` is `std::variant<DbAccessor *, SubgraphDbAccessor *,
  VirtualGraphDbAccessor *>` (`mg_procedure_impl.hpp:829`).
- `mgp_vertex` uses `VertexImpl = variant<VertexAccessor, SubgraphVertexAccessor,
  VirtualNode>` (`:657`); `mgp_edge` uses `EdgeImpl = variant<EdgeAccessor,
  VirtualEdge>` (`:739` - note: **2-way, no subgraph-edge arm**, an asymmetry).
- Dispatch is `std::visit`/`std::get_if`/`holds_alternative`, ~139 sites in
  `mg_procedure_impl.cpp`, centralized in that one file.

So procedures already run over a subgraph or projection: `CALL proc(graph)` rebinds
`mgp_graph.impl` (`operator.cpp:8039-8057`). **The issue-16 work is to generalize
this variant seam from the `mgp` boundary up into the Cypher operator and function
layers** - and to give `VirtualGraphDbAccessor` a scan surface (today it has only
name-mapping + `FindNode`, `db_accessor.hpp:1161`). The three accessor classes
(`DbAccessor`, `SubgraphDbAccessor`, `VirtualGraphDbAccessor`) are each `final`
with **no shared interface** - the variant *is* the current polymorphism.

## Tier 1 - The accessor seam (keystone target)

- `query/db_accessor.hpp` - `DbAccessor` (`:389`, final), `SubgraphDbAccessor`
  (`:1105`, final), `VirtualGraphDbAccessor` (`:1161`, final, no scan surface),
  `SubgraphVertexAccessor` (`:66`). No common base.
- `query/procedure/mg_procedure_impl.{hpp,cpp}` - the variant seam and its ~139
  `visit`/`get_if` sites. The prototype to lift, not rewrite.
- `query/plan/operator.cpp:8039-8057` - the only place the ambient graph is
  rebound for execution, and it is at the **procedure-call boundary**, not in
  `ScanAll`/`Expand`.

## Tier 2 - Read-path forks NOT on the seam (the gap `USE`/16 must close)

- **`ScanAll` / `Expand`** bind to a concrete `context.db_accessor`
  (`context.hpp:116`, a `DbAccessor *`). There is no projection arm - this is the
  *missing* fork (the "no graph-view switch" of issue 11). The seam must make
  these operators accept any accessor kind.
- `query/interpret/eval.cpp` - `PropertyLookup` and map-projection hand-dispatch
  per kind: `VirtualEdge`/`VirtualNode` arms at `:161,:239,:470,:477` (read
  `n.prop`, `properties(n)`), `VirtualGraph` arms at `:253,:558` (`g.nodes` /
  `g.edges`).

## Tier 3 - Function-layer forks (issue 15 territory)

`query/interpret/awesome_memgraph_functions.cpp` hand-rolls real-vs-virtual in
~15 built-ins:

- the predicate helpers `IsVertex() || IsVirtualNode()` / `IsEdge() ||
  IsVirtualEdge()` (`:172,:174`);
- per-function arms: `startNode`/`endNode` (`:428,:548`), `properties` (`:451,
  :486`), `type` (`:718`), `id` (`:1184,:1188`), `labels` (`:904`), and the
  keys/degree paths (`:827,:880`).
- `query/procedure/cypher_types.hpp:123,:133,:157` - procedure arg type-checks
  (`Vertex`/`Edge`/`Map` accept their virtual variants).

Per ADR 0004: topology functions (degree/neighbours/`startNode`) resolve via the
ambient accessor once the seam lands; only the value-functions
(`labels`/`properties`/`id`/`keys` over a standalone virtual value) need explicit
binding-aware arms.

## Tier 4 - Write-back forks (issue 08, shipped; still a fork)

- `query/plan/operator.cpp` - `SetPropertyOnVirtualNode` (`:4949`),
  `SetPropertiesOnVirtualNode` (`:4961`), called from `SetProperty`/`SetProperties`
  at `:5070,:5502`. `SET` branches to a virtual-node helper vs the real
  vertex path. The overlay binding routing lives in `VirtualNode::SetProperty`,
  so the operator fork is just the kind dispatch.

## Tier 5 - Serialization forks (Bolt / glue)

`src/glue/communication.cpp` - `ToBoltEdge(VirtualEdge)` (`:121`),
`ToBoltVertex(VirtualNode)` (`:142`), `ToBoltVirtualGraph` (`:365`), and the
`ToBoltValue` type-switch arms for `VirtualGraph`/`VirtualEdge`/`VirtualNode`
(`:241,:264,:269`). Behavioural (overlay nodes serialize at origin id; the
projection tag).

## Tier 6 - TypedValue enumeration forks (mechanical, low priority)

`query/typed_value.cpp` - `Virtual{Node,Edge,Graph}` arms in the equality, hash,
`ToString`, copy, and destructor switches (`:696,:735,:782,:822,:830,:882,:964`,
...). These enumerate the union and are not behavioural special-casing; they stay
as long as `TypedValue` is a tagged union, independent of the accessor seam.

## Synthesis

- **Collapse target:** Tiers 2-4. They re-implement "is this real / subgraph /
  projected?" without the variant seam Tier 1 already has.
- **The seam shape is proven:** the `mgp` variant model (one `std::variant` of
  accessors, `std::visit` at use sites) already works for procedures over
  subgraphs and projections. Two viable generalizations: (a) extend that variant
  to the operator/function layers, or (b) give the three accessors a shared
  interface and pass it by reference. (a) reuses the proven machinery and avoids
  virtual-call cost on the real hot path (the real arm of a `visit` is a direct
  call); (b) is cleaner but adds a vtable indirection to every real-graph read -
  the hot-path-cost decision the issue-16 design must settle.
- **Prerequisite for `USE` (11/14):** `VirtualGraphDbAccessor` needs a scan
  surface (`Vertices()`/`Edges()` over the `VirtualGraph`) before `ScanAll` can
  run inside a scope.
- **`mgp_edge` asymmetry:** the per-element edge variant is 2-way (no
  subgraph-edge arm) while the vertex variant is 3-way; reconcile when unifying.
