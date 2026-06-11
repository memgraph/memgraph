# ADR 0004: A projection is a bindable graph scope, not a value to thread

Status: accepted

## Context

A projection (or subgraph) is currently a **value** - a `VirtualGraph` /
`Graph` TypedValue threaded through `WITH` / `UNWIND` / `CALL`:

```cypher
MATCH p=(:N {id:1})-[:R]->(:N {id:2})
WITH derive(p, {virtualEdgeType:'E', sourceNodeLabels:['Expert']}) AS g
UNWIND g.nodes AS n
WITH g, n WHERE 'Expert' IN labels(n)
CALL read.subgraph_vertex_info(g, n) YIELD labels, score
RETURN labels, score
```

That single fact - the projection is a value you operate on - is the root of
three of the open projections issues:

- **Verbose surface (14).** You plumb the value through every clause; there is no
  declarative way to describe a view and query it.
- **No `MATCH` over a projection (11).** `MATCH`/`Expand` bind to the query's one
  `DbAccessor` (`context.db_accessor`, a concrete pointer), not to a value, so you
  can never write `MATCH (n) ...` against `g`.
- **Functions break over non-native nodes (15).** Every built-in must special-case
  "real accessor vs projection value".

The mechanism to make a projection the *ambient* graph already exists, but only
at the **procedure boundary**. `CALL proc(virtualGraphValue)` swaps the graph the
procedure sees:

```cpp
vg_acc = query::VirtualGraphDbAccessor(realDbAccessor, &virtual_graph);
graph.impl = &*vg_acc;   // the procedure now runs against the projection
```

`SubgraphDbAccessor` and `VirtualGraphDbAccessor` exist; `mgp_graph.impl` is a
variant rebound per call. This is why a procedure can already read a projection.
The Cypher **operator** layer has no equivalent: it uses a concrete
`DbAccessor *`, and `VirtualGraphDbAccessor` is only a partial accessor (name
mapping + `FindNode`, no scan).

## Decision

**A projection is a bindable graph scope, queried with `USE`, not operated on as
a value.** Inside a scope, `MATCH`, built-in functions, and (later) writes resolve
against the bound view; the view value is not threaded through the block.

```cypher
WITH derive(path, {...}) AS g          -- construction stays a value-producer
CALL {
  USE g                                -- bind g as the scope's ambient graph
  MATCH (n:Person)-[:KNOWS]->(m) WHERE m.churned
  RETURN n
}
SET n.at_risk = true                   -- write-back outside the scope, via the binding
```

Four decisions:

1. **Scope, not value.** Querying happens by binding the view for a `CALL { ... }`
   block, not by threading a value through functions. This is the same lever as
   issue 11 (`USE projection`); issue 14's conciseness is its by-product.

2. **A value names it; `USE` binds it.** Construction stays as the
   `derive`/`virtualGraph`/`project` value-producers already shipped. `USE` binds
   that value as the scope's ambient graph. The one new primitive is the
   graph-view switch. A fused construct-in-head surface (`USE PROJECTION
   derive(...) { ... }`) is deferred sugar that layers on top later.

3. **The keystone is one rebindable, scan-capable graph accessor (issue 16).**
   Generalize what `VirtualGraphDbAccessor` does for procedures up into the
   operator layer: real / subgraph / projection all present one accessor the
   operators (`ScanAll`, `Expand`, ...) use polymorphically, and `USE` rebinds
   which one is ambient. Issue 16 is therefore the foundation, not an independent
   cleanup; 11 and 14 are its payoff, and most of 15 falls out because functions
   reading the ambient graph go through the same accessor. Only functions on
   standalone virtual-node *values* still need separate handling.

4. **v1 is read-only, single-nesting, full-scan.** The first `USE` scope allows no
   `CREATE/SET/DELETE/MERGE` inside the block, a single level of nesting, and
   documented full-scan semantics (projections have no indexes or statistics). The
   write-back headline (compute a feature, persist to origin) is unaffected: it
   already works *outside* the scope via the procedure path plus the overlay
   binding. In-scope writes and deep nesting - the case-matrix and MVCC-
   consistency risks - are a documented v2.

## Consequences

- Issues **11, 14, 15, 16 are one arc**, not four independent tasks, sequenced
  **16 (the accessor seam) first**, then `USE`-scope (11/14) and the residual
  function work (15). The procedure path already proves the abstraction is viable.
- The construction layer (`derive`, `virtualGraph`, `project`, `virtualNode`,
  `virtualEdge`) is unchanged - it keeps producing values; `USE` consumes them.
- The prior-art warning holds (ADR review of GDS): the scope body is **real nested
  Cypher the planner sees**, never a Cypher string GDS-style. GDS deprecated its
  string-query projection for exactly this reason.
- Full-scan v1 means a different performance regime inside a scope than over the
  real, indexed graph; this is documented, not hidden, and motivates later index/
  statistics work on projections.

## Alternatives considered

- **Concise construction sugar over the value model.** A terser `PROJECT` clause
  that still yields a value to thread. Rejected as the primary direction: it
  treats the symptom (verbosity) and leaves 11 and 15 untouched. It remains
  available later as the deferred fused-head sugar.
- **Four independent efforts.** Tackle 11, 14, 15, 16 separately. Rejected: each
  would re-derive the same accessor abstraction, and 14/11/15 cannot actually land
  without the 16 seam, so independence is illusory.
- **In-scope writes in v1.** Rejected for v1: inner `CREATE/SET/DELETE/MERGE`
  multiplies the clause case-matrix (real vs subgraph membership vs overlay
  binding) and raises the MVCC inner-vs-outer-write consistency class. Deferred to
  v2; the headline write-back is served outside the scope meanwhile.
