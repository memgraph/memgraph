# Issue 17 design: USE-scope mechanism + GraphView interface shape

Status: agreed (human-in-the-loop, ratified)

Parent: `.scratch/projections/PRD.md`; concretizes ADR 0004 (projection as a
bindable graph scope) and ADR 0005 (the GraphView seam). This note fixes the
concrete AST, operator, and interface shapes that issues 18-23 build on. It is a
design, not an implementation; file paths and signatures below are the agreed
targets, grounded in the current `feat/projection` code.

## 1. AST and plan representation of `CALL { USE <expr> ... }`

`USE` rides on the existing call-subquery clause; it does not introduce a new
top-level clause.

- **Grammar** (`src/query/frontend/opencypher/grammar/MemgraphCypher.g4`). The
  `callSubquery` rule gains an optional use-clause between the scope clause and
  the body:

  ```antlr
  callSubquery : CALL ( '(' scopeClause? ')' )? useClause? '{' cypherQuery '}' ( periodicSubquery )? ;
  useClause    : USE expression ;
  ```

  `USE` becomes a keyword. `<expr>` is a general expression that must evaluate to
  a graph value (a `VirtualGraph` or `Graph` TypedValue) at runtime; it is
  usually a bare identifier bound by an outer `WITH derive(...) AS g`.

- **AST node** (`src/query/frontend/ast/ast.hpp`, `class CallSubquery` near line
  4021). Add one field beside the existing scope fields:

  ```cpp
  // The graph bound for this subquery's scope by `CALL { USE <expr> ... }`.
  // Null for an ordinary subquery; non-null binds <expr> as the ambient graph.
  memgraph::query::Expression *use_graph_{nullptr};
  ```

  `Accept` is unchanged in shape: the use-expression is visited before the inner
  `cypher_query_` so its symbols resolve in the outer scope (the bound value is
  produced outside the block).

- **AST construction** (`cypher_main_visitor.cpp`, `visitCallSubquery`, near line
  4271). After the scope clause is parsed and before the inner query is visited,
  populate `use_graph_` from `ctx->useClause()->expression()` when present.

- **Logical plan**: a dedicated **`BindGraphView`** operator (see section 2),
  emitted at the head of the subquery's plan. `Apply` / `PeriodicSubquery` are
  untouched - a `USE` scope is one self-contained operator wrapping the subquery
  branch, so an ordinary subquery plans exactly as today.

## 2. Binding the ambient GraphView

`ExecutionContext` (`src/query/context.hpp`) today carries `DbAccessor *db_accessor`
(line 116); every read operator reaches the graph through it at `Pull` time (e.g.
`ScanAll`'s vertex lambda calls `context.db_accessor->Vertices(view_)`). That field
is exactly the line the ambient graph sits on, per ADR 0005.

- **Add `GraphView *graph_view` to `ExecutionContext`**, the ambient view the
  migrated read operators read. It is bound to the identity view (the real
  `DbAccessor` wrapped as a `GraphView`) for the whole query by default.
  `db_accessor` stays as the interim bridge for operators not yet migrated
  (index/label scans, writes, the `mgp` procedure path); it is retired axis by
  axis later in the arc (issue 28). This keeps the existing suite green while the
  seam is introduced.

- **`BindGraphView` operator** (new, `src/query/plan/operator.{hpp,cpp}`):

  ```cpp
  class BindGraphView : public LogicalOperator {
    std::shared_ptr<LogicalOperator> input_;   // the subquery body's plan
    Expression *use_graph_;                     // evaluates to a graph value
  };
  ```

  Its cursor, on first `Pull`:
  1. evaluates `use_graph_` to a `VirtualGraph` / `Graph` TypedValue,
  2. constructs the matching `GraphView` (projection or subgraph view) and stores
     it in the cursor so it outlives the child's pulls,
  3. saves the current `context.graph_view`, rebinds it to the constructed view,
  4. pulls `input_` (the subquery body) under the rebound view,
  5. restores `context.graph_view` when the child is exhausted and on `Reset`.

  Nesting is composition: a `USE` inside a `USE` is simply two `BindGraphView`
  operators. v1 forbids it (section 4), but the operator shape does not special-
  case it.

- **Where operators read the bound view**: the read operators that run inside a
  scope - `ScanAll` and `Expand` - switch from `context.db_accessor` to
  `context.graph_view`. `ScanAll`'s lambda becomes
  `context.graph_view->Vertices(view_)`; `Expand` reaches a vertex's edges through
  the scanned element (section 5). Topology functions that read the ambient graph
  (issue 27) resolve through `context.graph_view` the same way.

## 3. GraphView interface signature

Lives at the `query/` accessor boundary, beside `db_accessor.hpp`, as
`src/query/graph_view.hpp` (ADR 0005). Minimal surface - only what an operator
asks of "the graph": a coarse, range-returning scan plus name<->id mapping.
Per-element property reads and edge expansion stay on the element, not here.

```cpp
namespace memgraph::query {

// One virtual call yields a whole vertex range; iteration is then over concrete
// elements with direct reads. The boundary is crossed once per scan, not per
// row, so the real-graph hot path is not regressed (ADR 0005).
class GraphView {
 public:
  virtual ~GraphView() = default;

  // Topology scan. Full-scan only inside a projection scope: projections expose
  // no label/property index, so there is no index-scan overload here. A
  // label-filtered MATCH inside a scope is ScanAll + Filter (section 4 / issue 23).
  virtual VertexRange Vertices(storage::View view) = 0;

  // Name <-> id mapping, mirroring the surface DbAccessor and
  // VirtualGraphDbAccessor already expose.
  virtual storage::LabelId NameToLabel(std::string_view name) = 0;
  virtual const std::string &LabelToName(storage::LabelId label) const = 0;
  virtual storage::PropertyId NameToProperty(std::string_view name) = 0;
  virtual const std::string &PropertyToName(storage::PropertyId prop) const = 0;
  virtual storage::EdgeTypeId NameToEdgeType(std::string_view name) = 0;
  virtual const std::string &EdgeTypeToName(storage::EdgeTypeId type) const = 0;
};

}  // namespace memgraph::query
```

### VertexRange and the common scan element

`Vertices()` returns a **type-erased `VertexRange`** (new), not the existing
`VerticesIterable`. `VerticesIterable` is already a closed variant over a storage
iterable and a `VertexAccessor` set (`db_accessor.hpp:121`); a projection's nodes
are `VirtualNode`, a third kind it cannot hold. `VertexRange` is the open
abstraction:

- A single virtual `Vertices()` call returns the range (coarse boundary).
- Iterating yields a **common scan element** that `ScanAll` writes straight to the
  frame. At the frame boundary the element resolves to the existing `TypedValue`
  sum, which already carries both `VertexAccessor` and `VirtualNode` variants - so
  `ScanAll`'s `frame_writer.Write(output_symbol_, *it)` is unchanged in shape and
  downstream operators (Filter, Produce) are untouched.
- The **identity view** wraps a `DbAccessor` and yields `VertexAccessor`,
  forwarding to `DbAccessor::Vertices(view)`. This is a pass-through: the real-
  graph scan produces identical results, which is what issue 18 must prove.
- The **projection view** (issue 19) yields `VirtualNode` from a `VirtualGraph`'s
  `nodes()`.

Choosing `VertexRange` over widening `VerticesIterable` means the common scan-
element handle lands in issue 18 rather than later. This narrows issue 24 (element-
read seam) to its other half: giving `VirtualNode` the `VertexAccessor`-shaped
`(View) -> Result<>` read signature and retiring the per-function `IsVertex() ||
IsVirtualNode()` forks. The scan/handle half is done once `VertexRange` exists.

### Edge expansion stays on the element

`Expand` already reads a vertex's edges through the element
(`VertexAccessor::InEdges/OutEdges`, returning `storage::Result<...>`), not through
the accessor. This is ADR 0005's "two cooperating seams" split. `GraphView` has no
edge method; the projection's expand surface (issue 22) is given to the scan
element - `VirtualNode` gains `InEdges/OutEdges` over the `VirtualGraph`'s existing
`OutEdges(gid)/InEdges(gid)` indexes - so `Expand` is polymorphic over the element,
not over a `GraphView` call.

## 4. Read-only enforcement (v1 boundary)

Per ADR 0004: inside a `USE` scope, no `CREATE/SET/DELETE/MERGE` and a single level
of nesting. Enforced in the **SymbolGenerator** (`src/query/frontend/semantic/`),
which already tracks scope context as it descends:

- Add an `in_use_scope` flag to the `Scope` struct, set in `PreVisit(CallSubquery&)`
  when `use_graph_` is non-null.
- The write-clause `PreVisit`s (`Create`, `SetProperty`, `SetProperties`,
  `SetLabels`, `RemoveProperty`, `RemoveLabels`, `Delete`, `Merge`, `Foreach`)
  throw a `SemanticException` when `in_use_scope` is set, naming the offending
  clause. `CallProcedure` with `is_write_` is rejected the same way.
- A nested `USE` (a `PreVisit(CallSubquery&)` with `use_graph_` while already
  `in_use_scope`) throws a clear "single level of nesting" error.

This runs in the phase that already owns scope context, gives a precise per-clause
error location, and needs no extra traversal. An ordinary subquery (`use_graph_`
null) is unaffected.

## 5. Full-scan planning inside a scope

A projection has no indexes and no statistics, and the bound graph is only known at
runtime (the `USE` expression is evaluated during execution). So the sub-plan for a
`USE` scope must be planned as if the graph has no index or statistics surface:

- Only `ScanAll` (+ `Filter`) is emitted for a `MATCH` inside the scope - never
  `ScanAllByLabel`, `ScanAllByLabelProperties`, or any index scan. A label-filtered
  `MATCH (n:Foo)` becomes a full `ScanAll` followed by a `Filter` on `labels(n)`
  (issue 23).
- Cost/statistics-driven choices fall back to their no-information defaults; the
  planner must not consult the outer real graph's indexes for clauses inside the
  scope.

This is the documented full-scan performance regime of ADR 0004 v1, and it
motivates later projection index/statistics work.

## Acceptance criteria - resolution

- AST/plan representation decided: `use_graph_` on `CallSubquery`; `BindGraphView`
  operator at the head of the subquery plan (section 1, 2).
- Ambient-binding mechanism decided: `ExecutionContext::graph_view`, rebound by
  `BindGraphView`, read by `ScanAll`/`Expand`; `db_accessor` retained as interim
  bridge (section 2).
- Read-only enforcement decided: SymbolGenerator `in_use_scope` flag, write clauses
  and nested `USE` error at symbol generation (section 4).
- Full-scan planning confirmed: `ScanAll` + `Filter` only inside a scope, no index
  or statistics assumptions (section 5).
- `GraphView` signature fixed: range-returning `Vertices()` + name mapping, with a
  type-erased `VertexRange` over a common scan element; expansion and property
  reads stay on the element (section 3).
