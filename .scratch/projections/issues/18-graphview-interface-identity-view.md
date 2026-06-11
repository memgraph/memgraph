# GraphView interface + real identity view

Status: done - `src/query/graph_view.hpp`, `tests/unit/query_graph_view.cpp`

## Parent

`.scratch/projections/PRD.md` (ADR 0005)

## What to build

Introduce the `GraphView` seam at the query accessor boundary (ADR 0005): a
range-returning interface for scanning vertices, expanding a vertex's edges, and
mapping names to/from ids. Make the real `DbAccessor` the **identity view**
implementing it, and switch the execution context so the read operators take a
`GraphView` instead of a concrete `DbAccessor *`.

This slice is a pure seam introduction: real-graph queries route through the
identity view and produce identical results. No projection path yet. The virtual
boundary must be coarse (a view call yields a range; per-element reads stay on
the concrete element) so the real read path is not regressed.

The concrete shapes are fixed in `.scratch/projections/issue-17-use-scope-design.md`.
Per that design, `Vertices()` returns a type-erased `VertexRange` (not the existing
`VerticesIterable`); introducing that range and its common scan-element handle is
part of this slice. The identity view's `VertexRange` is a pass-through over
`DbAccessor::Vertices(view)` yielding `VertexAccessor`. `ScanAll` and `Expand`
switch to reading `ExecutionContext::graph_view`; `db_accessor` stays as the
interim bridge for operators not migrated here.

## Acceptance criteria

- [x] `GraphView` exists with the scan / name-mapping surface agreed in issue 17
      - range-returning `Vertices()` + name mapping; per issue 17 expand stays on
        the element, so `GraphView` carries no edge method (issue 22 gives the
        element its expand surface)
- [x] `DbAccessor` is the identity `GraphView`; the execution context binds a `GraphView`
      - `DbAccessorGraphView`; `PullPlan` binds it into `ExecutionContext::graph_view`
- [x] Real-graph `MATCH`/`Expand` read through the identity view; the full existing test suite is green
      - `ScanAll` routes through the ambient view; `query_plan_match_filter_return`
        (148 tests) green. (`query_plan_edge_cases` fails to compile on this branch
        from a pre-existing `PrepareResult` arity drift, unrelated to this slice.)
- [x] No measurable regression on the real-graph read path (boundary crossed per scan, not per row)
      - one virtual `Vertices()` call per scan returns a `VertexRange`; per-row
        iteration is non-virtual over concrete elements

## Notes for the next slices

- `Expand` is unchanged here: for the identity view the scanned element is a
  `VertexAccessor`, so `InEdges/OutEdges` already work. Issue 22 gives the
  projection element its own expand surface.
- The `VertexRange` element is still `VertexAccessor`. Issue 19 widens it to also
  yield `VirtualNode` for a projection scan; `ScanAll` writes either to the frame
  unchanged since `TypedValue` already carries both.
- A `CALL { USE ... }` scope (issue 21) rebinds `ExecutionContext::graph_view`;
  outside a scope it is the identity view (or null -> the inline identity path).

## Blocked by

- `17-design-use-scope`
