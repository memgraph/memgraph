# GraphView interface + real identity view

Status: ready-for-agent

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

## Acceptance criteria

- [ ] `GraphView` exists with the scan / expand / name-mapping surface agreed in issue 17
- [ ] `DbAccessor` is the identity `GraphView`; the execution context binds a `GraphView`
- [ ] Real-graph `MATCH`/`Expand` read through the identity view; the full existing test suite is green
- [ ] No measurable regression on the real-graph read path (boundary crossed per scan, not per row)

## Blocked by

- `17-design-use-scope`
