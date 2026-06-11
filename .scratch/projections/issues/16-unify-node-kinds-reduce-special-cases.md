# Unify codebase to reduce node-kind special cases

Status: ready-for-human

## Parent

`.scratch/projections/PRD.md`

## What to do

The feature introduces three node kinds threaded through the read path, the
functions, Bolt marshalling, and `mgp_graph` dispatch: real, subgraph/overlay,
and synthetic. Left unchecked this multiplies `if (is_virtual) ... else ...`
branches across the codebase. Find the unifying abstraction that collapses these
special cases into one polymorphic seam.

Concretely:

- Map where node-kind dispatch currently forks (accessor reads, function calls,
  serialization, write-back).
- Propose a single interface/seam (e.g. an accessor abstraction the three kinds
  implement) so call sites stop branching on kind.
- Sequence the refactor so it lands without regressing the real-graph hot path.

## Why this is human-in-the-loop

Architectural consolidation requiring judgement about the right abstraction
boundary and the hot-path cost of indirection. Best done after the slices land
so the real special-case set is known. See the architecture-improvement skill.

## Acceptance criteria

- [x] An inventory of node-kind branch points - `.scratch/projections/issue-16-branch-inventory.md`
- [x] A proposed unifying abstraction with a hot-path-cost argument - `GraphView`
      (ADR 0005): a range-returning view at the `query/` accessor boundary, real
      `DbAccessor` as the identity view, subgraph/projection as views layered over
      a base. Hot path preserved by the coarse (per-scan) virtual boundary with
      concrete per-element reads; validated by issue 12 profiling.
- [~] A staged refactor plan, with follow-on issues - the staging is sketched in
      ADR 0005 Consequences (scan surface -> define `GraphView` -> route operators
      + functions); still needs breaking into agent-ready implementation issues

## Blocked by

- `05-projection-from-lists`
- `08-overlay-write-back`

## Arc (ADR 0004)

This is the **keystone** of the projections-query arc (issues 11, 14, 15). The
unifying abstraction is concrete: **one rebindable, scan-capable graph accessor**
that real / subgraph / projection all implement and the operators (`ScanAll`,
`Expand`, ...) use polymorphically. The procedure path already proves it -
`VirtualGraphDbAccessor` + the `mgp_graph.impl` variant rebind the ambient graph
when a procedure is called with a projection. The work is to generalize that from
the procedure boundary up into the Cypher operator layer (today operators use a
concrete `DbAccessor *`, and `VirtualGraphDbAccessor` has no scan surface).

Both blockers are now done, so this is unblocked. Sequence it **first**: 11 and
14 (`USE`-scope) and most of 15 (functions reading the ambient graph) land on top
of this seam.
