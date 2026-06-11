# Concise, declarative Cypher-like surface for projections

Status: design-settled (ADR 0004); implementation blocked on issue 16

## Parent

`.scratch/projections/PRD.md`

## What to do

Decide what a more **concise** Cypher-like surface for constructing and querying
projections should look like, and how to offer it as a Memgraph vendor extension
that **stays declarative** rather than degrading into imperative function-call
plumbing.

Today projections are assembled through function calls (`project()`, `derive()`,
`virtualNode()`, `virtualEdge()`) threaded through `WITH`/`CALL`. That is
verbose and procedural. Explore whether a declarative grammar extension reads
better while remaining standard-Cypher-compatible for clients that do not know
it.

Considerations:

- A vendor grammar extension must degrade gracefully and not break parsing for
  standard clients.
- It must remain declarative: the planner decides execution, the user describes
  the view.
- Relationship to the `USE projection` work in
  `11-cypher-on-projection-design` - this is about the construction/projection
  surface, that issue is about the execution target.

## Why this is human-in-the-loop

Language design decision with long-term API-compatibility consequences. Output
is a design doc / ADR and proposed grammar, not an implementation.

## Acceptance criteria

- [ ] A proposed concise surface with before/after query examples
- [ ] An argument for how it stays declarative and standard-client-safe
- [ ] Alignment with `11-cypher-on-projection-design`

## Blocked by

- `01-project-subgraph-constructor`
- `06-derive-overlay-read-through`
- `16-unify-node-kinds-reduce-special-cases` (the accessor seam this builds on)

## Resolution (ADR 0004)

Grilled and settled. The conciseness problem was a symptom: a projection is a
**value** threaded through `WITH`/`UNWIND`/`CALL`. The fix is not construction
sugar over the value model - it is to make a projection a **bindable graph
scope** (`USE`), which is the same decision as issue 11. The surface gets concise
because you bind the view once and query it with ordinary `MATCH` inside the
scope, instead of plumbing a value through every clause and passing it to every
function.

Settled decisions:

- **Scope, not value.** Query a projection by binding it for a `CALL { ... }`
  block, not by operating on it as a value.
- **A value names it; `USE` binds it.** `derive`/`virtualGraph`/`project` keep
  producing values; `USE` binds one as the ambient graph. A fused construct-in-
  head (`USE PROJECTION derive(...) { ... }`) is deferred sugar.
- **Keystone is issue 16** (the rebindable accessor seam); this and issue 11 are
  payoffs of it. Most of issue 15 falls out too.
- **v1:** read-only, single nesting, full-scan.

Remaining design work folded into issue 11 (the execution-target design) and
issue 16 (the accessor seam). This issue is design-complete; implementation is
the 16 -> 11/14 sequence.
