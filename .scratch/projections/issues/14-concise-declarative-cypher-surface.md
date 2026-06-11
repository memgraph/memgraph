# Concise, declarative Cypher-like surface for projections

Status: ready-for-human

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
