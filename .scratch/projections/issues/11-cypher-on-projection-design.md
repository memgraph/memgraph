# Cypher-on-projection (USE) - design

Status: ready-for-human

## Parent

`.scratch/projections/PRD.md`

## What to build

A **design**, not an implementation. Resolve how declarative Cypher could run
against a projection/subgraph, e.g. `CALL { USE projection MATCH (n) ... }`. This
is a second execution target for the entire read path, not a function, and
carries the largest blast radius in the feature. Output is a design doc / ADR and
a bounded v1 proposal, ready to spawn implementation issues.

## Risks and problems to resolve

- **No graph-view switch exists.** The graph (DbAccessor) is bound once for the
  whole query; MATCH/Expand resolve against that single accessor. `USE
  projection` means rebinding the graph for the scope of a CALL subquery - a
  concept the planner and operators do not have today.
- **No indexes or statistics on a projection.** A virtual graph has no label or
  property index and no cardinality stats. Inner `MATCH` either falls back to
  full scans (different perf regime, perf cliffs) or requires building index
  structures on projections. The cost model assumes real-DB stats and may
  produce bad plans.
- **Write semantics multiply.** Inner `CREATE/SET/DELETE/MERGE` on a virtual
  graph hits the in-memory graph; on a subgraph the membership rules apply;
  write-back re-enters the overlay binding. Each clause is a new case matrix.
- **Nesting and MVCC consistency.** Projection-of-a-projection, `USE` inside
  `USE`, and whether inner reads see the outer transaction's uncommitted writes
  (the projection is a snapshot; the outer txn may mutate). This is the subtle
  inconsistency class at its worst.

## Suggested containment for a v1

- Read-only inner Cypher (no `CREATE/SET/DELETE/MERGE`)
- Single level of nesting
- Full-scan semantics, documented (no index support in v1)
- Subgraph and virtual graph only

## Acceptance criteria

- [ ] A design doc / ADR covering graph-view binding, index/stat strategy, write semantics, and nesting/MVCC consistency
- [ ] A bounded, agreed v1 scope (or an explicit decision to defer)
- [ ] Follow-on implementation issues drafted from the agreed scope

## Blocked by

- `01-project-subgraph-constructor`
- `06-derive-overlay-read-through`
