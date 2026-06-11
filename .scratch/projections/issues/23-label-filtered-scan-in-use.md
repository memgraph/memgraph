# Label-filtered scan over a projection in a USE scope

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md` (ADR 0005)

## What to build

`MATCH (n:Label)` inside a `USE` scope returns the projection nodes carrying that
label, via a documented full-scan filter (a projection has no label index). A
property predicate in the `WHERE` also evaluates by scan.

## Acceptance criteria

- [ ] `MATCH (n:Label)` returns only the projection nodes with that label
- [ ] A property predicate filters correctly (full scan, no index)
- [ ] Full-scan semantics for projection scans are documented (no index/stats)
- [ ] e2e test

## Blocked by

- `21-scan-projection-nodes-in-use`
