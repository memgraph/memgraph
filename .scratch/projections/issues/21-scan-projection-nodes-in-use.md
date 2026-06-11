# Scan a projection's nodes in a USE scope (first end-to-end)

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md` (ADR 0004, ADR 0005)

## What to build

Wire the pieces so `CALL { USE g MATCH (n) RETURN n }` runs over a synthetic
`virtualGraph`: the scope binds the projection `GraphView` as the ambient view,
`ScanAll` reads the projection's nodes through it, and the rows return over Bolt.
Read-only, all-nodes, no label filter, no expand - the thinnest end-to-end path
that proves the seam.

## Acceptance criteria

- [ ] `WITH virtualGraph(...) AS g CALL { USE g MATCH (n) RETURN n.x } ...` returns the projection's nodes
- [ ] Inside the scope the bound view is the projection; outside it, the real graph
- [ ] Real-graph `MATCH` outside the scope is unchanged
- [ ] e2e test asserting the returned rows

## Blocked by

- `18-graphview-interface-identity-view`
- `19-projection-scan-surface`
- `20-parse-use-in-call-subquery`
