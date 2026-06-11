# Write-back to real nodes from a project() subgraph

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md`

## What to build

When an algorithm yields a node from a `project()` subgraph, a `SET` on that
node persists to the real store. This is the MVP spine - compute a feature over
a subgraph and write it back onto the real node for an ML pipeline. The
write-back path established here is reused by overlay write-back (slice 08).

## Acceptance criteria

- [ ] `... CALL pagerank.get(subgraph) YIELD node, rank SET node.rank = rank` persists `rank` on the real node
- [ ] A follow-up `MATCH (n) RETURN n.rank` in a separate query returns the written value
- [ ] Setting a new (previously absent) property on the real node creates it
- [ ] e2e test asserting the property persists across queries

## Blocked by

- `01-project-subgraph-constructor`
