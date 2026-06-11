# project() subgraph constructor

Status: done

## Parent

`.scratch/projections/PRD.md`

## What to build

A `project(...)` query function that yields a **subgraph** - a derived view
whose nodes and edges are real accessors. It accepts a path (and/or lists of
real nodes and edges) and returns a value an algorithm procedure can consume.
Running an algorithm over the subgraph and yielding nodes/edges works end to
end; the yielded elements are real accessors. Write-back is a separate slice;
this slice is the read/consume path.

The `Graph` / subgraph machinery (`SubgraphDbAccessor`, the `Graph` TypedValue,
`mgp_graph` dispatch) already exists - this wires the `project()` function on
top of it.

## Acceptance criteria

- [ ] `WITH project(p) AS subgraph CALL algo.get(subgraph) YIELD node, rank RETURN ...` runs and returns rows
- [ ] The subgraph contains exactly the nodes/edges of the supplied path/lists
- [ ] Nodes/edges yielded from the subgraph are real accessors (real GIDs/element_ids)
- [ ] An algorithm reading node properties over the subgraph sees real property values
- [ ] e2e test in the `tests/e2e/write_procedures` area asserting the above

## Blocked by

None - can start immediately.

## Comments

`project()` (the `PROJECT_PATH` / `PROJECT_LISTS` aggregation) and the subgraph
machinery already existed on master; this slice was already satisfied. Added a
confirming e2e test asserting an algorithm-shaped procedure reads real property
values over a `project()` subgraph and yields real accessors whose ids resolve
to the real store (`tests/e2e/write_procedures/read_subgraph.py`).
