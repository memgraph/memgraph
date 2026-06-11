# Update functions to handle non-native nodes

Status: ready-for-human

## Parent

`.scratch/projections/PRD.md`

## What to do

Audit the built-in functions and make them work over non-native nodes and edges
(overlay and synthetic), not just real accessors. Per user story 18, a user
should be able to call `degree(n)`, `properties(n)`, `labels(n)`, `id(n)`,
`startNode(e)`, etc. over projected elements and get correct results.

Concretely:

- Enumerate the functions that take a node/edge argument and assume a real
  accessor.
- Define correct behaviour over an overlay node (respecting the binding:
  read-through to origin, overlay shadows, hidden keys invisible) and a
  synthetic node (overlay store only).
- Make degree/neighbour functions resolve over the projection's topology, not
  the real graph's.

## Why this is human-in-the-loop

Requires deciding correct semantics per function over the binding rules before
implementing; some answers are non-obvious (e.g. `degree` over a `derive()`
view). Once semantics are fixed this can spawn agent-ready implementation issues.

## Acceptance criteria

- [ ] A table of node/edge functions and their defined behaviour over overlay
      and synthetic nodes
- [ ] Hidden-key and read-through semantics respected by `properties`/`labels`
- [ ] Follow-on implementation issues drafted

## Blocked by

- `06-derive-overlay-read-through`
- `03-virtual-node-constructor`

## Arc (ADR 0004)

Part of the projections-query arc. Split the audit in two:

- **Topology functions** (`degree`, neighbours, `startNode`/`endNode` resolution)
  read the *ambient graph*. Once the issue-16 accessor seam lands and `USE` binds
  a projection, these resolve against the projection automatically - they fall out
  of the keystone, not a per-function fix.
- **Value functions** (`labels`, `properties`, `id`, `keys` over a standalone
  virtual-node value) need the binding-aware behaviour this issue describes
  (read-through, overlay shadows, hidden keys invisible) regardless of any scope.
  Several already work (`labels`/`properties`/`id` are correct on virtual nodes
  today); the residual is the hidden-key and per-binding edge cases.

So this issue narrows to the value-function semantics; the topology half is
absorbed by issue 16.
