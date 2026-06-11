# virtualEdge() constructor

Status: done

## Parent

`.scratch/projections/PRD.md`

## What to build

A `virtualEdge(type, from, to)` query function that constructs a synthetic edge,
accepting either gids or virtual nodes as endpoints. Returns the existing
`VirtualEdge` TypedValue. Settle the semantics of referencing endpoints by gid
versus by virtual node.

## Acceptance criteria

- [x] `RETURN virtualEdge("TYPE", 1, 2)` constructs an edge between gids 1 and 2
- [x] `RETURN virtualEdge("TYPE", node_1, node_2)` constructs an edge between two virtual nodes
- [x] The edge carries its type and a synthetic GID
- [x] The edge serializes over Bolt and renders in Lab
- [x] e2e test asserting both endpoint forms

## Blocked by

- `03-virtual-node-constructor`

## Comments

Endpoint-by-gid semantics were settled in `03`: the `gid`/`from_gid`/`to_gid`
values are logical **handles**, not node identities (a synthetic node's identity
is its own synthetic gid). `virtualNode()` already accepts a handle but does not
store it. This slice should:

- Persist the handle on `VirtualNode` (a field, defaulting to none so
  `derive()`-built nodes are unaffected), set by `virtualNode()`.
- Resolve `virtualEdge("T", from, to)` endpoints given either a handle or a
  virtual node; standalone, an edge may hold unresolved handle endpoints that a
  projection assembly (`05`) binds to nodes by handle.

### Delivered

This plan contradicted ADR 0001 (which held the handle is stored nowhere and an
edge holds only node endpoints). A grilling session found ADR 0001's model
unimplementable under Cypher's eager evaluation: `virtualEdge("T", 1, 2)` becomes
a value before any assembly runs, so the handle and the edge's endpoints must be
stored. ADR 0002 supersedes ADR 0001's edge-and-handle decisions and records the
implemented model; CONTEXT.md gained the synthetic-edge and unresolved-endpoint
terms.

As built:

- `VirtualNode` carries an optional import handle, set by `virtualNode()`, absent
  on `derive()` nodes, never serialized and not exposed via `id()`.
- A `VirtualEdge` endpoint is a `variant<shared_ptr<const VirtualNode>, int64_t>`,
  settled per endpoint so node and handle forms may be mixed. `FromGid`/`ToGid`
  return the node gid when resolved and the handle when unresolved (so Bolt,
  indexing, and equality are unchanged); `From`/`To` throw on an unresolved
  endpoint; `FromHandle`/`ToHandle` expose handles for `05`'s assembly.
- `virtualEdge(type, from, to)` accepts a handle or a virtual node per endpoint;
  a real vertex is rejected with a message pointing at `id()`.
- The endpoints live in the heap `Impl` to keep the edge within the `mgp_edge`
  size budget.

Edge properties (a 4th `virtualEdge` argument) are not supported; the constructed
edge has an empty property store. Add when an import use case needs it.
