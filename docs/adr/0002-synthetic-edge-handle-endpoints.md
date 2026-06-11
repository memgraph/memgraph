# ADR 0002: Synthetic-edge endpoints carry unresolved import handles

Status: accepted

Supersedes the edge-and-handle decisions of ADR 0001 (its Decision point 3,
"Edges reference nodes, never ids", and the section "The user-supplied gid is an
import key, not node state"). The rest of ADR 0001 (one projected-node type,
read signature, the unification argument) stands.

## Context

`virtualEdge(type, from, to)` constructs a synthetic edge whose endpoints may be
given as virtual nodes or as user gids. ADR 0001 settled that the gid form
references nodes resolved only at projection assembly, with the gid "not stored
as unresolved endpoints on the edge and not materialized as placeholder nodes",
and that the `virtualNode(gid, ...)` import key "is not stored on the node and
not stored on the edge ... it lives in the assembly step and nowhere else".

That is not implementable for the assembly call it was meant to serve. Cypher
evaluates eagerly: in `project([...nodes...], [virtualEdge("T", 1, 2), ...])`,
each `virtualEdge("T", 1, 2)` becomes a value before `project` sees the list, so
that value must carry `1` and `2` somewhere. ADR 0001 forbids both places the
endpoints could live (on the edge, or as placeholder nodes), leaving no
mechanism. The same holds for `RETURN virtualEdge("T", 1, 2)`: a standalone
gid-form edge must exist as a value and serialize over Bolt.

The matching half has the same problem. The assembly step binds an edge's gid
endpoints "to nodes by their declared gid", but by the time assembly runs the
node list is a list of already-evaluated virtual-node values. If the import key
a user passed to `virtualNode(gid, ...)` is stored nowhere, the value carries no
declared gid to match against, and assembly cannot bind anything.

So the import key must be stored on the node, and an edge must be able to hold an
endpoint that is still a key rather than a node. ADR 0001 rejected exactly this
shape on the grounds that it forces `From()`/`FromGid()`/Bolt/`startNode` to
branch on resolved-vs-unresolved. That cost is real but small and contained:
those are the only consumers of an edge's endpoints, and a standalone
unresolved edge does have a defined serialization (its endpoints are the
handles).

## Decision

The user-supplied gid is a **handle**: an import key, distinct from a node's
synthetic identity, **stored on the node** and usable as an edge endpoint.

- `VirtualNode` gains an optional handle field, defaulting to none. `virtualNode`
  sets it from its first argument; `derive()`-built nodes carry none. The handle
  is not the node's identity (still a synthetic gid), is never serialized over
  Bolt, and is not exposed through `id()`. It exists only to be matched at
  assembly.

- A `VirtualEdge` endpoint is one of two cases, settled per endpoint so the two
  may be mixed:
  - a **resolved** endpoint: a virtual node, as today;
  - an **unresolved** endpoint: an `int64_t` handle, awaiting binding.

- `FromGid()`/`ToGid()` return the node's synthetic gid when resolved and the
  handle when unresolved. Bolt serialization, the `VirtualGraph` in/out index,
  and edge equality all flow through these unchanged. A standalone unresolved
  edge therefore serializes with its handle values as `from`/`to`.

- `From()`/`To()` (used only by `startNode`/`endNode`) return the node when
  resolved and throw a clear query error when unresolved: a standalone handle
  edge has no endpoint node until assembly binds it.

- `virtualEdge` accepts a handle (`Integer`) or a virtual node per endpoint. A
  **real vertex is rejected** with a message pointing at `id()`; a user wires a
  real node by passing its gid as a handle.

- Projection assembly (issue 05) reads an edge's unresolved handles, matches them
  against the node handles, and rebinds the endpoints to nodes via the existing
  rebinding `VirtualEdge` constructor.

## Consequences

- The resolved-vs-unresolved branch ADR 0001 wanted to avoid lives in exactly
  four places: `FromGid`/`ToGid`, `From`/`To`, Bolt edge serialization, and the
  assembly binding step. Every other edge consumer reads through `FromGid`/
  `ToGid` and is unaffected.
- A standalone gid-form edge is now a first-class value: it returns, serializes,
  and renders (as an edge between the handle ids). This is what issue 04 needs.
- Assembly binding (issue 05) is a node-handle lookup plus an endpoint rebind, on
  top of the rebinding constructor `VirtualGraph::Merge` already uses.

## Alternatives considered

- **ADR 0001's model (endpoints stored nowhere, resolved at assembly).** Rejected
  as not implementable under eager evaluation: the edge and node values exist
  before assembly and must carry their handles.
- **A handle-keyed node map at assembly (`project({1: vnode, ...}, ...)`).** Keeps
  the handle off the node, but contradicts "the user passes the handle to
  `virtualNode`", forces a map-shaped import API, and is more awkward than plain
  lists.
- **Materialize a placeholder node per handle.** Rejected (as in ADR 0001): it
  mints a second identity per handle that assembly must then merge away.
- **A third endpoint case for real vertices.** Rejected: it widens the endpoint
  variant and equality/Bolt/assembly plumbing to no benefit, since `id(node)` is
  already a usable handle.
