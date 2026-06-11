# Element read seam: VertexAccessor-shaped read signature on VirtualNode

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md` (ADR 0001, ADR 0005)

## What to build

Give `VirtualNode` the `VertexAccessor`-shaped read signature
(`(storage::View) -> storage::Result<...>`) that ADR 0001 deferred, so a single
call site can read a property or labels from either a real vertex or a projected
node. Internal refactor only: the existing read behaviour - lazy origin
read-through, overlay-key shadowing, hidden-key invisibility - is preserved.

This is the element half of the GraphView seam: it lets the read path stop
branching on `VertexAccessor` vs `VirtualNode` at the unified call site.

## Acceptance criteria

- [ ] `VirtualNode` exposes the `VertexAccessor`-shaped property/labels read signature
- [ ] Read-through, overlay-shadow, and hidden-key behaviour are unchanged (tests green)
- [ ] The unified read call site no longer needs a `VirtualNode`-vs-`VertexAccessor` branch

## Blocked by

- `18-graphview-interface-identity-view`
