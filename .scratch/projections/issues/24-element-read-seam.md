# Element read seam: VertexAccessor-shaped read signature on VirtualNode

Status: done - `VirtualNode` gains `GetProperty(View, PropertyId) -> Result` and `Properties(View) -> Result` in `src/query/virtual_node.hpp`; PropertyLookup and subscript read both a real vertex and a projected node through the shared `GetProperty` helper (`src/query/interpret/eval.{cpp,hpp}`). Labels are materialized on the node (not read-through), so the read-through seam is the property path; the label name/id fork rides with issue 28. Tests in `tests/unit/interpreter.cpp`; read-through/overlay/hidden covered by the existing e2e suite.

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

- [x] `VirtualNode` exposes the `VertexAccessor`-shaped property read signature (labels are materialized on the node, not read-through, so they are outside the read-through seam)
- [x] Read-through, overlay-shadow, and hidden-key behaviour are unchanged (tests green)
- [x] The unified read call site no longer needs a `VirtualNode`-vs-`VertexAccessor` branch

## Blocked by

- `18-graphview-interface-identity-view`
