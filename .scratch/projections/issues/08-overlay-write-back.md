# Overlay write-back

Status: done

## Parent

`.scratch/projections/PRD.md`

## What to build

Writes through an overlay node, following the static binding. A `SET` to an
`origin`-bound property persists to the real store (reusing the slice-02
write-back path). A `SET` to a declared `overlay` key mutates the overlay and is
not persisted (Memgraph as a compute layer). A `SET` to an **undeclared** key
goes to the **origin** - the single defined rule that forbids silently mutating
a discarded store. Because read source and write target are coupled per
property, an `origin` write to a key must never leave a stale shadowing overlay
value behind.

## Acceptance criteria

- [ ] `SET overlayNode.p = v` for an `origin`-bound `p` persists to the real node (verified by a follow-up `MATCH`)
- [ ] `SET overlayNode.p = v` for a declared `overlay` key updates the overlay and does not persist
- [ ] `SET overlayNode.p = v` for an undeclared key targets the origin
- [ ] Read-after-write through an overlay node returns the just-written value (no stale shadow)
- [ ] Algorithm write-back over a `derive()` projection persists computed scores onto origin nodes
- [ ] e2e test covering all three write targets and read-after-write consistency

## Blocked by

- `02-project-write-back`
- `07-per-property-binding`

## Comments

`VirtualNode::SetProperty`/`RemoveProperty` now route by binding. A node carries an
`overlay_bound` key set (declared `overlay` policy keys plus construction-time override
keys) alongside the existing hidden set; `IsOverlayBound` is the single predicate that
decides both read source and write target, keeping the two stores coupled per key. On an
overlay node, an overlay-bound key mutates the overlay (never persisted); any other key
persists to the origin vertex via the existing `VertexAccessor` write path and clears any
stale overlay entry, so read-after-write returns the persisted value. A synthetic node
(no origin) is unchanged: writes always hit its overlay and never error.

The undeclared-key rule is "writes to origin": an overlay node treats every non-overlay
key as origin-bound, which is exactly the read-through default, so no separate
undeclared-key bookkeeping was needed.

e2e (`tests/e2e/write_procedures/virtual_graph.py`, `TestOverlayWriteBack`) covers the
three write targets, read-after-write consistency, and algorithm write-back persisting
scores onto origin nodes through the procedure path (the yielded overlay node keeps its
origin across the mgp round-trip).

Deferred (not required by this slice): origin writes through an overlay node bypass
fine-grained auth checks and trigger-context collection that a direct real-node `SET`
performs; `REPLACE` (`SET n = {...}`) on an overlay node clears only the overlay. The
`SetProperty` signature stays `void` and throws on origin-write failure, matching the
read path; the `Result`-returning shape in ADR 0001 is deferred with the rest of the
read-signature unification.
