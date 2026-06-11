# Overlay write-back

Status: ready-for-agent

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
