# Per-property binding + hidden + construction-time conflict validation

Status: ready-for-agent

## Parent

`.scratch/projections/PRD.md`

## What to build

The declared property policy on a `derive()` projection. Each property has one
**binding**: `origin` (read-through), `overlay` (declared overlay value shadows
origin on read), or `hidden` (invisible to reads and to function calls). Read
source and write target are coupled to one store per property - they are not
independently selectable. Overlay overrides at construction
(`sourceNodeProperties` / `targetNodeProperties`) are honored. The projection
**schema is static**: the overlay/hidden key set is fixed at construction.

A conflict between `propertyPolicy` and the override maps (a key both overlaid
and bound to `origin`) is a **construction-time query error**, not a silently
resolved precedence.

## Acceptance criteria

- [ ] A property bound `overlay` (or given a `sourceNodeProperties` override) shadows the origin value on read
- [ ] A property bound `hidden` is absent from reads and from function calls over the node
- [ ] An unlisted property reads through to the origin
- [ ] A config that both overlays a key and binds it to `origin` fails at construction with a clear message
- [ ] The overlay/hidden key set is fixed at construction (no runtime additions in this slice)
- [ ] e2e test covering shadow, hidden, read-through, and the conflict error

## Blocked by

- `06-derive-overlay-read-through`
