# Virtual/overlay node memory layout - profile and optimise

Status: ready-for-human

## Parent

`.scratch/projections/PRD.md`

## What to do

The current in-memory layout of virtual (synthetic) and overlay nodes may not be
optimal. Before committing to it, build a **large** virtual graph and profile it
to understand where the memory and time actually go, then decide what to change.

Concretely:

- Construct a large virtual graph (many synthetic nodes/edges, and a `derive()`
  overlay over an embedding-heavy real graph) so the layout costs are visible.
- Profile allocation and access patterns: per-node overhead of the overlay
  property store, the origin reference, the synthetic-GID bookkeeping, and the
  cost of lazy read-through vs a real accessor.
- Confirm the headline memory claim holds: a `derive()` that does not read an
  embedding does not grow memory by the embedding size.
- Identify layout changes (struct packing, store representation, shared schema
  vs per-node copies) worth making before the data model ossifies.

## Why this is human-in-the-loop

Needs profiling judgement on a realistic workload and a perf/memory trade-off
decision, not a mechanical change. Output is a findings note plus any follow-on
implementation issues.

## Acceptance criteria

- [ ] A reproducible large-virtual-graph profiling setup (script/benchmark)
- [ ] Findings on per-node memory overhead and read-through cost
- [ ] A decision on layout changes, with follow-on issues if any

## Blocked by

- `04-virtual-edge-constructor`
- `06-derive-overlay-read-through`
