# Unify codebase to reduce node-kind special cases

Status: ready-for-human

## Parent

`.scratch/projections/PRD.md`

## What to do

The feature introduces three node kinds threaded through the read path, the
functions, Bolt marshalling, and `mgp_graph` dispatch: real, subgraph/overlay,
and synthetic. Left unchecked this multiplies `if (is_virtual) ... else ...`
branches across the codebase. Find the unifying abstraction that collapses these
special cases into one polymorphic seam.

Concretely:

- Map where node-kind dispatch currently forks (accessor reads, function calls,
  serialization, write-back).
- Propose a single interface/seam (e.g. an accessor abstraction the three kinds
  implement) so call sites stop branching on kind.
- Sequence the refactor so it lands without regressing the real-graph hot path.

## Why this is human-in-the-loop

Architectural consolidation requiring judgement about the right abstraction
boundary and the hot-path cost of indirection. Best done after the slices land
so the real special-case set is known. See the architecture-improvement skill.

## Acceptance criteria

- [ ] An inventory of node-kind branch points
- [ ] A proposed unifying abstraction with a hot-path-cost argument
- [ ] A staged refactor plan, with follow-on issues

## Blocked by

- `05-projection-from-lists`
- `08-overlay-write-back`
