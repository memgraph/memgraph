---
title: Add Expand::unique_pattern_id_, fuse narrow Expand→EUF case in planner
type: AFK
status: in-progress
---

## Parent

`IMPLEMENTATION_PLAN.md` — Stage 1 (tracer bullet).

## What to build

In the 4-hop `MATCH (a:Start)-[:E]->()-[:E]->()-[:E]->()-[:E]->(b)` profile, the
anonymous edge symbols are only ever read by the `EdgeUniquenessFilter`
directly above each Expand. Writing those edges through `FrameWriter::Write` →
`TypedValue::operator=` (variant switch + struct copy) costs ~7% of total
query time, plus ~2.5% in the corresponding `~TypedValue` destruction.

Eliminate the frame round-trip by extending `Expand` itself to maintain the
uniqueness set when its produced edge is consumed by nothing other than an
EUF directly above. The EUF disappears from the plan; Expand pushes the Gid
straight into the same per-context `absl::InlinedVector<uint64_t, 8>` container
EUF uses today (per c9178ab70b), checking for conflict before publishing the
expanded edge.

**Operator change** (`src/query/plan/operator.hpp/.cpp`):

- Add `int unique_pattern_id_{-1}` to `Expand`.
- `-1` means "no uniqueness duty" — Expand behaves exactly as today.
- A non-negative value selects the per-context container slot via the existing
  `GetUniquenessSet(context, unique_pattern_id_)` helper.
- When set, inside the edge-iteration loop: skip any edge whose Gid is already
  in the container; on accept, push the Gid before publishing. Use
  push/pop bookkeeping equivalent to EUF's `pushed_this_cycle_` so re-entry
  rolls back the prior accept.
- Plan-clone / serialization paths must copy the new member.

**Planner change** (`src/query/plan/rewrite/` or wherever EUF is inserted):

- Detect the exact pattern: `Expand(edge_sym = X) → EdgeUniquenessFilter(expand_symbol = X)`
  with no operator in between, `X` anonymous, single-hop (`Expand`, not
  `ExpandVariable`), and no downstream consumer of `X`.
- Rewrite to a single `Expand` with `unique_pattern_id_ = EUF.pattern_id_`,
  drop the EUF.

Scope is intentionally narrow — Stage 2 (#3) generalizes the planner pass to
non-anonymous-but-unused symbols; Stage 3 (#4) extends to `ExpandVariable`.

## Acceptance criteria

- [ ] `Expand` gains `int unique_pattern_id_{-1}`; clone/serialize handle it.
- [ ] When `unique_pattern_id_ >= 0`, Expand performs conflict-check + push +
      rollback against the shared container.
- [ ] Planner rewrites the narrow `Expand → EUF` adjacency for anonymous
      single-hop edges into the fused form.
- [ ] Planner does **not** rewrite when the edge symbol is referenced
      downstream, when a non-EUF operator sits between Expand and EUF, or when
      the Expand is var-length.
- [ ] Existing pattern / MATCH / uniqueness e2e tests pass unchanged.
- [ ] Existing operator + planner unit tests pass unchanged. Approval-style
      plan tests are updated only where fusion legitimately applies; diffs
      reviewed.
- [ ] `PROFILE MATCH (a:Start)-[:E]->()-[:E]->()-[:E]->()-[:E]->(b) RETURN count(DISTINCT b)`
      on the dense graph from `tools/profile_edge_uniqueness.sh`: EUF rows
      vanish from the operator table; net query time drops noticeably (target:
      ≥8% reduction).
- [ ] Flamegraph from the same workload: `TypedValue::operator=` and
      `~TypedValue` self-time from `FrameWriter::Write(edge_symbol, ...)` call
      sites drops to near zero.
- [ ] Existing `tests/benchmark/edge_uniqueness_filter.cpp` shows no
      regression on the EUF-specific microbench.

## Blocked by

None - can start immediately (independent of #0001).
