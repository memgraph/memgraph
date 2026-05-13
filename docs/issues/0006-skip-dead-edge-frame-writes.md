---
title: Skip dead edge-symbol frame writes on fused Expands
type: AFK
status: done
---

## Parent

Follow-up to Stages 1-4 (`docs/issues/0001..0005`). Implements the deferred
`B1` line from the original `IMPLEMENTATION_PLAN.md` "out of scope" section.

## Motivation

After Stages 1-3 fuse `Expand+EdgeUniquenessFilter` into a single Expand,
the fused Expand still writes the edge symbol to the frame:

```cpp
if (!try_commit_uniqueness(edge)) continue;
frame_writer.Write(self_.common_.edge_symbol, edge);   // <-- dead store
pull_node(edge, ...);
return true;
```

For the 4-hop benchmark `MATCH (a:Start)-[:E]->()-[:E]->()-[:E]->()-[:E]->(b)`
this dead store fires ~492M times on the topmost Expand alone. The pre-Stage-1
profile attributed ~7% of total samples to `TypedValue::operator=` and ~2.5%
to `~TypedValue` at edge-symbol frame-write call sites - that cost is still
being paid by the fused operator.

Edges where the write is genuinely dead:

| Edge | Read by | Write needed? |
|------|---------|---------------|
| anon1 | fused Expand(anon3)'s `unique_previous_symbols_ = [anon1]` seed | yes, today |
| anon3 | nobody | no |
| anon5 | nobody | no |
| anon7 (topmost) | nobody | no |

The only thing keeping anon1's frame-write alive is that the upper fused
Expand reads `frame[anon1]` to seed the shared uniqueness container. If we
let the *lower* Expand (which produces anon1) push its own Gid directly,
that read goes away too and all four frame-writes can be skipped.

## What to build

Two coordinated changes:

### 1. Push at the leading Expand of a fused chain

In the post-rewrite pass (`src/query/plan/rewrite/fuse_edge_uniqueness.hpp`),
after the fusion loop has stamped every eligible Expand:

- For each fused Expand `X` whose `unique_previous_symbols_ = [s]` (exactly
  one symbol), find the operator directly below that produces `s`. If it
  is a plain `Expand` with `unique_pattern_id_ == -1` and its edge symbol
  is not consumed anywhere else in the plan, stamp it:

  ```
  lower->unique_pattern_id_     = X->unique_pattern_id_;
  lower->unique_previous_symbols_ = {};
  lower->unique_is_topmost_      = false;   // it must push so X can check
  ```

  Then clear `X->unique_previous_symbols_` since the lower Expand now
  populates the shared container directly.

- Multi-symbol seeds (`previous_symbols_.size() > 1`, the "subquery /
  non-standard binding order" case noted in `rule_based_planner.hpp`) are
  out of scope - leave them as-is.

- This change must also work when a surviving (i.e. not fused) EUF sits
  above an Expand chain: if the EUF's `previous_symbols_ = [s]` and the
  Expand producing `s` is fusable as a leading pusher, do the same swap on
  the EUF.

### 2. Skip the edge-symbol frame write when nothing reads it

Add a single `bool skip_edge_frame_write_ = false` (or equivalent) on
`Expand`, set during the post-rewrite pass when *both* hold:

- `unique_pattern_id_ >= 0` (this Expand is fused), and
- `edge_symbol` is not in the post-pass's "referenced anywhere" set.

The collector already knows the answer to the second question - it's the
same set the fusion eligibility check used. Pass it to a second walk that
decides whether to set the bit.

In `ExpandCursor::Pull`, when `skip_edge_frame_write_` is set, omit both
the `frame_writer.Write(self_.common_.edge_symbol, edge)` calls. The
node-symbol writes stay - those are still consumed by the next Expand as
its `input_symbol_`.

### 3. ExpandVariable - explicitly out of scope

The var-length fused operator reads `frame[edge_symbol]` (the path list)
inside `TryCommitUniqueness` to validate the candidate path. Skipping the
list write requires restructuring the var-length DFS to track Gids
incrementally without the frame. Track in a separate issue if needed.

## Acceptance criteria

- [ ] Planner pass extends the chain so the leading Expand of any
      single-symbol-seeded fused chain becomes a pusher, and the upper
      Expand's `unique_previous_symbols_` is cleared.
- [ ] When a surviving EUF has `previous_symbols_ = [s]` and `s`'s
      producing Expand is fusable, the same swap is applied.
- [ ] `Expand` gains a `skip_edge_frame_write_` flag; clone handles it.
- [ ] `ExpandCursor::Pull` omits `frame_writer.Write(edge_symbol, ...)`
      both for the IN and OUT branches when the flag is set.
- [ ] Existing planner + e2e tests pass unchanged.
- [ ] `PROFILE` on the 4-hop dense graph: top Expand absolute time drops
      noticeably; `TypedValue::operator=` and `~TypedValue` samples for
      edge symbols disappear from the flamegraph.
- [ ] No regression on the EUF microbenchmark.

## Blocked by

- `0002-expand-unique-pattern-id-narrow-fusion.md` (done)
- `0003-planner-detect-any-unconsumed-edge-symbol.md` (done)
- `0004-expand-variable-unique-pattern-id.md` (done)
