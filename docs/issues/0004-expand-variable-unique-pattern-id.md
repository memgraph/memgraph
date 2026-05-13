---
title: Extend fusion to ExpandVariable (var-length scratch edges)
type: AFK
status: needs-triage
---

## Parent

`IMPLEMENTATION_PLAN.md` — Stage 3 (the `B4b` extension requested upstream).

## What to build

Extend the `unique_pattern_id_` mechanism from `Expand` to `ExpandVariable`.
Var-length expansions produce a `List<Edge>` symbol; when that symbol is
unconsumed downstream and the EUF directly above it is the only reader, the
fused operator pushes every Gid of the path into the shared uniqueness
container during expansion and rejects candidates whose path Gids would
collide with anything already in the container.

Backtracking inside the var-length DFS must pop exactly the Gids pushed for
the abandoned path prefix — extends the existing `pushed_this_cycle_`
bookkeeping to handle batch push/pop.

BFS / Dijkstra / AllShortestPaths variants are explicitly out of scope —
they don't go through EUF on the hot path; their uniqueness handling is
internal.

## Acceptance criteria

- [ ] `ExpandVariable` gains `int unique_pattern_id_{-1}` with the same
      semantics as on `Expand`.
- [ ] Push/pop bookkeeping correctly handles batch-push for `List<Edge>` and
      rolls back exactly the pushed Gids on backtrack / re-entry.
- [ ] Planner rewrite handles the var-length adjacency
      (`ExpandVariable → EUF`) with the same symbol-usage analysis from
      #0003.
- [ ] Existing var-length e2e tests pass unchanged, including ones that
      exercise partial-overlap rejection within a single path.
- [ ] `PROFILE` on a representative var-length pattern with unconsumed edge
      symbol (e.g. `MATCH (a)-[*1..3]->(b) WHERE a.id = 1 RETURN count(b)`):
      EUF row disappears from the operator table; query time drops.

## Blocked by

- `0003-planner-detect-any-unconsumed-edge-symbol.md`
