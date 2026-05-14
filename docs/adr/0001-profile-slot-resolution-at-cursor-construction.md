---
status: accepted
---

# Profile slot resolution moved from Pull time to cursor construction time

PROFILE-mode queries used to resolve each operator's `ProfilingStats`
slot lazily on every `Pull`: `ScopedProfile`'s ctor did a `find_if` over
the current parent's children to locate (or create) its own slot, then
manipulated `ExecutionContext::stats_root` as a push/pop stack. The
resolution is invariant per cursor, so doing it once per Pull cost
measurable wall-time on hot operators (~5 ns per Pull × hundreds of
millions of pulls on edge-uniqueness-heavy queries).

We changed `LogicalOperator::MakeCursor` to take a `ProfileContext`
shim (`{ProfilingStats *parent_stats; const DbAccessor *db_accessor;}`,
default-constructed = "no profiling"). Each cursor's base ctor appends
its own slot under `parent_stats` once, stores the resulting pointer in
a `profile_slot_` member, and passes that slot down to its children's
`MakeCursor`. Pull then reduces to a single trivial RAII timer over a
precomputed pointer. The stats tree mirrors the operator tree 1:1 by
construction, so no map keyed by operator pointer is needed.

## Considered alternatives

- **Lazy first-Pull caching.** Same end state but Pull 1 still does the
  find_if. Rejected because the construction-time form removes the
  cache-or-resolve branch from Pull entirely.
- **Pre-walk the operator tree before MakeCursor, building a
  `unordered_map<const LogicalOperator *, ProfilingStats *>`.** Adds a
  separate visitor and a hash lookup per cursor ctor. Rejected because
  the lockstep "parent passes slot to child via MakeCursor parameter"
  shape needs no map at all — the tree structure carries the
  information.
- **`thread_local` "current profile builder" during cursor
  construction.** Avoids the MakeCursor signature change but hides flow
  and breaks under any future parallel cursor construction. Rejected.

## Consequences

- `ProfilingStats::children` is now `std::deque<ProfilingStats>` (was
  `std::vector`) so that pointers into it survive subsequent
  `emplace_back` calls when a multi-input cursor like Cartesian appends
  both children sequentially.
- `ExecutionContext::stats_root` is removed. The push/pop discipline it
  supported no longer exists; the stats tree is built directly in the
  ctor chain.
- `SCOPED_PROFILE_OP` and `SCOPED_PROFILE_OP_BY_REF` macros are deleted.
  Each cursor's `Pull` now starts with an explicit
  `ScopedProfile profile{profile_slot_};` line.
- `MakeCursor` signature change ripples through every cursor (~50
  override sites). Tests that construct cursors directly continue to
  compile via the defaulted `ProfileContext{}` parameter.
- PROFILE output for `Once`, `CreateNode`, `ConstructNamedPath` and
  similar cursors is unchanged because their default `ToString()`
  returns `GetTypeInfo().name`, identical to today's literal-string
  variant of the deleted macro. The five specialised cursors inside
  `ExpandVariable` continue to use a literal-string base ctor flavour
  to preserve their existing bare-name profile output.
