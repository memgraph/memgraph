---
title: Resolve per-cursor ProfilingStats slot at cursor construction
type: AFK
status: done
---

## Parent

Follow-up to discussions on PROFILE-mode overhead. The hot Expand on the
4-hop benchmark spends measurable time inside `ScopedProfile` doing
work that is invariant across the cursor's lifetime.

## Motivation

`ScopedProfile`'s ctor today (`src/query/plan/scoped_profile.hpp:31`)
does, every single Pull:

1. Load `context_->is_profile_query` and branch on it.
2. Load `context_->stats_root` (current parent stats slot).
3. `std::find_if` over `root_->children` for a matching key.
4. Either append a new child or point to the existing one.
5. Push self as new `stats_root`.
6. `++actual_hits`, `ReadTSC()`.

Steps 1-5 are invariant: from Pull 2 onwards of the same cursor, the
resolved slot is identical. On the 4-hop benchmark's top Expand (492M
pulls) the wasted work is several seconds of PROFILE wall time.

## What to build

Resolve `profile_slot_` at cursor *construction* instead of at every
Pull. Pull becomes a single RAII timer over a precomputed pointer.

### Design

**`ProfileContext` shim** (new type):
```cpp
struct ProfileContext {
  ProfilingStats *parent_stats{nullptr};
  const DbAccessor *db_accessor{nullptr};
};
```
By value, default-constructed = "no profiling". Trivially copyable.

**`LogicalOperator::MakeCursor` signature**:
```cpp
virtual UniqueCursorPtr MakeCursor(utils::MemoryResource *mem,
                                    ProfileContext profile = {}) const = 0;
```
Default keeps existing test call-sites compiling unchanged.

**`Cursor` base**:
```cpp
class Cursor {
 protected:
  ProfilingStats *profile_slot_{nullptr};
  Cursor(ProfileContext profile, const NamedLogicalOperator &op);  // name = op.ToString()
  Cursor(ProfileContext profile, const char *name);                 // name = literal
  Cursor() = default;                                               // tests / no profiling
  ...
};
```

Each cursor's ctor:
```cpp
ExpandCursor::ExpandCursor(const Expand &self, MemoryResource *mem, ProfileContext profile)
    : Cursor(profile, self),                                          // base populates profile_slot_
      self_(self),
      input_cursor_(self.input_->MakeCursor(mem, ChildContext(profile, profile_slot_)))
{}
```
With `ChildContext(parent, slot)` = `{slot, parent.db_accessor}` — when `slot` is null, children get a no-profiling context, propagating the off state.

**Stats-tree structure**:
`ProfilingStats::children` changes from `std::vector<ProfilingStats>` to
`std::deque<ProfilingStats>` so that pointers into it survive subsequent
`emplace_back` calls (a Cartesian appends two children consecutively).

**`ScopedProfile` becomes a trivial RAII timer**:
```cpp
class ScopedProfile {
 public:
  explicit ScopedProfile(ProfilingStats *slot) noexcept : slot_(slot) {
    if (slot_) { slot_->actual_hits++; start_time_ = utils::ReadTSC(); }
  }
  ~ScopedProfile() noexcept {
    if (slot_) slot_->num_cycles += utils::ReadTSC() - start_time_;
  }
 private:
  ProfilingStats *slot_;
  uint64_t start_time_{0};
};
```

**Macros (`SCOPED_PROFILE_OP`, `SCOPED_PROFILE_OP_BY_REF`) are deleted.**
Each Pull's first line becomes:
```cpp
ScopedProfile profile{profile_slot_};
```
Explicit, single-arg, single-line.

**`PullPlan` reorders fields** so `ctx_` is constructed before `cursor_`,
allowing `cursor_(plan->plan().MakeCursor(execution_memory, profile))`
in the init list with `profile.parent_stats = &ctx_.stats` when
`is_profile_query`.

**`ctx_.stats_root` field is removed.** Nothing reads or writes it after
this change.

### Stats name resolution for specialised cursors

`ExpandVariable::MakeCursor` instantiates one of five cursor types
(DFS / BFS-SSSP / BFS-ST / Weighted / All / KShortest) sharing the
operator. The two base-ctor flavours let us pick:

- DFS uses `Cursor(profile, self)` → name from `ExpandVariable::ToString()`
  which already produces "ExpandVariable (a)-[*]->(b)" via OperatorName().
- The four specialised cursors use `Cursor(profile, "STShortestPath")`
  etc. — preserves today's bare-name PROFILE output exactly.

Operators whose `ToString()` returns `GetTypeInfo().name` (`Once`,
`CreateNode`, `ConstructNamedPath`, etc., via the default
`LogicalOperator::ToString()` at `operator.cpp:9576`) switch from the
literal-string variant to the op-ref variant with zero behaviour change.

## Acceptance criteria

- [ ] `MakeCursor` takes `ProfileContext profile = {}`; every override
      forwards it and constructs children via `ChildContext`.
- [ ] `Cursor` base gains `profile_slot_` and the two new ctors.
- [ ] `ProfilingStats::children` is `std::deque<ProfilingStats>`; all
      consumers (`ProfilingStatsToTable`, `ProfilingStatsToJson`, the
      parallel-exec merge in `operator.cpp:10634`) still compile and
      produce identical output on representative queries.
- [ ] `ScopedProfile` reduced to the trivial RAII form; both macros
      (`SCOPED_PROFILE_OP`, `SCOPED_PROFILE_OP_BY_REF`) deleted; every
      Pull uses `ScopedProfile profile{profile_slot_};` directly.
- [ ] `ExecutionContext::stats_root` removed.
- [ ] `PullPlan` declaration order updated so `ctx_` precedes `cursor_`;
      `MakeCursor` is called with the appropriate `ProfileContext` in
      the init list.
- [ ] All existing planner unit tests pass (196 + 148 + 88 + 4 + ...).
- [ ] PROFILE output on the 4-hop dense graph: top Expand absolute time
      drops by ~2-3s vs pre-#0007 baseline; total wall time drops
      similarly.
- [ ] Non-PROFILE wall time unchanged on the same benchmark.

## Blocked by

None (independent of #0001-#0006, though built on top of them).
