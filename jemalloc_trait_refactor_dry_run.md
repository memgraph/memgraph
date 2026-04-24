# Jemalloc Trait Refactor Dry Run

This is a sketch of what a traits-based cleanup could look like for the remaining
`#if USE_JEMALLOC` duplication.

It is not a proposed commit yet. The goal is to show the shape of the refactor
before touching code.

## What looks worth factoring

The current codebase has two kinds of jemalloc conditionals:

1. Real build-mode boundaries.
2. Small backend-specific branches where the control flow is the same and only
   the backend mechanics differ.

The second class is the candidate for traits.

Good candidates:

- [`src/memory/query_memory_control.cpp`](/home/andreja.linux/workspace/memgraph/src/memory/query_memory_control.cpp)
- [`src/memory/db_arena.cpp`](/home/andreja.linux/workspace/memgraph/src/memory/db_arena.cpp)

Likely not worth templating:

- `DbArenaScope`
- `DbAwareAllocator`
- `ArenaPool`
- any place where the compile-time split is already tiny and readable

## Suggested shape

### 1. Backend traits header

Create a small internal header, something like:

```cpp
namespace memgraph::memory::detail {

struct JemallocBackend {
  static constexpr bool kEnabled = true;

  static auto SaveArena(unsigned current) -> std::optional<unsigned>;
  static auto RestoreArena(std::optional<unsigned> prev) -> void;

  static auto ReadExtentHooks(unsigned arena_idx) -> extent_hooks_t *;
  static auto InstallExtentHooks(unsigned arena_idx, const extent_hooks_t *hooks) -> bool;
};

struct NoJemallocBackend {
  static constexpr bool kEnabled = false;

  static auto SaveArena(unsigned current) -> std::optional<unsigned>;
  static auto RestoreArena(std::optional<unsigned> prev) -> void;

  static auto ReadExtentHooks(unsigned) -> void *;
  static auto InstallExtentHooks(unsigned, const void *) -> bool;
};

}  // namespace memgraph::memory::detail
```

The point is not to mirror jemalloc APIs one-for-one. The point is to hide the
`#if USE_JEMALLOC` split behind a very small, explicit backend surface.

### 2. Cross-thread tracking

`CrossThreadMemoryTracking` currently has the same state machine in both build
modes, but the arena save/restore branches differ.

A trait-based version could look like:

```cpp
template <typename Backend>
class CrossThreadMemoryTrackingImpl {
 public:
  void StartTracking();
  void StopTracking();

 private:
  std::optional<unsigned> prev_arena_;
  bool started_{false};
};
```

Then the `.cpp` file would bind:

```cpp
using CrossThreadMemoryTracking =
    CrossThreadMemoryTrackingImpl<detail::JemallocBackend>;
```

or the no-jemalloc backend depending on the build.

Why this is attractive:

- the state machine stays in one place,
- the jemalloc/non-jemalloc split is local,
- and the zero-arena restore fix remains shared.

### 3. DbArena hook installation

The hook setup in `db_arena.cpp` could be split into a backend helper that owns
the mallctl interaction:

```cpp
template <typename Backend>
bool InstallDbArenaHooksImpl(unsigned arena_idx, DbArenaHooks &hooks, std::string_view context);
```

The backend would supply:

- how to read the current hooks,
- how to install the new hooks,
- and what to do in the non-jemalloc build.

That would let the higher-level `DbArena` constructor stay focused on policy:

- create the base arena,
- install hooks,
- fail if the base arena cannot be prepared,
- and fall back to the base arena only for later thread acquisitions.

## What should stay as preprocessor guards

Some code is already clear enough that a trait refactor would not help much:

- explicit jemalloc debug assertions,
- startup initialization that is only meaningful in jemalloc builds,
- and tiny one-off helpers where the `#if` is shorter than a trait abstraction.

If a conditional is already a 3-line branch, keeping the preprocessor may be the
cleanest option.

## What I would not do

I would not try to make the entire memory subsystem generic over backend traits.

That would likely:

- add template noise to the public headers,
- make debugging harder,
- and hide simple backend differences behind too much abstraction.

The sweet spot is a narrow backend trait for the duplicated state machines, not a
full rewrite.

## Dry-run conclusion

If we do this refactor, it should probably start with:

1. `CrossThreadMemoryTracking`
2. `InstallDbArenaHooks`

Those two are the best proof points because they have real jemalloc/non-jemalloc
duplication, but they still have a clear shared control flow.
