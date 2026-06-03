# PR7b-i — Light-edge delete lifecycle + graveyard GC routing (gated)

## What this PR implements

Adds the deferred-free path for **deleted light edges**. Light edges are
pool-allocated `Edge*` (introduced in PR7a) rather than skip-list nodes, so they
cannot be reclaimed by `edges_.run_gc()` like heavy edges. This PR routes a
deleted light `Edge*` into a per-storage **graveyard** at GC/commit/abort time
and frees it from a dedicated drain (`DrainLightEdgeGraveyard`) that runs each GC
cycle. Everything is gated by `config_.salient.items.storage_light_edge`; with
the flag off the storage is byte-behaviour-identical to PR7a.

This is the first half of the original PR7b. The second half (PR7b-ii) adds the
`EpochTracker`-backed iterable guard, the gated light `MakeEdgePin`, the
`LightEdgeIterable` analytical full-scan path, and the epoch-gated (`IsSafeToFree`)
drain. PR7b-i deliberately ships an **unconditional** drain (see "How").

## Why

PR7a made light-edge create/find work but left deletion unrealized: a deleted
light edge's `Edge*` was leaked because it is not a skip-list node. The graveyard
is the deferred-reclamation structure that lets GC hand off deleted light `Edge*`
for safe freeing after MVCC visibility has passed. Source intent and the two
locked danger-zone decisions are recorded in
`~/workspace/opencode-work/light_edges/pr-07b-graveyard-gc.md` (§I user
decisions): the GC edge-removal **ordering inversion is NOT ported** (gate-only
reconcile, heavy ordering preserved) and the dead `[[maybe_unused]]
transaction_id` param is **dropped**.

## How

### Data structures (storage.hpp)
- `struct LightEdgeGraveyardEntry { uint64_t guard_epoch; std::vector<Edge*,
  DbAwareAllocator<Edge*>> edges; };` — `guard_epoch` is reserved for the
  PR7b-ii epoch-gated drain; in PR7b-i it is a sentinel (`0`).
- `utils::Synchronized<std::list<LightEdgeGraveyardEntry>, utils::SpinLock>
  light_edge_graveyard_;` — per-storage queue, sibling of `deleted_edges_`.
- `LightEdgeGraveyardSizeForTest()` test helper; `DrainLightEdgeGraveyard()` decl.

### GC/commit/abort routing (storage.cpp) — gate-only reconcile
Three edge-removal blocks are wrapped as `if (light) { push to graveyard } else {
ORIGINAL heavy block, verbatim, in its original position }`:
1. `CollectGarbage` `// EDGES / LIGHT EDGES` — the light arm runs an
   **unconditional** light-only vector-edge-index cleanup before the push (the
   heavy arm only cleans when no vertices were deleted; the light `Edge*` and
   their `Vertex*` outlive the push, so the cleanup must always run). The heavy
   `else` arm is character-for-character identical to PR7a, **after** the VERTICES
   block — no ordering inversion.
2. `InMemoryAccessor::Abort` `// EDGES / LIGHT EDGES` — additive gate; ordering
   already metadata→vertices→edges on PR7a, unchanged.
3. `InMemoryAccessor::FastDiscardOfDeltas` — light arm pushes to graveyard, heavy
   arm keeps the original `deleted_edges_.insert`.

`Abort`'s `my_deleted_edges` local changed allocator from `std::vector<Edge*>` to
`std::vector<Edge*, DbAwareAllocator<Edge*>>` to enable `std::move` into the
graveyard entry — behaviour-neutral for heavy (transient local; allocator does
not affect skip-list removal).

### Drain (storage.cpp)
`DrainLightEdgeGraveyard()` early-returns when the flag is off, else swaps the
graveyard out under lock and frees every queued `Edge*` (via the
`edges_metadata_index_` **wrapper** `OnEdgeDeleted` + `DeleteLightEdge`).
Called in the ctor `free_memory_func_` lambda immediately after `edges_.run_gc()`.

### Why the unconditional drain is UAF-safe in PR7b-i (degenerate-safe invariant)
In PR7b-i **nothing pins light edges** — `MakeEdgePin` is still the heavy-only
`return edges_.access();` form, so no edge-index iterable holds a light `Edge*`
across a yield. The only readers are MVCC adjacency readers, and they are
protected by the standard MVCC GC gate: an `Edge*` is collected into
`current_deleted_edges`/`my_deleted_edges` only when its delete's
`unlinkable_timestamp < oldest_active_start_timestamp` (`storage.cpp:2949`),
which guarantees every concurrently active transaction has `start_ts > T_delete`
and therefore sees the edge as already-deleted under `View::OLD` — it can never
obtain the `Edge*` via adjacency delta reconstruction. The push and the
same-GC-cycle drain leave no gap (push in `CollectGarbage`, drain a few lines
later in the same lambda). Abort/FastDiscard are safe by stronger exclusion
arguments (aborted creations are visible only to the aborting txn; FastDiscard
runs only when the committing txn is the sole active transaction).
**Verified SAFE by logic-verifier** (2026-06-03). When PR7b-ii adds pinning
readers, the epoch tracker replaces this unconditional drain with `IsSafeToFree`.

### Teardown (storage.cpp)
`ClearLightEdges` (PR7a freed only live adjacency) now also drains and frees the
graveyard. The dtor / `Clear` / `DropGraph` already call `ClearLightEdges`
(gated, PR7a), so graveyard teardown is covered without a new call site.

## What changes (file map)
| File | Change |
|---|---|
| `src/storage/v2/inmemory/storage.hpp` | +`LightEdgeGraveyardEntry`, `light_edge_graveyard_`, `LightEdgeGraveyardSizeForTest`, `DrainLightEdgeGraveyard` decl (+26) |
| `src/storage/v2/inmemory/storage.cpp` | gated push at CollectGarbage/Abort/FastDiscard; `DrainLightEdgeGraveyard` def + ctor call; `ClearLightEdges` graveyard loop; `my_deleted_edges` allocator (+75/-17) |
| `tests/unit/storage_v2_light_edges.cpp` | +9 cases (delete/abort-delete/MVCC-delete/non-concurrent graveyard/graveyard-teardown) |
| `tests/unit/storage_v2_gc.cpp` | +`StorageV2GcLightEdge.{Sanity, ConcurrentEdgeOperationsAbortDeleteRepeat}` + helper |

## What does NOT change
- Heavy mode (flag off): proven byte-behaviour-identical to PR7a by VER-1
  (logic-verifier PASS) — same metadata→vertices→edges ordering, identical heavy
  else-arms, no `transaction_id` param, drain early-returns.
- `MakeEdgePin` is untouched (still heavy-only — light branch is PR7b-ii).
- `edges_metadata_` flattening stays DROPPED — the `EdgeMetadataIndex` wrapper
  (#4189) is used everywhere (`OnEdgeDeleted`).
- No durability (PR8) or replication (PR10) code; no `EpochTracker` /
  `LightEdgeIterable` / iterable-tracker construct (PR7b-ii).

## Test plan
- `storage_v2_light_edges`: 21/21 PASS (incl. 9 new 7b-i cases).
- `storage_v2_gc`: 26/26 PASS (24 heavy + 2 new light) — the heavy baseline is
  the in-practice GC-ordering regression net.
- Heavy regression net: `storage_v2_edge_inmemory` 45/45, `storage_v2_indices`
  116 passed / 0 failed (73 pre-existing disk-param skips).
- Deferred to PR7b-ii: reader-pin / partial-drain / analytical-full-scan /
  concurrent-CRUD-with-indices cases (need the epoch tracker).

## Dependencies
- Base: PR7a `light_edges-07a-storage-core` (`4d11a06d8`). Predecessor design
  doc: `docs/design/light_edges/PR-07a-*.md`.

## Follow-ups
- PR7b-ii `light-edge-iterable-guard-tracker` (base 7b-i): `EpochTracker`
  member, gated `MakeEdgePin`, `LightEdgeIterable` + analytical full-scan,
  epoch-gated drain (`guard_epoch = CurrentEpoch()` / `IsSafeToFree`).
- PR8 durability, PR10 replication.

## References
- Original messy branch: `light_edges` HEAD `222030a78` (source of truth for the
  copied test bodies and the reconciled hunks).
- Split plan: `~/workspace/opencode-work/light_edges/split-plan.md`.
- Reconcile + user decisions: `~/workspace/opencode-work/light_edges/pr-07b-graveyard-gc.md` (§0, §I, §J).
- EdgeMetadataIndex wrapper #4189.
- Verifications (2026-06-03): VER-1 heavy GC-ordering equivalence PASS;
  UAF-safety of unconditional drain SAFE (both logic-verifier).
