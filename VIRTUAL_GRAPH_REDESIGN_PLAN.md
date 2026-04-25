# VirtualGraph redesign — plan

Follow-up refactor on top of `extend-project-function-for-derived-edges`.
Net direction: shrink VirtualGraph to the data it actually needs, lift all
derive-aggregation bookkeeping into the aggregator, delete dead overlay
machinery.

## Target state (end-of-refactor)

### VirtualNode — standalone entity

```cpp
class VirtualNode final {
  storage::Gid gid_;                         // synthetic, from NextSyntheticGid()
  pmr::vector<pmr::string> labels_;
  property_map properties_;
};
```

No `original_gid_`, no `OriginalGid()`. A VirtualNode knows only itself.

### VirtualEdge — EdgeAccessor-style borrow

```cpp
class VirtualEdge final {
  storage::Gid gid_;
  const VirtualNode *from_, *to_;            // borrow into VirtualGraph::nodes_
  pmr::string type_;
  property_map properties_;
  size_t cached_hash_;
};
```

Endpoints are raw pointers into the owning VirtualGraph's node map. No
VirtualNode copies per edge. Lifetime anchored by the graph holding a
`shared_ptr` to its node map (see below).

### VirtualGraph — flat, self-contained, provenance-free

```cpp
class VirtualGraph final {
  // shared so escaping edges (e.g. g.edges returned through subqueries)
  // keep the node map alive via their VirtualEdge pointers.
  shared_ptr<pmr::unordered_map<Gid, VirtualNode>> nodes_;

  pmr::unordered_set<VirtualEdge> edges_;    // keyed by (from_gid, to_gid, type)
  pmr::unordered_map<Gid, pmr::vector<const VirtualEdge*>> out_index_;
  pmr::unordered_map<Gid, pmr::vector<const VirtualEdge*>> in_index_;
};
```

No `real_to_virtual_`, no `synthetic_to_original_`, no sub-store classes.
All three containers are private members; operations are methods directly
on VirtualGraph.

### DeriveAccumulator — aggregation-phase scratch

```cpp
class DeriveAccumulator {
  pmr::unordered_map<Gid, const VirtualNode *> real_to_virtual_;  // dedup
  VirtualGraph building_;
 public:
  void AddPath(const Path &p, const MapValue &options);
  VirtualGraph Finalize() &&;   // throws away real_to_virtual_
};
```

Owned by the aggregate-operator state; destroyed when aggregation finishes.

## Deletions

| Item | Why |
|---|---|
| `VirtualNode::original_gid_`, `OriginalGid()` | Moved to DeriveAccumulator |
| `VirtualNodeStore` class (both .hpp/.cpp) | Inlined into VirtualGraph |
| `VirtualEdgeStore` class (both .hpp/.cpp) | Inlined into VirtualGraph |
| `synthetic_to_original_` map | Dead after above |
| `FindBySyntheticGid` | Collapses into `nodes_->find` |
| `VirtualOverlay::virtual_only` bool | Always true; bit is meaningless |
| `IsVirtualOnly()` method | Equivalent to `IsVirtual()` |
| `mg_procedure_impl.cpp:3014-3023` and its out-edges twin (~3099-3108) | Hybrid path; unreachable |
| `vg->node_store().Find(acc.Gid())` translation at MGP edge iteration | Gone with the hybrid path |

## Invariants (post-refactor)

1. Every `VirtualEdge::from_` / `to_` pointer is an address of a value stored
   in `nodes_`. `pmr::unordered_map` gives pointer stability across insertions.
2. A `VirtualEdge` outlives the node map it points into iff a `shared_ptr<nodes_>`
   is kept alive by something reachable. The VirtualGraph itself holds one;
   TypedValues holding a VirtualGraph propagate it transitively. Standalone
   VirtualEdge TypedValues (e.g. from `collect(e)`) must hold their own
   `shared_ptr<nodes_>` anchor — see step 4.
3. `VirtualNode::Gid()` is globally unique across a process (counter decreases
   monotonically). `VirtualEdge::Gid()` shares that counter. Neither collides
   with storage Gids (which count up from 0).
4. `edges_` is the single source of truth for edge membership; `out_index_`
   and `in_index_` hold `const VirtualEdge*` into `edges_`'s buckets
   (`unordered_set` is also pointer-stable).
5. `DeriveAccumulator::real_to_virtual_` is scratch — it is not exposed on
   the final VirtualGraph and does not survive `Finalize()`.

## Steps

Each step compiles on its own. Commit after each.

### Step 1 — Delete the hybrid overlay code path

Foundation: remove dead code first so later steps don't have to carry it.

- `mg_procedure_impl.hpp`: drop `virtual_only` from `VirtualOverlay`;
  collapse `IsVirtualOnly()` into the existing `IsVirtual()` (rename if
  clearer).
- `mg_procedure_impl.cpp`: delete the two hybrid-overlay translation blocks
  (one in `mgp_vertex_iter_in_edges` at ~3014-3023, its out-edges twin at
  ~3099-3108). Simplify surrounding `if (!IsVirtualOnly()) { ... real ... }
  if (auto *vg = ...) { ... virtual ... }` into straight `if (IsVirtual())`
  / `else` branches.
- `operator.cpp:7817`: drop the `.virtual_only = true` designator.
- Anywhere `IsVirtualOnly()` was read, replace with `IsVirtual()`.

**Files touched:** `mg_procedure_impl.hpp/.cpp`, `operator.cpp`.
**Risk:** low. Removes code whose condition was never true.
**Commit title:** `Remove dead hybrid virtual-overlay path; collapse virtual_only flag`

### Step 2 — Inline stores into VirtualGraph

Structural flattening; no semantic change.

- Move `nodes_` (keyed by original_gid for now — we'll re-key in step 5),
  `synthetic_to_original_`, `edges_`, `out_index_`, `in_index_` onto
  `VirtualGraph` as private members.
- Expose methods directly on VirtualGraph: `InsertOrGetNode`, `FindNode`,
  `FindNodeBySyntheticGid`, `InsertEdgeIfNew`, `OutEdges`, `InEdges`,
  `nodes()`, `edges()`, `Merge(const&)`, `Merge(&&)`.
- Delete `virtual_node_store.hpp/.cpp` and `virtual_edge_store.hpp/.cpp`.
- Update call sites: `vg.node_store().X(...)` → `vg.X(...)`,
  `vg.edge_store().Y(...)` → `vg.Y(...)`. Sites to change: `operator.cpp`
  (derive), `mg_procedure_impl.cpp`, `eval.cpp`, `communication.cpp`, tests.
- Collapse VirtualGraph's `Merge` implementation: one method owns both
  halves, no DMG_ASSERT on sub-allocators.

**Files touched:** `virtual_graph.{hpp,cpp}`, delete `virtual_{node,edge}_store.*`,
`operator.cpp`, `mg_procedure_impl.{hpp,cpp}`, `eval.cpp`, `communication.cpp`,
`tests/unit/query_virtual_{node,edge}_store.cpp` (fold tests or drop).
**Risk:** medium — large surface, but each replacement is mechanical.
**Commit title:** `Inline VirtualNodeStore/VirtualEdgeStore into VirtualGraph`

### Step 3 — Switch VirtualEdge to pointer endpoints

Shrink the edge, eliminate per-edge node copies.

- `VirtualEdge`: replace `VirtualNode from_, to_` with
  `const VirtualNode *from_, *to_`. Keep `gid_`, `type_`, `properties_`,
  `cached_hash_`.
- Remove VirtualEdge's copy/move-with-allocator ctors for the node fields
  (pointers are trivial). Property map and type string still need allocator
  propagation.
- `FromGid()`/`ToGid()` dereference the pointer.
- `From()`/`To()` return `const VirtualNode&` by dereferencing the pointer.
- `InsertEdgeIfNew`: after `edges_.insert(...)`, lookup returns a stable
  VirtualEdge reference whose `from_`/`to_` already point into
  `VirtualGraph::nodes_` (passed at construction by derive()).
- Build-site update in `operator.cpp` (derive): pass
  `&stored_from_node, &stored_to_node` instead of references.

**Files touched:** `virtual_edge.hpp`, `virtual_graph.{hpp,cpp}`,
`operator.cpp` (derive construction), tests that build raw VirtualEdges.
**Risk:** medium-high — pointer stability must hold under every copy/move
path. Add a static_assert or runtime DMG_ASSERT on that in the debug build.
**Commit title:** `Store VirtualEdge endpoints by pointer into the node map`

### Step 4 — Anchor VirtualGraph's node map in a shared_ptr

Lifetime anchor for edges that escape the graph.

- `VirtualGraph`: change `nodes_` from `pmr::unordered_map<Gid, VirtualNode>`
  to `shared_ptr<pmr::unordered_map<Gid, VirtualNode>>` (allocated via
  `allocate_shared` with the graph's PMR allocator).
- VirtualEdge gains a `shared_ptr<const pmr::unordered_map<Gid, VirtualNode>>
  nodes_anchor_` field — one pointer per edge, bumped once per edge copy.
  Raw `from_`/`to_` pointers stay.
- TypedValue's VirtualEdge copy/move propagates the anchor (already uses
  unique_ptr<VirtualEdge>, so this just means VirtualEdge's copy ctor
  carries the anchor — one atomic per copy, not per access).
- Re-check `collect(e)` / `g.edges` return paths: escaping edges now keep
  the node map alive even if the source VirtualGraph is destroyed.

**Files touched:** `virtual_graph.{hpp,cpp}`, `virtual_edge.hpp`,
`typed_value.{hpp,cpp}` (verify propagation).
**Risk:** medium — PMR + `allocate_shared` is fiddly. Write a unit test
that proves `collect(e)` outlives its source graph.
**Commit title:** `Anchor VirtualGraph's node map in shared_ptr; VirtualEdge carries anchor`

### Step 5 — Drop OriginalGid from VirtualNode; move dedup to DeriveAccumulator

Makes VirtualNode a standalone entity; removes `synthetic_to_original_`.

- `VirtualNode`: delete `original_gid_` field, `OriginalGid()` method, and
  the 3-arg ctor parameter. Ctor now takes `(labels, properties, alloc)`
  and self-assigns `gid_ = NextSyntheticGid()`.
- `VirtualGraph::nodes_` re-keyed by `VirtualNode::Gid()` (synthetic) instead
  of original_gid. Edge indexes already key on synthetic gids, so no change
  there.
- Delete `synthetic_to_original_`, `FindNodeBySyntheticGid` (Find now does
  this job).
- Introduce `DeriveAccumulator` class in `operator.cpp` (or
  `virtual_graph_derive.hpp` if it grows). Field: `pmr::unordered_map<Gid,
  const VirtualNode *> real_to_virtual_`. Methods: `AddPath(path, options)`,
  `Finalize() &&`.
- Move all derive() aggregation logic from `UnifyAggregation::Update` into
  `DeriveAccumulator::AddPath`. The aggregator state holds a
  `DeriveAccumulator` instead of a `VirtualGraph`. On `TerminateValue` or
  equivalent, call `Finalize()`.
- Cross-accumulator merge (parallel aggregators): `DeriveAccumulator::Merge`
  merges both the building VirtualGraph and the real_to_virtual_ maps.
  Aliasing (same real vertex → two synthetic gids in two branches) is
  resolved here, not in the graph — `real_to_virtual_.try_emplace` keeps
  the first synthetic it saw; the edges from the second branch are rewritten
  to point to the canonical VirtualNode.
- `mgp_graph_get_vertex_by_id` (virtual-only path, `mg_procedure_impl.cpp:3482`):
  if the user holds a synthetic gid, `vg->FindNode(gid)` works directly.

**Files touched:** `virtual_node.{hpp,cpp}`, `virtual_graph.{hpp,cpp}`,
`operator.cpp` (UnifyAggregation rewrite), `mg_procedure_impl.cpp`,
`tests/unit/query_virtual_*`.
**Risk:** high — cross-branch merging semantics change. Write unit tests
for the parallel-aggregator merge case before touching, confirm they pass
on the new code.
**Commit title:** `Drop OriginalGid from VirtualNode; derive() owns dedup via DeriveAccumulator`

## Open questions

1. **`allocate_shared` vs raw `make_shared` with default allocator**: Is it
   worth the PMR complexity for one control block per graph? The alternative
   is `make_shared<...>()` using the default heap for the control block while
   the map itself stays PMR. One extra heap allocation per VirtualGraph; zero
   hot-path cost. Probably fine.

2. **Does VirtualEdge's shared_ptr anchor actually help, or do we also need
   one on standalone VirtualNode TypedValues?** If `startNode(e)` wraps the
   pointed-to node in a new `TypedValue::VirtualNode`, that TypedValue holds
   a *copy* of the VirtualNode (today it's value-copied). The copy is
   self-sufficient and doesn't need an anchor. So the answer is no — only
   edges need the anchor.

3. **What happens to `Gid()` semantics for externally-visible ids?** Users
   today see synthetic gids when iterating. That stays the same in the new
   design. The change is internal: fewer translation layers, not different
   visible ids.

4. **Do we keep the decrementing-counter scheme for `NextSyntheticGid`?**
   Yes. The rationale (no collision with real Gids) is still valid for
   VirtualEdges. Nodes now also benefit from it — consistent id-space,
   virtual-anything is "id > some_large_threshold."

## Branching strategy

- Cut a new branch off the current HEAD:
  `virtual-graph-standalone-redesign` (or similar).
- Land each step as one commit; do not squash until the final PR.
- The current PR `extend-project-function-for-derived-edges` can ship as-is
  once CI is green; this refactor lands as a follow-up PR.

## Validation plan

After each step:
- `cmake --build build --target memgraph -j4` passes.
- `ctest -R query_virtual` passes in `build/`.
- `python3 tests/e2e/runner.py --workloads-root-directory tests/e2e/virtual_graph/`
  (or wherever the derive e2e tests live) passes.

After step 4:
- New unit test: build a VirtualGraph, extract `g.edges()` as a list of
  TypedValues, destroy the graph TypedValue, verify the edge TypedValues
  are still usable (`From()`, `To()`, properties) without UB under ASan.

After step 5:
- Parallel-aggregator test: two branches produce VirtualGraphs with the
  same real vertex; after merge, there is exactly one VirtualNode for that
  vertex, and edges from both branches resolve to it.
