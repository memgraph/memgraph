# Branch-side change filters — design & plan (v1 draft)

**Goal:** eliminate the dominant branch-read hotspot — the per-branched-object `FindVertex` gid-lookup done to read a field of a branched vertex — by consulting a cheap, **branch-local, monotonic filter** that answers "did *our* branch change this (gid, kind)?" *before* paying the resolve. When the filter says "no", read the fork-state copy directly (no lookup); when it says "maybe", resolve as today.

Status: **v2 — post-skeptic-review.** Correctness model CONFIRMED sound (C1 by construction, §5); four completeness/ordering gaps found by review are addressed (edge two-choke-points §6, replay precedent §6, gate ordering §3, C1 upgraded §5). **One real open risk remains: ROI** — gated by GATE 0 (§12) before any code. Not implemented.

---

## 1. Motivation (grounded in profiling)

- On a branch, edge-traversal queries (`expansion_4`, `neighbours_2`) are the worst family (~1.7–1.9× main at 22% churn). The dominant self-time frame is `InMemoryStorage::FindVertex` (~31% self in `expansion_4` in this session's runs), reached via `BranchContext::ResolveVertex` from the `:User` label filter and `n.id` projection on traversed endpoints. **⚠ the exact 31% and its caller split (`HasLabel` vs `GetProperty`) must be re-established — GATE 0 §12 — as the in-tree profile artifacts were deleted; the whole ROI case rests on that split.**
- **Read-side asymmetry:** main reaches an endpoint by dereferencing the `Vertex*` stored in the edge triple (`std::tuple<EdgeTypeId, Vertex*, EdgeRef>`) — **0 gid-lookups**. A branch follows the same pointer, lands on main's fork-state copy, and if that vertex is `branched()` must fetch the branch's authoritative copy **by gid** (`FindVertex`) — **1 lookup per branched endpoint**.
- **BFS avoids this** because it traverses on **identity (gid)**, which is a field on the vertex object shared between main and diff copies — free from the pointer. `FindVertex` is therefore *"the cost of reading a modified object's field values,"* not "the cost of traversing a branch."
- **Two redundancies inflate it:**
  1. `main.branched()` is **global** (set by *any* branch, never cleared). A vertex changed by branch X forces a resolve on branch Y even though Y never touched it.
  2. The single bit says *something* changed, not *what*. Under `SET n.age` churn, reading `n.id` / `:User` resolves millions of times to fetch fields that are **identical to fork-state**.

## 2. Core idea

Keep the coarse `branched()` bit on **main** (free, existing, filters the un-branched majority). Add **per-branch monotonic filters, keyed by gid**, consulted only after the main bit trips, that answer "did *this* branch change *this kind* of thing on gid B?" A **clear** answer ⇒ definitely unchanged ⇒ read the fork copy directly, no resolve.

This lives entirely on the branch (`BranchContext`), so it needs no space on main's memory-maxed `Vertex`/`Delta` (which have no spare packed-pointer bit).

### Filters (start coarse, per-kind, keyed by gid)
- `any_change(gid)` — **correctness backstop**: set on *every* branch mutation of gid, unconditionally.
- `any_prop_change(gid)` — set when any property of gid changes on the branch.
- `any_label_change(gid)` — set when any label of gid changes.
- `any_edge_change(gid)` — set when gid's adjacency changes (edge add/remove/edge-prop).
- **Deletion:** *not* a new filter — the branch already tracks tombstones **exactly** in `tombstoned_vertices_` (small set). Checked first, unconditionally.

Per-field granularity (`(gid, property_id)`) is deliberately deferred — see §8.

## 3. Read-path flow

For a value read of vertex B on branch Y. The new filter checks slot **behind** the two already-shipped short-circuits, in this exact order (corrected per review — the real gate is `impl_.storage_ != &diff_engine() && vertex_->branched()`, `vertex_accessor.cpp:46/58/71/…`):

```
// (0) diff-resident short-circuit — SHIPPED (72d709fc5): impl_ already IS the authoritative copy
if (impl_.storage_ == &diff_engine())      return impl_.read(view);
// (1) main bit — SHIPPED: nobody touched B on any branch
if (!impl_.vertex_->branched())            return read_fork(B);
// (2) deletion — SHIPPED (exact set); MUST gate before trusting "unchanged".
//     NOTE: the error is View::NEW-only — a tombstoned gid raises at View::NEW but at View::OLD
//     falls through to impl_ unchanged (the shipped S2 FIX, vertex_accessor.cpp:58/77/96/115,
//     preserving subqueries.feature:424's non-erroring OLD case). deleted_handling(view) MUST
//     reproduce that view split; it is NOT an unconditional-across-views error.
if (view == View::NEW && Y.tombstoned_vertices.contains(B.gid)) return DELETED_OBJECT;
if (Y.tombstoned_vertices.contains(B.gid))  return read_fork(B);  // View::OLD: pre-delete value
// (3) NEW: some OTHER branch touched B, not us
if (!Y.any_change(B.gid))                  return read_fork(B);
// (4) NEW: we touched B, but not this kind
if (!Y.any_<kind>_change(B.gid))           return read_fork(B);
// (5) maybe-changed this kind -> authoritative copy (today's path)
return resolve(B.gid).read(view);
```

Steps 0–2 are exactly today's code; the design **only inserts steps 3–4**. `read_fork(B)` = read `impl_` exactly as the existing step-1 fast path already does — **see §5 for why this is the same operation and therefore already-correct by construction.**

## 4. Correctness model (the lynchpin)

Bloom/monotonic filters have **no false negatives**: a *clear* bit means the key was **never inserted**.
- **clear ⇒ definitely unchanged ⇒ read fork copy is correct.**
- **set ⇒ maybe changed ⇒ resolve** (a false positive only wastes a resolve — always safe; never a stale read).

Therefore correctness reduces to **one invariant**:

> **INV-1 (completeness):** every branch mutation inserts into `any_change(gid)` (and its kind filter) with **zero misses**, on both the live-write path *and* the change-log replay path.

`any_change` is the backstop: the read rule reads fork only if `any_change` is clear *or* (`any_change` set but the specific-kind filter is clear **and that kind is fully instrumented**). If some change kind is ever not categorized, `any_change` still forces a resolve.

## 5. Why `read_fork(B)` is already-correct and cheap (critical)

The new fast path reads `impl_` — the accessor the branch already holds for B from the scan/traversal — **the identical operation the existing `!branched()` fast path performs.** Two claims, both **CONFIRMED by adversarial review** (skeptic + logic-verifier + concurrency-debugger, 2026-07-16):

- **C1 — `impl_` reads fork-state, not main-live. CONFIRMED by construction.** Main's shared `Vertex` object is **never mutated by any branch** — a branch only flips main's 1-bit `branched()` flag and writes its own changes into a **separate** diff-engine object (`CowVertex` copies fork-state labels/props into a new object, never touches main's delta chain — `branch_engine.cpp:662-769`). `impl_`'s transaction reads main **as-of fork_ts** via `CreateHistoricalTransaction` (`start_timestamp = fork_ts`, `timestamp_` never advanced — `storage.cpp:3102-3131`). So `impl_.read()` yields the fork-state value *independent of any other branch's or main's post-fork activity*. Reading it for a "branched but this-kind-unchanged" B is the same, already-correct read. ⇒ the filter genuinely **removes** the `FindVertex`, not relocate it. *(This was the design's #1 risk; it passed.)*
- **C2 — endpoints are un-canonicalized raw accessors. CONFIRMED.** `756d20d19` made `To()/From()` return `VertexAccessor(impl_.ToVertex(), branch_ctx_)` unconditionally (`edge_accessor.cpp:29-33`) — a branched endpoint's `impl_` is the same *kind* of accessor as an un-branched one, so reading it needs no lookup.

**Regression test to lock C1:** main mutates vertex B's property *after* branch Y forks; Y (which never touched B) reads `B.prop` → must return the **fork-state** value, not main's post-fork value.

## 6. Population & seeding (where INV-1 is won or lost)

Insertion must happen at the **mutation operations**, not at `CowVertex` — `CowVertex` is idempotent (fires only on first touch), but a vertex can be mutated in *different kinds* over its branch life (COW'd for a property, later a label changed). **Review (skeptic) found edge mutations have TWO disjoint choke points, not one** — the §6 v1 table wrongly implied one:

| Mutation (branch path) | Choke point (file) | Inserts |
|---|---|---|
| `SetProperty`/`SetProperties`/`InitProperties` (vertex) | `db_accessor.hpp` | `any_change` + `any_prop_change` (gid) |
| `AddLabel`/`RemoveLabel` | `db_accessor.hpp` | `any_change` + `any_label_change` (gid) |
| **edge property set / pre-delete COW** | **`CowEdge` (`branch_engine.cpp:773-833`)** — derives `from_gid`/`to_gid` internally, COWs both | `any_change` + `any_edge_change` on **both** endpoint gids |
| **new edge topology (create)** | **`InsertEdge` (`db_accessor.hpp:1462-1477`)** — COWs both endpoints via `CowVertex(from)/CowVertex(to)`, **never calls `CowEdge`** | `any_change` + `any_edge_change` on **both** endpoint gids |
| vertex delete / detach-delete | `db_accessor.hpp:1568-1580,1691-1705` (cascades pre-COW incident edges via `CowEdge`) | `tombstoned_vertices_` (existing) + `any_change` |

Place the `any_edge_change` hook **inside `CowEdge`** (covers property-set + delete-cascade for existing edges) **and** duplicate it **inside `InsertEdge`** (brand-new topology never reaches `CowEdge`). Rationale from review — those are genuinely disjoint paths.

> **PREREQUISITE — `WalVertexRemoveLabel` replay gap (found in 2nd review, must resolve before the label filter is trustworthy).** `ReplayChangelogIntoDiffEngine`'s apply-visitor (`branch_engine.cpp:188-282`) has **no `WalVertexRemoveLabel` case** — it hits the generic no-op. `versioning::MergeBranch` **does** handle it first-class (`merge.cpp:416,628`), and the capture-side Scope comment (`branch_engine.cpp:160-166`) claims capture *never emits* `RemoveLabel` (lists only create/add-label/set-property/delete). These three cannot all be right. **Adjudicate first with a test:** on a branch do `REMOVE n:L`, commit, `CHECKOUT main`, `CHECKOUT` back (forces changelog replay), read `n`'s labels. If `L` is still present ⇒ a real, pre-existing data-loss bug in the shipped replay path (independent of this feature) — fix the apply-visitor **and** the `mark_if_main_vertex_gid` re-walk to add the `RemoveLabel` case, *then* extend the re-walk for `any_label_change`. If `L` is correctly gone ⇒ capture represents label-removal some other way; find it and make the re-walk cover that representation. Either way: **do not extend the re-walk by mechanically mirroring the existing (label-add-only) cases, or the label filter inherits the same blind spot** — an INV-1 violation for a vertex whose only branch history is a label removal.

**Re-seeding at checkout — use the existing precedent, don't invent a "classifier".** A re-checked-out branch rebuilds `BranchContext` by replaying its change-log into the diff engine (`ReplayChangelogIntoDiffEngine`, `branch_engine.cpp:178-289`), which calls **storage primitives directly, bypassing `query::DbAccessor`/`CowIfNeeded` entirely** — so the live-path hooks above are NOT hit on replay. **But this exact problem is already solved once:** `branch_engine.cpp:634-648` runs a second re-walk (`mark_if_main_vertex_gid`) that re-derives the `branched()` bit from the changelog after replay. **Extend that same re-walk** to also populate `any_change`/`any_prop_change`/`any_label_change`/`any_edge_change` per WAL-entry kind (`WalVertexSetProperty` → prop, `WalVertexAddLabel` → label, `WalEdge*` → edge on both endpoints, `WalVertexDelete` → tombstone). This makes replay-population a single, already-precedented site — not a parallel classifier that could drift. **Test (none exists today):** re-checkout a branch with a non-empty changelog, then read mutated + unmutated fields and assert correctness.

## 7. Concurrency

Monotonic filters: bits only ever get **set**, never cleared (v1 never un-diverges an object — same property as `branched()`). So:
- **write:** lock-free **atomic fetch-or** on the affected word(s).
- **read:** **relaxed atomic load**.
No lock, no torn read in the UB sense. Exclusive single-writer checkout already serialises branch *writes*; the parallel-scan-on-branch guard (`9b1352a0d`) already blocks concurrent branch reads through the chunked path — but the filters are safe even without that, by monotonic construction.

## 8. Memory & the fast probe (fits "low memory, fast")

- **Scales with the branch's change-count, not the graph.** ~25 KB per filter at 20k changed gids (tunable). This is *additional* per-branch memory (acknowledged non-free); bounded by the "small branch" design assumption.
- **Fast probe via the gid.** The gid is a unique, dense, high-entropy integer — no hash cascade needed. Use a **register-blocked bloom**: one multiply-shift (`gid * φ64 >> k`) selects **one 64-byte block (one cache line)**; a second cheap mix sets/tests 2–3 bits within it. ⇒ **one memory access, a few ALU ops.** (A `k=1` flat `bit[mix(gid) % N]` is even simpler but less space-efficient.) A **direct bit-array indexed by gid** is rejected: exact but sized to *main* (1 bit/possible vertex) — violates the branch-scaling requirement on large graphs.

## 9. Scope: start wide, measure, then decide on fine granularity

Coarse per-kind filters will **fully** eliminate:
- cross-branch resolves (`any_change`),
- the `:User` label read on property-churned vertices (`any_label_change` clear when only `age` changed),
- property reads on edge-only-changed vertices, etc.

They will **not** eliminate the `n.id` read on an `age`-churned vertex (`any_prop_change` set → can't distinguish `id` from `age` → resolve). That needs the fine `(gid, property_id)` filter. **Plan:** ship coarse, measure `expansion_4`/`aggregate`, and only add fine-grained property filters if the residual same-kind property resolves still dominate.

## 10. Analytical branch storage (simplifying lens, not a v1 change)

Treating the diff engine as `IN_MEMORY_ANALYTICAL` (no per-object MVCC delta chains) simplifies the *resolve* (no delta-walk) and shrinks each branch object — both aligned with priority #1. It doesn't change any filter design. Held as a lens; not flipped now.

## 11. Risks & open questions (post-review status)

1. ~~**C1**~~ — **RESOLVED / CONFIRMED by construction** (§5). Reading `impl_` yields fork-state; the filter removes the `FindVertex`, not relocates it. This was the #1 risk and it passed.
2. **INV-1 completeness (§6):** the mutation-site set must be *exhaustive*. Review confirmed `query::DbAccessor` is the sole *live* choke point (triggers gated off on-branch `interpreter.cpp:288-305`; mgp C-API funnels through `DbAccessor`; MERGE writes *main*, not the branch) — **but** edges have two disjoint sites (`CowEdge` + `InsertEdge`, §6) and replay bypasses `DbAccessor` (§6). Both addressed. Still: any *future* branch-mutation path added without a filter insert is a silent stale-read — `any_change` is the backstop, but this is a standing maintenance invariant to document at each site.
3. **Live vs replay divergence (§6):** RESOLVED — extend the existing `mark_if_main_vertex_gid` re-walk rather than a parallel classifier. Single source of truth; add the re-checkout-then-read test.
4. **`any_edge_change` on both endpoints (§6):** CONFIRMED sound *iff* the hook is inside `CowEdge` **and** duplicated in `InsertEdge`. Traversal reads a vertex's adjacency by that vertex's gid, so both endpoint gids must be flagged so a traversal from *either* side sees the change.
5. **False-positive rate under heavy churn:** near-100% churn ⇒ filters saturate ⇒ reads pay probe + resolve (slightly worse than today). The main `branched()` bit still gates un-branched reads out for free, so only the branched fraction pays the probe. Crossover to quantify in the perf gate.
6. **Gate composition (§3):** the diff-resident short-circuit (`impl_.storage_ == &diff_engine()`, step 0) and tombstone gate (step 2) precede the new filter checks — corrected in §3. No hole, provided that order is preserved.
7. **View semantics:** `read_fork` must return the fork-state value across `View::OLD`/`View::NEW`, matching what `resolve` would return for an unchanged field. Follows from C1 but must be tested on both views.

## 12. Build gate & verification plan

### GATE 0 — ROI proof BEFORE any filter code (the real open risk per review)
The coarse design **cannot** help the same-kind property axis (`n.id` on `SET n.age` churn — §9); it helps only the **label** axis (`:User`) and the **cross-branch** axis (`any_change`). The motivating "31% `FindVertex`" is not currently reproducible in-tree (the perf-box `*.flat.txt` were deleted). **Before building, re-profile `expansion_4`/`neighbours_2` at worst churn and attribute `FindVertex` self-time by CALLER** (`HasLabel` vs `GetProperty(id)`):
- if the `HasLabel`/cross-branch share is large ⇒ coarse filters are worth it; proceed.
- if `GetProperty(id)` dominates ⇒ coarse buys little for a permanent probe tax ⇒ **do not build coarse**; jump straight to the fine `(gid, property_id)` filter (§9) or drop the line.

### Then, if GATE 0 passes:
- **Correctness:** logic-verifier + skeptic on INV-1 (enumerate all mutation sites incl. the two edge choke points) . Functional tests: same-query & cross-query COW/label/edge/delete; **cross-branch** read (B changed by branch X, read on branch Y ⇒ Y reads fork); **C1 lock** (main mutates B post-fork, Y reads fork-state); **replay** (re-checkout with non-empty changelog, read mutated + unmutated).
- **Perf:** re-profile the same cells; confirm `FindVertex` drops on the label/cross-branch axes; measure residual same-kind property resolves to decide §9 fine-graining.
- **No-regression:** point reads and un-branched traversals byte-identical, +at most one relaxed load (main bit gates them out before any filter).
