# Storage-Layer Copy-on-Write / Tenant Overlay

**Author**
Anshul Omar (https://github.com/anomarweb)

**Status**
PROPOSED

**Date**
2026-07-03

## Problem

Multi-tenant graph deployments often center on one large shared base graph (a reference
knowledge graph, an industry ontology, an organization-wide dataset) that each tenant adapts
for its own use.
Adaptation means the ability to execute three operations:
- enrich the base with tenant-specific nodes, edges, types, and properties;
- override selected base values with tenant-specific ones; and
- hide (tombstone) base entities a tenant should not see.

Two constraints are required:
- no tenant can mutate the shared base; and
- one tenant's adaptations must be invisible to every other.

The straightforward implementation gives each tenant a full private database, namely a
copy of the base plus that tenant's recorded changes. Its dominant cost is a full copy of
the base in RAM for every tenant. That does not scale when the base is large and tenants
are numerous, especially since most tenants modify only a small fraction of the base.

The natural alternative keeps one shared base and joins it live with a small per-tenant
delta at query time, but it runs into a hard limitation: Memgraph does not allow a single
query to span two databases, so a base in one database and a tenant delta in another
cannot be read together.

This ADR decides how to serve a merged, tenant-private view of the graph while keeping a
single copy of the base resident in RAM and shared across tenants, so that per-tenant cost
scales with the size of each tenant's adaptation rather than the size of the base.

## Criteria

1. **Faithful overlay semantics** (35%): enrich, override, and tombstone; the base is never
   mutated; every read returns the correctly merged view across point lookup, scan,
   traversal, indexed lookup, and vector search.
2. **Shared base, thin per-tenant cost** (35%): one base resident in RAM and shared across
   tenants, so the incremental cost of a tenant approximates the size of that tenant's
   adaptation.
3. **Surgical and additive** (20%): reuse the existing in-memory engine (MVCC, garbage
   collection, durability, indices), with minimal changes.
4. **Isolation** (10%): a base shared in RAM implies a shared process, which is weaker
   than a runtime per tenant, so the decision must state where that is acceptable.

## Decision

Realize the overlay as an in-engine copy-on-write layer inside a single database: a
read-only base graph shared across tenants plus a per-tenant writable shadow, merged on
read. The merge runs inside one database, so it makes no cross-database query. Hence, the
limitation described in the Problem does not apply.

Two properties of the engine facilitate it. First, a committed object (one with
a null head delta) is unconditionally visible, so the frozen base needs no MVCC handling;
it is simply a set of committed objects shared by pointer. Second, the on-disk storage
already overlays committed state with in-transaction changes, using
`DELETE_DESERIALIZED_OBJECT` deltas in `src/storage/v2/disk/`. This decision is the
in-memory, shared-base analogue of that established pattern.

### Composition

The writable shadow is itself an in-memory storage instance, reusing its transaction
engine, write-ahead log, garbage collection, and indices unchanged. The base is a second,
read-only in-memory storage, shared by reference. The overlay storage and accessor add
only two responsibilities: base-merge on the read path, and override/tombstone bookkeeping.

Two alternatives were rejected: (a) a fresh storage that re-implements the whole accessor
surface would mean far more code and two divergent engines to maintain; and (b) deepening
MVCC so the base becomes a bottom layer does not work either, because a shadow delta cannot
chain onto a shared base without making that base writable.

### Data model

- **GID split**: the 64-bit GID space is partitioned at a fixed constant `kShadowGidStart`
  (`2^64 - 2^48`). Base GIDs occupy `[0, kShadowGidStart)` and
  shadow-added GIDs occupy `[kShadowGidStart, 2^64)`, so a GID alone distinguishes base from
  tenant-added. Since the boundary is fixed, the classification stays valid when the base
  grows or is upgraded: new base GIDs remain below it and never collide with shadow GIDs. The
  base keeps almost the whole range, while the small top slice is per-tenant-local. Rebasing an
  overlay onto an upgraded base is correct as long as the base preserves GIDs across versions
  (overrides and tombstones are keyed by base GID).
- **Enrich**: a tenant addition is an ordinary object in the shadow.
- **Override**: copy-on-write of a base object into the shadow under the same GID. Reads
  consult the shadow first, so the copy shadows the base while the base itself stays
  untouched.
- **Tombstone**: record the base GID in a per-tenant hidden set, and reads skip it.
- **Names**: labels, property keys, and edge types are interned as integer IDs. The overlay
  shares the base's name-to-id mapping so base IDs align, and tenant-introduced names
  extend the same space.

### Read merge

Point lookup and scan return shadow entries first, then the base entries that are neither
tombstoned nor overridden.

Traversal is merged one hop at a time. A node always contributes its own edges; if it is an
overridden base node, its base counterpart's edges are also read through from the frozen
base. Edges which the tenant has hidden are then removed. Every remaining edge has its
neighbour re-resolved: a tombstoned neighbour is dropped, and an overridden neighbour
resolves to its shadow copy. Untouched base nodes have no shadow copy, so walking through
them costs no more than ordinary base traversal. Thus, total cost scales with what the tenant
changed. Edge expansion lives on the vertex accessor rather than the storage accessor, so
this per-hop merge is applied through a storage-level post-expansion hook.

Vector (ANN) search queries both the base and shadow indices, drops tombstoned and
overridden base hits, and re-ranks the remainder. A similarity search can therefore seed a
multi-hop traversal across the overlay, provided the tenant's vectors come from the same
embedding model as the base's.

Indexed lookups merge the same way. A label-property index scan reads the base's prebuilt
index, dropping tombstoned and overridden entries and re-binding the remaining entries to the
overlay, together with the shadow's own index over tenant-added and overridden objects (an
override is indexed under its copied values when it is materialized, so it is found on the
shadow side rather than the dropped base side). The overlay advertises such an index to the
planner only when both the base and the shadow carry it; otherwise the planner falls back to
a full scan with a filter, which is slower but never misses an entry.

### Write path

Writes never touch the base. A write to a shadow object (a tenant-added object or an
existing override) proceeds normally. A write to a base object is copied-on-write first: a
barrier on the vertex and edge accessors, invoked before any mutation, redirects the write
onto a private shadow copy of that object under the same GID, materialized on the first
write. Writes reach objects through the vertex and edge accessors rather than through the
storage accessor, so the barrier is placed there; the effect is that an ordinary property
set, property update, or label change on base data just works, transparently, and the base
is left untouched by construction.

Writing an edge that belongs to the base additionally promotes both of its endpoints to
override copies, so the shadow edge is anchored in the tenant's own adjacency; the base edge
is then hidden from the tenant's traversal so it is not seen twice. An explicit override
operation is also exposed for callers that prefer to materialize a copy before writing.

## Consequences

Per-tenant RAM approximates the size of each tenant's adaptation because one base serves
the whole fleet. This directly addresses the dominant cost of the full-copy approach, while
keeping traversal and vector search inside the engine at native speed.

*Isolation trade-off*: given the base is shared in RAM, all tenants share one process and
one address space. This gives strong logical isolation, but not the physical isolation of a
separate runtime per tenant. The overlay therefore suits trusted or first-party
multi-tenancy over shared base data. Tenants that require a hard process boundary
should stay on separate runtimes. A hard-isolation variant would map the base into each
tenant process read-only, which needs a relocatable base layout.

Two considerations come with reusing the engine this way. First, a logical object that has
not yet been overridden exists as two runtime objects: the shared base copy and, once
written, the tenant's shadow copy. A query that reaches that object through more than one
reference and writes through one must still observe its own write through the others. This is
handled: the write barrier re-points the writing reference, and a read-side redirect resolves
a base vertex overridden in this transaction to its shadow on property and label reads.
An old-view read predating the override still sees the base. Second, the overlay hooks into
the engine's read paths, so a future engine change that adds a read path bypassing those hooks
would silently return base data without the overlay merge. The merged surface is therefore
kept explicit, and any path not yet merged fails loudly rather than quietly reading the shadow
alone.

Out of scope:

- Durability and replication of the shadow, whose durable records are the per-tenant changeset.
- Control-plane integration: sharing one base across many tenant databases, and the surface
  to create an overlay database.
