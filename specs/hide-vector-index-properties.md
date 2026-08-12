# PRD: Hide Vector-Index Properties on Return

**Status:** Draft — a runtime-configurable flag that omits vector-index-backed properties (embeddings) from a returned graph object, so `RETURN n` stops flooding clients and LLM agents with high-dimensional vectors, while the values stay fully accessible on explicit request and fully intact on disk.

## Problem Statement

A vector index stores an embedding as an ordinary property — a list of hundreds or thousands of floats. When a client asks for a whole object (`RETURN n`, an edge, a path), Memgraph serializes **every** property, embeddings included. That is almost never what the caller wants:

- **LLM agents drown in it.** An agent that does `MATCH ... RETURN n` gets every embedding dimension echoed back into its context — pure noise that inflates token cost and degrades the model, yet the embedding is still needed server-side for vector search.
- **It is a general client problem, not just an agent one.** Lab users expanding a graph pull back multi-kilobyte float lists on every node they touch (reported for the Lab); the payload dominates the wire and the UI.

The embedding must stay in the database (vector search depends on it) and must stay retrievable on demand (an operator debugging the index still wants to see it). What should change is only the **default whole-object wire representation**: returning `n` should not mean shipping its vectors.

There is no reliable way to guess "which property is an embedding" from its name or type — a string keyed `embedding`, a float list keyed `vector`, or serialized geospatial floats are all plausible false signals. The one authoritative signal the database already has is **vector-index membership**: a property that is the target of a vector index. That, not a name heuristic, defines what to hide.

## Solution

A single runtime-configurable boolean flag. When on, a returned graph object omits any property that is covered by a vector index. No new grammar — the operator toggles it live:

```cypher
-- turn it on for this running server (no restart)
SET DATABASE SETTING 'storage.omit_vector_index_properties_on_return' TO 'true';

CREATE VECTOR INDEX emb_idx ON :Doc(embedding) WITH CONFIG {...};

MATCH (n:Doc) RETURN n;                 -- object comes back WITHOUT `embedding`
MATCH (n:Doc) RETURN n.embedding;       -- still returns the vector (explicit access)
MATCH (n:Doc) RETURN properties(n);     -- still returns the vector (explicit all-props)

SHOW DATABASE SETTING 'storage.omit_vector_index_properties_on_return';
SET DATABASE SETTING 'storage.omit_vector_index_properties_on_return' TO 'false';   -- back to full objects
```

It is also settable at boot (`--storage-omit-vector-index-properties-on-return=true`) and, once set at runtime, persists across restarts. Default is `false`: an untouched server returns exactly what it returns today.

## User Stories

- As an AI-application developer, `RETURN n` stops feeding embedding dimensions into my agent's context, so the same query costs a fraction of the tokens — without me rewriting the query or teaching the agent to exclude properties.
- As a Lab user expanding a graph, node/edge payloads no longer carry multi-kilobyte vectors, so expansion is fast and readable.
- As an operator debugging the index, `n.embedding` and `properties(n)` still return the raw vector — hiding is a default, not a lock.
- As a DBA, I flip the behavior on a live server with one `SET DATABASE SETTING`; if it turns out wrong I flip it back, no restart, no redeploy.
- As a replication/backup owner, dumps, snapshots, and replicas still carry every embedding — the flag changes only what the client is shown.

## Implementation Decisions

### D1 — A runtime flag, not grammar
No new Cypher syntax; a single boolean registered in `src/flags/run_time_configurable.{hpp,cpp}` (gflag `--storage-omit-vector-index-properties-on-return`, setting key `storage.omit_vector_index_properties_on_return`). It follows the established pattern there: `DEFINE_bool`, `register_flag(...)` in `Initialize`, a `ValidBoolStr` validator, a file-scope `std::atomic<bool>` cache refreshed on change, and a getter read on the serialize path. Registered with `kRestore` so the operator's choice is persisted (like the `log.*` settings) yet still overridable via the CLI flag at boot. Default `false`. The value is process-global, matching the runtime-flags mechanism.

### D2 — Hide *vector-index* properties, not "embeddings"
The unit of hiding is "a property that is the target of a vector index," never a name (`embedding`) or a type (float list). The database cannot reliably identify an embedding otherwise — a string named `embedding`, a list named `vector`, and non-embedding float lists under an index all defeat a heuristic. Vector-index membership is the only authoritative signal, and "embeddings" is just its dominant use case. The flag name (`--storage-omit-vector-index-properties-on-return`) names the mechanism, not the use case, for the same reason.

### D3 — Filter only the whole-object serialization path
The filter lives in the glue conversion `ToBoltVertex` / `ToBoltEdge` (`src/glue/communication.cpp`) — the single site that materializes every property of a returned node/edge via `Properties(view)`, right beside the existing per-property authorization skip (`HasPropertyPermission`). This is deliberately narrow:

| Path | Call | Filtered? |
|---|---|---|
| `RETURN n` / edge / path / graph | glue `ToBoltVertex`/`ToBoltEdge` (paths & graphs reuse them) | **yes** |
| `RETURN n.embedding` | `PropertyLookup` → `VertexAccessor::GetProperty` | no — explicit access |
| `properties(n)`, `n {.*}` | `awesome_memgraph_functions` / map projection → `TypedValue` map | no — explicit all-props |
| `DUMP DATABASE` | `dump.cpp` → `Properties(View::OLD)` | no — full fidelity |
| snapshot / WAL / replication | storage / SLK formats | no — full fidelity |

Explicit access is the escape hatch; only the graph-object wire representation changes.

### D4 — Membership decided by the vector-index registry, per object
A vector-indexed property is stored as a special value but the storage decoder re-materializes it as a plain list on read, so the returned value alone cannot reveal it — the decision must consult the registry (`Indices::vector_index_` for nodes, `vector_edge_index_` for edges; both exist). The check is per object against the active-indices snapshot: a property is hidden only when a vector index's `(label-filter, property)` / `(edge-type-filter, property)` actually matches that object's labels/type — so an index on `:Doc(embedding)` does not hide `embedding` on an unrelated `:Note` that happens to share the name. (Perf: if the per-property registry lookup shows up in serialization profiling, precompute the set of vector-indexed `PropertyId`s per result stream; the correctness contract is unchanged.)

### D5 — No staleness, no plan-cache coupling
The filter reads live properties at serialize time and omits the vector ones — nothing is cached or materialized, so the "update a value and no longer see the latest" problem of a server-side materialized view does not arise, and explicit access always reflects the current value. Toggling the flag affects the next result immediately; serialization is not part of a cached query plan, so no plan invalidation is required.

### D6 — On-disk and replica fidelity preserved
Because the filter sits in the Bolt client glue and nowhere else, dumps, snapshots, WAL, and replication carry every embedding untouched. The flag is a presentation choice for the client wire, never a durability or replication change.

### D7 — Community edition, no license gate
The flag ships in Community. It is a plain output filter, and vector-index creation itself has no license check today, so gating only the "hide" half would be inconsistent. No `IsEnterpriseValid*` call is added.

## Testing Decisions

- **Unit — registry helpers** (`tests/unit/vector_index.cpp`, `tests/unit/vector_edge_index.cpp`): `VertexAccessor::VectorIndexedProperties` / `EdgeAccessor::VectorIndexedProperties` return the indexed property for a matching label / edge-type and nothing for a non-matching one — the per-object filter logic, in isolation.
- **Unit — glue conversion** (`tests/unit/vector_index_return_omission.cpp`): with the flag driven through the real settings mechanism (`Settings::SetValue`, exactly what `SET DATABASE SETTING` calls), `ToBoltVertex`/`ToBoltEdge` omit vector-indexed properties when on and keep them when off; other properties always survive; the edge vector index is covered; a node whose label does not match the index keeps the property.
- **E2E** (`tests/e2e/vector_index_return/`): runtime toggle via `SET DATABASE SETTING` over a live Bolt session — `RETURN n` omits the embedding while `n.embedding` and `properties(n)` still return it — and a fidelity check that `DUMP DATABASE` output is byte-identical with the flag on and off.

behave was considered and rejected: its result matching is row-oriented and cannot cleanly assert the *absence* of a property from a returned graph object, which is the whole point here. The two unit layers plus the e2e assert that directly and precisely.

## Out of Scope

- **Name- or type-based hiding of "embeddings."** Vector-index membership is the only signal; blacklisting property names or sniffing float-lists is deliberately excluded — it produces exactly the false positives/negatives Marko flagged.
- **Filtering explicit requests.** `n.property`, `properties(n)`, and `n {.*}` are intentionally never filtered — they are the documented escape hatch to the raw value.
- **Per-database / per-session scoping.** The flag is process-global (the runtime-flags mechanism has no per-database tier); a per-database value would need separate storage and is not built here.
- **A generic property-exclusion blacklist** (`exclude_properties(n, [...])`, `SET GLOBAL PROPERTY EXCLUSION [...]`). Hiding arbitrary named properties is a broader, separate feature; this spec is scoped to the vector-index use case that motivated the request.
- **Hiding on disk, in dumps, or to replicas.** Full fidelity is preserved everywhere except the client wire.
- **Virtual (projected / derived) nodes and edges.** These are query-constructed and have no vector-index backing, so their separate serialization path is not filtered; a virtual node carrying a copied embedding is returned in full even with the flag on.
