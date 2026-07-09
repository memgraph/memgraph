# PR Review — Defunct/Broken Tenant Recovery (`--storage-allow-recovery-failure`)

**Branch:** `feature/storage-allow-recovery-failure`
**Base:** merge-base with `origin/master` (`0257d9f`)
**Spec:** `specs/storage-allow-recovery-failure.md` (+ `specs/issues/01-08*.md`)
**Reviewers:** 5 parallel agents — HA, Storage, C++23, Correctness, Memgraph-expert
**Date:** 2026-07-09

---

## What the PR does

When `--storage-allow-recovery-failure` is set, an `InMemoryStorage` tenant that fails durability
recovery (corrupt snapshot/WAL) boots **empty and marked "broken"** instead of `LOG_FATAL`-crashing
the whole process. A broken tenant fail-closed-rejects all data-touching queries via an allowlist
gate until cured. Cure paths: `RECOVER SNAPSHOT` (clears the flag), replica self-heal via the normal
snapshot/WAL replication handlers, and `DROP DATABASE`. Health is surfaced in `SHOW DATABASES`
(`Health` column) and `SHOW STORAGE INFO` (`health` key).

Core mechanism: catch recovery failure in the `InMemoryStorage` constructor → scrub via `Clear()` →
flip an in-memory-only `std::atomic<bool> broken_` → gate the query surface → reuse replication's
snapshot/WAL path as the cure. **No new persisted state, no RPC wire-format change, ISSU-safe.** The
overall design is sound and idiomatic; no reviewer found a data-corruption or MVCC-visibility bug.

---

## Consolidated findings (severity-ranked)

**4. Spec ↔ code terminology has diverged completely; the spec was not updated.**
*(Memgraph-expert #1.)* Spec + all 8 issue files say **"defunct"** (`SHOW DATABASES` → `Status`
column, values `ready`/`defunct`); shipped code says **"broken"** everywhere (`IsBroken()`/`SetBroken()`,
`kBrokenDatabaseError`, `SHOW DATABASES` column named `Health` with `ready`/`broken`, `SHOW STORAGE
INFO` key `health` not spec's `status`). The rename is internally consistent (not a bug), but the
checked-in spec is now actively misleading for later slices/agents that treat it as ground truth.
**Fix:** reconcile — update the spec to "broken", or rename the code back to "defunct" before merge.

### 🔵 MINOR

**6. Query-gate allowlist deviates from the spec.** *(Memgraph-expert #2/#3.)*
`src/query/interpreter.cpp` allowlist:
- Includes `LockPathQuery` and `FreeMemoryQuery` as *permitted* — spec says storage-touching queries
  (incl. lock path) should be *rejected* on a broken tenant. `LOCK/UNLOCK DATA DIRECTORY` toggling the
  storage dir lock on an inert/mid-recovery tenant is exactly what the spec wanted fenced off.
- Drops `DatabaseInfoQuery`, which the spec explicitly lists as allowed. The code comment gives sound
  reasoning (`SHOW INDEX/CONSTRAINT INFO` would return a misleading clean 0-row result), but it's an
  undocumented deviation — fold it back into the spec so it reads as intentional.

**9. `catch (...)` around `RecoverData()` has no net for the scrub itself.**
*(Storage #1.)* `src/storage/v2/inmemory/storage.cpp` — inside the catch, `Clear()` /
`name_id_mapper_->Clear()` / `description_store_.Clear()` / `ttl_.Disable()` are all non-`noexcept`.
If any throws (e.g. bad_alloc during `Clear()`'s GC), the exception escapes the constructor and
`std::terminate()`s — the very crash the feature avoids, just uglier. Low probability (mostly frees
memory); worth a nested try/catch that still guarantees `SetBroken(true)`, or at least a comment.
Related open question: should the scrub differentiate data-driven exceptions (`RecoveryFailure`,
`bad_alloc`, `length_error`, `OutOfMemoryException`) from unrelated ones (host OOM, permission errors)
rather than treating every exception as "defunct"?
