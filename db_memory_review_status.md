# DB Memory Review Status

Compact status view of your original notes against the current branch.

## Done and Verified

- Arena drop now returns arena indices to the reusable free list instead of destroying arenas.
- First arena creation is mandatory at DB bootstrap, and later arena acquisition or hook-install failures fall back to the DB base arena.
- `DbArenaScope` is limited to intentional same-pool TLS nesting.
- TLS scope and DB-aware workers now resolve the DB arena pool, not one permanently captured arena.
- `ArenaAwareAllocator` is retired; `DbAwareAllocator` is the active default for transient DB-scoped work.
- Debug checks now validate the captured ownership model more precisely.
- `CrossThreadMemoryTracking` now restores arena `0` correctly and has a regression test.
- Communication/bolt, communication/v2, `SessionHL`, `Interpreter`, streams, triggers, GC, recovery, and replication paths were audited for DB arena scope.
- Tenant profile durability is verified: KV payload stores `memory_limit` and `databases`, plus a version key.
- The added DB memory tests were aligned with the final model.
- `review_issue_table.md` is no longer tracked by git.

## Paused

- `dbmem-19` remains paused.

## Resolved Naming

- Tenant-profile queries now require `PROFILE_RESTRICTION`.
- `MULTI_DATABASE_EDIT` remains for actual multi-database mutation queries.
- If the product wants a more specific future name, `PROFILE_RESTRICTION` is already the better fit than `DB_EDIT`.

## Likely Not Worth Pursuing

- `dbmem-19`: the current jemalloc hook path already charges memory in page-sized chunks through extent hooks, so a separate TLS byte counter that only flushes every N KB would add complexity for relatively little gain. If we revisit it, it should be as a carefully designed coarse-grained accounting mode, not a simple increment-threshold tweak.

## Jemalloc Conditionals

- Most `#if USE_JEMALLOC` blocks are real build-mode boundaries and should probably stay.
- The small number of duplicated “same logic, different backend” branches are the only places that look worth factoring into helper templates or backend traits.
- The main candidates are the scope/restore paths in `src/memory/query_memory_control.cpp` and the arena setup helpers in `src/memory/db_arena.cpp`, but only if the refactor keeps the non-jemalloc path simple and readable.

## Notes

- The compressor and property-buffer paths are already using the `DbAwareAllocator` model.
- `ExecutionContext::db_arena_idx` is the DB base arena for the query context, not a promise that every worker thread uses one concrete arena.
