# DB Memory Tracking E2E Matrix

## Tracked Objects

| Object / ownership | Tracking mechanism | Allocate via query | Deallocate via query / lifecycle | E2E status |
|---|---|---|---|---|
| Vertices / edges / properties | DB storage tracker via per-DB arena + arena-aware allocators | `CREATE`, `MERGE`, bulk `UNWIND CREATE` | `DELETE`, `DETACH DELETE`, `ROLLBACK`, `DROP DATABASE` | Covered on create and delete |
| Delta / transactional storage state | DB storage tracker via DB arena-backed delta slabs | write in explicit transaction before commit | `ROLLBACK`, `FREE MEMORY`, GC, `DROP DATABASE` | Covered |
| Label index | DB storage tracker | `CREATE INDEX ON :Label` | `DROP INDEX ON :Label` or `DROP DATABASE` | Covered on create and drop |
| Label-property index | DB storage tracker | `CREATE INDEX ON :Label(prop)` | `DROP INDEX ...` or `DROP DATABASE` | Covered on create and drop |
| Edge index | DB storage tracker | `CREATE EDGE INDEX ON :TYPE` | `DROP EDGE INDEX ...` or `DROP DATABASE` | Covered on create and drop |
| Edge-property index | DB storage tracker | `CREATE EDGE INDEX ON :TYPE(prop)` | `DROP EDGE INDEX ...` or `DROP DATABASE` | Covered on create and drop |
| Unique constraint SkipLists | DB storage tracker | `CREATE CONSTRAINT ... IS UNIQUE` | `DROP CONSTRAINT ...` or `DROP DATABASE` | Covered on create and drop |
| Trigger store entries | DB storage tracker | `CREATE TRIGGER ...` | `DROP TRIGGER ...` or `DROP DATABASE` | Covered on create and drop |
| Trigger-created graph objects | DB storage tracker | write query that fires trigger | delete created objects / `DROP DATABASE` | Covered |
| Plan cache entries | DB storage tracker | many unique cacheable queries | cache eviction / process lifetime / `DROP DATABASE` | Covered on growth + DB isolation |
| Query execution PMR state | DB query tracker via `QueryAllocator` | query module procedure that allocates with `mgp_alloc` and sleeps while executing | query end, transaction end, `DROP DATABASE` | Covered in autocommit and explicit transaction |
| Vector index allocations | DB embedding tracker | `CREATE VECTOR INDEX`, insert/update vectors | `DROP VECTOR INDEX`, `DROP DATABASE` | Covered on create/drop and delete edge case |
| Per-DB totals | `db_storage + db_embedding + db_query` | any of the above | any of the above | Covered |
| Cross-DB isolation | separate per-DB trackers | perform operation in DB A only | inspect DB B | Covered |
| `USE DATABASE` switching | separate per-DB trackers selected by current session DB | `USE DATABASE db_a` after allocating in A | `USE DATABASE db_b`, `DROP DATABASE` | Covered |
| Database destructor release | DB drop purges arena/hooks and nested trackers | `CREATE DATABASE`, allocate inside it | `DROP DATABASE` | Covered |
| Streams map / consumer metadata | DB storage tracker | `CREATE KAFKA/PULSAR STREAM ...` | `DROP STREAM ...` / `DROP DATABASE` | Not covered in this environment |
| Stream-ingested graph writes | DB storage tracker + full-pinned consumer thread | external broker + running stream | stop stream / delete data / `DROP DATABASE` | Not covered in this environment |

## Test Matrix

| Test | Main assertion | Domain |
|---|---|---|
| `test_show_storage_info_contains_db_split_fields` | split fields exist | reporting |
| `test_db_total_equals_storage_plus_embedding_plus_query` | `db_memory_tracked` equals split sum | reporting |
| `test_storage_memory_grows_for_autocommit_node_and_edge_creation` | storage grows on one-shot writes | storage |
| `test_explicit_transaction_storage_visible_before_commit_and_persists_after_commit` | uncommitted explicit writes already charge DB and survive commit | storage / explicit tx |
| `test_explicit_transaction_rollback_returns_storage_near_baseline` | rollback releases transactional storage state | storage / abort |
| `test_storage_memory_drops_after_delete_and_free_memory` | deleting graph objects releases DB storage memory | storage / delete |
| `test_query_memory_is_visible_while_query_is_executing_and_drops_after_finish` | active query PMR memory shows in `db_query_memory_tracked` during in-flight execution | query |
| `test_query_memory_is_isolated_per_database` | active query in DB A does not charge DB B | query / multi-tenancy |
| `test_query_memory_in_explicit_transaction_drops_after_query_finishes_before_commit` | explicit transaction queries charge DB query memory only while alive, not until commit | query / explicit tx |
| `test_label_and_property_index_creation_grows_db_storage_memory` | node indices are charged to DB storage | schema |
| `test_edge_indices_and_unique_constraint_grow_db_storage_memory` | edge indices and unique constraints are charged to DB storage | schema |
| `test_index_and_constraint_drop_returns_storage_near_baseline` | dropping schema structures releases DB storage memory | schema / drop |
| `test_trigger_store_entries_and_after_commit_trigger_writes_are_attributed` | trigger definitions and trigger writes both charge the DB | triggers |
| `test_trigger_definition_drop_returns_storage_near_baseline` | dropping triggers releases trigger-store memory | triggers / drop |
| `test_plan_cache_growth_is_attributed_to_db_storage_and_isolated` | many unique prepared plans grow DB storage in one DB only | prepare-time long-lived memory |
| `test_use_database_switch_changes_visible_db_metrics` | switching current DB changes which split metrics are visible | multi-tenancy / switching |
| `test_vector_index_memory_uses_embedding_tracker_and_returns_on_drop` | vector index grows `db_embedding` and returns on drop | embeddings |
| `test_vector_index_memory_is_isolated_per_database` | vector index memory stays isolated per DB | embeddings / multi-tenancy |
| `test_vector_deletes_do_not_free_embedding_memory_until_index_drop` | vector payload removal / vertex delete do not free the index arena before `DROP VECTOR INDEX` | embeddings / edge case |
| `test_drop_database_releases_global_memory` | dropping a DB releases instance-global graph memory | database lifecycle |

## Notes

| Area | Note |
|---|---|
| Streams | Proper stream coverage needs Kafka/Pulsar brokers in the E2E environment. The current `configuration` workload does not provision them, so stream-attribution tests remain an environment gap rather than a query-gap. |
| Existence constraints | Intentionally not targeted here because they are not part of the meaningful tracked long-lived structures discussed in this PR. |
| Query memory | The tests focus on PMR-backed query execution state. Raw non-PMR query allocations remain the known undercount gap. |
