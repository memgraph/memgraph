# PRD: Defunct tenants on recovery failure (`--storage-allow-recovery-failure`)

**Status:** ready-for-agent
**Area:** storage/v2 (durability), dbms, query (interpreter + frontend), replication

## Problem Statement

Today, if a single tenant (database) fails durability recovery at startup — a
truncated WAL, a corrupt snapshot, a missing prefix WAL file — the **entire
Memgraph process refuses to boot**. Recovery hits an `MG_ASSERT` / `LOG_FATAL`
deep in the durability layer and the process dies. A single corrupt tenant takes
down every other (healthy) tenant on the instance, and in a clustered deployment
it takes down that instance's role in the cluster.

From an operator's perspective: one bad data directory means total outage, with
no way to bring the instance up, inspect which tenant is broken, or repair it
online. The only recourse is to manually restore the whole data directory from a
backup before the process will start at all.

## Solution

Introduce an opt-in flag, `--storage-allow-recovery-failure` (default `false`).
When enabled, a tenant that fails recovery does **not** crash the process.
Instead it comes up in a **defunct** state: an empty, inert in-memory placeholder
whose on-disk durability files are left untouched. The instance boots; all
healthy tenants work normally.

A defunct tenant:
- is reported as `defunct` by `SHOW DATABASES` (new `Status` column) and by
  `SHOW STORAGE INFO` (new `status` row);
- rejects every query that touches its data with a clear, actionable
  `QueryException` explaining how to recover;
- can be brought back online **in place** by the operator via `RECOVER SNAPSHOT`
  (load a known-good snapshot) or the new `REPAIR DATABASE` query (reset to an
  empty database, then re-import);
- can be dropped outright via `DROP DATABASE` if the operator chooses to abandon
  it.

In a clustered (HA) deployment a defunct **replica** tenant self-heals: the main
detects it is not caught up and full-syncs it (snapshot + WAL), which clears the
defunct state. A defunct **main** tenant is left for the admin to recover while
replicas continue serving reads of their healthy copy.

Scope is **in-memory storage only**. On-disk (`ON_DISK_TRANSACTIONAL`) storage
keeps today's fatal behavior.

## User Stories

1. As an operator, I want Memgraph to boot even when one tenant's durability
   files are corrupt, so that a single bad tenant does not cause a total outage.
2. As an operator, I want this behavior to be opt-in via a flag, so that my
   existing fail-stop-on-corruption guarantee is preserved unless I deliberately
   choose availability over it.
3. As an operator, I want the flag to default to off, so that upgrading Memgraph
   does not silently change how corruption is handled.
4. As an operator, I want a corrupt tenant's on-disk snapshot and WAL files to be
   left untouched when it goes defunct, so that I retain every option to recover
   the data (RECOVER, REPAIR, or restore the directory from a backup).
5. As an operator, I want `SHOW DATABASES` to show a `defunct` status for failed
   tenants, so that I can immediately see which tenant did not load.
6. As an operator, I want healthy tenants to also show a `ready` status in
   `SHOW DATABASES`, so that my monitoring tooling can key on a stable column.
7. As an operator, I want `SHOW STORAGE INFO` to report the defunct/ready status
   of a tenant, so that I can confirm a tenant's health from the storage view as
   well.
8. As a user, I want any query that hits a defunct database to fail with a clear
   error explaining the cause and the recovery options, so that I am never served
   silently wrong (empty) results from a tenant that failed to load.
9. As a user, I want the defunct error message to tell me exactly which queries
   recover the database (`RECOVER SNAPSHOT`, `REPAIR DATABASE`) and that
   restoring the whole data directory from a backup is an option, so that I can
   self-serve recovery.
10. As an operator, I want to switch to a defunct database with `USE DATABASE`,
    so that I can run the recovery queries against it.
11. As an operator, I want `RECOVER SNAPSHOT` to work against a defunct database,
    so that I can restore it from a known-good snapshot.
12. As an operator, I want a defunct database that I recover via `RECOVER SNAPSHOT`
    to leave the durability directory in a clean state, so that the tenant
    recovers normally on the next restart rather than going defunct again.
13. As an operator, I want a new `REPAIR DATABASE` query that resets a defunct
    database to an empty working state, so that I can re-import my data when I do
    not have a good snapshot to recover from.
14. As an operator, I want `REPAIR DATABASE` to move the corrupt durability files
    aside into a `.old` directory (when backup directories are enabled), so that
    I do not irreversibly lose the corrupt files.
15. As an operator, I want `REPAIR DATABASE` to delete the corrupt durability
    files when backup directories are disabled, so that the configured backup
    policy is respected.
16. As an operator, I want a confirmation notification after a successful
    `REPAIR DATABASE` telling me I can now run my import queries, so that I know
    the next step.
17. As an operator, I want `REPAIR DATABASE` to be rejected on a healthy
    (non-defunct) database, so that I cannot accidentally wipe a working tenant.
18. As an operator, I want both `RECOVER SNAPSHOT` and `REPAIR DATABASE` to be
    available in Community edition, so that single-tenant deployments can recover
    a corrupt default database.
19. As a single-tenant (Community) user, I want the default `memgraph` database
    to boot defunct rather than crash, so that I can recover it without
    multi-tenancy features.
20. As a single-tenant user, I want to recover my defunct default database
    directly (without `USE DATABASE`, which is Enterprise-only), so that the
    recovery flow works in Community.
21. As an operator, I want to `DROP DATABASE` a defunct tenant, so that I can
    abandon a tenant I no longer wish to recover and reclaim its disk.
22. As an operator, I want dropping a defunct tenant to also remove its corrupt
    on-disk files, so that no stale data is left behind.
23. As a cluster operator, I want a replica that boots with a corrupt tenant to
    automatically re-sync that tenant from the main, so that I do not have to
    manually intervene on replicas.
24. As a cluster operator, I want a defunct replica tenant to refuse to silently
    accept incremental deltas, so that it cannot diverge or report false progress
    to the main.
25. As a cluster operator, I want a defunct tenant on one instance to not affect
    replication of the other healthy tenants on that instance, so that the blast
    radius of corruption stays contained.
26. As a cluster operator, I want reads of a tenant to keep being served by
    replicas when that tenant is defunct on the main, so that read availability
    survives main-side corruption.
27. As a cluster operator, I want `REPAIR DATABASE` to be main-only, so that
    repairs happen authoritatively on the main and replicas re-sync from it.
28. As a cluster operator, I want a main that is defunct for one tenant but
    healthy otherwise to remain main (no automatic failover), so that the admin
    controls recovery rather than the cluster thrashing.
29. As an operator, I want the recovered/repaired tenant to participate in
    replication normally once cured, so that the cluster returns to a fully
    healthy state.
30. As an operator, I want periodic and exit snapshots to be suppressed while a
    tenant is defunct, so that an empty placeholder never overwrites my corrupt
    durability files with an empty snapshot.
31. As an operator, I want a defunct tenant to never write WAL, so that its
    inert state cannot corrupt or extend the on-disk durability.
32. As an operator running on-disk storage, I want the current fatal behavior to
    be preserved, so that there is no half-supported defunct path on a storage
    engine that lacks the recovery tooling.
33. As a developer, I want the defunct state to be re-derived on each startup
    rather than persisted, so that fixing the underlying data out-of-band does
    not leave a tenant stuck defunct.

## Implementation Decisions

### Flag
- New gflag `--storage-allow-recovery-failure`, **default `false`**. Carried in
  `storage::Config` (durability sub-config), not read via global `FLAGS_` deep in
  the durability core. It governs **recovery-time behavior only** and applies
  uniformly to all tenants in the process.

### Durability recovery: fatal → catchable (in-memory, flag-gated)
- Convert the **data-driven** failure points in the `RecoverData` call tree to
  `throw RecoveryFailure` **when the flag is enabled**, while keeping the current
  `MG_ASSERT` / `LOG_FATAL` behavior when it is disabled. This covers: "no usable
  snapshot", snapshot/WAL file-enumeration failures, the edges-metadata size
  mismatch, the structural WAL-chain checks (missing prefix WAL; snapshot with no
  covering pre-snapshot WAL; WAL sequence-number gap), and delta-level WAL load
  failure.
- Keep **always fatal** the genuine code-logic invariants that only an internal
  bug (not corrupt input) can trigger: an edge index present while
  `properties_on_edges` is disabled.
- Dividing line: *"could a malformed/truncated/partial file on disk trigger
  this?"* → `RecoveryFailure`; *"only a logic error in our own code triggers
  this?"* → keep the assert.

### Defunct representation — single construction with internal catch
- The `InMemoryStorage` constructor wraps `RecoverData` in `try/catch`. On
  `RecoveryFailure` with the flag set, it: (a) scrubs partial recovery state by
  reusing the existing `Clear()` reset-to-empty path (plus resetting the name-id
  mapper and description store for completeness); (b) sets an **in-memory**
  `defunct` flag on the storage; (c) **skips the file-move-to-`.backup` branch**
  so the corrupt `snapshots/`/`wal/` files are left byte-for-byte untouched.
- Because construction now **succeeds** (yielding a valid-but-defunct storage),
  the `Gatekeeper` build succeeds and the existing `DbmsHandler` assertion that a
  database was created still holds. This unifies the default-database path, the
  Community single-database path, and the Enterprise restore loop with **no
  try/catch needed in `DbmsHandler`**.
- The defunct flag is **never persisted**; it is re-derived on every startup from
  whether recovery throws.
- Defunct state is exposed from `Storage` → `Database` for the query and
  reporting layers to read.

### Query gating — broad, fail-closed
- A defunct tenant rejects any query that operates on its data. The gate sits at
  the point where a query would acquire a storage accessor on the current
  database (just before transaction setup) and additionally covers the
  storage-touching queries that do not request an accessor (streams, create
  snapshot, free memory, storage mode, isolation level, edge-import mode, lock
  path).
- The thrown error is exactly:
  > `Database is in the defunct state because the recovery process failed. Please recover your database using the RECOVER SNAPSHOT query or REPAIR DATABASE query + run your import queries. If you have a backup of the whole data directory, please replace the current data directory with the backup one and restart the process.`
- **Allowlist** (permitted against a defunct current database): the cure queries
  `RECOVER SNAPSHOT` and `REPAIR DATABASE`, plus read-only meta / session / info
  queries that never touch the tenant graph. The implemented set is the union of:
  `RecoverSnapshotQuery`, `DatabaseInfoQuery`, `SystemInfoQuery`,
  `ReplicationInfoQuery`, `ShowConfigQuery`, `ShowQueryCallableMappingsQuery`,
  `SettingQuery`, `VersionQuery`, `UseDatabaseQuery`, `MultiDatabaseQuery`,
  `ShowDatabaseQuery`, `ShowDatabasesQuery`, `ShowMemoryInfoQuery`,
  `SessionTraceQuery`, `SessionSettingQuery`. (`REPAIR DATABASE` joins this list
  when that query is added in a later slice.) Everything else — Cypher, DDL,
  `CREATE SNAPSHOT` — is rejected. The gate is fail-closed: a query type not on
  the allowlist is rejected by default.
- Note: `RECOVER SNAPSHOT` requires `UNIQUE` storage access, so it is explicitly
  exempted rather than relying on "no accessor ⇒ allowed".

### Background durability while defunct — guard the writes
- Background tasks (GC, snapshot scheduler, async indexer) start normally. The
  **snapshot-write** entry points (periodic snapshot handler and the
  exit-snapshot path) early-return while `defunct`. WAL is never written because
  commits are rejected by the gate. TTL stays disabled while defunct. No
  deferred-startup machinery is introduced.

### Cures (in-place on the same storage object)
- **`RECOVER SNAPSHOT`** (Community + Enterprise; current database): loads the
  chosen snapshot into the empty placeholder. The existing implementation already
  moves all prior snapshots and WAL files to `.old` (or deletes them when backup
  directories are disabled), leaving a clean single-snapshot directory that
  recovers cleanly on the next restart. On success it **clears the defunct flag**.
  No defunct-only restriction (it remains usable on ordinary empty databases).
- **`REPAIR DATABASE`** (new query; Community + Enterprise; current database;
  **main-only** in a cluster): hard-gated to defunct-only — throws on a healthy
  database. It moves the tenant's corrupt `snapshots/`/`wal/` files to `.old` when
  backup directories are enabled (consistent with the existing `.old` convention)
  or deletes them otherwise, resets the tenant to an empty working state, and
  clears the defunct flag. Takes `UNIQUE` access. On success it emits an INFO
  notification (new `REPAIR_DATABASE` notification code): title "Database '<name>'
  repaired.", description noting the database is now empty and import queries can
  be run.
- Both cures clear the defunct flag under the storage's exclusive (`UNIQUE`)
  access, serializing against concurrent queries.
- **`DROP DATABASE`** on a defunct tenant is allowed and is not caught by the
  gate (it targets a named database, not the current tenant's graph); the existing
  deferred directory removal cleans the corrupt files.

### Reporting
- **`SHOW DATABASES`**: add a second column, `Status`, with values `ready` /
  `defunct` (a result-schema change, accepted). Populated by resolving each name
  to its database and reading the defunct flag.
- **`SHOW STORAGE INFO`**: append an always-present key/value row
  `status` = `ready` / `defunct` (non-breaking; the output is already key/value
  rows).

### Replication / HA
- **Replica self-heal:** a defunct replica tenant reports an initial commit
  timestamp with a fresh epoch (a consequence of `Clear()`), so the main always
  drives it into the `RECOVERY` state and sends a full snapshot before any
  incremental delta. The replica-side snapshot-receive handler **clears the
  defunct flag on a successful snapshot load**, after which the tenant resumes
  normal replication.
- **No defunct check in the incremental commit handler** — only a comment
  documenting the invariant: a defunct tenant is always routed through
  `RECOVERY` → snapshot (which clears defunct) before any incremental delta can
  arrive, so the handler is never reached while defunct.
- **Main-side defunct tenant:** rejected for queries on the main; replicas keep
  serving reads of their healthy copy; the admin cures the main with
  `RECOVER SNAPSHOT` / `REPAIR DATABASE`.
- **Non-goals (explicit):** coordinator-driven failover based on per-tenant
  defunct state (a main defunct for one tenant but otherwise healthy stays main),
  and any replica re-sync mechanism beyond the existing snapshot+WAL path.

### Scope boundary
- In-memory storage only. On-disk storage keeps today's crash-on-recovery-failure
  behavior; the flag does not change it.

## Testing Decisions

A good test asserts **external, observable behavior** — does the instance boot,
what status is reported, what error is thrown, does the data come back after a
cure, does a replica self-heal — and avoids coupling to internal implementation
details (private members, exact call sequences). The `Clear()`-based scrub, for
example, is verified through "the defunct tenant is observably empty and its
on-disk files are unchanged", not by inspecting internal containers.

### Unit tests (`tests/unit/`, GoogleTest)
- Constructing an `InMemoryStorage` over a corrupt durability directory with the
  flag enabled yields a storage that is **defunct**, **empty**, and leaves the
  on-disk `snapshots/`/`wal/` **byte-for-byte unchanged**.
- Each converted recovery failure point throws `RecoveryFailure` when the flag is
  on, and remains fatal when the flag is off.
- Cure behavior at the storage level: `RecoverSnapshot` on a defunct placeholder
  clears defunct and leaves a clean single-snapshot directory; the repair path
  resets to empty, clears defunct, and moves files to `.old`; repair on a healthy
  storage is rejected.
- A `cypher_main_visitor` parse test asserting that `REPAIR DATABASE` produces the
  corresponding AST node.
- **Corruption injection** for these tests uses the byte-flip-every-offset
  technique: copy a real WAL file, flip the byte at each offset in turn, attempt
  construction / WAL read, and assert the corruption is detected (a
  `RecoveryFailure` is thrown / the tenant goes defunct) rather than crashing.
- Prior art: existing storage recovery unit tests and the existing WAL-info /
  recovery tests under `tests/unit/`; existing `cypher_main_visitor` query parse
  tests.

### E2E tests (`tests/e2e/durability/`, pytest + `interactive_mg_runner`)
- Boot an instance with a corrupted tenant and the flag set: the instance comes
  up; `SHOW DATABASES` shows `defunct`; `SHOW STORAGE INFO` shows
  `status` = `defunct`; a data query throws the exact defunct message.
- `USE DATABASE <defunct>` + `RECOVER SNAPSHOT` → tenant cured, queries succeed,
  state survives a restart.
- `USE DATABASE <defunct>` + `REPAIR DATABASE` → tenant cured to empty, the repair
  notification is present, import queries succeed, state survives a restart.
- `DROP DATABASE <defunct>` succeeds.
- Community default-database-defunct flow (no `USE DATABASE`).

### HA e2e tests (`tests/e2e/durability/`, coordinator-based, styled after `tests/e2e/high_availability/distributed_coords.py`)
Follow the `distributed_coords.py` conventions (`test_name` fixture, `file = ...`,
fixtures, `interactive_mg_runner` instance-description dicts, Raft coordinators +
data instances):
1. A replica boots with a corrupted tenant while the main is healthy → the main
   full-syncs it → the replica clears defunct and serves reads.
2. The main boots with a corrupted tenant → the admin recovers it via
   `RECOVER SNAPSHOT`.
3. Both main and replica boot with the corrupted tenant → verify the
   `defunct` / `ready` status via **both** `SHOW STORAGE INFO` and
   `SHOW DATABASES` on **both** instances.
4. The `REPAIR DATABASE` + import-queries cure path (in addition to
   `RECOVER SNAPSHOT`) in the HA setting.

### Modules to be tested
- The durability recovery layer (fatal → catchable conversions; placeholder
  construction; scrub-to-empty).
- The cure operations (`RECOVER SNAPSHOT` defunct-clear; `REPAIR DATABASE`).
- The query frontend (`REPAIR DATABASE` parsing).
- The end-to-end operator and HA flows (boot, report, throw, cure, drop,
  self-heal).

## Out of Scope

- On-disk (`ON_DISK_TRANSACTIONAL`) storage: keeps today's fatal behavior; the
  flag has no effect there.
- Coordinator-driven failover decisions based on per-tenant defunct state. A main
  that is defunct for one tenant but otherwise healthy remains main.
- Any automatic replica re-sync mechanism beyond the existing snapshot + WAL
  recovery path.
- Best-effort / partial salvage recovery (loading a snapshot then replaying WAL
  up to the first corrupt delta). `REPAIR DATABASE` is a destructive reset, not a
  partial recovery.
- Persisting the defunct state across restarts (it is intentionally re-derived).
- Additional defunct sub-states (e.g. a distinct "recovering" status). Status is
  binary: `ready` / `defunct`.

## Further Notes

- The defunct error message is fixed (operator-facing) and is reproduced verbatim
  in the Implementation Decisions section.
- The placeholder's "inert until cured" property is the central safety invariant:
  an empty defunct tenant must never write a snapshot or WAL, so the operator's
  corrupt files are preserved until they explicitly choose RECOVER, REPAIR, or a
  full data-directory restore.
- Because `RECOVER SNAPSHOT` already moves prior/corrupt files to `.old` and
  `REPAIR DATABASE` uses the same `.old` convention (gated on
  `--storage-backup-dir-enabled`), both cures leave the durability directory in a
  state that recovers cleanly on the next restart — a tenant does not re-enter the
  defunct state after a successful cure.
- The default `memgraph` database benefits the most in single-tenant Community
  deployments, where a corrupt default database currently prevents the instance
  from starting at all.
