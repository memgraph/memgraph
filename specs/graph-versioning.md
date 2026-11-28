# Graph Versioning (branches)

**Status:** Experimental, Enterprise, off by default
**Author:** Andreja Tonev (https://github.com/andrejtonev)
**Reviewers:** Colin Barry (code), Josip Mrđen (outside)

> Fork a database into named, writable **branches** inside the same Memgraph instance. A branch
> is a view over the database, not a copy. Queries and writes on a branch are isolated, the
> database keeps serving reads and writes, and the branch is thrown away when no longer needed.
> A branch never changes the database it was forked from.

---

## 1. Overview

Users want to change a graph tentatively: model a what-if scenario, try an enrichment, let an
agent propose edits, and inspect the result before deciding anything. Today the options are a
full copy of the database (double RAM, load time, drifts from production) or one
`BEGIN … ROLLBACK` transaction (one session only, does not survive a restart).

A **branch** fills the gap. It is a writable overlay on a database as it was at the moment the
branch was created. It shares all unchanged data with the database, survives restarts, can be
used by many sessions at once, and is discarded with `DROP BRANCH`.

The v1 use case is **what-if analysis**: fork production, apply a scenario, run the same queries
on the branch and on production, compare, drop. v1 is **discard-only**: there is no merge.

## 2. Concepts

| Term | Meaning |
|---|---|
| Branch | A named, writable, isolated overlay of one database. Branches are per database; in a multi-tenant instance each database has its own branches. |
| `main` | The database's own graph (production data). Always exists, cannot be created, dropped or forked from anything. `main` is a reserved branch name. |
| Fork point | The point in main's commit history at which a branch was created. The branch sees main's data exactly as of the fork point plus its own changes, forever, including after restarts. Main's later commits are invisible to it. |
| Current branch | The branch a session runs its queries on. `main` by default. |

Main stays fully readable and writable while branches exist. Main's writes are invisible to
branches; a branch's writes are invisible to main and to every other branch. Main and branch
writes never conflict. Internal branch identifiers exist (directory names, the registry) but are
never shown to users.

## 3. Prerequisites

`CREATE BRANCH` checks every row, in this order, and reports the first failure. Every other branch
query skips rows 3 and 5.

| # | Requirement | Error when not met |
|---|---|---|
| 1 | Enterprise build (versioning is compiled only under `MG_ENTERPRISE`) | `Query is part of the Enterprise feature. In order to run it, you need an Enterprise license.` |
| 2 | Started with `--experimental-enabled=versioning` (startup-only) | `Graph versioning is disabled. Start Memgraph with --experimental-enabled=versioning to use branch queries.` for every branch query (§8) |
| 3 | Valid Memgraph Enterprise license (runtime check) | Without it branches are read-only and droppable (§10); `CREATE BRANCH` and branch writes fail with `Graph versioning is an enterprise feature. A valid Memgraph Enterprise License is required.` |
| 4 | The database runs in `IN_MEMORY_TRANSACTIONAL` | `Graph versioning requires IN_MEMORY_TRANSACTIONAL storage mode. It is not available in IN_MEMORY_ANALYTICAL or ON_DISK_TRANSACTIONAL.` A database that did not start in that mode: `Graph versioning is not available for this database: it did not start in IN_MEMORY_TRANSACTIONAL storage mode.` |
| 5 | Durability: WAL on (`--storage-wal-enabled=true`, i.e. `PERIODIC_SNAPSHOT_WITH_WAL`) **and** `--data-recovery-on-startup=true` | With the flag on, Memgraph does not start when WAL or recovery on startup is off or the startup storage mode is not `IN_MEMORY_TRANSACTIONAL`; the log gives the row 4/5 text followed by ` Memgraph does not start with --experimental-enabled=versioning and these settings: change them, or start without the flag.` `CREATE BRANCH` returns `Graph versioning requires the WAL (PERIODIC_SNAPSHOT_WITH_WAL) and --data-recovery-on-startup=true.` only for a database whose settings differ from the startup flags. |
| 6 | The instance is MAIN | `Graph Versioning branches are not supported with replication in v1: branch operations are refused on a REPLICA. Run them on the MAIN instance.` |

Requirement 5 exists because a branch must survive a restart; otherwise it is no better than an
open transaction. Without WAL or recovery it would be silently lost at the next startup.

## 4. Queries

All user-facing syntax is in this section. `<b>` is a symbolic name or string literal.
All management queries (everything except `USING BRANCH`) are autocommit only: inside
`BEGIN … COMMIT` they return the standard multicommand-transaction error, as multi-database
queries do.

### 4.1 Query table

| Query | Effect | Privilege | Errors (besides §3 and name rules) |
|---|---|---|---|
| `CREATE BRANCH <b>` | Forks the current database's `main` at its latest commit. The session does not switch. Issued from a branch, it still forks from `main`. Result: no rows. | `BRANCH_EDIT` | `Branch '<b>' already exists.`; `Cannot persist the branch registry: …` (nothing created) |
| `DROP BRANCH <b>` | Discards the branch: its changes, files and fork pin. If **this** session is on `<b>`, it moves to `main` once the drop succeeded. "In use" means any other session attached to `<b>`: running or idle, a Bolt-pinned pooled connection, or an in-flight `USING BRANCH`. | `BRANCH_EDIT` | `Branch '<b>' does not exist.` (also for `main`); `Cannot drop branch '<b>': it is in use by another session. Use DROP BRANCH '<b>' FORCE.`; `Cannot persist the branch registry: …` (branch kept) |
| `DROP BRANCH <b> FORCE` | Drops the branch first, then terminates the running transactions of every other session on `<b>` (best effort, as `DROP DATABASE … FORCE`). A transaction that cannot be terminated keeps running on its open view. Sessions are not closed: an idle session on `<b>` stays attached, and its next statement gets the dead-branch error (§5.3) until it switches away or disconnects. | `BRANCH_EDIT` and `TRANSACTION_MANAGEMENT` | `Branch '<b>' does not exist.` |
| `USE BRANCH <b>` | Switches this session to `<b>` (or back with `USE BRANCH main`) until the next `USE BRANCH`, a `USE DATABASE` to another database, or disconnect. Result: one `STATUS` row, `Using branch <b>` or `Already using branch <b>`. | `BRANCH_USE`, except `USE BRANCH main` (§4.2) | `Branch '<b>' does not exist.` (session stays where it was); `Branch switching is prohibited if the session explicitly defines the database.` (Bolt `db` pin, §4.3) |
| `USING BRANCH <b> <query>` | Runs one autocommit query on `<b>` (any branch or `main`). The session's current branch is unchanged. Combinable with other `USING` directives. Honoured under `EXPLAIN`/`PROFILE`. | `BRANCH_USE` (not for `main`), plus the query's own privileges | `Branch '<b>' does not exist.`; inside `BEGIN`: `USING BRANCH is not allowed inside an explicit transaction.`; `Branch can be set only once in the USING statement.`; on a subquery: `USING BRANCH applies to a whole query, not to a subquery.` |
| `SHOW BRANCHES [FOR DATABASE <db>]` | Lists `main` and every branch of the current database (or `<db>`). Columns below. | `BRANCH_USE`, authorised against the **listed** database | Unknown `<db>`: standard error |
| `SHOW BRANCH [<b>]` | One row: the session's current branch, or `<b>`. Same columns. | `BRANCH_USE` | `Branch '<b>' does not exist.`; session on a dropped branch: dead-branch error (§5.3) |
| Bolt `db="<database>@<b>"` | Runs the transaction on `<b>` of `<database>` (§4.3). `db="<database>"` means `main`. | `BRANCH_USE` when `<b>` is not `main` | `Branch '<b>' does not exist.` on BEGIN/RUN |

**`SHOW BRANCHES` / `SHOW BRANCH` columns** (11): `name` (string; `main` first), `created_at`
(creation time as in `SHOW SNAPSHOTS`: ISO 8601 with microseconds and UTC offset; null for
`main`), `sessions` (number of sessions currently on the branch), `current` (true for the
calling session's branch), `fork_timestamp` (null for `main`), `age_seconds` (null for `main`;
counts from `CREATE BRANCH` and keeps counting across restarts), `holds_back_main_gc` (null for
`main`; true on the branch, or branches, with the oldest fork pin among this database's
branches: that fork bounds how far main's GC can reclaim, §7. It compares branches only: a main
transaction older than every fork can be the real GC horizon while the oldest branch still shows
true, and two branches with the same `fork_timestamp` both show true), `main_unreleased_deltas`
(main's own count of delta objects not yet freed by GC, the same on every row; equal to
`unreleased_delta_objects` in `SHOW STORAGE INFO` on `main` and to the
`memgraph_unreleased_delta_objects` metric; branch deltas are not included), `unreleased_deltas`
(the branch's own count of delta objects not yet freed by its GC; null for `main`; 0 while the
branch's in-memory structures are not built, §5.1, or the branch is unavailable), `status`
(`available` / `unavailable`), `status_reason` (null unless unavailable). After a restart
`fork_timestamp` is the pin on the rebuilt history and may differ numerically from before; the
pin, and the history it retains on main, restart from startup, while `age_seconds` still counts
from creation, so it then overstates how long the pin has been held.

**Name rules** (`CREATE`): 1–63 characters; letters, digits, `.`, `_`, `-`; start and end with a
letter or digit; `main` is reserved. Errors: `Branch name must not be empty.`,
`'main' is a reserved branch name.`, `Branch name must be at most 63 characters.`,
`Branch name must not contain '@': it names a branch as <database>@<branch>.`,
`Branch name may contain only letters, digits, '.', '_' and '-'.`,
`Branch name must start and end with an alphanumeric character.`
Names are unique among a database's live branches; a dropped branch's name can be reused.

**Database names and `@`.** `@` is reserved for `<database>@<branch>` only while
`--experimental-enabled=versioning` is on; otherwise nothing changes, so this is not a breaking
change.
- With the flag on, `CREATE` / `RENAME DATABASE` reject names containing `@`:
  `Database name '<db>' is invalid: '@' is reserved for <database>@<branch> addressing.`
- With the flag on, a Bolt `db` value is split at its last `@`. Errors:
  `Invalid database name '<db>': both parts of <database>@<branch> must be non-empty.` and
  `Invalid database name '<db>': '@' may appear only once, as in <database>@<branch>.`
- **Upgrade:** with the flag on, Memgraph does not start if any existing database name contains
  `@`, naming each one. Start without the flag, `RENAME DATABASE` each of them, then enable it again.

**Unavailable branches** (§8). `USE BRANCH`, `USING BRANCH` and `db="x@b"` fail with
`Branch '<b>' is unavailable: <reason>. Its files were left as they are in '<dir>'. To bring it back, restore them from a backup and restart Memgraph; to remove the branch and its files, run DROP BRANCH '<b>'.`
`SHOW BRANCH[ES]` lists it with `status` `unavailable`; `DROP BRANCH` works.

**Keywords.** New keyword: `BRANCH` (and `BRANCHES`). `USE`, `USING`, `SHOW`, `CREATE`, `DROP`,
`FORCE` already exist. A keyword must be reserved only when the parser cannot tell it from an
identifier in some position. Every new rule starts with a fixed prefix (`CREATE BRANCH`,
`USE BRANCH`, `USING BRANCH`, …), so `BRANCH` and `BRANCHES` are added to the non-reserved
keyword list (`symbolicName`) and stay usable as labels, properties and variable names.

### 4.2 Privileges

`BRANCH_USE` and `BRANCH_EDIT` are new privileges, shaped like `MULTI_DATABASE_USE` /
`MULTI_DATABASE_EDIT` but independent of them: a user can work with branches without access to
other databases. They are ordinary privileges: granted with the usual `GRANT`/`REVOKE`/`DENY`,
part of `ALL` (`GRANT ALL`, `DENY ALL`, `REVOKE ALL`, `SHOW PRIVILEGES`) whether or not versioning
is enabled, and part of the replicated auth data. Users granted `ALL` before the upgrade that
introduced them do not have them; run `GRANT ALL` again (versioning is experimental, so there is no
automatic migration). `DROP BRANCH … FORCE` also needs `TRANSACTION_MANAGEMENT`, because it
terminates other users' transactions (as `DROP DATABASE … FORCE` does). In addition:

- The user needs normal access to the database the branch belongs to (`GRANT DATABASE`).
- `BRANCH_USE` is checked when a session attaches to a branch other than `main`, as
  `MULTI_DATABASE_USE` is on `USE DATABASE`: `USE BRANCH <b>`, `USING BRANCH <b>` and Bolt
  `db="x@b"` (each BEGIN/RUN). `main` needs database access only (`USE BRANCH main`,
  `USING BRANCH main`, `db="x@main"`), so a revoked user can always get back to `main`. Queries of
  an attached session are not re-checked: a `REVOKE BRANCH_USE` takes effect on the session's next
  attach or re-authentication.
- Queries run on a branch need their usual privileges (`MATCH` → READ, `CREATE` → WRITE, …).
  Fine-grained (label/property) access control applies on a branch exactly as on `main`.
- There are no per-branch ACLs: any holder of `BRANCH_USE` on a database can use any of its
  branches, and any holder of `BRANCH_EDIT` can drop any of them.

A missing branch privilege is named:
`You are not authorized to execute this query on database "<db>"! Missing privilege: <privileges>. Ask your database administrator to grant it.`
A Bolt `db="x@b"` without `BRANCH_USE` fails with
`You are not authorized to use branches on the database "<db>"! Please contact your database administrator.`

### 4.3 Drivers and pooled connections

A driver sends the `db` field on every BEGIN/RUN, so a pooled connection carries no hidden branch
state. Setting the database name to `"<database>@<branch>"` therefore pins each transaction to
a branch exactly as `db="<database>"` pins it to a database:

```python
with driver.session(database="shop@supplier_fail") as s:   # every tx on branch supplier_fail
    s.run("MATCH (p:Plant {at_risk: true}) RETURN count(p)")
with driver.session(database="shop") as s:                 # main
    ...
```

- The branch is resolved at BEGIN (explicit tx) or RUN (autocommit). A `db` that cannot be
  resolved is a client error.
- Routing is not branch-aware: HA is not supported in v1 (§9).
- As with a `db` pin, `USE DATABASE` and `USE BRANCH` are refused on such a session;
  `USING BRANCH` is allowed (it changes no session state).
- `USE BRANCH` (session state) is for interactive clients (mgconsole, Lab). Applications with
  pooled drivers should use `db="x@b"` or `USING BRANCH`.

## 5. Sessions and transactions

### 5.1 Session binding

Branches mirror databases. A session holds its current branch the way it holds its database
(attached for the whole session); each transaction takes storage access for its own duration.
A transaction binds one storage engine at BEGIN, which is why `USING BRANCH` is autocommit only
(switching inside a transaction would need an atomic commit across two engines). An explicit
transaction on a branch is opened with `USE BRANCH` or `db="x@b"` before BEGIN.

- Many sessions can share a branch. They see each other's committed writes and get normal MVCC
  behaviour (snapshot reads, `SERIALIZATION_ERROR` on concurrent writes to the same object).
- Explicit transactions and periodic commit work on a branch; rollback undoes the branch changes.
- `USE DATABASE` to another database moves the session to that database's `main`; to the same
  database it keeps a live branch. Bolt `RESET` keeps the branch; `LOGOFF` returns to `main`.
- A branch's in-memory structures are built on first use, not at `CREATE`, so `CREATE` is cheap
  and the first use costs more.
- Starting a branch transaction and the first-attach build both take main's shared storage access
  and wait for it at most `--storage-access-timeout-sec`, as main's transactions do; on timeout they
  fail with the standard retryable `Cannot get shared access to the storage. …` error.

### 5.2 Isolation levels

`SET SESSION/NEXT TRANSACTION ISOLATION LEVEL` apply on a branch as on `main` (SI, RC, RU) for
the branch's own changes. The branch's view of main's data is the fixed fork point, so it is the
same at every level. `SET GLOBAL TRANSACTION ISOLATION LEVEL` is a database setting; it applies
to every branch transaction opened after it, unless a session or next-transaction level overrides it.

### 5.3 A branch that disappears

A branch can disappear under a session through `DROP BRANCH … FORCE` (after `DROP DATABASE … FORCE`
the session gets the database error instead). The session is **never** silently moved to `main`.
Every following query except `USE BRANCH` and `USE DATABASE` fails with
`Branch '<b>' no longer exists. Run USE BRANCH or USE DATABASE to continue.`
until the session switches. With `db="x@b"` every new transaction fails with
`Branch '<b>' no longer exists.` until the client changes `db`; the session is not terminated.
If a branch with the same name was created since, the errors are
`Branch '<b>' was dropped (a branch with the same name was created since). Run USE BRANCH <b> to use the new one, or USE DATABASE to continue.`
and, with `db="x@b"`,
`Branch '<b>' was dropped (a branch with the same name was created since); this connection stays on the dropped one. Reconnect, or change the client's database, to use the new '<b>'.`

## 6. Behaviour on a branch

Every area below is meant to work on a branch. **Not in v1** marks what this version does not do
yet (§12 says what each needs). Only triggers, TTL and streams are main-only by design.

| Area | On a branch |
|---|---|
| Reads: `MATCH`, traversals, shortest paths, aggregations, subqueries | Yes |
| Create/delete vertices and edges, labels, properties (on fork-era and branch-created objects) | Yes. Plain `DELETE` of a vertex with edges fails, as on `main`. |
| Index-backed scans (label, label+property incl. composite/nested/range, edge-type, edge-type+property, global property) | Yes: main's indexes as of the fork plus the branch's changes. Results equal a full scan. |
| Point functions (`point.distance`, `point.withinbbox`) | Correct results via full scan plus filter; using the point index is not in v1 |
| Text search, vector search | Not in v1 (refused); required long-term (agent workloads). These indexes live outside the MVCC store, so they need branch-aware versions (§12). Errors: `Text search is not supported on a branch in v1. Run it on main: USE BRANCH main.` and `Vector search is not supported on a branch in v1. Run it on main: USE BRANCH main.` The `text_search.*` and `vector_search.*` procedures behave the same |
| Parallel execution | Not in v1. `USING PARALLEL EXECUTION` on a plan that could be parallelized fails with `USING PARALLEL EXECUTION is not supported on a branch in v1. Remove the directive, or run the query on main: USE BRANCH main.`; otherwise the query falls back to single-threaded with the usual notification |
| Constraints (existence, unique, type) | Not in v1; required long-term. A branch has no constraints: its writes are not checked against main's, `SHOW CONSTRAINT INFO` on a branch is empty, and constraint DDL on a branch is refused (schema DDL row below) |
| Enums | Main's enums can be used in values |
| Query modules / procedures | Yes; nested queries stay on the caller's branch. Approximate counts reflect the branch: main at the fork plus the branch's changes. |
| Periodic commit | Yes |
| Edge properties | Follow main's `--storage-properties-on-edges` |
| Triggers, TTL, streams | Not planned for branches: they act on `main` only; branch writes fire no triggers |
| `DUMP DATABASE` | The branch's data with main's indexes visible at the fork that main still has; ones created on main after the fork are left out. No constraints are written: a branch has none |
| `CREATE SNAPSHOT`, `SHOW SNAPSHOTS`, `SHOW NEXT SNAPSHOT`, `FREE MEMORY`, `SET GLOBAL TRANSACTION ISOLATION LEVEL`, `LOCK` / `UNLOCK DATA DIRECTORY`, `DATA DIRECTORY LOCK STATUS` | Run on `main`, with an INFO notification `<statement> ran on main, not on branch '<b>'.` |
| Schema DDL (index, constraint, enum, TTL, trigger, stream, description), `DROP ALL INDEXES` / `DROP ALL CONSTRAINTS`, `SHOW TRIGGERS`, `SHOW STREAMS`, `SHOW DESCRIPTIONS`, `ANALYZE GRAPH`, `DROP GRAPH`, `STORAGE MODE`, `RECOVER SNAPSHOT`, `EDGE IMPORT MODE`, `schema.assert` | Not in v1: in v1, schema and storage-wide state are `main`'s (§12 says what each item needs; TTL, trigger and stream DDL stay main-only). Error: `Schema and storage-global operations are not allowed on a branch. Run USE BRANCH main first.`; `schema.assert` under `USING BRANCH <b>`: `… Run the query on main: USING BRANCH main.` |
| `SHOW INDEX INFO` | Main's current indexes, including ones created after the fork that the branch does not use |
| `SHOW CONSTRAINT INFO` | Empty: a branch has no constraints |
| `SHOW SCHEMA INFO` | Not in v1. Refused: `SHOW SCHEMA INFO on a branch is not in v1: schema info is tracked per storage engine, so main's does not include the branch's changes. Run USE BRANCH main first.` |
| Label / edge-type listings | Main's ∪ the branch's |
| `SHOW STORAGE INFO` | Vertex/edge counts are the branch's: main's approximate counts at the fork, plus objects the branch created, minus base objects deleted by committed branch transactions; independent of main's later writes. Average degree is derived from them as on main; `unreleased_delta_objects` is the branch's own count (as `unreleased_deltas` in `SHOW BRANCHES`); a `branch` row names the branch |

Vertex and edge ids (`id(n)`) of branch-created objects come from main's counters, so ids are
unique across `main` and all branches of the database.

**Performance.** Scans over data the branch did not touch stream main's data directly. Measured
costs per operation are under **Costs on a branch** (§7).

## 7. Interactions with main

| Operation on the database while it has branches | v1 | Why |
|---|---|---|
| Reads and writes on `main` | Allowed | Invisible to branches |
| Index, constraint, TTL, trigger DDL on `main` | Allowed | Branches keep reading main@fork. A branch uses an index only if it was visible at the fork; an index created, or dropped and re-created, on `main` after the fork is not used and the branch falls back to a scan. An index dropped on `main` falls back to a scan. Main's constraints never apply to a branch (§6). |
| `CREATE ENUM`, `ALTER ENUM … ADD/UPDATE` | Refused: `Cannot create an enum while branches exist: in v1 each branch keeps its own copy of the enum store, so the change would not reach it. Drop all branches first.` (`alter an enum` for `ALTER ENUM`) | Each branch has its own copy of the enum store, taken when the branch is first used (§12) |
| `STORAGE MODE` (to another mode), `DROP GRAPH`, `RECOVER SNAPSHOT` | Refused: `Cannot {switch storage mode\|drop the graph\|recover a snapshot} while branches exist: it would replace the data every branch reads. Drop all branches first.` Unavailable branches also block. `STORAGE MODE` to the current mode is not refused. With versioning off nothing is refused, as on master (§8) | They replace the data every branch reads |
| Durability settings (WAL mode, recovery on startup) | Startup-only | Changing them across a restart does not drop branches (§8) |
| `RENAME DATABASE` | Allowed | Branches follow the database |
| `SUSPEND` / `RESUME DATABASE` (COLD tenants) | Allowed on MAIN; `SUSPEND` is refused while any session, including one on a branch, is on the database | Branches are kept on disk and rebuilt on resume |
| `DROP DATABASE` | Allowed; deletes its branches | Without `FORCE` it fails while any session is on the database or one of its branches (standard "in use" error); with `FORCE` those sessions are terminated |

`CREATE BRANCH` holds main's shared (READ) storage access, with no timeout, until the branch is
registered, plus the branch registry lock; `DROP BRANCH` takes only the registry lock. The refused
operations check for branches under main's exclusive (UNIQUE) access; `RECOVER SNAPSHOT` checks
when it is prepared and holds that access for its whole run. So a branch cannot be created while a
refused operation is running, and none of them can start once the branch exists.

While a branch transaction is open, UNIQUE-access statements on the database wait: point, text
and vector index DDL, `DROP ALL INDEXES` / `DROP ALL CONSTRAINTS`, TTL, descriptions,
`schema.assert`, `STORAGE MODE`, `DROP GRAPH`, `RECOVER SNAPSHOT` and enum DDL. MVCC index and
constraint DDL do not wait. A waiting UNIQUE statement also delays new branch transactions and
first-attach builds, each for at most `--storage-access-timeout-sec` (§5.1).

**Costs on `main`** (no cap on count, size or age in v1):

| What | Grows with | Freed when |
|---|---|---|
| Memory: main's old versions newer than the **oldest** fork point (GC cannot reclaim them), plus the undo buffers of main transactions that abort after it | Main's write churn since that fork, not branch count or size. Per retained property write ~113 B real (tracked as ~355 B: allocator page padding, as on master with any long-running reader, and the memory limit uses the tracked figure); several KB per `DETACH DELETE`d vertex. Measured: ~620k main writes with one empty branch alive held +58 MB RSS / +157 MB tracked | The next GC after the oldest branch is dropped (within one to two `--storage-gc-cycle-sec` periods, or at once with `FREE MEMORY`). Versions newer than the next-oldest fork stay until that branch is dropped too; dropping a branch that is not the oldest frees nothing on main. A branch dropped with `FORCE` while sessions are on it, and a branch transaction still running, keep the pin until they end |
| Disk: at most one snapshot beyond `--storage-snapshot-retention-count` (the newest at or below the oldest fork point) plus the WAL after it; with no such snapshot, WAL from the first file | Main's write volume since that fork | The oldest branch is dropped |
| UNIQUE-access statements on `main` wait for open branch transactions (above; idle branch sessions block nothing) | n/a | Transaction ends |

Branch memory is accounted to the database's memory usage.

**Costs on a branch.** Ratios are branch time ÷ main time for the same query on the same server,
with equal results checked on both sides. Setup: a 100k-vertex, 1.77M-edge Pokec subset on a
shared VM, one session, main's WAL on, main idle after the fork. "0/10/50/90%" is the share of
`:User` vertices the branch changed (each gets a property; every 2nd also a label, every 5th also
a new edge). Writes ran on a fresh `(:V)` chain graph, 10k–40k rows in one transaction. Reads were
measured at `c29ca14d5`, writes at `5e0cfb888`; both builds predate the branch WAL, so its cost is
not in these numbers.

| Operation | Cost vs main | Grows with | Measured |
|---|---|---|---|
| RAM, disk | Branch: per changed object plus fixed per built branch. Main: retained history (above) | Branch change count, not graph size; disk with branch write history (§8) | RAM per change: 77–189 B at 90% changed, 101–507 B at 10% (new edges cost most; `5e0cfb888`). Disk not yet measured |
| First use after `CREATE` or restart | Builds the branch's in-memory structures (§5.1) | Branch size | Not yet measured |
| Point read via index | Untouched object: one hint-bit check, then main's object at the fork. Touched object: stub lookup by gid | Touched objects among those read | 200 seeks: ×1.04 / 1.06 / 1.03 / 1.03; zero changes, 20k seeks: ×1.07 |
| Full scan, count, label scan + filter | Unchanged data streams from main; changed objects are merged from their stubs | Changed objects in the scanned set | `count(n)` ×1.00 at all levels; label scan + filter ×1.02–1.03 |
| Read every vertex's properties | As above, plus reading the branch's version of each changed object | Changed objects read | ×1.02 / 1.05 / 1.12 / 1.15 |
| Scan of a label the branch added (no index) | Main's vertices at the fork checked against the branch's per-label membership | Vertices whose label set the branch changed | ×1.07 / 1.16 / 1.25 / 1.31. Branch removing the scanned label from main's vertices: not yet measured |
| Index scan, `ORDER BY … LIMIT` | Main's index at the fork plus the branch's index. An index not visible at the fork is not used: full scan (§7) | Changed objects carrying the label | `ORDER BY n.id DESC LIMIT 10` over all `:User`: ×1.00–1.01 |
| Expand / traversal | Adjacency of a touched vertex is main's at the fork merged with branch-added and branch-deleted edges | Touched vertices on the path, their edge changes | 1 hop ×1.03–1.06 at 0–10%, ×1.18–1.28 at 90%; 2 hops ×1.02–1.10; 20 BFS ×1.05 / 1.09 / 1.16 / 1.22 |
| Writes (incl. commit, no branch WAL) | First write to a fork-era object creates its stub; that and later writes are ordinary MVCC deltas on the stub | Rows written; width of the object (wide objects not yet measured) | SET property ×0.90–0.94, add label ×1.11–1.23, remove label ×1.03–1.13, edge property ×0.51–0.55, create edge ×0.96–0.97, create vertex ×0.60–0.63, `DETACH DELETE` ×0.46–0.58 |
| Commit path | Branch commits use the branch's own commit lock and clock, not main's; no constraint checks (§6); the branch WAL is written and flushed with main's WAL settings | Txn size; WAL sync policy | Not yet measured with the branch WAL |
| Reading main@fork after main kept writing | An object main changed after the fork is read by walking main's undo chain back to the fork; main's post-fork index entries are skipped | Main's writes to the read objects since the fork (main write rate × branch age) | Not yet measured (all runs above had main idle) |
| Planning | Estimates are main's current index sizes plus the branch's own; with no usable index, the branch's total. A plan can differ from main's for the same query. Results are unaffected | Gap between main's current data and main@fork plus the branch | Not yet measured |
| Several sessions on one branch | Sessions share one branch core; no parallel execution within a query (§6); writers conflict only on the same object | Session count | Correctness only (TSan, stress); throughput not yet measured |
| Restart | Main replays its WAL from the oldest fork point as MVCC transactions, then each branch replays its whole WAL (§8) | Main's WAL since the oldest fork; branch write history (no compaction in v1) | Not yet measured |
| Main's own queries while branches exist | Retained history costs memory and GC work (above); main's readers start from the newest version and do not walk it. Each built branch runs its own GC on main's interval. Writes to the word holding a vertex/edge's delta pointer and flags are relaxed atomic stores (still serialised by the object's lock), so the hint bit can be read without a lock | Main write rate × age of the oldest branch; branch count | Not yet measured with live branches. The CI benchmark job (no branches) passed on `87cddc09d`; not compared against master |

## 8. Durability and recovery

Every branch writes its own WAL; changes are durable before visible. A branch survives clean
restarts and crashes with its fork point and contents intact. Sessions must select it again after
a restart.

A branch failing to recover never stops startup and does not affect main or any other branch.
Only two things stop startup: with the flag on, the configuration checks (WAL or recovery on startup off,
a non-transactional startup mode, `@` in a database name; §3, §4.1), and a database whose branch
ledger cannot be read. A branch that cannot be rebuilt
correctly is **unavailable**: it stays registered (ledger entry, files, fork pin and retention floor
kept), cannot be selected (the error names the reason), `SHOW BRANCH[ES]` lists it with `status`
`unavailable` and the reason, and `DROP BRANCH` removes it. A warning naming
`Graph Versioning` is logged. Conditions, checked in order:

| Condition at startup | Result |
|---|---|
| Branch ledger unreadable or corrupt | That database does not start: `Database '<db>' (<uuid>) cannot start: <reason>. The branch ledger is stored in the DBMS durability store (databases/.durability); restore it from a backup.` At startup Memgraph stops, as for any database that cannot be recovered; `RESUME DATABASE` of a cold database fails and the database stays cold. Nothing is deleted. |
| WAL off, recovery on startup off, or a non-transactional mode, for a database whose settings differ from the startup flags | That database's branches are unavailable (kept) |
| Main cannot be replayed to the fork points | Bounded recovery or the tail replay fails: main is recovered unbounded and every branch is unavailable. Main's recovered history ends before a branch's fork point: that branch is unavailable |
| A branch's directory or its `wal/` directory is missing (`CREATE BRANCH` makes both) | Unavailable: `The branch directory '<dir>' is missing, so the branch's committed writes are lost` (or `The WAL directory …`) |
| A branch's WAL is damaged | Main's WAL rules apply to the file still being written (unreadable or empty: skipped; otherwise used up to its last complete transaction). If no file is readable the branch starts as forked, with a warning. A finalized file that cannot be read makes the branch unavailable, because it held committed transactions. Where main would refuse to start (e.g. a gap in the sequence), only that branch is unavailable. |
| A gid the branch created is used by a main object (main reused it while the branch was not recovered, e.g. with the flag off) | Unavailable: `gid conflict with main: <vertex\|edge> <gid> exists in main, which reused it while this branch was not recovered; only DROP BRANCH resolves it` |
| A `branches/<n>` directory with no ledger entry (a create that crashed before its ledger write, or a drop's leftovers) | Warning naming the directory; it is ignored and left as is. A later `CREATE BRANCH` that gets number `<n>` clears it first |
| Main itself cannot be recovered | Standard Memgraph behaviour |

**Flag off while branches exist.** Without `--experimental-enabled=versioning` the ledger is not
read, as on master: branches are not loaded, every branch query fails with the disabled-flag error
(§3), and nothing on main is refused or retained for them (§7 refusals, fork pins, snapshot/WAL
retention floor). Main may therefore delete the snapshot and WAL history the branches are rebuilt
from, and hand out vertex/edge ids a branch created. Restarting with the flag on restores every
branch that can still be rebuilt; the others are unavailable (conditions above) and `DROP BRANCH`
removes them. The same holds for a build without Graph Versioning (Community, or an older version)
started on a data directory that has branches.

Branch WALs are not compacted in v1, so a long-lived branch's restart time grows with its write
history.

**Format.** Branch changes are stored in each branch's own WAL as ordinary deltas. There is no new
WAL record type and no durable-format version bump; main's snapshot and WAL formats are unchanged.

## 9. Replication and HA (v1)

Not supported in v1.

- Every branch query fails on a REPLICA with
  `Graph Versioning branches are not supported with replication in v1: branch operations are refused on a REPLICA. Run them on the MAIN instance.`
- Branches are dropped on a REPLICA (on resync, restart, resume of a cold database, and coordinator
  demotion).
- A user `SET REPLICATION ROLE TO REPLICA` is refused while branches exist:
  `Cannot set the replication role to REPLICA while branches exist: Graph Versioning branches are not supported with replication in v1. Drop the branches first with DROP BRANCH, then retry. Databases with branches: <list>.`
- Routing is not branch-aware.
- The role is checked when `CREATE BRANCH` is prepared, so a `CREATE BRANCH` racing a coordinator
  demotion can leave one branch on the new REPLICA. Every branch query is refused there, and the
  next resync or restart as REPLICA drops it.

## 10. Licensing and flag

| Build / license | Branch behaviour |
|---|---|
| Community build | No versioning logic is compiled in (all versioning code is under `#ifdef MG_ENTERPRISE`). `--experimental-enabled=versioning` is rejected as an unknown experiment. Every branch query fails with `Query is part of the Enterprise feature. In order to run it, you need an Enterprise license.` |
| Enterprise build, no valid license | Read-only and droppable. `CREATE BRANCH` and writes on a branch are refused. `SHOW BRANCH[ES]`, `USE BRANCH` / `USING BRANCH` for reads, switching back to `main`, and `DROP BRANCH [FORCE]` work, so a session is never stuck on a branch and main's retention can be released. Startup restores branches. |
| Enterprise build, valid license | Everything in this spec |

**Flag.** `--experimental-enabled=versioning` is startup-only. With it off Memgraph behaves as
master: branch queries fail with the disabled-flag error, nothing on main is refused, and the
branches are neither loaded nor retained (§8). Restarting with it on restores every branch that can
still be rebuilt.

## 11. Observability

**Metrics** (enterprise builds only; Prometheus; also in `SHOW METRICS INFO`; totals across
databases, no per-database label). Counters are per process: they reset on restart, and restored
branches are not counted as created. `dropped_total` includes automatic drops; branches removed
together with their database (`DROP DATABASE`) are not counted. `active_branches` counts live and
unavailable branches of loaded databases; a suspended (cold) database's branches
leave the gauge until it is resumed.

| Metric | `SHOW METRICS INFO` name | Type |
|---|---|---|
| `memgraph_versioning_branches_created_total` | `VersioningBranchesCreated` | counter |
| `memgraph_versioning_branches_dropped_total` | `VersioningBranchesDropped` | counter |
| `memgraph_versioning_active_branches` | `VersioningActiveBranches` | gauge |
| `memgraph_versioning_active_sessions` | `VersioningActiveSessions` | gauge (branch attachments: counts a session parked under `USING BRANCH` and each in-flight `USING BRANCH` query, so one session can count twice) |

**Logs.** Every automatic drop (on a REPLICA: resync, restart, resume, coordinator demotion), every
branch set aside as unavailable at startup, every branch directory without a ledger entry, and every
branch directory that cannot be removed is logged as a warning naming `Graph Versioning`; a failed
automatic drop is an error starting with `Failed to drop the branches of database <db>`.

Not exposed in v1: per-branch memory and disk (branch memory shares the database's allocator), and
how much of main's retained history each fork pin holds. `SHOW BRANCHES` shows which branch holds
the oldest pin (`holds_back_main_gc`), main's unreleased delta count (`main_unreleased_deltas`,
which also includes ordinary not-yet-collected garbage) and each branch's own
(`unreleased_deltas`); branch deltas are in no metric. There is no cap on, or warning about, the
retained history in v1.

## 12. Not in v1

Everything below is intended to be supported later; the last column says what it needs. Text
search, vector search and constraints are required long-term (agent workloads), even if
they need refactoring. Triggers, TTL and streams are the only features not planned for branches:
they stay main-only (§6).

| Feature | In v1 | What it needs |
|---|---|---|
| Merge a branch into `main` | Not in v1 | A design for conflicts with main's concurrent writes, schema drift, triggers, replication and audit. `BRANCH` is non-reserved, so adding `MERGE BRANCH` is backward compatible. |
| Diff / log / revert of a branch | Not in v1 | Implementation only |
| Nested branches (fork a branch) | Not in v1 | A third visibility layer: a child's base is parent-engine@ts ⊕ parent-stubs@ts ⊕ main@fork, so it needs historical reads on the branch engine and its stubs, GC and WAL pins on the parent, ordered recovery and 3-layer scans |
| Fork at a past point in time | Not in v1 | A retention window on `main`: today main keeps no history older than the oldest open transaction or fork pin |
| Replication / HA, incl. branch reads routed to replicas | Not in v1 | §9 |
| Analytical or on-disk storage | Not in v1 | A fork-point read source: analytical mode writes no undo history, and on-disk has no in-memory MVCC history |
| Index or constraint created on a branch | Not in v1 | A branch-side index type: fork data lives in main's storage and indexes, and a branch-private index there would be maintained by main's write path and seen by main's planner. Workaround: create the index on `main` (allowed) |
| `ANALYZE GRAPH` on a branch | Not in v1 | Verifying the branch planner's use of stats stored in the branch engine |
| Constraints on a branch | Not in v1 (required); a branch has none (§6) | Existence and type checks of the branch's writes, and a unique check against main@fork plus the branch's changes under concurrent branch sessions |
| Text, vector search on a branch | Not in v1 (required) | Branch-aware versions of these indexes. They live outside the MVCC store and reflect only main's current state, so answering "as of the fork plus the branch's changes" needs a fork-point view of main's index plus a branch-side index over the branch's changes. Until then the query errors instead of returning a stale answer |
| Point index on a branch | Not in v1 (full-scan fallback, correct results) | Implementation only |
| Parallel execution on a branch | Not in v1 (`USING PARALLEL EXECUTION` errors when the plan could be parallelized; otherwise single-threaded, §6) | Making the branch scans (fork stream ⊕ stubs ⊕ branch-created objects) splittable into chunks |
| Branch WAL compaction (branch snapshots) | Not in v1 | Implementation only |
| Caps on branch count, size, age | Not in v1 (names are limited to 63 characters, §4.1) | Operating experience to pick bounds |
| Per-branch memory/disk reporting | Not in v1 | Per-branch accounting: branch memory shares the database allocator |
| Per-branch ACLs / ownership | Not in v1 | Implementation only |
| `CREATE` / `ALTER ENUM` on `main` while branches exist | Not in v1 | Sharing main's enum store with branches: each branch has its own copy, so a change on `main` would not reach it |

## 13. Worked example

Server started with `--experimental-enabled=versioning`, WAL and recovery on, a valid license;
the user has `BRANCH_USE`, `BRANCH_EDIT` and READ/WRITE on database `shop`.

```cypher
CREATE BRANCH supplier_fail;                       // fork main now; session stays on main
USE BRANCH supplier_fail;

// Apply the scenario: branch only.
MATCH (:Supplier {id: 'S1'})-[r:SHIPS_TO]->() DELETE r;
MATCH (p:Plant) WHERE NOT exists((p)<-[:SHIPS_TO]-()) SET p.at_risk = true;

// Compare with production without leaving the branch.
MATCH (p:Plant {at_risk: true}) RETURN count(p);                         // branch
USING BRANCH main MATCH (p:Plant {at_risk: true}) RETURN count(p);       // main

USE BRANCH main;
SHOW BRANCHES;
//   name: "main",          created_at: null,                               sessions: 1, current: true,  …
//   name: "supplier_fail", created_at: "2026-10-29T09:15:02.418305+0000", sessions: 0, current: false, …
// (… = fork_timestamp, age_seconds, holds_back_main_gc, main_unreleased_deltas, unreleased_deltas, status, status_reason; §4.1)

DROP BRANCH supplier_fail;
```

A pooled application uses `driver.session(database="shop@supplier_fail")` instead of
`USE BRANCH` (§4.3).

---

## Architecture (for reviewers)

**Fork pin.** `CREATE BRANCH` records `fork_ts` under `engine_lock_` without consuming a
timestamp: it is the exclusive bound of what a `BEGIN` on main would see at that moment (the next
timestamp; with `--experimental-enabled=commit-lock-narrowing`, the last published commit + 1, since
commits publish in timestamp order). Branches work the same with narrowing on or off, and the same
files recover the same branches in either mode; the branch's own engine never narrows. Base reads see
exactly the commits with timestamp `< fork_ts`; an in-flight main commit, or the next one minted,
gets a timestamp `≥ fork_ts` (possibly equal) and is excluded, so the cut is consistent. Pins are a
multiset (branches and historical accessors may share a `fork_ts`; each registration is released
once). Main's GC horizon is clamped to the oldest pin, and a main commit's deltas are fast-discarded
only while the oldest pin is strictly greater than its commit timestamp. Base reads use a read-only historical accessor on main that
takes only main's shared storage lock and always reads snapshot-isolated at `fork_ts`.

**Branch core** (one per branch, shared by its sessions, built on first use):

- *Diff engine:* a private `InMemoryStorage` (config copied from main, own GC, not snapshotted in v1)
  sharing main's name-id mapper and allocator. It holds **branch-native** objects (gid ≥ the gid
  floor, i.e. main's counter value at the fork, recorded in the ledger; gids come from main's counters).
- *Stubs:* changes to **fork-era** objects are stock MVCC deltas on per-object stub vertices and
  edges owned by the stub index (`OverlayIndex`, `EdgeOverlayIndex`), found by gid and merged over the
  fork object on read. They run on the diff engine's clock, so SI/RC/RU, rollback and
  `SERIALIZATION_ERROR` between branch sessions behave as on main. Branch edges attach to stubs.
- *`branched()` hint bit* on main objects a branch touched: the only write to main's objects, so
  untouched objects skip the stub lookup. Set under the object's lock before the branch's first
  write to it, read without a lock. One bit is shared by all branches, so it is never cleared, not
  even by `DROP BRANCH`: an object any branch ever touched costs every branch the stub lookup
  until restart. It lives only in memory; recovery sets it again by replaying each branch's WAL
  through the same write path.
- *Visibility* = main@fork ⊕ stubs ⊕ native objects. Scans stream main@fork from main's index and
  resolve only gids in the branch's touched sets (insert-only, conservative), then merge the diff
  engine's index.
- Lock order: main shared storage guard → diff engine. One transaction per session.

**Durability.** The registry (ledger) is one key per database, `branches:<database uuid>`, in the
instance's database-registry KVStore (`databases/.durability`), next to the database's own record.
The value is versioned JSON; each write is synced to the KVStore's WAL before it returns, so a
written registry is durable. The key follows the uuid, so renaming a database needs no change, and it
is deleted with the database. It is
written on create and drop; a failed write fails the query and changes nothing. `<storage_dir>/branches/<id>/wal/` holds each branch's WAL as ordinary deltas;
markers on stubs are encoded as the equivalent user operation. A
`VersionStore` per database publishes `oldest recorded fork_ts − 1` as main's snapshot/WAL
retention floor.

**Recovery bound.** At startup the ledger is read before main's storage exists. Main is recovered
only up to `min(fork_ts) − 1`; the remaining WAL is replayed as real MVCC transactions, re-registering
each fork pin just before the first transaction with timestamp ≥ its `fork_ts`. Main's clock is then
bumped to `max(last durable ts + 1, newest fork_ts)` so no future main commit can land below a
fork point. Each branch core is rebuilt with its recorded floors and its WAL is replayed with main's WAL
rules; main's gid counters are then raised past the highest gid each branch created. A restored branch's `fork_timestamp` is the pin on the rebuilt history (replay recommits main's tail
at fresh MVCC timestamps, so it can differ from the fork point in main's durable timestamps); the
durable fork point is what the ledger keeps and what bounds retention and recovery. If bounded recovery fails, main is recovered
unbounded and every branch is unavailable (§8).
