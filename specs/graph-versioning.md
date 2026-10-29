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

Every branch query checks these, in this order, and reports the first failure:

| # | Requirement | Error when not met |
|---|---|---|
| 1 | Enterprise build (versioning is compiled only under `MG_ENTERPRISE`) | `Query is part of the Enterprise feature. In order to run it, you need an Enterprise license.` |
| 2 | Started with `--experimental-enabled=versioning` (startup-only) | `Graph versioning is disabled. Start Memgraph with --experimental-enabled=versioning to use branch queries.` |
| 3 | Valid Memgraph Enterprise license (runtime check) | Without it branches are read-only and droppable (§10); `CREATE BRANCH` and branch writes fail with `Graph versioning is an enterprise feature. A valid Memgraph Enterprise License is required.` |
| 4 | The database runs in `IN_MEMORY_TRANSACTIONAL` | `Graph versioning requires IN_MEMORY_TRANSACTIONAL storage mode.` |
| 5 | Durability: WAL on (`--storage-wal-enabled=true`, i.e. `PERIODIC_SNAPSHOT_WITH_WAL`) **and** `--data-recovery-on-startup=true` | `CREATE BRANCH` fails: `Graph versioning requires the WAL (PERIODIC_SNAPSHOT_WITH_WAL) and --data-recovery-on-startup=true.` |
| 6 | Not started with `--experimental-enabled=commit-lock-narrowing` | `Graph versioning is not supported with --experimental-enabled=commit-lock-narrowing.` |
| 7 | The instance is MAIN | `Branch operations are forbidden on a REPLICA. Run them on the MAIN instance.` |

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
| `DROP BRANCH <b>` | Discards the branch: its changes, files and fork pin. If **this** session is on `<b>`, it moves to `main` first. | `BRANCH_EDIT` | `Branch '<b>' does not exist.` (also for `main`); `Cannot drop branch '<b>': it is in use by another session. Use DROP BRANCH <b> FORCE.`; `Cannot persist the branch registry: …` (branch kept) |
| `DROP BRANCH <b> FORCE` | As above, but first terminates the transactions of every other session on `<b>`; those sessions then get the dead-branch error (§5.3). Mirrors `DROP DATABASE … FORCE`. | `BRANCH_EDIT` and `TRANSACTION_MANAGEMENT` | `Branch '<b>' does not exist.` |
| `USE BRANCH <b>` | Switches this session to `<b>` (or back with `USE BRANCH main`) until the next `USE BRANCH`, a `USE DATABASE` to another database, or disconnect. | `BRANCH_USE` | `Branch '<b>' does not exist.` (session stays where it was); `Branch switching is prohibited if the session explicitly defines the database.` (Bolt `db` pin, §4.3) |
| `USING BRANCH <b> <query>` | Runs one autocommit query on `<b>` (any branch or `main`). The session's current branch is unchanged. Combinable with other `USING` directives. Honoured under `EXPLAIN`/`PROFILE`. | `BRANCH_USE`, plus the query's own privileges | `Branch '<b>' does not exist.`; inside `BEGIN`: `USING BRANCH is not allowed inside an explicit transaction.` |
| `SHOW BRANCHES [FOR DATABASE <db>]` | Lists `main` and every branch of the current database (or `<db>`). Columns below. | `BRANCH_USE`, authorised against the **listed** database | Unknown `<db>`: standard error |
| `SHOW BRANCH [<b>]` | One row: the session's current branch, or `<b>`. Same columns. | `BRANCH_USE` | `Branch '<b>' does not exist.`; session on a dropped branch: dead-branch error (§5.3) |
| Bolt `db="<database>@<b>"` | Runs the transaction on `<b>` of `<database>` (§4.3). `db="<database>"` means `main`. | `BRANCH_USE` when `@<b>` is present | `Branch '<b>' does not exist.` on BEGIN/RUN |

**`SHOW BRANCHES` / `SHOW BRANCH` columns:** `name` (string; `main` first), `created_at`
(creation time as in `SHOW SNAPSHOTS`: ISO 8601 with microseconds and UTC offset; null for
`main`), `sessions` (number of sessions currently on the branch), `current` (true for the
calling session's branch).

**Name rules** (`CREATE`): non-empty; not `main`
(`'main' is a reserved branch name.`); must not start with `.`; must not contain `/`, `\` or `@`.
Names are unique among a database's live branches; a dropped branch's name can be reused.

**Database names and `@`.** Database names accept any character today (including `@`, `/`, NUL and
the empty string), so every separator needs this compatibility rule; `@` reads naturally. (1) `CREATE`/`RENAME DATABASE` reject names containing `@` from now on; (2) a Bolt `db`
value first matches an existing database name **exactly** (pre-existing `@` names keep working
after upgrade), otherwise it splits at the **last** `@` into `<database>@<branch>`; (3) `CREATE
BRANCH b` on database `a` is refused if a database literally named `a@b` exists.

**Keywords.** New keyword: `BRANCH` (and `BRANCHES`). `USE`, `USING`, `SHOW`, `CREATE`, `DROP`,
`FORCE` already exist. A keyword must be reserved only when the parser cannot tell it from an
identifier in some position. Every new rule starts with a fixed prefix (`CREATE BRANCH`,
`USE BRANCH`, `USING BRANCH`, …), so `BRANCH` and `BRANCHES` are added to the non-reserved
keyword list (`symbolicName`) and stay usable as labels, properties and variable names.

### 4.2 Privileges

`BRANCH_USE` and `BRANCH_EDIT` are new privileges, shaped like `MULTI_DATABASE_USE` /
`MULTI_DATABASE_EDIT` but independent of them: a user can work with branches without access to
other databases. They are granted with the usual `GRANT`/`REVOKE`/`DENY` and are part of the
replicated auth data. `DROP BRANCH … FORCE` also needs `TRANSACTION_MANAGEMENT`, because it
terminates other users' transactions (as `DROP DATABASE … FORCE` does). In addition:

- The user needs normal access to the database the branch belongs to (`GRANT DATABASE`).
- Queries run on a branch need their usual privileges (`MATCH` → READ, `CREATE` → WRITE, …).
  Fine-grained (label/property) access control applies on a branch exactly as on `main`.
- There are no per-branch ACLs: any holder of `BRANCH_USE` on a database can use any of its
  branches, and any holder of `BRANCH_EDIT` can drop any of them.

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

- The branch is resolved at BEGIN (explicit tx) or RUN (autocommit). Routing (`ROUTE`) uses the
  database part.
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
- `USE DATABASE` to another database moves the session to that database's `main`.
- A branch's in-memory structures are built on first use, not at `CREATE`, so `CREATE` is cheap
  and the first use costs more.

### 5.2 Isolation levels

`SET SESSION/NEXT TRANSACTION ISOLATION LEVEL` apply on a branch as on `main` (SI, RC, RU) for
the branch's own changes. The branch's view of main's data is the fixed fork point, so it is the
same at every level. `SET GLOBAL ISOLATION LEVEL` is a database setting; it applies to `main`
and to branch engines built after it.

### 5.3 A branch that disappears

A branch can disappear under a session: `DROP BRANCH … FORCE`, demotion to REPLICA, or
`DROP DATABASE … FORCE`. The session is **never** silently moved to `main`. Every following query
fails with
`Branch '<b>' no longer exists. Run USE BRANCH or USE DATABASE to continue.`
until the session switches. With `db="x@b"` every new transaction fails with
`Branch '<b>' no longer exists.` until the client changes `db`; the session is not terminated.

## 6. Behaviour on a branch

Every area below is meant to work on a branch. **Not in v1** marks what this version does not do
yet (§12 says what each needs). Only triggers, TTL and streams are main-only by design.

| Area | On a branch |
|---|---|
| Reads: `MATCH`, traversals, shortest paths, aggregations, subqueries | Yes |
| Create/delete vertices and edges, labels, properties (on fork-era and branch-created objects) | Yes. Plain `DELETE` of a vertex with edges fails, as on `main`. |
| Index-backed scans (label, label+property incl. composite/nested/range, edge-type, edge-type+property, global property) | Yes: main's indexes as of the fork plus the branch's changes. Results equal a full scan. |
| Point functions (`point.distance`, `point.withinbbox`) | Correct results via full scan plus filter; using the point index is not in v1 |
| Text search, vector search | Not in v1; required long-term (agent workloads). These indexes live outside the MVCC store, so they need branch-aware versions (§12) |
| Parallel execution | Not in v1: branch queries run single-threaded |
| Existence and type constraints | Enforced at branch commit, against main's **current** constraints |
| Unique constraints | Not in v1 (**not enforced**); required long-term. `SHOW CONSTRAINT INFO` marks them `unique (not enforced on branch)`. |
| Enums | Main's enums can be used in values |
| Query modules / procedures | Yes; nested queries stay on the caller's branch. Approximate counts reflect main at the fork. |
| Triggers, TTL, streams | Not planned for branches: they act on `main` only; branch writes fire no triggers |
| `DUMP DATABASE` | The branch's data with main's schema |
| `CREATE SNAPSHOT` | Snapshots `main` |
| Schema DDL (index, constraint, enum, TTL, trigger, stream, description), `ANALYZE GRAPH`, `DROP GRAPH`, `STORAGE MODE`, `RECOVER SNAPSHOT`, `EDGE IMPORT MODE`, `schema.assert` | Not in v1: in v1, schema and storage-wide state are `main`'s (§12 says what each item needs; TTL, trigger and stream DDL stay main-only). Error: `Schema and storage-global operations are not allowed on a branch. Run USE BRANCH main first.` |
| `SHOW INDEX INFO` / `SHOW CONSTRAINT INFO` | Main's |
| `SHOW SCHEMA INFO` | Not in v1: schema info is tracked per storage engine, so main's does not include the branch's changes |
| Label / edge-type listings | Main's ∪ the branch's |
| `SHOW STORAGE INFO` | Vertex/edge counts and average degree are `NULL` (exact counts need a full scan); a `branch` row names the branch |

Vertex and edge ids (`id(n)`) of branch-created objects come from main's counters, so ids are
unique across `main` and all branches of the database.

**Performance.** Scans over data the branch did not touch stream main's data directly. Measured
costs per operation are under **Costs on a branch** (§7).

## 7. Interactions with main

| Operation on the database while it has branches | v1 | Why |
|---|---|---|
| Reads and writes on `main` | Allowed | Invisible to branches |
| Index, constraint, TTL, trigger DDL on `main` | Allowed | Branches keep reading main@fork. A branch uses an index only if it was visible at the fork; an index created, or dropped and re-created, on `main` after the fork is not used and the branch falls back to a scan. An index dropped on `main` falls back to a scan. |
| `CREATE ENUM`, `ALTER ENUM … ADD/UPDATE` | Refused | Each branch has its own copy of the enum store, taken when the branch is first used (§12) |
| `STORAGE MODE`, `DROP GRAPH`, `RECOVER SNAPSHOT` | Refused: `… while branches exist … Drop all branches first.` | They replace the data every branch reads |
| Durability settings (WAL mode, recovery on startup) | Startup-only | Changing them across a restart drops branches (§8) |
| `RENAME DATABASE` | Allowed | Branches follow the database |
| `SUSPEND` / `RESUME DATABASE` (COLD tenants) | Allowed on MAIN | Branches are kept on disk and rebuilt on resume |
| `DROP DATABASE` | Allowed; deletes its branches | Without `FORCE` it fails while any session is on the database or one of its branches (standard "in use" error); with `FORCE` those sessions are terminated |

`CREATE BRANCH` and `DROP BRANCH` take main's shared storage access, so they serialise against
the refused exclusive operations: a branch cannot be created while one of them is running, and
none of them can start once the branch exists.

**Costs on `main`** (no cap on count, size or age in v1):

| What | Grows with | Freed when |
|---|---|---|
| Memory: main's old versions newer than the **oldest** fork point (GC cannot reclaim them) | Main's write churn since that fork | The oldest branch is dropped; with overlapping branches most is freed only after the newest overlapping one is dropped |
| Disk: snapshot and WAL files needed to rebuild the oldest fork point (may exceed configured retention) | Main's write volume since that fork | The oldest branch is dropped |
| Exclusive operations on `main` wait for open branch transactions (idle branch sessions block nothing) | n/a | Transaction ends |

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
| Point read via index | Untouched object: one hint-bit check, then main's object at the fork. Touched object: overlay lookup by gid | Touched objects among those read | 200 seeks: ×1.04 / 1.06 / 1.03 / 1.03; zero changes, 20k seeks: ×1.07 |
| Full scan, count, label scan + filter | Unchanged data streams from main; changed objects are merged from the overlay | Changed objects in the scanned set | `count(n)` ×1.00 at all levels; label scan + filter ×1.02–1.03 |
| Read every vertex's properties | As above, plus reading the branch's version of each changed object | Changed objects read | ×1.02 / 1.05 / 1.12 / 1.15 |
| Scan of a label the branch added (no index) | Main's vertices at the fork checked against the branch's per-label membership | Vertices whose label set the branch changed | ×1.07 / 1.16 / 1.25 / 1.31. Branch removing the scanned label from main's vertices: not yet measured |
| Index scan, `ORDER BY … LIMIT` | Main's index at the fork plus the branch's index. An index not visible at the fork is not used: full scan (§7) | Changed objects carrying the label | `ORDER BY n.id DESC LIMIT 10` over all `:User`: ×1.00–1.01 |
| Expand / traversal | Adjacency of a touched vertex is main's at the fork merged with branch-added and branch-deleted edges | Touched vertices on the path, their edge changes | 1 hop ×1.03–1.06 at 0–10%, ×1.18–1.28 at 90%; 2 hops ×1.02–1.10; 20 BFS ×1.05 / 1.09 / 1.16 / 1.22 |
| Writes (incl. commit, no branch WAL) | First write to a fork-era object copies its full state (labels, properties) into an overlay version; later writes add full-state versions | Rows written; width of the object (wide objects not yet measured) | SET property ×0.90–0.94, add label ×1.11–1.23, remove label ×1.03–1.13, edge property ×0.51–0.55, create edge ×0.96–0.97, create vertex ×0.60–0.63, `DETACH DELETE` ×0.46–0.58 |
| Commit path | Branch commits use the branch's own commit lock and clock, not main's; existence/type constraints are checked on the txn's touched vertices; the branch WAL is written and flushed with main's WAL settings | Txn size; WAL sync policy | Not yet measured with the branch WAL |
| Reading main@fork after main kept writing | An object main changed after the fork is read by walking main's undo chain back to the fork; main's post-fork index entries are skipped | Main's writes to the read objects since the fork (main write rate × branch age) | Not yet measured (all runs above had main idle) |
| Planning | Cardinality estimates come from the branch's own engine (branch-created objects only), not main@fork, so a plan can differ from main's for the same query. Results are unaffected | Gap between branch-created data and main@fork | Not yet measured |
| Several sessions on one branch | Sessions share one branch core; each query is single-threaded (§6); writers conflict only on the same object | Session count | Correctness only (TSan, stress); throughput not yet measured |
| Restart | Main replays its WAL from the oldest fork point as MVCC transactions, then each branch replays its whole WAL (§8) | Main's WAL since the oldest fork; branch write history (no compaction in v1) | Not yet measured |
| Main's own queries while branches exist | Retained history costs memory and GC work (above); main's readers start from the newest version and do not walk it. Each built branch runs its own GC on main's interval. Vertex/edge flag writes are atomic read-modify-writes so the hint bit can be read without a lock | Main write rate × age of the oldest branch; branch count | Not yet measured with live branches. The CI benchmark job (no branches) passed on `87cddc09d`; not compared against master |

## 8. Durability and recovery

Every branch writes its own WAL; changes are durable before visible. A branch survives clean
restarts and crashes with its fork point and contents intact. Sessions must select it again after
a restart.

Nothing branch-related can stop the database from starting. A branch that cannot be rebuilt
correctly is **dropped** with a warning naming `Graph Versioning`; its files are kept for
inspection where noted. Conditions, checked in order:

| Condition at startup | Result |
|---|---|
| Branch registry unreadable or corrupt | All branches dropped |
| WAL off, or recovery on startup off | All branches dropped (main's writes from this run would be missing from the history the branches are rebuilt from) |
| Database starts in a non-transactional mode | All branches dropped |
| Started without `--experimental-enabled=versioning` | Branches are hidden only: not visible or usable, branch queries fail with the disabled-flag error. All durable state stays (ledger, each branch's directory and WAL). Restarting with the flag on restores every branch with its data. |
| Started with `--experimental-enabled=commit-lock-narrowing` | All branches dropped durably |
| Main cannot be replayed to the fork points | All branches dropped; main recovers normally |
| A branch's WAL is damaged | Main's WAL rules apply (unreadable or empty file skipped, unfinished file used up to its last complete transaction). Where main would refuse to start (e.g. a gap in the sequence), only that branch is dropped and its files are kept. |
| Main itself cannot be recovered | Standard Memgraph behaviour |

**Flag off while branches exist.** The ledger's fork points still bound main's snapshot and WAL
retention, so main keeps a snapshot and its WAL files back to the oldest fork point even with the
flag off; otherwise the branches could not be rebuilt when the flag is turned back on. Runtime GC
fork pins are not held while the flag is off (nothing reads the branches), so main's memory is not
retained. Main's clock and vertex/edge id counters still move past every recorded fork point and
branch-created id, so writes made while the flag is off never land in a branch's fork base or reuse
a branch's ids. The main-side refusals of §7 still apply while the ledger holds branches.

Branch WALs are not compacted in v1, so a long-lived branch's restart time grows with its write
history.

**Format.** The feature adds a WAL record type and bumps the durable format version shared by
WAL and snapshot files, for all instances. Upgrade replicas before MAIN. Downgrade after the new
version has written files is not supported.

## 9. Replication and HA (v1)

Branches are single-instance in v1. Main's writes replicate as usual; branch writes do not enter
the replication stream.

- Every branch query fails on a REPLICA. A promoted replica starts with no branches.
- Branches are dropped durably, including those sessions are on (§5.3), when the instance is
  demoted to REPLICA (before the role changes; if the drop fails, the demotion fails), when a
  replica resyncs a database, when an instance restarts as REPLICA, and when a COLD database is
  resumed on a REPLICA.
- Failover loses branches: the new MAIN has none and the old MAIN drops its own when it rejoins.

Replication is deferred for scheduling, not architecture: commit timestamps are identical on MAIN
and replicas, so a fork point means the same on both. It needs branch create/drop as system
transactions (like `CREATE DATABASE`), a fork pin and retention floor on replicas, branch WAL
streaming, and resync that ships branch files **plus main's WAL files back to the oldest fork
point**. Replicas would then serve `SHOW BRANCHES`, `USE BRANCH` and `USING BRANCH` for reads.

## 10. Licensing and flag

| Build / license | Branch behaviour |
|---|---|
| Community build | No versioning logic is compiled in (all versioning code is under `#ifdef MG_ENTERPRISE`). Every branch query fails with `Query is part of the Enterprise feature. In order to run it, you need an Enterprise license.` |
| Enterprise build, no valid license | Read-only and droppable. `CREATE BRANCH` and writes on a branch are refused. `SHOW BRANCH[ES]`, `USE BRANCH` / `USING BRANCH` for reads, switching back to `main`, and `DROP BRANCH [FORCE]` work, so a session is never stuck on a branch and main's retention can be released. Startup restores branches. |
| Enterprise build, valid license | Everything in this spec |

**Flag.** `--experimental-enabled=versioning` is startup-only. Restarting without it hides all
branches but keeps their durable state; restarting with it restores them (§8).

## 11. Observability

**Metrics** (Prometheus; also in `SHOW METRICS INFO`; totals across databases, no per-database
label):

| Metric | `SHOW METRICS INFO` name | Type |
|---|---|---|
| `memgraph_versioning_branches_created_total` | `VersioningBranchesCreated` | counter |
| `memgraph_versioning_branches_dropped_total` | `VersioningBranchesDropped` | counter |
| `memgraph_versioning_active_branches` | `VersioningActiveBranches` | gauge |
| `memgraph_versioning_active_sessions` | `VersioningActiveSessions` | gauge (sessions on a branch) |

**Logs.** Every automatic drop (startup, demotion, resync, REPLICA restart or resume) and every
branch directory that cannot be removed is logged as a warning naming `Graph Versioning`; a failed
automatic drop is an error starting with `Failed to drop the branches of database <db>`.

Not exposed in v1: per-branch memory and disk (branch memory shares the database's allocator), and how
much of main's history is retained because of branches.

## 12. Not in v1

Everything below is intended to be supported later; the last column says what it needs. Text
search, vector search and unique constraints are required long-term (agent workloads), even if
they need refactoring. Triggers, TTL and streams are the only features not planned for branches:
they stay main-only (§6).

| Feature | In v1 | What it needs |
|---|---|---|
| Merge a branch into `main` | Not in v1 | A design for conflicts with main's concurrent writes, schema drift, triggers, replication and audit. `BRANCH` is non-reserved, so adding `MERGE BRANCH` is backward compatible. |
| Diff / log / revert of a branch | Not in v1 | Implementation only |
| Nested branches (fork a branch) | Not in v1 | A third visibility layer: a child's base is parent-engine@ts ⊕ parent-overlay@ts ⊕ main@fork, so it needs historical reads on the branch engine and overlay, GC and WAL pins on the parent, ordered recovery and 3-layer scans |
| Fork at a past point in time | Not in v1 | A retention window on `main`: today main keeps no history older than the oldest open transaction or fork pin |
| Replication / HA | Not in v1 | §9 |
| Analytical or on-disk storage | Not in v1 | A fork-point read source: analytical mode writes no undo history, and on-disk has no in-memory MVCC history |
| Index or constraint created on a branch | Not in v1 | A branch-side index type: fork data lives in main's storage and indexes, and a branch-private index there would be maintained by main's write path and seen by main's planner. Workaround: create the index on `main` (allowed) |
| `ANALYZE GRAPH` on a branch | Not in v1 | Verifying the branch planner's use of stats stored in the branch engine |
| Unique constraints enforced on a branch | Not in v1 (required) | A check against main@fork plus the branch's changes under concurrent branch sessions |
| Text, vector search on a branch | Not in v1 (required) | Branch-aware versions of these indexes. They live outside the MVCC store and reflect only main's current state, so answering "as of the fork plus the branch's changes" needs a fork-point view of main's index plus a branch-side index over the branch's changes. Until then the query errors instead of returning a stale answer |
| Point index on a branch | Not in v1 (full-scan fallback, correct results) | Implementation only |
| Parallel execution on a branch | Not in v1 | Making the branch scans (fork stream ⊕ overlay ⊕ branch-created objects) splittable into chunks |
| Branch WAL compaction (branch snapshots) | Not in v1 | Implementation only |
| Caps on branch count, size, age | Not in v1 | Operating experience to pick bounds |
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
//   name: "main",          created_at: null,                               sessions: 1, current: true
//   name: "supplier_fail", created_at: "2026-10-29T09:15:02.418305+0000", sessions: 0, current: false

DROP BRANCH supplier_fail;
```

A pooled application uses `driver.session(database="shop@supplier_fail")` instead of
`USE BRANCH` (§4.3).

---

## Architecture (for reviewers)

**Fork pin.** `CREATE BRANCH` records `fork_ts` = main's commit timestamp under `engine_lock_`;
in-flight main commits get timestamps ≥ `fork_ts`, so the cut is consistent. The pin clamps
main's GC horizon to the oldest pin. Base reads use a read-only historical accessor on main that
takes only main's shared storage lock and always reads snapshot-isolated at `fork_ts`.

**Branch core** (one per branch, shared by its sessions, built on first use):

- *Diff engine:* a private `InMemoryStorage` (config copied from main, own GC, not snapshotted in v1)
  sharing main's name-id mapper and allocator. It holds **branch-native** objects (gid ≥ the gid
  floor, i.e. main's counter value at first build; gids come from main's counters).
- *Overlay MVCC* (`VertexOverlayStore`): changes to **fork-era** objects are per-object MVCC
  chains of full-state versions on the diff engine's clock, so SI/RC/RU, rollback and
  `SERIALIZATION_ERROR` between branch sessions behave as on main. Own GC with snapshot pins.
- *Anchor stubs:* empty vertices in the diff engine where branch edges attach to fork vertices.
- *`branched()` hint bit* on main objects a branch touched: the only write to main's objects, so
  untouched objects skip the overlay lookup.
- *Visibility* = main@fork ⊕ overlay ⊕ native objects. Scans stream main@fork from main's index and
  resolve only gids in the branch's touched sets (insert-only, conservative), then merge the diff
  engine's index.
- Lock order: main shared storage guard → diff engine. One transaction per session.

**Durability.** `<storage_dir>/branches/ledger` is the registry: one file replaced atomically
(temp, fsync, rename, dir fsync) with a `MGBL <ver> <size> <crc32>` header and JSON body. It is
written on create, drop and the first gid-floor record; a failed write fails the query and changes
nothing. `<storage_dir>/branches/<id>/wal/` holds each branch's WAL: native changes as ordinary
deltas, overlay changes as `DELTA_OVERLAY_PUT` records in the same WAL transaction. A
`VersionStore` per database publishes `oldest recorded fork_ts − 1` as main's snapshot/WAL
retention floor.

**Recovery bound.** At startup the ledger is read before main's storage exists. Main is recovered
only up to `min(fork_ts) − 1`; the remaining WAL is replayed as real MVCC transactions, re-registering
each fork pin just before the first transaction with timestamp ≥ its `fork_ts`. Main's clock is then
bumped to `max(last durable ts + 1, newest fork_ts)` so no future main commit can land below a
fork point. Each branch core is rebuilt with its recorded floors, its WAL is replayed with main's
WAL rules, and main's gid counters are raised above every recovered native gid. If bounded recovery
fails, pins are released, main is recovered unbounded, and all branches are dropped (§8).
