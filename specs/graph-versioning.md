# Graph Versioning

**Status:** Experimental — Enterprise (runtime license check), off by default
**Author:** Andreja Tonev (https://github.com/andrejtonev)
**Reviewers:** Colin Barry (code), Josip Mrđen (outside)
**Last updated:** 2026-10-28

> Fork a database into named, writable **what-if branches** inside the same Memgraph
> instance. Branches are views over the database, not copies. Run queries and writes on a branch
> in isolation while the main graph keeps serving reads and writes, then throw the branch
> away. A branch never changes the main graph.

> **Revision note (2026-10-28).** This spec was rewritten to describe v1 as implemented.
> The earlier draft (2026-07-09) proposed a different design: a per-query change-log replayed
> onto `main`, a conflict-checked `MERGE BRANCH`, a dedicated `BRANCH` privilege, nested
> branches, a branch size/age cap, a mandatory WAL, and diff/revert/graph-view commands. None of
> that ships in v1. v1 is **discard-only**. Branches are persistent overlays backed by a private
> storage engine. Merge and the other items are listed only as future work (§12).

---

## 1. Motivation

Users often want to change a graph's data tentatively. They want to model a "what if"
scenario, try out an enrichment, or let an AI agent propose edits, and look at the result
before deciding anything. Today they have two options, and neither fits:

- **A copy of the database.** This costs a full second copy in RAM and load time, and the copy
  starts drifting from production as soon as it is made.
- **A single `BEGIN … ROLLBACK` transaction.** It cannot outlive one session, cannot be shared
  with a colleague or a second agent, and does not survive a restart.

Graph versioning fills the gap with a **branch**: a named, isolated, writable version of a
database, forked from the live graph at a point in time. A branch shares all unchanged data
with the main graph. It survives restarts, can be used by several sessions at once, and is
discarded when no longer needed.

The v1 use case is **what-if analysis**. An analyst, a data scientist or an agent forks
production, applies a scenario ("this supplier fails", "this route is blocked", "these
accounts churn"), runs the same analytical queries they run on production, and compares the
answers. Scenarios that need different assumptions get separate branches. Production is never
touched.

Promoting a branch's changes back into production (merge) is **not** part of v1 (§7 D1).

---

## 2. Core principle

> **A branch is a writable overlay on the main graph as it was at the moment the branch was
> created. Nothing done on a branch ever changes the main graph, and the main graph stays
> fully readable and writable while branches exist.**

When a branch is created it records a **fork point**, which is a moment in main's commit
history. From then on the branch sees:

- main's data **exactly as of the fork point**, and none of main's later commits, plus
- the branch's own changes.

Main continues to accept writes. Those writes are invisible to every branch that was forked
earlier, and branch writes are invisible to main and to every other branch. Main and branch
writes never conflict with each other.

The rest of the design follows from this principle. Branches need multi-version storage, so
they are available only in `IN_MEMORY_TRANSACTIONAL` mode. Main must keep its history back to
the oldest fork point, which costs memory and disk (§4.5). Anything that would change main's
schema or replace main's storage is unavailable on a branch, and some of it is blocked on
main while branches exist.

---

## 3. Concepts and terminology

- **Branch (version).** A named, writable, isolated version of one database. The query
  surface uses `BRANCH`. Messages and result columns use "version", for example
  `Switched to version 'b'.`.
- **`main`.** The database's own graph, i.e. production data. It is always **version
  number 1** and is never created, dropped or forked. `main` is a reserved name.
- **Fork point.** The point in main's commit history at which a branch was created. The branch
  reads main as of this point forever, including after restarts. v1 does not expose it.
- **Branch number.** Every branch gets a number when it is created (2, 3, …). Numbers are
  monotonic per database and **never reused**, even after a branch is dropped.
- **Checkout.** Attaching *your session* to a branch, so that all later queries in that session
  read and write the branch. Checkout is per-connection state (§4.3).
- **Discard.** `DROP BRANCH` removes a branch and all its changes. In v1 this is the only way a
  branch ends.

Branches are **per database**. In a multi-tenant instance, each database has its own set of
branches, and a branch belongs to exactly one database.

---

## 4. User-facing surface

### 4.1 Prerequisites

A versioning query is accepted only when all of the following hold. The checks run in
this order:

1. **Startup flag.** `--versioning-enabled=true`. The default is `false` and the flag is
   startup-only.
2. **Enterprise license.** A valid Memgraph Enterprise license. The check happens at
   runtime. The feature is compiled into every build, so a community build without a license
   gets the license error.
3. **Storage mode.** The current database runs in `IN_MEMORY_TRANSACTIONAL`.

In addition:

- **Privilege.** Every versioning query (`CREATE`, `CHECKOUT`, `DROP`, `SHOW BRANCH[ES]`)
  requires **`MULTI_DATABASE_EDIT`**. This is the privilege that governs `CREATE`/`DROP
  DATABASE`. v1 has no per-branch privilege or ownership (§7 D4).
- **MAIN instance only.** Every versioning query, including `SHOW`, is rejected on a
  REPLICA. A REPLICA holds no branches (§8).
- **Autocommit only.** Branch management queries cannot run inside an explicit
  `BEGIN … COMMIT` transaction, in the same way as multi-database management queries.

### 4.2 Queries

v1 adds the queries `CREATE BRANCH`, `CHECKOUT BRANCH`, `SHOW BRANCH`, `SHOW BRANCHES`,
`DROP BRANCH` and the `USING VERSION` directive. `MERGE BRANCH` is reserved. §5 Query reference
gives each query's exact syntax, meaning, gate, where it may run, and errors. A worked example is in
§5.8.

`BRANCH`, `BRANCHES` and `CHECKOUT` are new keywords. They can still be used as identifiers.

**Not exposed in v1:** the fork point (timestamp or age), a branch's size, and any diff or log of
a branch's changes. To compare a branch with main, run the same query on both (`USING VERSION
'main'`, §5.6) and compare the results on the client.

### 4.3 Sessions and checkout

- Checkout is **per session (connection)**. If several checkouts are issued, the last one wins.
- **Many sessions can share one branch.** They see each other's committed writes and get
  normal MVCC behaviour between them: snapshot reads, and a serialization error when two
  open transactions write the same object, as on main.
- Explicit transactions (`BEGIN … COMMIT`/`ROLLBACK`) and periodic commit work on a branch.
  A rollback undoes all of the transaction's branch changes.
- `USE DATABASE` to a different database moves the session to that database's `main`.
  Re-selecting the current database keeps the checkout.
- When the session ends, its checkout is released.
- A branch's in-memory structures are built on the **first checkout**, not at `CREATE`.
  Creating a branch is cheap. The first checkout costs more.
- If a session's branch is dropped by another path (for example, when the instance is demoted to
  REPLICA, §8), the next Cypher query on that session fails once with an error and moves the session
  back to `main` (§4.6). Running `CHECKOUT BRANCH` selects a new version. Until then, `SHOW BRANCH` on
  that session returns `Branch management operations forbidden on REPLICA! This operation must be
  executed on the MAIN instance.` while the instance is a REPLICA. After the instance is promoted
  back to MAIN, it fails with `The checked-out version '<name>' no longer exists. Use CHECKOUT
  BRANCH to select another version.`

### 4.4 What works on a branch

| Area | On a branch |
|---|---|
| Reads: `MATCH`, traversals, shortest paths, aggregations, subqueries, hops limit | Yes |
| Create / delete / `DETACH DELETE` vertices | Yes. A plain `DELETE` of a vertex with edges fails, as on main. |
| Add/remove labels, set/remove properties, map replace (on pre-existing and branch-created objects) | Yes |
| Create edges (including between pre-existing vertices), delete edges, set/remove edge properties | Yes |
| Index-backed scans: label, label+property (composite, nested, range), edge-type, edge-type+property, global vertex/edge property | Yes. Scans use main's indexes as of the fork point, combined with the branch's changes. |
| Point queries (`point.distance`, `point.withinbbox`) | Yes, with correct results, but without the point index. The planner falls back to a full scan plus filter. |
| Text search, vector search | **No.** Rejected. |
| Parallel execution | **No.** Not yet implemented. |
| Existence and type constraints | Enforced when a branch transaction commits, against main's **current** constraint set |
| Unique constraints | **Not enforced** on a branch (§11) |
| Enums | Main's enums as of the first checkout can be used in values |
| Query modules / procedures | Yes. They run on the branch, and nested queries from a procedure stay on the caller's branch. Approximate counts reflect main at the fork. |
| Triggers | Branch writes fire no triggers |
| TTL, streams | Run only on main. They never act on a branch. |
| `DUMP DATABASE` | The branch's data, with main's schema |
| `CREATE SNAPSHOT` | Allowed. It snapshots **main**. |
| Index / constraint / enum / TTL / trigger / stream / description DDL, `ANALYZE GRAPH`, `DROP GRAPH`, `STORAGE MODE`, `RECOVER SNAPSHOT`, `EDGE IMPORT MODE`, `schema.assert` and schema-changing procedures | **No.** Rejected while checked out. |
| `SHOW SCHEMA INFO` | **No.** Rejected. |
| `SHOW INDEX INFO` | Lists main's indexes |
| `SHOW CONSTRAINT INFO` | Main's constraints. Unique constraints are marked `unique (not enforced on branch)`. |
| Label / edge-type listings | Main's ∪ the branch's |
| `SHOW STORAGE INFO` | Vertex count, edge count and average degree are `NULL`. Exact counts would need a full scan. Extra `branch` and `branch_note` rows are shown. |

Branch-created vertices and edges get ids (`id(n)`) from the same counters as main, so an id is
unique across main and all of the database's branches.

### 4.5 Effects on main while branches exist

Main stays fully readable and writable. Creating branches has these effects on it:

- **Blocked on main while any branch of the database exists:** `STORAGE MODE`, `DROP GRAPH`,
  `RECOVER SNAPSHOT`, `CREATE ENUM`, `ALTER ENUM`. Each of these would change or replace the base
  that the branches read. The error message ends with "Drop all branches first."
- **Memory retention.** A branch pins main's history at its fork point. Main's garbage collector
  cannot reclaim old versions newer than the **oldest** live fork point. As long as a branch
  exists, main's memory grows with **main's own write churn** since that fork, independently of
  activity on the branch. When an older branch is dropped while a newer one still exists, little
  memory comes back. Most of the retained history is freed only after the newest overlapping
  branch is also dropped.
- **Disk retention.** Main's snapshot and WAL retention keeps the files needed to rebuild the
  oldest fork point. Disk usage can therefore exceed the configured retention while branches
  exist. Normal cleanup resumes after the last branch is dropped.
- **No cap.** v1 enforces no limit on branch count, size or age. The operator is responsible for
  dropping branches (§7 D5).
- **Exclusive operations wait.** A branch transaction holds main's storage in shared mode while
  it is open, the same as a main transaction. Operations on main that need exclusive storage
  access wait for open branch transactions to finish. Idle branch sessions do not block anything.

### 4.6 Gate and restriction errors

The errors for each query are listed in §5. The errors below are shared by many queries.

| Situation | Message |
|---|---|
| Flag off | `Versioning is disabled. Start Memgraph with --versioning-enabled=true to use branch queries.` |
| No valid license | `Graph versioning is an enterprise feature. A valid Memgraph Enterprise License is required.` |
| Wrong storage mode | `Graph versioning requires IN_MEMORY_TRANSACTIONAL storage mode. It is not available in IN_MEMORY_ANALYTICAL or ON_DISK_TRANSACTIONAL.` |
| On a REPLICA | `Branch management operations forbidden on REPLICA! This operation must be executed on the MAIN instance.` |
| `CREATE` / `CHECKOUT` while the instance is being demoted to REPLICA | `Graph Versioning branches are not available on a REPLICA.` |
| A Cypher query on a session whose branch was dropped under it | `Branch '<name>' no longer exists (it was dropped, or this instance became a REPLICA); the session is back on main.` The session is now on `main`. |
| Inside `BEGIN` | the standard multicommand-transaction error for "Managing graph versioning branches" |
| DDL / storage-global op on a branch | `Schema-plane and storage-global operations (indexes, constraints, enums, TTL, ANALYZE GRAPH, DROP GRAPH, STORAGE MODE, RECOVER SNAPSHOT, TRIGGER, STREAM, DESCRIPTION) are not allowed while a versioning branch is checked out. Run CHECKOUT BRANCH 'main' first.` |
| Text / vector search on a branch | The message is prefixed with the procedure name, for example `text_search.search: Text search is not supported on a checked-out branch.` / `vector_search.search: Vector search is not supported on a checked-out branch.` |
| Parallel scan on a branch | `Parallel (chunked) vertex scans on a versioned branch is not implemented yet.` (`edge scans` for edges) |
| `SHOW SCHEMA INFO` on a branch | `SHOW SCHEMA INFO is not supported on a versioning branch: … Run CHECKOUT BRANCH 'main' first.` |
| Blocked op on main | for example, `Cannot switch storage mode while versioning branches exist: it would strand their fork-point history. Drop all branches first.` |

---

## 5. Query reference

This section lists every versioning query. Other sections refer here and do not repeat the syntax.

**Common rules for the branch management queries** (`CREATE`, `CHECKOUT`, `SHOW BRANCH[ES]`,
`DROP`, `MERGE`):

- **Gate.** `--versioning-enabled=true`, then a valid enterprise license, then
  `IN_MEMORY_TRANSACTIONAL` for the current database. Checks run in that order and the first
  failing check is reported (messages in §4.6).
- **Privilege.** `MULTI_DATABASE_EDIT`.
- **Where.** The query may run with the session on `main` or on a branch. It is rejected inside
  an explicit `BEGIN … COMMIT` transaction. It is rejected on a REPLICA, including `SHOW`.

`<name>` is a symbolic name or a string literal. `<text>` is a string literal.

**Branch name rules.** A name must be non-empty, must not be `main`, must not start with `.`, and
must not contain `/` or `\`. Names are unique among a database's live branches. A dropped
branch's name can be used again, and the new branch gets a new number. Numbers are never reused.

**Name errors.** These apply to `CREATE` and `CHECKOUT … FROM`:

- `Branch name must not be empty.`
- `'main' is a reserved branch name and cannot be created, checked out, or dropped explicitly.`
  (`DROP BRANCH main` does not return this message. It returns `Version 'main' does not exist.`,
  §5.5.)
- `Branch name must not start with '.'.`
- `Branch name must not contain '/' or '\'.`
- `Branch 'b' already exists.`

### 5.1 `CREATE BRANCH`

```
CREATE BRANCH <name> [WITH DESCRIPTION <text>] FROM <name>
```

- **Meaning.** Creates a branch of the current database, forked at main's latest commit. The
  session does **not** switch to it.
- **Gate / privilege / where.** As in the common rules above. Issued from a branch, it still
  forks from main.
- **Result.** One row with the columns `version, number, description, parent`. When main has no
  WAL, the result also carries a warning notification with the code `BranchNotDurable`:
  `Branch '<name>' is not durable: the WAL is disabled, so it will be dropped when Memgraph
  restarts.` The branch is still created (§7 D6).
- **Errors.**
  - Name errors (above).
  - A parent other than `main`: `CREATE BRANCH FROM a non-main branch (nested branches) is not
    supported in this version; branch only from 'main'.`
  - A failed registry write: `Cannot persist the branch registry: …`. Nothing is created.
  - While the instance is being demoted to REPLICA: `Graph Versioning branches are not available
    on a REPLICA.`

### 5.2 `CHECKOUT BRANCH`

```
CHECKOUT BRANCH <name>
CHECKOUT BRANCH <name> [WITH DESCRIPTION <text>] FROM <name>
```

- **Meaning.**
  - Without `FROM`: switches this session to an existing branch, or back with `CHECKOUT BRANCH
    main`.
  - With `FROM`: creates the branch exactly as in `CREATE BRANCH` and switches to it.
- **Gate / privilege / where.** As in the common rules above. The checkout lasts until the next
  `CHECKOUT`, a `USE DATABASE` to another database, the end of the session, or until the branch is
  dropped under the session. The last case happens when the instance is demoted to REPLICA (§8).
  The session's next Cypher query then fails with `Branch '<name>' no longer exists (it was
  dropped, or this instance became a REPLICA); the session is back on main.` and the session is on
  `main` from then on.
- **Result.**
  - Without `FROM`: `STATUS` = `Switched to version '<name>'.`
  - With `FROM`: the same row as `CREATE BRANCH`, including the `BranchNotDurable` notification
    when main has no WAL.
- **Errors.**
  - Unknown target: `Version '<name>' does not exist.` The session stays where it was.
  - If the branch is dropped concurrently, or its in-memory state cannot be built, the session
    ends up on `main` and the error is returned (for a concurrent drop, for example, `Branch was
    dropped.`).
  - While the instance is being demoted to REPLICA: `Graph Versioning branches are not available
    on a REPLICA.`
  - With `FROM`: the `CREATE BRANCH` errors.

### 5.3 `SHOW BRANCH`

```
SHOW BRANCH
```

- **Meaning.** Shows this session's current version.
- **Gate / privilege / where.** As in the common rules above.
- **Result.** One row with the columns `version, number, description, created_at`. `main` is
  number 1, and its `description` and `created_at` are null. `created_at` is the branch's creation
  time as a string, formatted like `creation_time` in `SHOW SNAPSHOTS` (ISO 8601 with microseconds
  and the UTC offset, in the instance's timezone). It is null for a branch recorded before
  Memgraph stored the creation time.
- **Errors.** If the session's branch was dropped: `The checked-out version '<name>' no longer
  exists. Use CHECKOUT BRANCH to select another version.`

### 5.4 `SHOW BRANCHES [FOR DATABASE]`

```
SHOW BRANCHES
SHOW BRANCHES FOR DATABASE <database>
```

- **Meaning.** Lists `main` and every branch of the current database, or of `<database>`, ordered
  by number.
- **Gate / privilege / where.** As in the common rules above. With `FOR DATABASE`, the storage
  mode check applies to the **target** database. `FOR DATABASE` also requires a multi-tenant
  enterprise build.
- **Result.** One row per version, with the columns `number, version, description, created_at`.
  The columns mean the same as in `SHOW BRANCH`. The `main` row has null `description` and
  `created_at`.
- **Errors.**
  - Community build with `FOR DATABASE`: `SHOW BRANCHES FOR DATABASE requires an enterprise
    multi-tenant build.`
  - Unknown target database: the standard "database does not exist" error.

### 5.5 `DROP BRANCH`

```
DROP BRANCH <name>
```

- **Meaning.** Discards the branch: all its changes, its files on disk, and its fork pin. If this
  session is on the branch, the session is moved to `main` first.
- **Gate / privilege / where.** As in the common rules above.
- **Result.** No rows.
- **Errors.**
  - Unknown, including `main`: `Version '<name>' does not exist.`
  - Another session is on it: `Cannot drop '<name>': it is checked out by another session. Check
    out 'main' there first.`
  - Concurrent race: `Version '<name>' could not be dropped (it no longer exists or is checked out
    by another session). Retry DROP BRANCH.`
  - A failed registry write: `Cannot persist the branch registry: …`. The branch is kept.

### 5.6 `USING VERSION`

```
USING VERSION <literal> <cypher query>
```

This is a pre-query directive. It can be combined with other `USING` directives, separated by
commas.

- **Meaning.** Routes this one query to `main` while the session stays checked out on a branch.
  The session's checkout is not changed.
- **Gate / privilege.** None beyond the wrapped query's own privileges. The directive is not a
  management query.
- **Where.**
  - `'main'` is accepted with the session on `main` or on a branch.
  - The name of the session's current branch is accepted and has no effect.
  - Inside `BEGIN`, the directive is rejected.
  - Inside `EXPLAIN`/`PROFILE`, it is ignored and the query uses the session's checkout.
- **Errors.**
  - Any other target: `Not yet implemented: USING VERSION for a branch other than 'main' or the
    currently checked-out branch is not yet supported`.
  - Inside `BEGIN`: `Not yet implemented: USING VERSION inside an explicit (BEGIN) transaction is
    not yet supported`.

### 5.7 `MERGE BRANCH` (parsed, always rejected)

```
MERGE BRANCH <name>
```

- **Meaning.** None in v1. The keyword is reserved for future work (§12).
- **Gate / privilege / where.** As in the common rules above. Those checks run first.
- **Errors.** Always `MERGE BRANCH is not supported: branches are discard-only.`

### 5.8 Worked example: a what-if session

The example assumes the server was started with `--versioning-enabled=true`, has a valid license
and runs in `IN_MEMORY_TRANSACTIONAL`, and that the user has `MULTI_DATABASE_EDIT`. It uses one
connection throughout.

```cypher
// 1. Fork production. The session stays on main.
CREATE BRANCH supplier_fail WITH DESCRIPTION 'Supplier S1 stops shipping' FROM main;
//   version: "supplier_fail", number: 2, description: "Supplier S1 stops shipping", parent: "main"
//   (2 if this is the first branch ever created in this database)
//   Without a WAL, the result also carries the BranchNotDurable notification (§5.1).

// 2. Switch this session to the branch.
CHECKOUT BRANCH supplier_fail;
//   STATUS: "Switched to version 'supplier_fail'."
SHOW BRANCH;
//   version: "supplier_fail", number: 2, description: "Supplier S1 stops shipping",
//   created_at: "2026-10-28T09:15:02.418305+0000"

// 3. Apply the scenario. These writes go to the branch only.
MATCH (:Supplier {id: 'S1'})-[r:SHIPS_TO]->() DELETE r;
MATCH (p:Plant) WHERE NOT exists((p)<-[:SHIPS_TO]-()) SET p.at_risk = true;

// 4. Compare the scenario with production without leaving the branch.
MATCH (p:Plant {at_risk: true}) RETURN count(p) AS at_risk;                        // branch
USING VERSION 'main' MATCH (p:Plant {at_risk: true}) RETURN count(p) AS at_risk;   // main

// 5. Main is untouched and still writable. Another session's writes on main stay invisible here.
CHECKOUT BRANCH main;
SHOW BRANCHES;
//   number: 1, version: "main",          description: null,                         created_at: null
//   number: 2, version: "supplier_fail", description: "Supplier S1 stops shipping", created_at: "2026-10-28T09:15:02.418305+0000"

// 6. Discard the scenario.
DROP BRANCH supplier_fail;
```

---

## 6. Lifecycle

```
                 CREATE BRANCH b FROM main           CHECKOUT BRANCH b
   (no branch) ─────────────────────────────▶ CREATED ─────────────────▶ IN USE
                                                 │  ▲                       │
                                                 │  └── last session leaves ┘
                                                 │      (CHECKOUT main / USE DATABASE / disconnect)
                                                 │
                                     DROP BRANCH b (no other session attached)
                                                 ▼
                                             DISCARDED  (data and files removed; number retired)
```

- **Create** records the branch durably and pins the fork point. The branch costs almost nothing
  until it is first checked out. It does start holding back main's garbage collection and file
  retention immediately (§4.5).
- **Use.** Any number of sessions can check out, read and write the branch. Branch changes are
  durable when main runs with a WAL (§7 D6). Without a WAL, `CREATE` warns with the
  `BranchNotDurable` notification.
- **Restart.** The branch comes back with the same fork point and contents (§7 D6). Sessions have
  to check it out again. If Memgraph restarts with `--versioning-enabled=false`, every recorded
  branch is dropped durably at startup instead, and branch numbering is kept.
- **Drop** removes the branch's in-memory state and its directory on disk, releases its fork
  pin, and lets main's retention catch up. If a session still holds the branch when it is dropped
  (a drop by demotion, §8), its directory is removed when that session lets go of it.

There is no merge transition. A branch ends only by being dropped, by being dropped automatically
at startup (feature disabled, or the branch cannot be rebuilt correctly, §7 D6), or by being dropped
when the instance becomes a REPLICA (§7 D7, §8).

---

## 7. Product decisions and rationale

### D1 — v1 is what-if only: branches are discard-only.

v1 ships create, checkout, isolated read/write, show and drop. There is no merge. `MERGE
BRANCH` parses but always returns an error.

*Rationale.* What-if analysis needs a branch that is isolated, durable, shared between sessions
and cheap to throw away. It never needs to write back. A correct merge is a separate large
problem: conflicts with main's concurrent writes, schema drift, trigger and replication
semantics, and an audit trail. Shipping isolation first, with a merge-free surface, gives the
analysis use case a sound foundation and does not commit v1 to merge semantics before they are
designed. The keyword is reserved so that adding merge later does not break the grammar.

### D2 — Live main; a branch is pinned to its fork point.

Main is never frozen. A branch reads main as of its fork point, and main's later commits are
invisible to it.

*Rationale.* Freezing main would turn every experiment into a production outage. Pinning keeps
production writable and gives each branch a stable base. The cost is retention: main must keep its
history back to the oldest live fork point, in memory and on disk (§4.5). The product accepts that
cost in exchange for zero-copy branches and a writable main.

### D3 — Flat branches, forked from `main` only.

`FROM main` is the only accepted parent.

*Rationale.* Nested branches multiply the retention and recovery problem: a branch's base would be
another branch's history. They also add little to discard-only what-if analysis. If a scenario
builds on another one, apply the shared setup on each branch.

### D4 — Access control: `MULTI_DATABASE_EDIT`, FGA applies, no per-branch ACL.

All versioning queries need `MULTI_DATABASE_EDIT`. Any holder can check out or drop any branch of a
database they can access. Fine-grained (label/property) access control applies to reads on a branch
exactly as it does on the database's main graph.

*Rationale.* Creating branches has instance-wide costs (memory and disk retention, §4.5) and blocks
some main operations. That makes it an administrative act of the same weight as creating a
database, so it reuses that privilege rather than adding a new one. Applying FGA on branch reads
keeps versioning from becoming a way around access control. Per-branch ownership and sharing are
future work.

### D5 — No size, age or count cap in v1.

v1 enforces no limit on how many branches exist, how large they grow, or how long they live.

*Rationale.* Choosing a correct bound needs operating experience that does not exist yet, and a bad
bound breaks legitimate long analyses. v1 makes the costs visible (§4.5, §9) and leaves the
policy to the operator. This is listed as a limitation (§11).

### D6 — Durability follows main's WAL setting, and a broken branch never blocks startup.

When main runs with a write-ahead log (`PERIODIC_SNAPSHOT_WITH_WAL`), every branch writes its own WAL
and survives clean restarts and crashes. Its fork point and contents are restored exactly. Without a
WAL, branches can still be created and used, but they are **in-memory only** and are dropped at the
next startup. `CREATE BRANCH` and `CHECKOUT BRANCH … FROM` then return a warning notification with
the code `BranchNotDurable`: `Branch '<name>' is not durable: the WAL is disabled, so it will be
dropped when Memgraph restarts.`

At startup, nothing versioning-specific can stop main from starting. A branch that cannot be rebuilt
correctly is **dropped** instead, and a warning is logged. The conditions are checked in the order
of the table:

| Condition at startup | Result |
|---|---|
| Branch registry file unreadable or corrupt | All branches dropped. Numbering continues past existing branch directories, so numbers are not reused. |
| Started with `--versioning-enabled=false` | All branches dropped durably, numbering kept. Warning: `Graph Versioning is disabled (--versioning-enabled=false); the <n> existing branches [<names>] are dropped.` |
| WAL off, or recovery-on-startup off | All branches dropped, numbering kept |
| Database starts in a non-transactional mode | The registry is durably emptied |
| Main cannot be replayed to the fork points | All branches dropped. Main is recovered normally, as if no branches existed. |
| A branch's WAL files are damaged | Handled with the **same rules main's WAL recovery uses** for damaged WAL files. An unreadable file, or one with no complete transaction, is skipped. An unfinished file is used up to its last complete transaction. Where main would refuse to start (for example, a missing file in the sequence), only **that branch** is dropped. Its files are left on disk for inspection, and main starts. |
| Main itself cannot be recovered | Standard Memgraph behaviour, the same as without versioning |

*Rationale.* A branch must survive a restart. Otherwise it is no better than an open transaction.
Branch durability needs no new configuration: it follows main's existing durability choice. A
branch is a derived, discardable artifact, so failing to rebuild one must never take production
down. Showing a branch on a wrong base would be worse than losing it, so the policy is to drop it
loudly and keep its files.

### D7 — Single instance; branches are not replicated, and a replica has none.

Branches exist only on the MAIN instance where they were created. A REPLICA never holds branches.
Every path that makes an instance a REPLICA, or that replaces a replica's data, drops the branches
durably first: demotion, a replica resync, and a restart as REPLICA. See §8.

*Rationale.* Branch replication would need the system-replication path used by `CREATE`/`DROP
DATABASE`, plus replica-side fork pins and recovery. That is a separate body of work. The product
framing is "many versions within one instance". A branch is forked from MAIN's own history. On a
replica that history is replaced by the new MAIN's, so a kept branch would sit on a base that no
longer exists. A replica's branch registry would also silently diverge from MAIN's.

### D8 — `IN_MEMORY_TRANSACTIONAL` only.

Versioning is rejected in `IN_MEMORY_ANALYTICAL` and `ON_DISK_TRANSACTIONAL`.

*Rationale.* A branch reads main at a past point in time, which requires the multi-version history
that only the in-memory transactional engine keeps. Analytical mode keeps no such history, and
on-disk mode is not a current priority. Switching main's storage mode is blocked while branches
exist for the same reason.

### D9 — The schema is main's; branches are data-only.

Indexes, constraints, enums, TTL, triggers, streams and descriptions belong to main and cannot be
changed on a branch. Some main-side operations that would invalidate the branches' base are
blocked while branches exist (§4.5).

*Rationale.* A branch is a data scenario on top of production's schema. Allowing schema changes on a
discard-only branch would mean previewing and rolling back global structures, such as index builds,
trigger registries and stream consumers, that are not designed to be per-version. Background jobs
(TTL, streams, triggers) act on production only. A what-if branch should not fire production
side-effects.

### D10 — Checkout is per-connection. v1 has no write-routing rail.

A session's checkout lives on its connection. Nothing forces every connection of an application onto
the same branch.

*Rationale.* Per-connection state is the established model (compare `USE DATABASE`) and needs no
driver changes. The consequence is that a pooled client can check out a branch on one connection and
then send a write over another connection that is still on `main`. That write lands in production.
Until a rail exists (for example, a per-transaction version target or a rejection of untargeted
writes), applications must pin one connection per branch session (§11).

### D11 — The base is called `main`, and the naming collisions are accepted.

The base version is `main`, following current git convention. The known collisions are kept and must
be disambiguated in the documentation:

- `main` (the base version) and **MAIN** (the replication role).
- `MERGE BRANCH` (reserved, always an error in v1) and Cypher's `MERGE` (upsert).
- `USING VERSION` / `SHOW BRANCH` and the existing `SHOW VERSION` (server build version).

### D12 — Text and vector search are not branch-aware.

Text and vector search queries are rejected on a branch, not answered from main's index.

*Rationale.* These indexes are maintained outside the multi-version store and cannot answer "as of the
fork point plus the branch's changes". A silently stale answer would be worse than an error.

### D13 — Unique constraints are not enforced on a branch.

Existence and type constraints are checked when a branch transaction commits, against main's
**current** constraint set. Unique constraints are not checked. `SHOW CONSTRAINT INFO` on a branch
says so.

*Rationale.* Enforcing uniqueness on a branch means checking a value against main-as-of-fork with
the branch's changes on top, under concurrent branch sessions. That is a substantial piece of work.
For what-if analysis, a temporary duplicate is acceptable if it is clearly signalled.

---

## 8. Replication and high availability

- **Branches are not replicated.** Branch writes go to the branch's own storage engine and never
  enter the replication stream. Main's writes replicate as usual while branches exist.
- **Replicas have no branches.** Every versioning query, including `SHOW BRANCHES`, is rejected on a
  REPLICA. Branches are never created there, so a replica promoted to MAIN starts with no branches.
- **Demotion drops branches.** When a MAIN is demoted to REPLICA (by `SET REPLICATION ROLE TO
  REPLICA` or by the coordinator), every database's branches are dropped durably **before** the role
  changes, including branches that sessions still have checked out. `CREATE BRANCH` and `CHECKOUT
  BRANCH` are refused from that moment (`Graph Versioning branches are not available on a
  REPLICA.`). If a drop fails (the branch registry cannot be written), the demotion fails, the
  instance stays MAIN, and its branches stay usable. Branch numbering is kept. A dropped branch's
  directory is removed with the drop, or, for a branch a session still has checked out, when that
  session lets go of it.
- **Sessions on a dropped branch.** A session still checked out on a branch dropped this way gets,
  on its next Cypher query, `Branch '<name>' no longer exists (it was dropped, or this instance
  became a REPLICA); the session is back on main.` The session is on `main` from then on.
- **Promotion allows branches again.** After a promotion to MAIN, branches can be created again. The
  new MAIN starts with no branches.
- **A replica resync drops branches.** Before a replica wipes its storage to load a snapshot or WAL
  files from MAIN, it drops any branches of that database. A failed drop is logged as an error and
  does not block the resync.
- **A restart as REPLICA drops branches.** Databases are recovered before the replication role is
  known, so a REPLICA restart first rebuilds its branches and then drops them durably. The warning is
  `Instance is a REPLICA; dropping its <n> Graph Versioning branches [<names>] in database <db>.` A
  failed drop is logged as an error and does not block startup.
- **Failover loses branches.** After a failover, the branches that existed on the old MAIN are not
  available on the new one, and the old MAIN drops them when it rejoins as a REPLICA. Treat branches
  as instance-local scratch state.
- **Main's replication history is preserved.** When an instance with branches restarts, main's
  epoch history is restored as usual.
- **Upgrade order.** This feature bumps the durable file format version shared by WAL and snapshot
  files (§13). Replicas must be upgraded before the MAIN. A replica on an older version cannot read
  the new files.

---

## 9. Observability

**Prometheus metrics.** These are global, not per database. They are also listed in `SHOW METRICS INFO`.

| Metric | `SHOW METRICS INFO` name | Type | Meaning |
|---|---|---|---|
| `memgraph_versioning_branches_created_total` | `VersioningBranchesCreated` | counter | Branches created |
| `memgraph_versioning_branches_dropped_total` | `VersioningBranchesDropped` | counter | Branches dropped |
| `memgraph_versioning_active_branches` | `VersioningActiveBranches` | gauge | Existing branches |
| `memgraph_versioning_active_checkouts` | `VersioningActiveCheckouts` | gauge | Sessions currently checked out on a branch |

With several databases, the two gauges are totals across all databases. The metrics have no
per-database label.

**Queries.** `SHOW BRANCHES [FOR DATABASE db]` lists branches. `SHOW BRANCH` shows the session's
version. On a branch, `SHOW STORAGE INFO` names the branch. Branch memory is accounted to the owning
database's memory usage.

**Logs.** Every automatic branch drop at startup, and every branch directory that cannot be removed,
is logged as a warning that names `Graph Versioning` (§7 D6, §8). A failed drop during a demotion, a
replica resync or a REPLICA startup is logged as an error that starts with `Failed to drop the
branches of database <db>`.

**Notifications.** `CREATE BRANCH` and `CHECKOUT BRANCH … FROM` return the `BranchNotDurable`
warning when main has no WAL (§5.1).

**Not exposed:** fork point or branch age, per-branch memory/disk, and the amount of main history
retained because of branches. No telemetry is sent.

---

## 10. Guarantees and non-goals

**Guarantees**

- Nothing done on a branch changes main's data (§2).
- Main stays fully readable and writable while branches exist. Main and branch writes never conflict.
- A branch sees main exactly as of its fork point, including after restarts. Main's commits after
  the fork never become visible to it.
- Branches are isolated from each other.
- Sessions sharing a branch get the same MVCC isolation and write-conflict behaviour as on main.
- Vertex and edge ids are unique across main and all branches of a database.
- With a WAL, a branch's contents survive clean restarts and crashes.
- A branch that cannot be rebuilt correctly is dropped loudly. It is never served on a wrong base,
  and it never prevents main from starting.
- A REPLICA holds no branches (§8).

**Non-goals for v1**

- Merging, rebasing, cherry-picking or resolving conflicts between a branch and main.
- A change history, diff, log or revert for a branch.
- Nested branches, or branching from a point in time other than "now".
- Replication or HA of branches.
- Analytical and on-disk storage modes.
- Per-branch ACLs or ownership.
- Schema changes and background jobs (TTL, streams, triggers) on a branch.

---

## 11. Known limitations

- **Unique constraints are not enforced on a branch** (D13). A branch can contain duplicates that
  main would reject.
- **Without a WAL, branches are lost at restart.** `CREATE BRANCH` succeeds without a WAL. It returns
  the `BranchNotDurable` notification (§5.1), but such branches are dropped at the next startup.
- **Pooled clients can send a query to main by mistake** (D10). Checkout is per connection. A pooled
  driver can check out a branch on one connection and run the next query on another connection that
  is still on `main`. There is no rail that rejects untargeted writes. Pin one connection per branch
  session.
- **The fork point is not shown.** Users cannot see how old or stale a branch's base is.
- **No cap, and costs grow with main's churn.** Main's retained memory grows with main's writes since
  the oldest fork point, and main's snapshot/WAL files are kept back to that point. Dropping an older
  branch frees little while a newer overlapping branch exists.
- **Branch WALs are never compacted.** A branch is never snapshotted, so a long-lived branch's WAL
  grows with every write until the branch is dropped, and restart time grows with it. Compaction
  needs snapshots of the branch's diff engine (§12).
- **Disabling the feature drops all branches.** Restarting with `--versioning-enabled=false` drops
  every recorded branch durably at startup. Turning the flag back on does not bring them back.
- **Branches survive a lost license.** Startup recovery does not consult the license. If Memgraph
  restarts without a valid license, existing branches are still rebuilt and keep pinning main's
  history, but no query can drop them. Drop branches before the license lapses.
- **Main-side operations blocked** while any branch exists: `STORAGE MODE`, `DROP GRAPH`,
  `RECOVER SNAPSHOT`, `CREATE`/`ALTER ENUM`.
- **Unsupported on a branch:** text and vector search, parallel execution, `SHOW SCHEMA INFO`, all
  schema DDL. Point queries work but do not use the point index (full scan plus filter).
  `USING VERSION` reaches only `main` or the current branch.
- **Read performance on heavily changed branches.** Scans over data the branch did not touch stream
  main's data directly. When about 90% of the scanned data was changed on the branch, reads have been
  measured at roughly 2.6–7× slower than the same read on main.
- **Demotion to REPLICA drops all branches** (§8). This includes branches that sessions still have
  checked out. They are not restored when the instance is promoted again.
- **No downgrade.** Once this version has written WAL or snapshot files, older Memgraph versions cannot
  read them (§13).

---

## 12. Future work (not designed here)

- `MERGE BRANCH`: write a branch's changes back to main, with conflict detection.
- Branch diff/log and per-commit revert. Comparing two branches.
- Nested branches. Arbitrary `USING VERSION` targets and time travel.
- A write-routing rail for pooled clients.
- Showing the fork point, branch age and size. Optional caps on count, size and age.
- Enforcing unique constraints on a branch. Branch-aware text, vector and point indexes. Parallel
  execution on a branch.
- Branch WAL compaction (snapshotting a branch).
- Per-database labelling of metrics. Per-branch ownership/ACLs.
- Replication and HA of branches.

---

## 13. Availability

Graph versioning is an **enterprise** feature. The code is in every build and is gated at runtime: it
requires a valid Memgraph Enterprise license, the startup-only flag `--versioning-enabled=true`
(default `false`), and an `IN_MEMORY_TRANSACTIONAL` database. `SHOW BRANCHES FOR DATABASE` also
requires a multi-tenant enterprise build. The feature is **experimental**.

**Upgrade and downgrade.** The feature adds a new WAL record type for branch changes. WAL and snapshot
files share one durable format version, and this version is bumped for **all** files, including
those of instances that never create a branch. Upgrade replicas before the MAIN. Downgrading to an
older Memgraph version after the new version has written files is not supported.

---
---

# Appendix A — Engineering overview

This appendix is for engineers. It summarises the implementation the spec describes. Code
references are to the development branch and are approximate.

### A.1 Fork pin and visibility

- `CREATE BRANCH` takes a **fork pin** at main's current commit timestamp `fork_ts`, under
  `engine_lock_` (`InMemoryStorage::RegisterBranchForkPin`). The branch's base is "commits with
  `ts < fork_ts`".
- Base reads go through a read-only **historical accessor** on main. It always uses snapshot isolation,
  whatever the database's isolation level, and refuses writes (`Transaction::is_historical_`). It takes
  only main's shared storage lock, so main stays writable.
- The fork pin clamps main's GC horizon to the oldest pin. Undo deltas are freed only when their mark
  falls behind the oldest pin, so with overlapping pins A < B, releasing A frees little until B goes.

### A.2 Branch core: diff engine and overlay

One `BranchCore` exists per branch and is shared by all its sessions. It is built lazily on the first
checkout.

- **Diff engine.** This is a private `InMemoryStorage`. Its config is copied from main, with a fresh
  UUID, metrics registration off, replication restore off, and never snapshotted. It shares main's
  `NameIdMapper`, arena pool and embedding-memory tracker, so branch memory is accounted to the
  tenant. It keeps main's database *name*, so auth, FGA and `DUMP` behave as on main. It has its own GC.
  Main's index definitions and enums are mirrored into it at build time, outside its WAL.
- **Overlay (`VertexOverlayStore`).** Changes to **fork objects** (gid below the floor) are stored as
  per-object MVCC chains of full-state versions, keyed by vertex gid or edge key, on the diff engine's
  clock. Write-write conflicts between branch sessions surface as `SERIALIZATION_ERROR`. Overlay
  versions are committed by the diff engine's commit callback, or, for overlay-only transactions,
  stamped under the diff engine's `engine_lock_`. The overlay has its own GC with a snapshot-pin
  registry and runs every 64 overlay transactions.
- **Branch-native objects** (gid at or above the floor) live in the diff engine as ordinary objects.
- **Anchor stubs.** When a branch-created edge attaches to a fork vertex, an empty, delta-less stub
  vertex is materialised in the diff engine. Stubs are owned by a sparse, page-keyed, sharded `StubIndex`.
- **`branched()` hint bit.** This is the only write to main's objects. It is set on a main vertex/edge
  that a branch touched, so branch reads can skip the overlay lookup for objects no branch has touched.
- **Visibility** = main@fork ⊕ overlay ⊕ native objects. Deleted fork vertices hide their incident
  fork edges.
- **Lock order:** main READ storage guard → diff engine. One transaction per session.

### A.3 Gid floors

Branch-native objects take gids from **main's** counters, so gids are unique across main and all
branches. The floor is main's counter value when the core is first built: gid < floor means a fork
object, gid ≥ floor means a native object. Floors are written to the ledger on the first checkout.
After branch recovery, main's counters are raised above every floor and every recovered native gid.

### A.4 Scans

Each label, property, edge-type, etc. scan streams main@fork from main's index and resolves only the
gids in the branch's **touched sets**, then merges in the diff engine's mirrored index. The touched
sets are lock-free per-kind bloom filters, monotonic "may have touched" gates, and exact insert-only
per-label/per-property gid sets with versioned snapshot caches. A scan on an untouched branch is a pure
fork stream. Touched sets never shrink while the branch lives, and rolled-back writes stay recorded.
This is a conservative over-approximation and is correct. Point, text, vector and chunked (parallel)
scans have no branch path. Point indexes are not mirrored into the diff engine, so on a branch the
planner sees no point index and plans a full scan plus filter. Edge-property scans on a branch stay
ordered even if main drops the index after the fork.

### A.5 Memory model

| Who pays | What grows | Grows with |
|---|---|---|
| Main | Retained undo deltas (GC horizon clamped to the oldest fork pin) | Main's write churn since the oldest fork |
| Main (disk) | Snapshot + WAL files kept back to `oldest recorded_fork_ts − 1` | Main's write volume since the oldest fork |
| Branch | Overlay versions, stubs, touched sets, diff-engine objects | Branch changes. Touched sets are insert-only. |
| Branch (disk) | Branch WAL files, never compacted | Full branch write history |

Branch memory goes to the tenant's arena and trackers. There is no cap.

### A.6 Durability layout and format

- `<storage_dir>/branches/ledger` is the registry. `<storage_dir>/branches/<number>/wal/` holds each
  branch's WAL. A legacy `versioning/` directory from earlier builds is deleted at startup.
- **Ledger.** A single file, replaced atomically (temp file, fsync, rename, directory fsync), with the
  header `MGBL <ver> <size> <crc32>` and a JSON body. A file with bad magic, size or CRC is rejected.
  Each branch entry has an optional `created_at_us` (wall-clock creation time, microseconds since the
  Unix epoch). Ledgers written before the field existed still read, and those branches show a null
  `created_at`.
  The ledger is written on `CREATE`, `DROP` and the first floor record. If the write fails, the query
  fails and in-memory state is unchanged. If only the directory fsync after the rename fails, this is
  logged and the operation counts as written.
- **Branch WAL.** It is written only if main runs `PERIODIC_SNAPSHOT_WITH_WAL`. Native-object changes
  are ordinary WAL deltas. Overlay changes are `DELTA_OVERLAY_PUT` records (key, deleted flag,
  added/removed labels, removed properties, properties, edge endpoints). They are appended in the same
  WAL transaction as the native deltas, or as a WAL-only transaction, and are **durable before
  visible**.
- **Format version.** WAL and snapshot files share one `kVersion`, bumped for this feature for all files.
  An overlay record in main's WAL is a recovery failure. On a replica's stream it is an apply error.
  Both are defensive, since branches never write to main's WAL.

### A.7 Startup recovery

1. Read the ledger before main's storage exists and sort branches by `fork_ts`. With
   `--versioning-enabled=false`, drop all branches (the ledger is rewritten empty, numbering kept).
   Replay is possible only with `IN_MEMORY_TRANSACTIONAL` + `recover_on_startup` + WAL. Otherwise,
   drop all branches.
2. **Bounded recovery.** Recover main only up to `min(fork_ts) − 1`. The remaining WAL files form the
   recovery tail.
3. **Tail replay.** Replay the tail as real MVCC transactions with WAL appends suppressed. Re-register
   each branch's fork pin just before the first transaction whose durable timestamp is ≥ its
   `fork_ts`. Record main's epoch history per file.
4. **Clock rule.** Bump main's clock to `max(last durable ts + 1, newest fork_ts)` and mark the skipped
   ids finished. This prevents any future main commit from receiving a timestamp below a branch's
   `fork_ts`, which would leak it into the branch's base.
5. Restore the registry with the new runtime pins. The ledger `fork_ts` is kept as `recorded_fork_ts`
   for retention.
6. **Branch contents.** For each branch, build the core with its recorded floors and replay its WAL
   using main's WAL selection and chain rules (§7 D6). The branch continues its WAL sequence numbers
   across restarts, as main does, so a file missing from any run is a gap. Then raise main's gid
   counters.
7. Failure policy: see the table in §7 D6. If bounded recovery or tail replay fails, pins are released,
   the partial storage is discarded without an exit snapshot, main is recovered unbounded, and all
   branches are dropped.
8. **REPLICA role.** Databases are recovered before the replication role is known. If the instance
   starts as a REPLICA, the replication handler then calls `VersionStore::FreezeAndDropAll` on every
   database, which drops the restored branches durably (§8).

### A.8 Retention floor

`VersionStore` publishes `oldest recorded_fork_ts − 1`, or nothing when no branches exist, to main's
snapshot/WAL retention. It republishes after restore, after a failed create and after a drop.
Retention never deletes the files needed to rebuild a fork point. Dropping the last branch lets cleanup
proceed.

### A.9 Registry and locking

`VersionStore` (one per IN_MEMORY_TRANSACTIONAL database) maps names to
`BranchInfo{number, parent, fork_ts, recorded_fork_ts, description, gid floors, created_at_us}`. Lock
order: `ledger_mutex_` → `lock_`, and separately `build_mutex_` → `cores_mutex_`. Checkout does not
take the ledger lock, except for the one-time floor write.

**Replica freeze.** `FreezeAndDropAll` sets a `frozen_` flag (so `CreateBranch` and
`TryAcquireCheckout` refuse), then writes an empty ledger and drops every branch, including attached
ones. If the ledger write fails, the store is left unchanged, including its previous frozen state.
Demotion calls it on every database before taking the replication state lock, and unfreezes the
stores again if any drop fails or the instance is still MAIN afterwards. Promotion unfreezes all
stores. A session attached to a dropped branch is detected on its next transaction by
`VersionStore::IsLive(name, number)`, and a dropped branch's core is never rebuilt
(`GetOrBuildCore` checks liveness before and after the build).
