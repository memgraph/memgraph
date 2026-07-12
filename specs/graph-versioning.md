# Graph Versioning

**Status:** Draft / Experimental — Enterprise (MEL), target Memgraph v3.13
**Author:** Andreja Tonev (https://github.com/andrejtonev)
**Reviewers:** Colin Barry (code), Josip Mrđen (outside)
**Last updated:** 2026-07-09

> Branch your graph like git. Fork the production graph into named, writable
> **versions** — lightweight overlays inside a *single* Memgraph instance, not copies —
> experiment or curate on a branch, inspect exactly what changed, and merge verified
> changes back to `main`. A branch never mutates production data until an explicit,
> gated merge.

> **Note on scope of this document.** Per `specs/README.md`, a spec describes *product
> behavior* (what/why for the user), and architecture belongs in an ADR. This document
> keeps the product spec in §§1–11 and then retains the full **engineering grounding**
> (Appendices A–E: master-architecture analysis, risk register, build order, MVP
> definition-of-done, discovery links) so nothing discovered during design is lost. Those
> appendices are candidates to migrate into one or more ADRs (`ADRs/`) once the design
> settles; they are kept here *for now* on purpose.

---

## 0. Decisions awaiting product sign-off

This feature is in Discovery. The decisions below are the calls that determine the shape of
v1; each has a proposal for product to accept or redline. They are expanded with rationale
in §6, and the ones that constrain the engine are grounded in Appendix A. Everything else in
this document is a proposal you can change.

| # | Decision | Proposal (accept / redline) |
|---|---|---|
| D1 | **MVP scope line** | **Phased.** MVP = create/checkout/write/diff(table)/merge/drop + `USING VERSION` + enterprise gate + one `BRANCH` privilege. Beyond-MVP = `REVERT` with replay-conflict-check, `SHOW VERSIONING GRAPH`, `FORMAT GRAPH`, `FOR ALL DATABASES`, per-branch ownership/sharing, collaborative mode. *(Agreed in principle; confirm the line.)* |
| D2 | **Implementation approach: the "replay overlay"** | Among the consistency-preserving implementations (Appendix F), v1 uses the **replay overlay**. During branch work, **`main` is read-only to the branch**: the branch *reads* `main`'s live in-memory state time-traveled to `fork_ts` (no file replay, no clone of `main`'s data or index *contents*) and applies its own changes to a **branch-private overlay** (its own delta space / durable WAL change-log) — **never onto `main`'s shared object chains**. Because a branch never writes a shared object, the engine's write-conflict check (`PrepareForWrite`) is **never triggered against `main` during branch work** → the branch's own writes never conflict with newer `main` commits, and no branch delta ever blocks `main` or another branch. Write-conflict detection fires at **merge only**, when the branch's deltas are replayed onto *current* `main` as a committing transaction (→ D3 redo-on-conflict). Rejected: **the "live durable transaction"** (put branch deltas on the shared chain) — its own writes conflict with newer `main` commits *and* it blocks every other writer until merge/drop (Appendix F, `PrepareForWrite`); and **the "anonymous tenant"** (too heavy). **Branch-aware dual-chain** is the north-star (Option 3). Cost: per-query time-travel + reconciliation against `main`'s reused (read-only) indexes (R1/R26/R33/R34) + retention pin (R13). Branch-specific state is a **small fork-era index/constraint catalog** (definitions only — R36) + the **delta-scale overlay** — **not** a main-scale data/index clone (A.7); so "no duplication" means no *main-scale* clone, not literally zero. Bounded by D5. |
| D3 | **One-way merge, redo-on-conflict** | Merge only branch → parent. A conflict (the branch and the parent both changed the same object since the fork) is **detected** by the engine and the merge is rejected; the user redoes the work on a fresh branch off the current parent. No 3-way merge / conflict-resolution UI in v1. |
| D4 | **Ownership & sharing** | v1 = a single `BRANCH` enterprise privilege (you can use versioning or you cannot). Per-branch ownership, "agent creates / human reviews & merges", and sharing/ACL are Beyond-MVP (D1). Confirm this is acceptable for the first customer demos (Nvidia/Adobe). |
| D5 | **Branch lifetime / retention ceiling** | Because a live branch pins its parent's history (D2, R13), v1 bounds it: a configurable cap on a branch's accumulated changes and/or age, enforced with a clear error. Confirm a bound is acceptable versus unbounded long-living branches (which would grow memory with the parent's churn). |
| D6 | **Durability & HA** | Branch registry + change-logs are durable (survive restart; in-memory-vs-durable mechanism is D9). **Replication/HA of versions is out of scope for v1** — versions are single-instance only (matches the diagram: "within one Memgraph instance"). Confirm. |
| D7 | **Naming: `main` vs `master`** | The product-facing base name is **`main`**. The POC used `"master"` internally; v1 standardizes the user-facing token to `main`. Confirm no objection. |
| D8 | **Storage-mode support** | Versioning works **only in `IN_MEMORY_TRANSACTIONAL` mode**. Both `IN_MEMORY_ANALYTICAL` (no MVCC deltas, no rollback or time-travel — structurally breaks the overlay, Appendix A.1) and `ON_DISK_TRANSACTIONAL` are **unsupported** and rejected with a clear error. On-disk is out of scope for this feature entirely — not "later," not a priority — v1 and the whole design assume in-memory transactional. Confirm. |
| D9 | **Branches are durable (survive restart) via the existing snapshot + WAL** | In-memory-transactional storage is already durable through periodic snapshots + WAL; **branches use that same durability and survive restart** (they are *not* in-memory-only). Durable state = branch registry + each branch's change-log + `main`'s snapshot/WAL retained back to the oldest fork (R15). **Steady-state reads do NOT replay durable files** — the base is read by in-memory MVCC time-travel of `main`'s live chains at `fork_ts` (Appendix A.5); durable files are used only at **restart**, where recovery rebuilds `main`'s delta history back to the oldest fork so time-travel resumes (a recovery extension, new work — R16). **Consequence: versioning requires WAL enabled** (`--storage-wal-enabled=true` / `PERIODIC_SNAPSHOT_WITH_WAL`; off by default) — branch operations are rejected if durability/WAL is off. No per-query durable replay, no duplicate tenant. Confirm WAL-required. **v1 IMPLEMENTATION STATUS (2026-07-12): the registry + change-logs ARE durable and reload, but the recovery extension that rebuilds `main`'s delta history to the oldest fork (and re-registers the GC pin) is DEFERRED to v1.1 — so a branch survives restart as a record but cannot be checked out/merged until then (fail-stop, not corrupting). See §10 "Restart / process durability".** |
| D10 | **Safety rail against silent writes to `main`** | Because `CHECKOUT` is per-connection (§4.1), a pooled client can silently write to `main` while believing it is on a branch — violating the core guarantee (§2). v1 needs an *enforced* rail, not opt-in advice. Proposal: a session that has ever checked out a branch must re-establish its version explicitly per connection, and a write with no resolved active version on a versioning-enabled database is **rejected** rather than silently hitting `main` (or: require `USING VERSION` for writes from pooled clients). Product picks the rail. Surfaced by all three skeptics; the current spec's "prefer `CREATE BRANCH` + `USING VERSION`" is advice, not a safety property. |
| D11 | **Merge preserves an audit trail** | `MERGE` currently drops the branch and its change-log, so after a merge there is **no queryable record** of what was merged — which is the agent-curation use case (a human reviews what the agent did). Proposal: on merge, retain an immutable, queryable merge record (who/when/branch/summary or the full change-log) rather than deleting it. Product decides whether v1 keeps it and at what fidelity. |
| D12 | **Branch ownership / access control** | v1's single `BRANCH` privilege (D4) lets *any* holder merge or drop *anyone's* branch, and it is unstated whether fine-grained (label/property) access control filters a branch's reconstruction the way it filters a live query on `main`. The two-actor pitch (agent creates, human merges) is a social convention, not enforced. Proposal: confirm v1 ships without per-branch ACLs (accept the gap) **and** decide whether FGA must apply to branch reads (recommended: yes — a branch must not leak data a user can't see on `main`). Branching the system `memgraph` database is rejected. |
| D13 | **Compare & history surface** | §1 promises users "compare the projected impact" of scenarios, but there is no branch-vs-branch (or branch-vs-`main`) compare command, and no commit-grouped history to find the `txn_ts` that `REVERT` needs (only a flat per-delta table). Proposal: add at least a commit-grouped `SHOW BRANCH LOG` (Beyond-MVP) and document that comparison in v1 is done by running the same query under two `USING VERSION` targets. Product decides MVP scope. |
| D14 | **Naming redlines** | Three collisions with existing Memgraph vocabulary: (1) the base version **`main`** clashes with the replication **MAIN** instance-role — concretely, `SHOW REPLICATION ROLE` returns the literal string `"main"` and `SHOW BRANCH` on the base version also returns `"main"`, so a monitoring/dashboard query gets two unrelated facts as the same token; (2) **`MERGE BRANCH`** reuses the Cypher `MERGE` keyword, whose established meaning (idempotent upsert) is the opposite of a one-shot destructive branch fold, with no `MERGE DATABASE` precedent to anchor it; (3) `SHOW VERSIONING GRAPH` sits next to the long-standing `SHOW VERSION` (build version). Product decides whether to keep the names (documenting the distinctions) or rename before the surface ships. |
| D15 | **A branch is data-plane only — no background machinery** | Every periodic/background subsystem is a **`main`-only** feature and does **not** run on a branch: storage GC, periodic snapshots, WAL rotation, the async index builder, TTL, streams (Kafka/Pulsar), and after-commit triggers all belong to the parent `Storage`. A branch is a passive overlay, not a `Storage` — it spawns no threads, and in particular **has no snapshot of its own** (only its change-log is persisted; its base is `main`'s live in-memory state time-traveled to `fork_ts`). This is what makes "a branch is not a database" (A.5) concrete, and it directly bounds the memory/GC model (Appendix A.6). Confirm branches are read/write data only, with background features operating on `main` alone. |
| D16 | **Full-text & vector search are not branch-aware in v1** | Text (Tantivy) and vector search read the parent's *committed, latest* index, not a branch's fork-point-plus-overlay view — both are per-DB shared structures that are **not** MVCC-visibility-filtered (Appendix A.7). So on a branch, a query using full-text or vector search returns `main`'s results, not the branch's; and because the vector index is updated READ_UNCOMMITTED in the write path, a branch reconstruction that touches vector-indexed properties is briefly visible to concurrent vector queries on `main`. v1 decision: **a branch query that uses full-text or vector search is rejected with a clear error** (it is *not* silently answered from `main`'s index — silent-wrong-results is the worst outcome for the GraphRAG/embedding persona), and reconstruction avoids feeding the shared vector index. Confirm acceptable, or fund branch-scoped search indices (large). Contrast: the label/label-property/edge-type/edge-property/point indices *are* reused safely on a branch (A.7). |
| D17 | **Foundational: is branch isolation (consistency) required?** *(product-open)* | The upstream decision that gates everything else. The POC (PR #4263) applied a branch's forward deltas onto **current `main`** at each transaction — so a branch's base silently shifts as other branches merge, and an "impact analysis" is contaminated by unrelated changes. If **consistency is NOT required**, that POC model is by far the simplest (no fork-point, no retention, no replay). If **consistency IS required** (isolated, controlled baseline — the position engineering recommends, since impact analysis without it is meaningless), v1 uses the replay overlay (D2) and the five-way trade-off in **Appendix F** applies. **Product must rule on this first** — it is still under discussion. Everything in D2–D16 assumes consistency = yes. |

---

## 1. Motivation

Users increasingly want to change a graph's *data* tentatively — to model a "what if" scenario,
curate or enrich a dataset, or let an AI agent propose edits — and then decide whether those
changes should become real. (v1 scopes this to **data** changes; testing a *schema* change —
new index/constraint/enum — on a branch is out of scope, see D15/R14.) Today the only tools for that are a full copy of the database (expensive,
and it drifts from production the moment it is made) or a single `BEGIN … ROLLBACK`
transaction (which cannot outlive one session, cannot be reviewed by a second person, and
cannot be inspected). Neither lets a change *live* long enough to be reviewed, compared, and
approved.

Graph versioning fills that gap. It lets a user fork the production graph into a named,
isolated, writable **version** (branch) inside the *same* Memgraph instance — no copying, no
second database — work on it (up to a configurable size/age bound, D5), see exactly what changed,
and either **merge** the change back into production or **throw it away**. A branch's changes
become production data only through an explicit, gated merge; for that guarantee to hold on a
pooled connection, the write-routing rail of D10 must be in place (§4.1).

Two headline use cases drive the design:

1. **Impact & what-if analysis.** Analysts, data scientists, and domain experts explore
   scenarios in a sandbox version — "what if this customer churns / this supplier fails to
   ship / this route is blocked?" — and compare the projected impact, all without affecting
   production. Each scenario is its own branch.
2. **Agent curation.** An AI agent works on a curation branch (detect issues → review &
   validate → resolve & enrich → verified), and **only verified changes merge back** through
   an approval gate. A human reviews what the agent did before it becomes real.

The feature is targeted at enterprise customers doing (a) risk-free what-if simulation and
(b) AI-agent-driven data curation with a human-gated merge to production.

---

## 2. Core principle

> **A branch is a live overlay pinned to its parent's state at fork time, not a copy.
> Reading or writing a branch never mutates the parent's stored data, and data becomes real only
> on an explicit merge. The parent stays writable.**
>
> *(Caveat: because `CHECKOUT` is per-connection, this "never mutates `main`" property holds for a
> pooled client only once the D10 write-routing rail is in place — see §4.1. That rail is a
> product decision still open in §0.)*

Everything in this spec follows from that sentence. `main` is the base graph. When you fork a
branch, it records the parent's **fork point** — a commit timestamp — and from then on reads
the parent *as of that point* plus its own recorded changes. A branch reads *identical* to its
parent-at-fork until you write to it; each write is recorded as a **forward change** (a
concrete, already-evaluated mutation: "vertex 5, set property `x` = 10") in the branch's own
**change-log**. Those forward changes are the same records Memgraph already writes to its
**write-ahead log** (see §3 and Appendix A) — they are materialized values, not queries, so
replaying them is deterministic (a `time()` call is evaluated once, when the write runs, and
its *result* is stored).

When you query a branch, the engine reconstructs the branch view — the parent as of the fork
point, with the branch's forward changes applied on top — runs your query against it, and then
discards the reconstruction, leaving the parent untouched. Because the branch is pinned to the
fork point rather than to "current parent," **the parent (`main` included) can keep changing
while the branch lives** without disturbing what the branch sees. Merging a branch is the one
and only operation that turns its forward changes into real, committed data on the parent.

This is why the feature can promise "one instance, many versions": branches are lightweight
overlays that share the base graph, not independent copies of it. It is also why several
constraints below exist — transactional-mode-only (the overlay needs MVCC and rollback), the
per-query reconstruction cost, and the memory retention needed to keep a parent's history back
to the oldest branch's fork point. If a decision in this document seems surprising, check it
against this principle first.

---

## 3. Concepts and terminology

- **Version / branch** — a named, writable fork of the graph. Used interchangeably; the
  query surface says `BRANCH`, the model calls them versions. Every version has a monotonic
  **number** (see below), a **fork point**, an optional description, and its own change-log.
- **`main`** — the base graph; always **version number 1**; has no overlay and no fork point.
  This is production data, and it remains fully writable even while branches exist.
- **Fork point (base timestamp)** — the parent's commit timestamp captured when a branch is
  created. The branch reads its parent *as of* this timestamp; the parent may advance past it
  freely. This is what makes the parent's continued writes invisible to the branch (and is the
  source of the retention cost — the parent's history back to the oldest live fork point must
  be kept; see R13).
- **Change-log** — the ordered list of **forward, materialized changes** (create/delete vertex
  or edge, set property, add/remove label — with concrete values) recorded on a branch. It is
  *not* a log of query text; it is the same forward-delta format Memgraph writes to its
  write-ahead log, so it is deterministically replayable (Appendix A.1). In v1 the change-log is
  **durable** — persisted as WAL files (D9) — so branches survive restart. At read time the branch's
  fork-point base is obtained by **in-memory MVCC time-travel** of `main`'s live chains (not by
  replaying files); the durable snapshot+WAL are used only to recover across restart (D9, A.5).
  A branch *is* its parent-at-fork plus its change-log.
- **Version tree** — versions form a **tree** rooted at `main`. Any existing version can be a
  parent, so you can branch off `main` or **branch off another branch** (nested branching), and
  a database can hold **many** branches at once. Version numbers (2, 3, …) are monotonic and
  **never reused**, even after a branch is dropped, so a number always refers to at most one
  version in a database's history.
- **Checkout** — pointing your session at a version, so subsequent queries read and write that
  version. Per-connection state.
- **Merge** — replaying a branch's change-log onto its parent as real committed data, then
  dropping the branch. One-way (branch → parent) only, and conflict-checked (D3).
- **Revert** *(Beyond-MVP)* — removing one commit's worth of changes from a branch's change-log.

---

## 4. User-facing surface

### 4.1 Prerequisites & default behaviour

- **Off by default.** Versioning is enabled with the startup flag `--versioning-enabled=true`.
  It is **startup-only** — it cannot be toggled at runtime. With it disabled, every versioning
  query returns a "versioning disabled" error.
- **Enterprise (MEL) gated.** Versioning is an enterprise feature. Without a valid Memgraph
  Enterprise License, versioning queries return a license error even when the flag is on.
- **In-memory transactional mode only** (D8). Versioning requires `IN_MEMORY_TRANSACTIONAL`.
  `IN_MEMORY_ANALYTICAL` keeps no MVCC deltas and has no rollback or time-travel, so the overlay
  model cannot work; `ON_DISK_TRANSACTIONAL` is **unsupported** (out of scope entirely). Versioning
  queries in any mode other than in-memory transactional are rejected with a clear error.
- **WAL (durability) required for branches** (D9). Branches are durable and survive restart using
  in-memory-transactional's existing snapshot + WAL durability, so versioning requires the database
  to run with periodic snapshots **and** WAL (`--storage-wal-enabled=true`, i.e.
  `PERIODIC_SNAPSHOT_WITH_WAL`; WAL is **off by default**). With versioning enabled, a branch
  operation on a database without WAL is rejected. (This is in-memory-transactional durability, not
  on-disk storage mode — which is unsupported, D8. Steady-state reads use in-memory time-travel, not
  file replay; durable files are consulted only at restart, A.5.)
- **Management queries are autocommit-only.** The versioning *management* queries — `CREATE`,
  `CHECKOUT`, `MERGE`, `DROP`, `REVERT`, and the `SHOW BRANCH*` family — must each run as a
  standalone, autocommit query, never inside an explicit multi-command `BEGIN … COMMIT`
  transaction. They are control-plane operations that change session or registry state (e.g.
  `CHECKOUT` changes the connection's active version; `MERGE` commits to the parent), so —
  exactly like `CREATE DATABASE` and index DDL — they are not allowed to be entangled in a data
  transaction's atomicity.
- **Branch data writes may use explicit transactions.** Ordinary reads and writes against a
  checked-out branch are *not* restricted to autocommit. A `BEGIN … COMMIT` on a branch is
  fine; only the committed result matters — the whole transaction's forward changes become one
  commit (one `txn_ts`) in the branch's change-log. (Holding a transaction open across
  statements keeps the branch's reconstruction pinned for its duration, which interacts with
  retention/GC — see R13.)
- **Per-connection session version.** `CHECKOUT BRANCH` sets the active version for the current
  Bolt connection only. It does **not** survive a connection pool — a pooled client (e.g.
  Memgraph Lab, which may run each query on a different pooled connection) can land a follow-up
  query on a different connection that is still on `main`. **Pooled clients should use
  `CREATE BRANCH` + `USING VERSION` instead of `CHECKOUT`** (see below). The review/merge commands
  name the branch explicitly (`SHOW BRANCH DIFF '<name>'`, `MERGE BRANCH '<name>'`) and require no
  checkout (§4.2), so pooled clients can complete the full create→write→review→merge flow without
  ever relying on per-connection state; and D10 proposes an enforced rail so a mis-routed write is
  *rejected* rather than silently applied to `main` — without that rail this per-connection behavior
  is a data-safety hazard, not just a UX quirk.
- **Composable-query default.** A plain query with no `USING VERSION` directive and no active
  checkout runs against `main`. `USING VERSION 'x'` targets version `x` for that one query only,
  without changing session state.
- **Version names.** A version name is a string literal (`'my-branch'`) or a bare symbolic name
  (`my_branch`). It must be non-empty, cannot be `main` (reserved), cannot start with `.`, and
  cannot contain `/` or `\`. Prefer quoted string literals for names with `-` or other
  non-identifier characters.

### 4.2 Commands — MVP

```cypher
-- Fork a new branch off a parent WITHOUT switching your session onto it.
CREATE BRANCH '<name>' [ WITH DESCRIPTION '<text>' ] FROM '<parent>';

-- Switch the session's active version (create-and-switch when FROM is given).
CHECKOUT BRANCH '<name>';
CHECKOUT BRANCH '<name>' [ WITH DESCRIPTION '<text>' ] FROM '<parent>';
CHECKOUT BRANCH 'main';                 -- back to the base graph

-- Run ONE query against a version without changing the session's active version.
USING VERSION '<name>' MATCH (n) RETURN n;   -- composes with HOPS_LIMIT etc.

-- Inspect.
SHOW BRANCH;                            -- what version am I on
SHOW BRANCHES [ FOR DATABASE <db> ];    -- list all versions
SHOW BRANCH DIFF [ '<name>' ] [ FORMAT TABLE ];  -- a version's recorded changes (name optional; no checkout needed)

-- Fold a branch into its parent (branch named explicitly; no checkout required; branch then dropped).
MERGE BRANCH '<name>';

-- Throw a branch away.
DROP BRANCH '<name>';
```

- **`CREATE BRANCH … FROM`** forks `<name>` off `<parent>` (`main` or an existing version)
  **without** switching onto it, capturing the parent's current state as the branch's fork
  point; returns one row `version, number, description, parent`. Use a subsequent `CHECKOUT` to
  work on it, or `USING VERSION` to target it per query. `FROM` is required. **Preferred for
  pooled clients.**
- **`CHECKOUT BRANCH '<name>'`** sets the active version for the session; subsequent
  reads/writes target it. `CHECKOUT BRANCH 'main'` clears the active version. Fails if
  `<name>` does not exist. With `FROM` it is a combined create-and-switch and returns one row
  `version, number, description, parent`.
- **`SHOW BRANCH`** returns the current session's version (`version, number, description`).
- **`SHOW BRANCHES`** lists every version including `main`, ordered by number
  (`number, version, description`). `FOR DATABASE <db>` targets another tenant's versions.
- **`SHOW BRANCH DIFF [ '<name>' ]`** dumps a version's change-log, one row per recorded change, in
  insertion order (columns `op, entity, gid, detail, txn_ts, ledger_time, query`). With no name it
  uses the session's active version and errors on `main`; the **`'<name>'` form** targets a version
  without a checkout, so **pooled clients (Lab) can review a branch they created via
  `CREATE BRANCH` without the unsafe `CHECKOUT`** (Skeptic A#1). Note this is *not* a compare of two
  versions — it is one version's own change-log against its fork point (see D13 for branch-vs-branch
  compare, and Beyond-MVP `SHOW BRANCH LOG` for a commit-grouped view). `op` is one of `CREATE_VERTEX | DELETE_VERTEX | ADD_LABEL
  | REMOVE_LABEL | SET_PROPERTY | CREATE_EDGE | DELETE_EDGE`; `entity` is `vertex` or `edge`;
  `gid` is the object's global id; `detail` is a human-readable rendering (labels / `prop =
  value` / edge `type (from->to)`); `txn_ts` is the MVCC logical start timestamp of the
  producing transaction; `ledger_time` is the UTC wall-clock instant the change was appended.
  The `query` column is the originating query text and is **provenance/display only** — it lets
  a reviewer see which query produced a change; it is **not** what replay uses (replay applies
  the materialized change, §2).
- **`MERGE BRANCH '<name>'`** names the branch explicitly and **does not require a checkout** — so
  pooled clients (Lab) can merge a branch they created without the unsafe `CHECKOUT` (Skeptic A#1).
  The branch must have **no children**. It replays the branch's change-log onto its parent (`main`
  for a top-level branch) as a new transaction; if the branch and the parent both changed the same
  object since the fork point, the engine detects the conflict and the merge is **rejected** (D3).
  After the merge commits durably, the branch is **dropped**; if the session happened to be checked
  out onto it, the active version is cleared to the parent — and the D10 write-routing rail covers
  this post-merge case too, so a subsequent write cannot silently land on `main`. Returns one row
  `merged, into`. Conflict granularity is whole-object (any object changed on both sides), and
  create-vs-create collisions need explicit handling (Appendix B, R11/R19); the conflict error names
  the colliding objects so the redo is targeted.
- **`DROP BRANCH '<name>'`** permanently removes the branch's change-log and registry entry;
  fails if it does not exist; returns no rows. Dropping does not reposition the session.

### 4.3 Commands — Beyond-MVP

These are specified for completeness (the query surface is already designed and prototyped)
but are **out of scope for v1** (D1). They are documented here so product can see the full
shape; §10 lists them as future direction.

```cypher
-- Undo one whole commit's changes from the active version's change-log.
REVERT BRANCH COMMIT WITH TIMESTAMP <txn_ts>;

-- Visualize a branch's changes / the version tree in Memgraph Lab.
SHOW BRANCH DIFF FORMAT GRAPH;
SHOW VERSIONING GRAPH [ FOR DATABASE <db> | FOR ALL DATABASES ];
```

- **`REVERT BRANCH COMMIT WITH TIMESTAMP <x>`** removes every change in the active version's
  change-log that came from the commit whose `txn_start_timestamp == <x>` (find `<x>` in the
  `txn_ts` column of `SHOW BRANCH DIFF`). `<x>` is the integer logical MVCC start timestamp,
  not a wall-clock time; one commit (one autocommit write, or one explicit `BEGIN … COMMIT`)
  shares one `txn_ts`, so this undoes exactly that commit's changes. It is **commit-granular and
  conflict-checked**: the pruned change-log is replayed in strict mode as a dry run, and if
  removing the commit would orphan a surviving change (e.g. a `SET_PROPERTY` on a vertex whose
  `CREATE_VERTEX` was in the reverted commit), the query is **rejected** and the change-log is
  left unchanged. Requires an active version with no children. Returns
  `reverted_commit, removed_deltas, remaining_deltas`.
- **`SHOW BRANCH DIFF FORMAT GRAPH`** returns the change-log as a virtual graph for Lab: each
  change becomes a `:Delta:<OP>` node linked to the entity it changed.
- **`SHOW VERSIONING GRAPH`** returns a virtual graph of the version tree: one `:Version` node
  per version (plus `:CurrentVersion` on the session's active version) with `BRANCHED_TO` edges
  parent → child, each tenant anchored by a `:Database` node. Scope: no clause = current
  database; `FOR DATABASE <db>` = that tenant; `FOR ALL DATABASES` = every tenant as its own
  subtree.

### 4.4 Error messages

Each error states the cause and the corrective action.

| Condition | Message |
|---|---|
| Versioning disabled | `Versioning is disabled. Start Memgraph with --versioning-enabled=true to use branch queries.` |
| No enterprise license | `Graph versioning is an enterprise feature. A valid Memgraph Enterprise License is required.` |
| Unsupported storage mode | `Graph versioning requires IN_MEMORY_TRANSACTIONAL storage mode. It is not available in IN_MEMORY_ANALYTICAL or ON_DISK_TRANSACTIONAL.` |
| Durability/WAL disabled | `Graph versioning requires durability with WAL enabled (--storage-wal-enabled=true). Enable periodic snapshots + WAL to use branches.` |
| Management query in explicit txn | `Versioning management queries (CREATE/CHECKOUT/MERGE/DROP/REVERT/SHOW BRANCH) must run in autocommit mode; they cannot run inside an explicit BEGIN … COMMIT transaction.` |
| Invalid name | `Invalid version name '<x>': names must be non-empty, cannot be 'main', cannot start with '.', and cannot contain '/' or '\'.` |
| Branch/vector/text search on a branch | `Full-text and vector search are not supported against a branch view (they would reflect 'main', not the branch). Run the query on 'main', or use label/property/edge indices which are branch-aware.` (D16) |
| Merge with children | `Cannot merge '<x>': it has child branches. Merge or drop its children first.` |
| Merge conflict | `Merge of '<x>' into '<parent>' conflicts: '<parent>' changed objects that '<x>' also changed since the fork. Redo the work on a fresh branch off the current '<parent>'.` |
| Branch retention cap exceeded | `Version '<x>' has reached the maximum size/age limit (<N>). Merge or drop it, or raise --versioning-max-changelog-length.` |
| Branch not found | `Version '<x>' does not exist.` |
| Name already exists | `Version '<x>' already exists.` |
| Write with no resolved version (D10) | `This connection has no active version resolved. Use CHECKOUT BRANCH or USING VERSION; writes are not applied to 'main' by default while versioning is in use on this connection.` (exact behavior pending D10) |
| Storage-mode switch with live branches | `Cannot switch storage mode while versioning branches exist: it would strand their fork-point history. Merge or drop all branches first.` (guards STORAGE MODE; the RECOVER SNAPSHOT and DROP GRAPH variants carry the analogous message — R17) |
| Schema-plane DDL on a branch | `Schema-plane and storage-global operations (indexes, constraints, enums, TTL, ANALYZE GRAPH, DROP GRAPH, STORAGE MODE, RECOVER SNAPSHOT) are not allowed while a versioning branch is checked out. Run CHECKOUT BRANCH 'main' first.` (R14) |
| Parallel (chunked) scan on a branch | `Parallel (chunked) vertex/edge scans on a versioned branch` (NotYetImplemented — the planner's single-threaded, union-aware scans are used instead) |

### 4.5 Config flags

| Flag | Type | Default | Notes |
|---|---|---|---|
| `--versioning-enabled` | bool, **startup-only** | `false` | Master switch. |
| `--versioning-max-changelog-length` | uint | *TBD* | Bounds a branch's accumulated changes / retention (D5 / R13). Open: default value + whether it is runtime-settable. |

---

### 4.6 Interactions with other features

Versioning shares a `Database` with features that operate on the *parent's* real storage, so their
behavior on a branch needs to be stated rather than assumed. These were surfaced by the skeptic
review and several are verified against master; the resolutions below are proposals for v1.

**Governing principle (D15): all background machinery is `main`-only.** A branch is a passive
overlay, not a `Storage`, so none of the parent's periodic/background subsystems run on it. The
per-feature rows below are consequences of that one rule.

- **Triggers.** Trigger definitions are per-`Database`, and (verified in code) branch writes end in
  `Abort()` while only `MERGE` commits — so with the naive design **no trigger fires during branch
  work**, and **all fire once at merge** seeing the whole branch as a single transaction's
  `createdObjects`/`updatedObjects`. v1 proposal: **branch writes do not fire triggers** (a branch
  is not production), and a merge fires triggers **once** for the merged change-set, documented as
  such; a trigger that fails at merge rejects the merge (add to §4.4). (Skeptic A#3 / B#9.)
- **TTL.** The TTL reaper is a background job scanning the parent's real storage, so a `:TTL`
  vertex created only on a branch is invisible to it and **will not expire while the branch lives**;
  it becomes eligible only after merge. v1 documents this; TTL does not act on branch overlays.
  **Gotcha:** a `:TTL` vertex whose expiry has already passed by merge time will be deleted by the
  reaper moments after `MERGE` — so a long-lived branch can have "reviewed" data vanish right after
  it becomes real, undermining the agent-curation review step; document as a known trap. (Skeptic A#4/A#8.)
- **Streams (Kafka/Pulsar).** Streams ingest continuously into the parent storage and are not
  version-aware; **a stream cannot target a branch** in v1 (ingestion always lands on `main`).
  (Skeptic A#4.)
- **Indexes / constraints / enums (schema plane).** *Creating* these on a branch is **not supported
  in v1** (R14) — they mutate storage-global state and don't preview/roll back per-transaction. Enum
  DDL is especially dangerous: enum value ids are **positional** and are **not** rolled back on abort
  (Appendix A.7), so an aborted branch would permanently corrupt mainline enum identity — the ban is
  a **correctness requirement**, not a convenience (R14). Branch changes are data-plane only. By
  contrast the *data-plane* label / label-property / edge-type / edge-property / point indices **are
  reused safely** on a branch: reconstruction maintains them through the normal write path, they are
  unwound on abort, and scans are re-checked against MVCC visibility (Appendix A.7).
- **Full-text (Tantivy) & vector search.** **Not branch-aware** (D16): a branch view returns `main`'s
  committed search results, not the branch overlay, because these indices are shared per-DB and not
  MVCC-filtered (Appendix A.7). v1 scopes branch full-text/vector search out; the vector index is
  additionally READ_UNCOMMITTED, so reconstruction must avoid feeding it.
- **Edge gids on replay.** The edge-metadata index is a shared gid→vertex map that **hard-asserts
  (crashes) on a gid collision** (`MG_ASSERT(inserted)`, `inmemory/storage.cpp:865`; R11), so branch
  reconstruction and merge **must** assign globally-unique edge gids — a hard requirement, not an
  optimization.
- **Storage mode & `RECOVER SNAPSHOT`.** Switching to `IN_MEMORY_ANALYTICAL` (or restoring a
  snapshot) while branches exist would strand every branch's fork-point base; v1 **rejects** the
  switch/restore while branches exist (error in §4.4; R17).
- **Explicit transactions & `PeriodicCommit`.** Data writes on a branch may use `BEGIN … COMMIT`
  (§4.1), but note that **merge replays the whole change-log as one transaction** — a branch large
  enough to have needed `PeriodicCommit` while building will pay that one-giant-transaction cost at
  merge time (R18). v1 documents this; chunked merge is future work.
- **Access control (RBAC / FGA).** A branch's reconstruction must apply the **same fine-grained
  access filtering** a live query on `main` would, so a branch cannot leak data a user can't
  otherwise see (D12). Whether per-branch ownership is enforced is a separate product call (D12).
- **System `memgraph` database.** Branching the system database is **rejected** — its auth /
  multi-tenancy metadata does not live in per-database graph storage, so a "branch" of it would
  capture only incidental graph data, not the state a user would expect (D12).
- **`SHOW TRANSACTIONS`.** A long branch transaction pins retention (R13); v1 should surface, in
  `SHOW TRANSACTIONS`, that a given transaction is a branch/reconstruction transaction so an
  operator can identify what is holding retention (otherwise R13's own mitigation is unobservable).
- **Restart / process durability (v1 status: USE-after-restart DEFERRED to v1.1).** D9 describes the
  *target* design (branches survive restart via `main`'s snapshot+WAL, recovery rebuilding `main`'s
  delta history back to the oldest fork so time-travel resumes — a recovery extension, R16). **That
  recovery extension is not implemented in v1.** What v1 *does* persist and reload: the branch
  registry (a kvstore-backed `VersionStore` — names, `fork_ts`, parent, never-reused `number`,
  description) and each branch's on-disk WAL change-log (re-discovered by disk scan at checkout/merge).
  What v1 does **not** restore: the in-memory GC **fork-pin** is never re-registered on startup, and
  `main`'s recovery rebuilds only *current* state — not the MVCC delta chain back to `fork_ts` that
  `HistoricalAccess(fork_ts)` walks. **Consequence:** after a restart a branch remains *listed* but
  can no longer be checked out or merged — both fail loud with `fork timestamp … is not (or no longer)
  pinned` (branch_engine / merge pin-guards). This is **fail-stop, not data-corrupting** (the pin
  guards prevent any torn historical read). v1.1 will close this via one of two scoped approaches:
  (a) materialize the read-only fork-state view to disk at `CREATE BRANCH` (base read from the
  snapshot after restart, diff WAL replays on top), or (b) bounded recover-to-`fork_ts` — retain and
  rebuild `main`'s delta history to the oldest live fork and re-register the pins (the deeper recovery
  change D9/R16 envisions). Product-agreed to ship v1 with branches process-scoped *for use*.

---

## 5. Lifecycle

```
  CREATE BRANCH 'x' FROM 'main'        (captures main's current state as x's fork point)
        │                               (main stays fully writable afterwards)
        ▼
     branch 'x'  ── work: normal Cypher writes, recorded into x's change-log ──┐
        │                                                                      │
        │  SHOW BRANCH DIFF  (review what changed)                             │ (loop)
        │◀───────────────────────────────────────────────────────────────────┘
        │
        ├── MERGE BRANCH 'x'  ──▶  changes replayed onto current parent (conflict-checked);
        │                          on success 'x' dropped, session → parent
        └── DROP  BRANCH 'x'  ──▶  changes discarded; 'x' gone
```

Typical flow:

```cypher
CHECKOUT BRANCH 'feature-x' WITH DESCRIPTION 'prototype' FROM 'main';
CREATE (:Person {name: 'Ada'});
MATCH (p:Person) SET p.active = true;
SHOW BRANCH DIFF;                 -- review
MERGE BRANCH 'feature-x';         -- replay onto main (rejected if it conflicts), then drop
-- ...or throw it away:
-- CHECKOUT BRANCH 'main'; DROP BRANCH 'feature-x';
```

**`main` stays live.** Unlike an earlier proposal that froze `main` the moment a branch existed,
`main` (and any parent) remains fully writable while branches exist. A branch is pinned to its
parent's fork point (§2), so the parent's later writes do not disturb the branch's view. You can
keep working on `main`, hold several branches at once, and branch off different points in time.

**Merge is conflict-checked, not blind.** Because the parent can move after a branch forks,
`MERGE` replays the branch's changes onto the *current* parent and the engine checks whether the
parent changed any object the branch also changed since the fork. If so, the merge is rejected
and the user redoes the work on a fresh branch off the current parent (D3). There is no
automatic 3-way merge in v1.

**The cost of a live parent is retention.** Keeping a branch's fork-point view valid means the
parent's history back to the oldest live branch's fork point must be retained in memory (R13).
A branch that lives a long time while `main` churns heavily therefore grows memory; v1 bounds
this with a retention cap (D5).

---

## 6. Product decisions and rationale

The decisions below are the *why* behind the feature's shape. They mirror the table in §0.

### D1 — Phased MVP: a small correct core now, the rest explicitly later.

MVP is create/checkout/write/diff(table)/merge/drop, plus `USING VERSION`, the enterprise
gate, and a single `BRANCH` privilege. `REVERT`, the graph-visualization views
(`SHOW VERSIONING GRAPH`, `FORMAT GRAPH`), `FOR ALL DATABASES`, per-branch ownership/sharing,
and collaborative mode are deferred.

*Rationale.* The full query reference is large, and several of its pieces (conflict-checked
`REVERT`, cross-tenant visualization, per-branch ACLs) are individually substantial and carry
their own correctness risk. Shipping the smallest slice that is genuinely useful — fork,
write, review, merge or discard — lets us validate the overlay model with real customers
before investing in the harder surface. The deferred items are specified (§4.3, §10) so the
cut is deliberate, not accidental.

### D17 — Is branch isolation (consistency) required? *(foundational, product-open)*

The upstream decision. The POC replayed a branch's forward deltas onto **current `main`** at each
transaction — constant automatic rebasing — so the branch's base moved whenever anyone else merged.

*Rationale.* For the headline use cases this is fatal: an *impact-analysis* branch measures "what
does my change do," but if unrelated merges keep landing in its base, the branch shows *my change +
everyone else's* and the analysis is meaningless. A curation branch a human is reviewing can change
underneath the reviewer. Engineering's position (and, we believe, the right call) is that
**consistency is the point of the feature** — without it, versioning is barely more than the POC's
scratch-editing. This is a genuine product decision and is still under discussion, so it is recorded
here as foundational and open: **if product decides consistency is not required, the whole design
below collapses to the far simpler POC model** (no fork point, no retention, no replay). Everything
in D2–D16 assumes the answer is *yes*.

### D2 — Implementation approach: the replay overlay (chosen), and why not the alternatives.

Given consistency = yes (D17), a branch must read a **fixed** base (its parent as of `fork_ts`) plus
its own changes, while `main` and other branches keep running. The full option comparison is
**Appendix F**; this records the choice and the rejections.

**Chosen — replay overlay.** A branch pins its parent's fork commit-timestamp; its changes live
**only in a durable WAL change-log**; each branch query reconstructs the fork-point base (MVCC
time-travel, `mvcc.hpp:32`), replays the change-log, runs, and **aborts** — so **no branch delta
ever persists as a live delta on the shared object chains.** That single property is what lets
`main` stay fully writable and lets multiple branches touch the same objects without blocking.

**Rejected — "live durable transaction" (leave the branch's MVCC transaction uncommitted until
merge).** This is the *simplest* consistency-preserving option and the one first proposed, but it is
**broken for live production.** A branch's writes would sit as live uncommitted deltas on the shared
chains; `PrepareForWrite` (`mvcc.hpp:112`) lets a second writer proceed only if the head delta is
its own or committed before its snapshot, else it forces a serialization error (`mvcc.hpp:134`). A
live branch delta carries the branch's `transaction_id` in the uncommitted high-bit space
(`≥ 1ULL<<63`, `transaction_constants.hpp:19`), while any real transaction's `start_timestamp` is
`< 2^63` — so the "committed before my snapshot" escape can *never* fire. The result: **`main` (and
every other branch) cannot write any object a live branch has touched, until that branch merges or
drops.** Not slow — a hard block on production writes. That disqualifies it despite its simplicity.

**Rejected — "anonymous tenant"** (stand up a full duplicate storage + indices per branch): correct
and isolated, but duplicates everything — far too heavy for the "small change, test, merge quickly"
use case (Appendix F, Option 2; A.5).

**North-star — "branch-aware dual chain"** (branch changes in a separate, `main`-invisible delta
chain): the only option with cheap reads *and* no blocking *and* no pollution — but it requires
rewriting the storage engine and every index to be branch-aware. A v2 the replay overlay grows into,
not a v1 (Appendix F, Option 3).

*The replay overlay is therefore the minimum viable design that gives consistency **without**
blocking production* — earlier "freeze `main`" and "live transaction" drafts both bought simplicity
by breaking the live-`main`/multi-branch requirement. Its cost is deliberate: per-query replay
(R1/R26) + a retention pin on the parent's history back to the oldest fork (R13), bounded by D5.
This is the "time-travel" cost from the discovery notes, and it is the *right* trade — read cost in
exchange for isolation without production breakage.

### D3 — One-way merge, engine-detected conflict, redo on conflict.

Merge is only branch → parent. If the branch and the parent both changed the same object since
the fork, the engine detects the conflict and rejects the merge; the user redoes the work on a
fresh branch off the current parent. There is no 3-way merge or conflict-resolution UI in v1.

*Rationale.* Confirmed with the team on the June 16 sync: a one-way merge with
redo-on-conflict is the simplest contract that is still useful, and it matches how the
interested customers described their workflows. Crucially, with the pinned-fork model (D2) the
conflict check is not hand-waved — replaying the branch onto the parent as a transaction whose
snapshot is the fork point makes the engine's ordinary **write-conflict detection**
(`PrepareForWrite`, a serialization error when an object was committed on the parent after the
branch's fork timestamp) reject exactly the colliding merges. Interactive conflict resolution
is a large design of its own and is not needed to deliver the headline use cases.

### D4 — A single `BRANCH` privilege in v1; ownership and sharing come later.

v1 gates versioning behind one enterprise privilege: a user either can use versioning or
cannot. Per-branch ownership, the "agent creates / human reviews & merges" split, and
sharing/ACLs are Beyond-MVP.

*Rationale.* The customer conversations make clear that real deployments will eventually want
ownership and sharing (an agent creates a change, a *different* person reviews and merges it —
so there must be a concept of sharing and access control). But that is a permissions design
in its own right. For the first demos (Nvidia, Adobe) a single capability privilege is enough
to exercise the feature; product should confirm that is acceptable for those demos.

### D5 — Bound a branch's lifetime/size to bound retention.

Because a live branch pins its parent's history (D2), v1 enforces a configurable cap on a
branch's accumulated changes (and/or age).

*Rationale.* Two costs grow with a branch: reconstructing its view replays its change-log
(O(change-log length), Appendix B R1), and keeping its fork-point view valid retains the
parent's history since the fork (R13). Both are bounded by capping how large/old a branch may
get, which keeps worst-case latency and memory predictable and gives a clear error instead of
silent degradation. Removing the cost structurally — an incremental/materialized overlay and
smarter retention — is a substantially larger project and is deferred. Product should confirm a
bound (rather than unbounded long-living branches) is acceptable for v1; engineering will supply
the measured latency/memory curves to set the number.

### D6 — Durable, single-instance. No replication/HA of versions in v1.

The branch registry and change-logs are durable and survive restart. Replicating versions to
replicas, or preserving them across failover, is out of scope for v1.

*Rationale.* Durability is required — without it, versioning would be no better than a
`BEGIN … ROLLBACK` transaction (confirmed on the June 16 sync). But version *replication* is a
separate, sizable body of work (it would follow the system-replication path that
`CREATE`/`DROP DATABASE` uses — see Appendix A.3), and the product framing is explicitly
"within one Memgraph instance." v1 delivers durable, single-instance versioning; HA is a
recognized future item (§10).

### D7 — The base version is called `main`.

The user-facing base name is `main` (not `master`).

*Rationale.* `main` is the current git-ecosystem norm and matches the query reference the
customers were shown. The POC used `"master"` internally; v1 standardizes on `main` at the
user-facing surface to avoid a naming split.

### D8 — In-memory transactional mode only; analytical and on-disk both rejected.

Versioning is available **only** in `IN_MEMORY_TRANSACTIONAL`. Both analytical and on-disk are
rejected.

*Rationale.* The overlay model depends on the engine's in-memory MVCC: a branch reads the parent by
**time-travelling its live in-memory delta chains** to the fork point (no reconstruction from disk),
overlays the branch's changes, and rolls them back so the parent is untouched. Analytical mode keeps
no MVCC deltas and has no rollback or time-travel (Appendix A.1) — none of that works there.
`ON_DISK_TRANSACTIONAL` is **out of scope for this feature entirely** — the whole design is built on
in-memory MVCC time-travel, and on-disk is not a target now or "later." Rejecting the unsupported
modes with a clear error is honest; silently corrupting or partially applying changes is not.

### D9 — Branches are durable and survive restart, via the existing snapshot + WAL; WAL required.

In-memory-transactional storage is durable today (periodic snapshots + WAL). Branches use that same
durability: the branch registry and each branch's change-log are persisted, and `main`'s snapshot/WAL
are retained back to the oldest live fork. Branches therefore survive restart. A branch is **not** a
create-then-discard copy of the database (Appendix A.5).

*Rationale.* Durability is a stated requirement (D6): a branch a customer curates over days must not
vanish on a restart or deploy. The important distinction (raised in design): **durability does not
mean replaying durable files on every read.** Steady-state reads use in-memory MVCC time-travel of
`main`'s live chains at `fork_ts` (Appendix A.5) — no per-query file replay, no duplication. The
durable snapshot+WAL are used only at **restart**, where — for a versioning-enabled database —
recovery must rebuild `main`'s delta history back to the oldest fork so in-memory time-travel resumes
(a recovery extension, new engine work, R16). Because this relies on the WAL, versioning **requires
WAL enabled** (`PERIODIC_SNAPSHOT_WITH_WAL`; `storage_wal_enabled` is off by default, `general.cpp:114`),
and branch operations are rejected otherwise. This is in-memory-transactional durability used as-is,
retained deeper — **not** on-disk storage mode (which is unsupported, D8) and **not** a per-branch
duplicate tenant.

### D10 — An enforced rail against silent writes to `main`.

Per-connection `CHECKOUT` (§4.1) means a pooled client can write to `main` while believing it is
on a branch, with no error.

*Rationale.* This is the single most dangerous gap all three skeptics converged on: it violates the
feature's own core guarantee ("`main` is never touched until an explicit, gated merge", §2), and it
is worst for the agent-curation use case where an agent drives a pooled driver. The current
mitigation ("prefer `USING VERSION`") is advice, not a safety property. v1 must make a mis-routed
write **fail loud** rather than silently land on production — e.g. once a connection has engaged
versioning, a write with no resolved active version is rejected, or pooled writes must carry
`USING VERSION`. Product picks the exact rail; doing nothing is not acceptable for a "risk-free"
sandbox.

### D11 — Merge should leave an audit trail.

`MERGE` drops the branch and its change-log; after a successful merge nothing records what was
merged.

*Rationale.* The agent-curation use case is explicitly "a human reviews what the agent did before
it becomes real" (§1), and the future ownership story (D4) assumes an auditable "who changed what,
who approved it." Deleting the change-log on merge destroys exactly that artifact — an internal
inconsistency between the motivation and the mechanics. v1 should retain an immutable merge record
(at minimum who/when/branch and a change summary; ideally the full change-log). Product decides the
fidelity; telemetry counters (§8) are not a substitute.

### D12 — Access control: FGA on branch reads, no per-branch ACL in v1, system DB not branchable.

A single `BRANCH` privilege (D4) lets any holder merge/drop any branch, and it is unstated whether
fine-grained access control filters branch reconstruction.

*Rationale.* Per-branch ownership/sharing is legitimately Beyond-MVP (D4), and that is acceptable
for the first demos — but two sub-points are not deferrable. First, a branch read **must** apply
the same label/property access filtering as a query on `main`; otherwise versioning becomes an FGA
bypass (a user reconstructs a branch and sees data they are denied on `main`). Second, the system
`memgraph` database must be non-branchable, because its auth/multi-tenancy state is not in
per-database graph storage. Product confirms FGA-applies-to-branches (recommended yes) and
system-DB-not-branchable.

### D13 — A compare/history surface, or an explicit acknowledgement of its absence.

§1 sells "compare the projected impact" of scenarios, but v1 offers no branch-vs-branch compare and
no commit-grouped history.

*Rationale.* Users coming from git expect `diff a b` and `log`. v1's `SHOW BRANCH DIFF` is a
one-branch change-log, and finding the `txn_ts` for `REVERT` means eyeballing a flat delta table.
At minimum the spec must state that v1 comparison = run the same query under two `USING VERSION`
targets and diff client-side; better, add a commit-grouped `SHOW BRANCH LOG` (Beyond-MVP) so
`REVERT` has a usable companion. Product sets the line.

### D14 — Resolve or consciously accept the naming collisions.

`main` vs the replication **MAIN** role; `MERGE BRANCH` vs Cypher `MERGE`; `SHOW VERSIONING GRAPH`
vs `SHOW VERSION`.

*Rationale.* These are cheap to change now and expensive later (docs, drivers, muscle memory). The
`MERGE` collision is the sharpest — a Cypher-fluent user reads `MERGE` as idempotent upsert, the
opposite of a one-shot destructive fold — and unlike `CREATE BRANCH`/`DROP BRANCH` there is no
`MERGE DATABASE` to anchor it. The `main`/MAIN clash matters specifically because the audience is
HA/replication-heavy. Product either keeps the names (and the docs must explicitly disambiguate) or
renames before the surface ships; flagging while the surface is still unshipped.

### D15 — A branch is data-plane only; all background machinery is `main`-only.

No periodic/background subsystem runs on a branch: GC, periodic snapshots, WAL rotation, async
index build, TTL, streams, and after-commit triggers operate on the parent `Storage` only. A
branch has no threads and no snapshot of its own.

*Rationale.* This is the concrete form of "a branch is not a throwaway database" (A.5). A branch is
a passive overlay reconstructed on demand, not a `Storage` instance — so it never instantiates the
per-`Storage` background threads (`gc_runner_`, `snapshot_runner_`, WAL rotation, `async_indexer_`)
or the `Database`-level features (TTL, streams, triggers) that a real tenant carries. Beyond keeping
the feature small, this is what makes the memory/GC model tractable (Appendix A.6): with no
persistent per-branch state and no per-branch threads, there is nothing to GC on a branch and
nothing to evict in v1 — the only cost is the fork-point pin on `main`'s GC (R13) and the transient
per-query reconstruction. It also means branch data is captured into the branch's change-log at
write time but is **never snapshotted**; durability of the base is entirely `main`'s snapshot+WAL.
(The Beyond-MVP materialized "hot branch" is the one exception that would hold resident state and
therefore need its own eviction — reusing hot/cold, A.5.)

### D16 — Full-text and vector search are not branch-aware in v1.

Text (Tantivy) and vector indices are per-database shared structures that are not filtered by MVCC
visibility (Appendix A.7), so they cannot present a branch's fork-point-plus-overlay view.

*Rationale.* Unlike the label/property/edge indices — which are candidate-generators re-checked
against the delta chain at the transaction's timestamp, and so naturally serve a historical +
overlay view — full-text and vector search return whatever is committed in the single shared per-DB
index. A branch view therefore cannot get branch-correct search results without a branch-scoped
search index (a large project). Worse, the vector index is updated READ_UNCOMMITTED in the write
path, so a branch reconstruction's half-built vertices are briefly visible to concurrent vector
queries on `main`. v1 scopes these out: full-text/vector search on a branch is unsupported (reflects
`main`), and the reconstruction path must not feed the shared vector index. Product confirms, or
funds branch-scoped search.

---

## 7. Replication and high availability

**Out of scope for v1 (D6).** Versions live in a single Memgraph instance; they are not
replicated to replicas and are not preserved across failover/promotion. A REPLICA-role instance
does not see the MAIN-role instance's branches, and a promoted instance starts with no branches.
(Note the terminology clash flagged in D14: the replication **MAIN**/**REPLICA** *roles* are
distinct from the base *version* named `main`; this section means the instance roles.)

When version replication is picked up (a recognized future item, §10), the natural path is
the **system-replication** channel that `CREATE` / `DROP DATABASE` already use: a mutating
branch operation becomes an ordered system action, streamed to replicas by UUID (so it is
tenant-scoped on the replica), validated against the replica's system clock, and converged by
the same full-state recovery mechanism that reconciles the tenant set. High-frequency change-log
data, by contrast, would be a better fit for the per-storage data (WAL) channel — which is
convenient, because a branch's change-log already *is* WAL data (Appendix A.1). This is recorded
here so the future design does not start from scratch; it is **not** part of v1. Details in
Appendix A.3.

---

## 8. Observability

Anonymous aggregate telemetry (respecting the existing telemetry opt-out) records enough to
understand adoption and health without exposing any data:

- versioning enabled (bool);
- number of branches created / merged / dropped (lifetime and current);
- maximum change-log length observed and oldest live fork point (retention depth);
- number of merge-conflict rejections;
- number of retention-cap rejections;
- number of `USING VERSION` queries.

**No branch names, no query text, and no graph data are collected.** The change-log-length and
retention-depth signals in particular give early warning of the R1/R13 scaling ceilings in the
field (a fleet trending toward the cap, or holding old fork points while `main` churns, tells us
where the incremental-overlay / smarter-retention investment would pay off).

Operator-facing visibility is via the query surface itself: `SHOW BRANCHES` (what versions
exist), `SHOW BRANCH` (where am I), and `SHOW BRANCH DIFF` (what changed).

**Prometheus / `SHOW METRICS INFO` metrics (IMPLEMENTED, v1).** Six GLOBAL metrics under group
`Versioning`, exposed automatically on the OpenMetrics `/metrics` scrape and in `SHOW METRICS INFO`
+ the JSON metrics endpoint:

| Prometheus name | `SHOW METRICS INFO` row | Type | Meaning |
| :--- | :--- | :--- | :--- |
| `memgraph_versioning_branches_created_total` | `VersioningBranchesCreated` | Counter | branches created (lifetime) |
| `memgraph_versioning_branches_dropped_total` | `VersioningBranchesDropped` | Counter | branches dropped (lifetime) |
| `memgraph_versioning_branches_merged_total` | `VersioningBranchesMerged` | Counter | branches merged into `main` (lifetime) |
| `memgraph_versioning_branch_commits_captured_total` | `VersioningBranchCommitsCaptured` | Counter | branch commits captured to a change-log (lifetime) |
| `memgraph_versioning_active_branches` | `VersioningActiveBranches` | Gauge | branches currently existing |
| `memgraph_versioning_active_checkouts` | `VersioningActiveCheckouts` | Gauge | branches currently checked out |

`active_branches` doubles as an early-warning signal for the R13 retention ceiling (branches held
open while `main` churns). Not yet exposed (candidate follow-ups): change-log length / oldest-live-
fork-point retention-depth gauge, merge-conflict-rejection and retention-cap-rejection counters,
`USING VERSION` query counter, and a merge-latency histogram — noted as KPI/observability items in
Appendix D.

---

## 9. Guarantees and non-goals

**Guarantees (v1)**

- Reading or writing a branch never mutates the parent's stored data; only `MERGE` turns branch
  changes into real committed data (§2). `main` stays writable throughout. **Contingent on the D10
  write-routing rail** for pooled connections (open decision, §0/§4.1) — until that rail ships, a
  mis-routed pooled write can still reach `main`.
- A branch reads its parent as of the fork point: the parent's later writes are invisible to the
  branch until (and unless) the user rebases by re-branching. This holds regardless of the
  database's configured isolation level — the branch-reconstruction accessor is **hard-pinned to
  snapshot semantics at the fork timestamp** internally, so a database running under
  READ_COMMITTED/READ_UNCOMMITTED does not leak newer parent writes into a branch view (Skeptic
  A#7; the pin is part of the new historical-timestamp accessor, R16).
- A branch's registry record and change-logs are durable and survive restart (D6). **v1 status:
  branches survive restart AS RECORDS but cannot be checked out/merged until the fork-point base is
  re-materialized on startup — that recovery extension is deferred to v1.1 (fail-stop, not
  corrupting; see §10 "Restart / process durability" and D9).**
- Versioning state is per-database: a branch in database A is invisible from database B.
- Versioning is only ever active in in-memory transactional mode; it never silently runs in a
  mode where it could corrupt or lose data (D8).
- Merge is atomic-durable and conflict-checked: a merge either applies cleanly and drops the
  branch, or is rejected with a clear error, leaving both parent and branch unchanged (D3).
- Every versioning error names its cause and the corrective action (§4.4).

**Non-goals (explicitly out of scope for v1)**

- **`REVERT BRANCH COMMIT`, `SHOW VERSIONING GRAPH`, `SHOW BRANCH DIFF FORMAT GRAPH`,
  `FOR ALL DATABASES`** — deferred (D1, §4.3).
- **Full-text (Tantivy) and vector search against a branch view** — unsupported in v1; a branch
  query using them is **rejected with a clear error** (not silently answered from `main`'s index),
  because these indices are shared per-DB and not MVCC-filtered (D16, A.7).
- **Per-branch ownership, sharing, and ACLs; collaborative (multi-user) branches** — deferred
  (D4).
- **Automatic 3-way merge / rebase / conflict-resolution UI** — v1 detects conflicts and asks
  the user to redo on a fresh branch (D3).
- **Replication / HA of versions** — single-instance only (D6, §7).
- **Analytical or on-disk storage modes** — both unsupported; in-memory transactional only (D8).
- **Unbounded long-living branches** — a branch's size/age is capped to bound retention (D5).
- **Schema-plane branch changes** (creating indexes/constraints/enums on a branch) — v1 scopes
  branch changes to the data plane (Appendix B, R14). Enforced: schema-plane and storage-global DDL
  (indexes, constraints, enums, `TTL`, `ANALYZE GRAPH`, `DROP GRAPH`, `STORAGE MODE`,
  `RECOVER SNAPSHOT`) is **rejected while a branch is checked out**; the storage-destructive subset
  (`STORAGE MODE`, `RECOVER SNAPSHOT`, `DROP GRAPH`) is additionally **rejected while any branch
  exists** (R17), since it would strand every branch's fork-point base.
- **Parallel (chunked) scans on a branch** — the enterprise `ScanParallel*` operators are **rejected
  on a branch** (fail-loud). A branch's reads route through the union-aware single-threaded scans
  instead; a correct chunked union scan is out of scope for v1.

---

## 10. Known limitations and future direction

- **Retention / the "time-travel" cost (the big one).** A live branch pins its parent's history
  back to the fork point, so memory grows with the parent's churn while old branches live
  (Appendix A.4, R13). v1 caps branch size/age (D5). The clear next step is smarter retention
  (e.g. copy-on-write of just the objects a branch actually needs) plus an incremental/
  materialized overlay that also removes the per-query replay cost (R1).
- **Durable-file retention (inverted cleanup).** Durable branches (D9) require the parent's
  snapshot + WAL files back to the oldest live fork point to be *retained* against Memgraph's normal
  aggressive cleanup (R15) — the opposite of the usual "delete as much as possible." v1 pins them via
  the file retainer, bounded by D5; this is the same retain-to-rebuild behavior hot/cold COLD
  tenants already rely on (A.5).
- **Restart recovery must rebuild history to the oldest fork.** Branches are durable and survive
  restart (D9), but that needs a recovery extension: for a versioning-enabled database, recovery must
  rebuild `main`'s in-memory delta history back to the oldest fork (from the retained snapshot+WAL)
  so in-memory time-travel resumes — plain recovery only restores `main`'s current state (R16/R15).
  New engine work, bounded by D5.
- **Reusing tenant / hot-cold machinery.** A branch is deliberately *not* a throwaway database
  (A.5); a Beyond-MVP "hot branch" that materializes a heavily-queried branch to avoid per-query
  reconstruction (R1) would reuse the hot/cold suspend/resume machinery rather than reinvent it.
- **Per-query reconstruction cost.** Reading a branch replays its change-log on top of the
  fork-point base, costing O(change-log length) (R1); bounded by D5, removed structurally by the
  incremental-overlay work above.
- **`REVERT` and graph visualization.** Conflict-checked `REVERT BRANCH COMMIT`, the
  `SHOW VERSIONING GRAPH` version-tree view, and `SHOW BRANCH DIFF FORMAT GRAPH` are designed
  (§4.3) but deferred to Beyond-MVP.
- **Ownership, sharing, collaborative mode.** The "agent creates / human reviews & merges"
  workflow needs per-branch ownership and sharing/ACLs, and multi-user collaborative branches
  are a further step (D4). These are the most-requested follow-ups from customer conversations.
- **Replication / HA.** Versions are single-instance in v1 (§7); replicating them and
  surviving failover is future work — eased by the change-log being WAL data already.
- **3-way merge / rebase.** v1 detects merge conflicts and asks for a redo (D3); a real 3-way
  merge or a "rebase a branch onto the current parent" operation is future work.
- **Schema-plane changes.** Index/constraint/enum DDL on a branch does not preview/roll back as
  cleanly as data changes (R14); v1 scopes branches to data changes and either rejects or clearly
  documents schema DDL on a branch.
- **Storage-mode coverage.** v1 is **in-memory transactional only** (D8); analytical and on-disk are
  unsupported, out of scope for the feature. The edge representation is also configuration-dependent
  (`properties_on_edges`, light edges — R12); v1 grounds on the default heavy-edge configuration.
- **Core-engine work is required, not just wiring (post-skeptic reframe).** A code-grounded review
  refuted the "reuse existing MVCC/WAL for free" framing: the fork-point read, the WAL-format replay
  surface, the per-branch log, the create-collision-safe merge, and the bounded recover/retain-to-
  `fork_ts` are all **new** storage-engine work (R16–R21, R23). The model is sound; the effort and
  review surface are larger than a v1-as-wiring estimate would suggest.
- **Staleness is invisible.** A branch is pinned to its fork point and does not see later `main`
  writes (no rebase in v1), yet `SHOW BRANCH`/`SHOW BRANCHES` don't even expose the fork timestamp —
  so a long-lived what-if branch silently computes against stale `main` with no drift indicator.
  Exposing `fork_ts` / a drift measure, and a real rebase, are follow-ups (D13, Skeptic C#3).
- **Compare & history.** No branch-vs-branch (or branch-vs-`main`) compare and no commit-grouped
  history/log in v1 (D13); comparison is client-side via two `USING VERSION` runs, and finding a
  `txn_ts` for `REVERT` means scanning a flat delta table.
- **git-analogy day-one gaps.** No `log`/`stash`/`cherry-pick`/tags/`checkout -`; merging a branch
  with children requires manually merging/dropping children bottom-up (no cascade); management
  queries are autocommit-only, which trips ORMs/drivers that wrap statements in explicit
  transactions; and there are three independent startup preconditions (versioning flag, enterprise
  license, WAL — the last off by default) before the first `CREATE BRANCH` works (Skeptic C#6/#9).

---

## 11. Availability

Graph versioning is an **enterprise** feature (MEL). It is compiled behind the enterprise
build gate and additionally checked at runtime against the enterprise license, exactly like
multi-tenancy: in a community build the versioning queries are unavailable, and on an
enterprise build without a valid license they return a license error. On top of the license,
the feature is **off by default** and must be turned on with the startup-only flag
`--versioning-enabled=true`, and it is only active in `IN_MEMORY_TRANSACTIONAL` mode (D8). It is
**experimental** for v3.13.

---
---

# Engineering appendices

> The appendices below are **engineering grounding**, not product spec. They are the record of
> a systematic read of current master (`c312db1bc`) done to validate the design before
> implementation, plus the risk register, build order, and MVP definition-of-done. Per
> `specs/README.md` this material would normally live in an ADR (`ADRs/`); it is retained here
> *for now* so nothing is lost, and is a candidate to migrate into one or more ADRs once the
> design settles. Every claim carries a `file:line` anchor into the `feat/versioning-v1`
> worktree (which is on clean master `c312db1bc`).

## Appendix A — Architecture grounding (current master `c312db1bc`)

**Headline finding: the overlay/replay model is *native* to Memgraph's engine, not a bolt-on.
The change-log is the WAL, rollback is `Abort()`, time-travel is snapshot isolation, and merge
conflict-detection is the ordinary write-conflict check — three constraints the POC underweights
(id/gid handling, edge-config, retention) must still be designed in.**

### A.1 The change-log reuses the WAL *record format*; the surrounding machinery is new work

> **Correction after code-grounded skeptic review (2026-07-09).** An earlier draft of this appendix
> claimed the design "reuses existing MVCC/WAL machinery for free." A source-grounded audit (twice
> cross-confirmed) **refuted the literal-reuse framing** on four points — the *model* is right and
> the record formats and precedents are reusable, but each pillar needs **new core-storage engine
> work**, not wiring of existing entry points. These are now risks **R16–R19** and the build order
> (Appendix C) reflects them. Specifically: (i) there is no primitive to open a transaction at an
> arbitrary past `fork_ts` — `CreateTransaction` always stamps the current clock (R16); (ii)
> `LoadWal` mutates the live storage containers in place and cannot replay into a scratch target
> (R20); (iii) the WAL is one contiguous, sequence-checked stream per storage, so a
> branch's log reuses the WAL *format* but is a separate log, not "the WAL" (R21); (iv)
> `CreateVertexEx`/`CreateEdgeEx` bypass conflict detection and are single-writer-only, so
> create-vs-create merge collisions are not caught by `PrepareForWrite` (R19). Read the rest of this
> appendix as "the right model, modeled on these components," not "already implemented."

Memgraph's **WAL record format is a durable, versioned, replayable encoding of forward, materialized
changes** — exactly the shape the branch change-log needs (the branch log reuses this format, in its
own per-branch stream). `WalDeltaData` (`wal.hpp:439`) is a
`std::variant` of forward ops: `WalVertexCreate/Delete/AddLabel/RemoveLabel/SetProperty`,
`WalEdgeCreate/Delete/SetProperty`, `WalTransactionStart/End`, plus index/constraint/enum ops.
`EncodeDelta` (`wal.hpp:487`, impl `wal.cpp:1118`) converts an MVCC undo-`Delta` into a *forward*
WAL record at commit time — so the "MVCC deltas are undo records, we need forward records"
problem is *already solved* by the existing WAL writer. `ReadWalDeltaData` (`wal.hpp:478`) decodes
and `LoadWal` (`wal.hpp:559`, `wal.cpp:1262`) replays. Values are materialized (a
`WalVertexSetProperty` carries the concrete value, not a query), so replay is deterministic —
answering the "a query log isn't replayable" concern: the change-log stores results, not calls.

The other primitives are equally native:

- **Rollback ("reconstruct branch view, then discard") = `Abort()`**
  (`inmemory/storage.cpp:1483`). It walks `transaction_.deltas` and restores each object to its
  pre-transaction state; for a read-only replay it is a **no-op fast path**
  (`inmemory/storage.cpp:1493`), and the accessor destructor auto-aborts
  (`inmemory/storage.cpp:690`) so even an exception mid-replay leaves the parent byte-for-byte
  clean.
- **Time-travel to the fork point = snapshot isolation.** A transaction reads each object as of
  its `start_timestamp` by walking the delta chain in `ApplyDeltasForRead` (`mvcc.hpp:32`)
  honoring `View::OLD/NEW`. Pinning a branch to its parent's fork commit-timestamp and
  reconstructing the parent's view as-of that timestamp is exactly this mechanism — which is why
  the parent can stay live (D2).
- **Write-conflict detection happens at MERGE ONLY — never during branch work. This is the hinge
  that separates the replay overlay from the rejected "live transaction" (Option 1), and it turns
  entirely on *where the branch's deltas go*:**
  - *During branch work* (reads and branch-local writes), **`main` is read-only to the branch.** The
    branch **reads** `main` time-travelled to `fork_ts` via `ApplyDeltasForRead` (`mvcc.hpp:32`); and
    — **verified: every `PrepareForWrite` call site is a write path** (`vertex_accessor.cpp:191,203,
    265,277,425,511,580,639`, `edge_accessor.cpp:194,261,315,360`, `storage.cpp:407,509,590,678`),
    **none are reads** — so time-travelling past `main`'s newer commits can never raise a conflict.
    The branch's **own writes are applied to a branch-private overlay** (its own delta space /
    change-log, keyed by gid), **never onto `main`'s shared object chains.** Because a branch never
    writes a shared object, `PrepareForWrite` is **never invoked against `main`** during branch work
    → the branch's writes cannot conflict with `main`'s newer commits, and the branch never leaves a
    live delta that could block `main` or another branch.
  - *At MERGE*, the branch's WAL is replayed onto **current** `main` in a real committing transaction
    snapshotted at `fork_ts`; now `PrepareForWrite` (`mvcc.hpp:112`) *does* fire for any object `main`
    committed after the fork that the branch also changed — which is exactly the intended
    **redo-on-conflict** (D3, R9). Conflict detection at merge is the feature; conflict during branch
    work would be the bug.
  - **Implementation hazard — do NOT build it this way:** if the reconstruction applied the branch's
    deltas to `main`'s *live* objects (normal write path, fork-pinned transaction), it would (a) hit
    `PrepareForWrite` serialization errors on the branch's own writes to any object `main` moved since
    the fork, and (b) leave live uncommitted deltas that block `main` and other branches — i.e. it
    would silently *become* Option 1. Branch deltas must stay in the private overlay (R35).
- **Delta replay has a working precedent: `ReplicationAccessor`** (`inmemory/storage.hpp:1046`).
  It already re-applies a WAL-format delta stream into a live accessor using explicit-Gid creates
  (`CreateVertexEx`/`CreateEdgeEx`, `inmemory/storage.hpp:724,726`) and a controllable commit
  timestamp. **Merge = replay branch WAL onto current `main` then `PrepareForCommitPhase`
  (conflict-checked); branch-work read = time-travel `main` (read-only) + apply the branch overlay
  privately, then discard** — never a write to `main`.

**Consequence:** a branch's change-log should *be* a WAL segment, reusing the existing writer,
`ReadWalDeltaData`/`LoadWal` reader, and the recovery/replication apply path — not a bespoke
`OverlayDelta` format. Capture becomes "tee the WAL the storage already emits on commit."

### A.2 A branch must be an overlay, never a clone

`utils::SkipList` (`utils/skip_list.hpp:641`) is move-only, copy-deleted (`skip_list.hpp:1228`)
with no O(1) structural share; `InMemoryStorage` holds `SkipListDb<Vertex> vertices_` /
`SkipListDb<Edge> edges_` keyed by `Gid` (`inmemory/storage.hpp:870,898`). Forking a branch as a
physical copy would be an O(N) deep copy of every node — non-viable. And `Database` owns exactly
**one** `Storage` (`database.hpp:279`), so "branches as separate storages" doesn't fit the model
either. Therefore a branch is a **fork-point snapshot of the shared base + a WAL-segment
change-log**, reconstructed at read time; because SkipList iteration is gid-ordered, the branch
read is a streamable **merge** of the ordered base scan (time-travelled to the fork point) with
the ordered change-log. Reconstruction happens **per query** — the source of the R1 cost — and
the fork-point base is what forces retention (R13).

### A.3 Where branch state lives (grounded layering)

The existing ownership boundaries invite a clean split:

- **Per-tenant branch store on `Database`** (`dbms/database.hpp:61`) — a
  `versioning::VersionStore` member alongside `trigger_store_`/`streams_`/`plan_cache_`
  (`database.hpp:279-283`), holding the registry and each branch's WAL-segment change-log. This
  gives tenant isolation *for free* (a branch in DB A is unreachable from DB B because you only
  reach it through that tenant's `DatabaseAccess`), refcount-safety (the `Gatekeeper` won't
  destroy the `Database`/version store while any accessor is live, `database_handler.hpp:29-36`),
  recovery-with-the-DB (durability under `databases/<uuid>/…`, following the
  `RestoreTriggersFor`/`RestoreStreamsFor` choreography in `dbms_handler.hpp:768-797`), and — key
  for R10 — a **shared `NameIdMapper`**: branches on the same storage share the parent's id space,
  so captured WAL records replay with consistent label/property/edge-type ids.
- **Current-branch pointer per session** on `CurrentDB` (`interpreter.hpp:230`) — "which version
  am I on" is per-connection state (matches `CHECKOUT`'s per-connection semantics), while the
  branch *payload* lives on `Database`.
- **`DbmsHandler` stays the tenant registry** (`dbms_handler.hpp:134`); it should not hold branch
  payloads. `SHOW BRANCHES FOR DATABASE <db>` has no existing `FOR DATABASE` precedent except
  **SHOW STORAGE INFO** (`interpreter.cpp:7298`), which resolves the tenant via
  `dbms_handler->Get(db_name)` (a refcounted `DatabaseAccess`). **Correction (Skeptic B#12):** that
  handler does *not* call `GetDatabasePrivileges`, so it is only a resolution precedent, not an
  auth one — `FOR DATABASE` must add its own per-user privilege check (R24). `FOR ALL DATABASES`
  iterates `ForEach` under the shared `lock_`.
- **Future replication** (§7) follows the `CREATE`/`DROP DATABASE` system-transaction template:
  a `system::ISystemAction` subclass + a `…Rpc` carrying a UUID, `AddAction` on the system
  transaction, a replica handler validating `main_uuid`/`expected_group_timestamp` and registered
  behind `RejectIfNoLicense`, and a `SystemRecovery` extension for convergence.

### A.4 Why we pin the fork timestamp instead of freezing the base (D2)

A branch's view is *its base's state at the fork point + its forward changes.* An earlier draft
kept this well-defined by **freezing** the base (making `main`/parent read-only while descendants
existed). That is simple but breaks the feature: `MERGE` must write to `main`; a second branch is
stranded the moment the first merges; and the base can never advance to give different fork
points. The correct approach uses MVCC snapshot isolation (`mvcc.hpp:32`): the branch pins the
parent's commit timestamp and reads the parent *as of* it, so the parent stays writable and
merges are conflict-checked by the ordinary write-conflict path (A.1). The price is **retention**
— the parent's delta history back to the oldest live fork point cannot be reclaimed by GC
(`inmemory/storage.cpp:2983,3123`), so memory grows with the parent's churn while old branches
live (R13). That is the "time-travel" cost, bounded by D5, and it is a memory trade-off, not a
correctness one.

### A.5 A branch is not a throwaway database — but reuse the tenant and hot/cold machinery

A branch must **not** be implemented as "create a database, copy/replay into it, discard it." That
is a full copy (violating §2 and the explicit "no create-then-discard database" requirement) and
inherits a whole tenant's weight. A branch is an overlay on the *shared* base storage (A.2). What
*is* worth reusing from the multi-tenant and hot/cold subsystems:

- **Durability layout & lifecycle.** Per-tenant durability under `databases/<uuid>/…` and the
  `Gatekeeper` refcount lifecycle (`database_handler.hpp:29-36`) are the right places to hang a
  branch's durable change-log and to keep it alive while in use (A.3).
- **Hot/cold suspend/resume is direct prior art for two things v1 needs.** (1) *Retaining durable
  files rather than deleting them* — a COLD tenant keeps its snapshot + WAL so it can be rebuilt
  losslessly; versioning needs the same "don't delete the files a fork point depends on" behavior
  (R15). (2) *Materialize-on-demand / evict-when-idle* — hot/cold `Resume` rebuilds in-memory
  storage from durable files and `Suspend` tears it down. A **Beyond-MVP "hot branch"**
  (materializing a heavily-queried branch once, instead of reconstructing per query, to remove the
  R1 cost) is essentially "resume a branch from its base snapshot + WAL + its change-log, and
  suspend it when idle" — it should reuse that machinery rather than reinvent it.
- **The base is read by in-memory MVCC time-travel — never rebuilt from disk.** v1 reads the parent
  as-of the fork point by walking `main`'s **live** in-memory delta chains (`mvcc.hpp:32`), with the
  `fork_ts` pin (R13) keeping that history alive. **Nothing about `main` is materialized or copied** —
  this is the whole reason to pin, and it is what distinguishes the replay overlay from the tenant
  (Option 2). The only per-query cost is (i) the delta-walk to time-travel each *touched* object back
  to the fork (R34) and (ii) replaying the branch's own (bounded) change-log (R1); the base itself is
  free of duplication. Rebuilding the base from `main`'s durable snapshot + WAL is explicitly **not**
  done — that is the tenant-style duplication this design exists to avoid.
- **Durability & restart (branches DO survive restart).** In-memory-transactional storage is durable
  today via periodic snapshots + WAL, and branches use that same durability — they are **not** an
  in-memory-only, lost-on-restart concept. Durable state = the branch registry + each branch's
  change-log, plus `main`'s snapshot+WAL **retained back to the oldest live fork** (R15). The subtlety
  is that plain recovery rebuilds only `main`'s *current* state (each object returns with a single
  `DELETE_DESERIALIZED_OBJECT` base delta, `snapshot.cpp:13401`/`mvcc.hpp:253`) — no walkable history
  — so in-memory time-travel to a pre-restart `fork_ts` needs recovery, for a versioning-enabled
  database, to **rebuild `main`'s delta history back to the oldest fork** from the retained
  snapshot+WAL (a recovery extension, new engine work — tie to R16). That reuses the existing durable
  files and rebuilds `main`'s *own* history; it is **not** a per-query durable replay and **not** a
  separate duplicate tenant. So: steady state is in-memory time-travel (no duplication); restart pays
  a one-time history-rebuild bounded by how far back the oldest fork sits (bounded by D5).

### A.6 Memory & lifecycle model — durable vs in-memory, allocation, GC, tracking

This follows directly from D15 (a branch is a passive overlay with no background threads). It answers
"what is durable vs in-memory, what is allocated/freed, and how is memory tracked."

**What is durable (source of truth, on disk):**
- the branch **change-log** — WAL-format, append-only, written at each branch-commit (R21);
- the **registry** row `{name, number, parent, fork_ts, description}`.
That is all. A branch has **no snapshot of its own** (D15); its base is `main`'s **live in-memory
state, time-traveled to `fork_ts`** — the durable snapshot+WAL are consulted only to rebuild that
in-memory history at restart, never per query.

**What is in-memory (v1 = ephemeral):**
- only the **transient per-query reconstruction**: the base is read from `main`'s already-resident
  chains via time-travel (**`main` is read-only — nothing copied, nothing written**), and the branch's
  change-log is applied on top in a **branch-private overlay** (a scratch delta space keyed by gid,
  *not* `main`'s shared objects), then **discarded** when the query ends. The branch's writes never
  touch `main`, so `PrepareForWrite` is never invoked against `main` during branch work (A.1) — no
  conflict, no blocking.
- Between queries, nothing branch-specific is resident. (The Beyond-MVP "hot branch" is the exception:
  a persistent materialized reconstruction that trades this per-query rebuild for resident RAM and
  therefore needs eviction — reuse hot/cold, A.5.)

**GC:** There is **no per-branch GC** in v1, because there is nothing persistent to collect. Branch
writes live in the per-query private overlay, which is **discarded** when the query ends, so the
overlay's objects are reclaimed immediately; they never commit to `main` and never sit on `main`'s
chains. Only `MERGE` commits branch changes to `main`, after which `main`'s normal GC
(`gc_runner_`, `storage.cpp:940`) owns them like any other committed data. The **one** GC interaction is the *pin*: a live `fork_ts` holds `main`'s
`oldest_active_start_timestamp` (`storage.cpp:2983,3123`), preventing reclamation of `main`'s versions
older than the oldest branch's fork point (R13). That is a pin on `main`'s garbage, not garbage of the
branch's own.

**Allocation / deallocation:**
- **Base graph:** shared; already allocated by `main`'s storage in its per-DB jemalloc arena
  (`DbAwareAllocator`, `utils/db_aware_allocator.hpp`). Reconstruction *reads* it via time-travel and
  allocates nothing for the base.
- **Reconstruction scratch** (branch-created/modified objects + their deltas): allocated in the
  **parent DB's arena** during the query and **freed on `Abort()`** through the existing
  transaction-abort deallocation path. v1 needs **no new allocator** — it reuses the tenant's arena
  and the abort/GC free path.
- **Durable change-log:** streamed file I/O; negligible resident RAM.

**Memory tracking:** The reconstruction allocates through the parent DB's arena and is accounted by
the **per-DB query memory tracker** (`QueryAllocator`/`ThreadSafeQueryAllocator` +
`ResourceWithOutOfMemoryException`, `interpreter.hpp:47-114`). So a branch's replay counts against the
parent DB's memory and the query memory limit — a large branch transiently materializes its whole
overlay and can hit that limit (R25), which must fail cleanly (OOM exception, query rejected) rather
than crash. The retained fork-point history (R13) is `main`'s memory, tracked against the parent DB.
**Open sub-question (Beyond-MVP):** a materialized hot branch holds resident memory — is it tracked
per-branch or lumped into the DB, and what evicts it? Deferred with the hot-branch work (A.5).

### A.7 Live derived structures — how a branch reuses main's indices without writing or cloning them

The overlay reconstructs graph *data* by time-travel, but indices, constraints, the edge-metadata
index, schema info, enums, and id mappers are **live derived structures**. Because a branch **never
writes `main`'s structures** (R35), reuse works in three parts — not the "insert-into-main-and-abort"
model an earlier draft described (that would write `main`'s indices, R35):

- **Index *contents* — reused READ-ONLY, filtered to `fork_ts`.** An MVCC-family index scan (label
  `inmemory/label_index.cpp:219`, label-property `:650,755`, edge-type, edge-type-property,
  edge-property) is a *candidate generator + per-object visibility re-check* against the delta chain
  at the reader's view (`indices_utils.hpp:412`, e.g. `HasLabel`). The branch **reads** `main`'s live
  index and re-checks each candidate at `fork_ts` → it recovers `main`'s fork-state for that label
  with **no content clone and no write to `main`**; the fork pin (R13) keeps the fork-needed entries
  alive.
- **Index/constraint *catalog* — as-of `fork_ts` (a small definitions snapshot, not contents).** A
  branch sees the **schema that existed at its fork**. The subtle correctness rule (R36): **an index
  created *after* the fork is unusable by the branch** — it was populated from post-fork state and
  lacks entries for label/property states that ended before it existed, so a `fork_ts` scan of it
  would be *incomplete*. The branch uses only fork-era indexes (correct + complete under the re-check
  above) and otherwise falls back to a scan; an index dropped after the fork is gone → scan. This
  needs the fork-era catalog of index/constraint *definitions* (from the metadata-delta log) — cheap,
  not a content clone.
- **The branch's OWN changes — reconciled against the overlay, not indexed in `main`.** The branch's
  objects live in the private overlay (not `main`'s SkipList), so `main`'s index can't reference them
  without polluting concurrent `main` scans. A branch index scan therefore **reconciles**: take
  `main`'s candidates (filtered to `fork_ts`), override/remove per the overlay by gid, and **scan the
  (D5-bounded) overlay** for the branch's own additions/matches. No private index in MVP (a private
  index, or Option 3's native branch-aware index, is the Beyond-MVP optimization); the cost is
  per-query, proportional to the overlay (R33), degrading toward a full scan as a branch grows.

Master maintains the underlying structures under **three disciplines** (verified), which is what makes
the read-only reuse above sound:

- **(A) eager-into-shared + MVCC re-check on scan.** Label / label-property / edge-type /
  edge-type-property / edge-property indices — a scan is a candidate set re-checked by the delta-walk
  at the reader's view, so a read at `fork_ts` is correct. (The **vector** index is mechanically here
  but is READ_UNCOMMITTED with **no** visibility filter on `SearchNodes` → not branch-safe; R28/D16.)
- **(B) collect-during-tx, install-at-commit.** Point index (`storage.cpp:1342`), text (Tantivy),
  unique constraint (`unique_constraints.cpp:335`), and **`SchemaInfo` (TRANSACTIONAL)** — never
  touched by an in-flight reader, so no pollution.
- **(C) eager-into-shared, no MVCC.** `EnumStore`, `NameIdMapper`, analytical `SchemaInfo` — leak or
  corrupt if written by a branch (see below); branches don't write them (schema/enum DDL banned, R14).

**Reuse verdict (v1): reuse index *contents* read-only + snapshot the fork-era *catalog* + reconcile
the overlay — NO main-scale duplication, but NOT zero.** The only branch-specific state is a small
fork-era catalog (index/constraint definitions) plus the delta-scale overlay — versus the tenant's
full main-scale clone of every object *and* every index. Per-structure handling:

| Structure | Discipline | Reused safely? | Handling in v1 |
|---|---|---|---|
| Label / label-property index | A | **Read-only reuse** | Contents reused + visibility-filtered to `fork_ts`; branch's own changes reconciled against the overlay (not inserted into `main`); only fork-era indexes usable (R36). |
| Edge-type / edge-type-prop / edge-prop index | A | **Read-only reuse** | As above (R33 reconciliation cost, R36 fork-era-only). |
| Point index | B | **Yes** | Reused (minor: a branch won't see its own overlay point-writes mid-tx — read-your-writes gap, not pollution). |
| Existence / type constraint | validate-only (read) | **Yes** | Preview never validates (abort); a preview that wants to report violations must call the validator itself. |
| `SchemaInfo` (TRANSACTIONAL) | B | **Yes** | Per-tx diff; concurrent `SHOW SCHEMA INFO` never sees branch state. |
| `NameIdMapper` | C | **Yes (benign)** | An aborted/dropped branch's new names persist, but name→id is content-stable so it can't mis-resolve — only id-space growth (R21-adjacent, R10 non-issue). |
| Storage counters (`vertex_count`/`edge_count`) | eager, abort-restore | **Yes** | Self-heal on abort; transiently dirty-readable via `GetInfo` (no MVCC filter) during reconstruction (R31). |
| **Vector index** | A, but READ_UNCOMMITTED | **No (pollutes)** | Unwound on abort, but visible to concurrent `main` vector queries during reconstruction, and no branch-scoped view → **branch vector search unsupported** (D16); reconstruction avoids feeding it. |
| **Text index (Tantivy)** | B | **Abort-safe, but not filtered** | No branch-scoped full-text view (returns `main`'s committed index) → **branch full-text search unsupported** (D16). |
| **Unique constraint** | B | **Abort-safe, but merge-only** | Global skiplist validated at *merge* against latest-committed `main` at a fresh commit-ts, not the fork-point/overlay (R30); preview never validates. |
| **Edge-metadata index** | eager, no MVCC | **Abort-safe, but crashes** | Hard-asserts on gid collision → replay/merge **must** assign globally-unique edge gids (R11/R30-adjacent). |
| **`EnumStore`** | C | **No (corrupts)** | Positional enum-value ids, not rolled back → an aborted branch permanently corrupts mainline enum identity → enum DDL on a branch is **banned** (R14/R27). |
| **`SchemaInfo` (ANALYTICAL)** | C | **No** | Mitigated by D8 (analytical rejected); confirm schema tracking never runs in analytical for versioning. |

**Fine-grained access control (FGA) applies to branch reads for free (code-verified).** FGA is
enforced at the **cursor level** and is accessor-agnostic — `context.auth_checker->Has(vertex,
view, READ)` in the ScanAll/expand cursors (`src/query/plan/operator.cpp:1110`) — and a branch
reconstruction runs the user query through the same cursor path, so the same label/property
filtering that applies on `main` applies to a branch view automatically (D12). Two things still
need explicit handling: (a) a **test** that a user denied a label on `main` cannot see it via a
branch; (b) whether `MERGE` **re-validates** the *merging* user's write privileges against the
objects being folded in (vs. trusting the check done when the branch write originally happened) —
recommend re-validating at merge. Both belong in the DoD (Appendix D).

---

## Appendix B — Risk register

Severity reflects impact × likelihood for v1. R9–R14 were surfaced by the master grounding; R9
and R3/R10 are substantially eased by the pinned-fork (D2) and WAL-foundation (A.1) decisions.

| # | Risk | Severity | Mitigation (v1) |
|---|---|---|---|
| R1 | **Per-query reconstruction cost is O(change-log length).** Every branch operation — read **and write** (R26) — time-travels the base to the fork point and replays the change-log (A.2). | High | Cap branch size/age (D5); publish the measured latency curve (Appendix D); the materialized hot branch (A.5, Beyond-MVP) removes the per-op replay. |
| R2 | **Locking / serialization.** The POC used one global recursive mutex for all versioning; commit-path work is already serialized by `engine_lock_`. | Med | v1: per-`Database` locking (state is per-tenant, A.3), not one global lock. Versioning is opt-in, low-QPS control-plane; document. |
| R3 | **Durable change-log format.** — *Largely dissolved by A.1:* the change-log is a WAL segment, and the WAL is already a versioned (`kVersion`), CRC-checked format with a maintained reader/writer. | Low | Reuse the WAL format + `LoadWal`/`ReadWalDeltaData`; inherit its existing cross-version handling; add a versioning-specific recovery test. |
| R4 | **Enterprise leak** — versioning usable on a community/expired license. | High | `MG_ENTERPRISE` compile gate + `IsEnterpriseValidFast()` runtime check (mirror multi-tenancy, `interpreter.cpp:7847`) + enterprise-leak test in the DoD. |
| R5 | *(retired)* **Frozen-main UX surprise.** — Removed: D2 no longer freezes `main`. | — | N/A. The replacement cost is retention (R13), not a UX freeze. |
| R6 | **Query-path blast radius** — the POC added ~1171 lines to `interpreter.cpp`; risk of regressing the hot path when versioning is *off*. | Med | Zero-cost when the flag is off; benchmark the flag-off path vs master as a no-regression gate; keep dispatch out of the per-Pull inner loop. |
| R7 | **Per-tenant durability recovery** — registry + change-logs must recover consistently with each database and survive a crash around merge. | Med | Per-database recovery tests; crash-consistency test bracketing `MERGE`; store under `databases/<uuid>/…` (A.3). |
| R8 | **Capture completeness** — every mutating op must map to a forward WAL record or the branch view silently diverges from the merge result. | High | Reusing the WAL (A.1) means the ops are the WAL's own set; still add a property/fuzz test: random writes on a branch, assert `branch-view == merged-result` for every op type, incl. cascade edge-delete and edge-property (R12). |
| R9 | **Merge reconciliation against a *moved* parent (this is the ONLY place conflicts occur).** At merge, replaying the branch onto *current* `main` in a transaction snapshotted at `fork_ts` makes `PrepareForWrite` (`mvcc.hpp:112`) reject collisions automatically — desired (D3). During branch work there is no such check (A.1: `main` is read-only, branch writes go to a private overlay), so this fires at merge only. | Med | Rely on the native conflict check *at merge*; reject-and-redo (D3). Test concurrent parent activity vs merge; verify the serialization-error path yields the D3 error, not a partial merge. |
| R10 | **Id-mapper divergence — non-issue in v1.** A branch lives on the *same* `Storage` as its parent, so there is only ever **one** `NameIdMapper`; the earlier "shared mapper mitigation" was vacuous (it protects against a divergence that cannot occur in this design). Ids only diverge if a change-log is ever serialized and replayed against a *different* storage. | Low | No action in v1 beyond keeping branches on the parent's storage. Only revisit if change-logs move across storages (e.g. future cross-instance replication, §7). |
| R11 | **Gid collisions / dangling endpoints on replay.** Gids come from monotonic per-storage counters (`storage.hpp:432`); replaying branch-created objects must not collide with gids the parent handed out since the fork, and edge endpoints (`from_gid`/`to_gid`) must resolve. | High | Define a gid strategy: reserve/allocate fresh gids on merge and remap endpoint references consistently across the change-log, or reserve a per-branch gid range at fork. Test create-vertex-then-edge replay + merge. **Escalated (A.7): the edge-metadata index hard-asserts (`MG_ASSERT(inserted)`, `inmemory/storage.cpp:865`) on a gid collision — a collision is a process crash, not a silent bug — so globally-unique edge gids on replay/merge are mandatory.** |
| R12 | **Edge model is configuration-dependent.** Under `properties_on_edges=false` or `storage_light_edge=true` there is **no global `SkipList<Edge>`** — edges live only in vertex adjacency lists (`config.hpp:47,53`; `edge_ref.hpp` union). Also: **delete-vertex must cascade** edge-removal for every incident edge in both directions, and edge-property-set is a distinct op (the WAL has `WalEdgeSetProperty`, so this is covered by reusing the WAL). | High | v1 grounds on the heavy config (`properties_on_edges=true, storage_light_edge=false`) and documents it; rely on the WAL's own edge ops (which already cascade correctly at the storage layer). Test each edge mode or reject unsupported ones. |
| R13 | **Retention: a live branch pins the parent's history.** An open fork point keeps `oldest_active_start_timestamp` low (`inmemory/storage.cpp:2983,3123`), so GC cannot reclaim the parent's versions back to that point → memory grows with the parent's churn (the D2 trade-off, and now the central scaling risk). | High | Bound branch size/age (D5); expose retention depth via telemetry (§8); smarter retention (copy-on-write of only the objects a branch reads) is the key Beyond-MVP investment. Keep per-query reconstruction transactions short-lived so *reads* don't pin further than the fork point already does. Note this is the *only* GC interaction: there is no per-branch GC (nothing persistent to collect — branch writes abort per query; Appendix A.6). |
| R14 | **Schema-plane changes preview/roll back less cleanly than data — and enum DDL is a correctness hazard.** Index/enum/constraint DDL mutate storage-global state under the unique lock and via `md_deltas`. **Enum DDL specifically: enum value ids are positional (`EnumValueId{pos}`, `enum_store.hpp:41`) and `EnumStore` is eager-into-shared with NO abort unwind (A.7)** — an aborted branch's `CREATE ENUM`/`ALTER … ADD/UPDATE` permanently occupies/shifts/renames positions visible to mainline, corrupting enum identity. | Med→High | v1 scopes branch changes to the **data plane** and **rejects** schema DDL (index/constraint/enum) on a branch; for enums this ban is a correctness requirement (R27), not just tidiness. |
| R15 | **Durable-file retention conflicts with normal cleanup.** Master aggressively deletes old durable files — `DeleteOldSnapshotFiles` keeps only `snapshot_retention_count` (default 3) snapshots (`snapshot.cpp:13010`, `config.hpp:91`) and pre-snapshot WAL files are removed (`EnsureNecessaryWalFilesExist`, `snapshot.cpp:12910`; "deleting N pre-snapshot WAL file(s)", `snapshot.cpp:12967`). Surviving a restart (D9) needs `main`'s snapshot + WAL chain covering **every live fork point** so recovery can rebuild `main`'s history back to the oldest fork — which the default retention would delete. (This is a **restart-only** need; steady-state reads use in-memory time-travel, not these files — A.5.) | High | Make durability retention branch-aware: pin the snapshot + WAL files each live fork point needs via the existing `FileRetainer`, so cleanup skips them (the inverse of the usual "delete as much as possible" policy). Plus a recovery mode that rebuilds `main`'s delta history to the oldest fork (new engine work, R16). Bounded by D5. Durable-file analog of the in-memory R13 pin. |
| R16 | **No primitive to open a transaction at a past `fork_ts`.** `CreateTransaction` always stamps the current clock under `engine_lock_` (`inmemory/storage.cpp:2823-2836`); `ReplicationAccessor` overrides only the *commit* timestamp, never `start_timestamp`. The D2/D3/D9 "read/replay as of the fork point" all depend on a primitive that does not exist. | **Critical** | **Mechanism corrected after implementation-plan verification (2026-07-10, two agents cross-confirmed against code).** The GC horizon is `commit_log_->OldestActive()` (`inmemory/storage.cpp:2983`) over a **monotonic bitset with `MarkFinished` only — no reactivation op** (`commit_log.hpp:48-67`), so you **cannot** "register a past `fork_ts` as active." Correct design: the pin must be a **long-lived, non-finalizing transaction opened at `CREATE BRANCH`** (its `start_timestamp` *is* `fork_ts`, captured at the then-current frontier) and held until `DROP` — this reuses the ordinary live-transaction GC-pin exactly (`MarkFinished` at `:1363/:1876` is what a live txn defers). Equivalently, a dedicated **GC floor** modeled on the existing schema-update floor (`storage.cpp:2989-2998`), taking `std::min` at `:2983`, **defaulting to `UINT64_MAX`** when no branches exist (so flag-off DBs are untouched — see R38). The per-query historical read accessor then opens at `start_timestamp = fork_ts` **without** ever `MarkFinished`-ing that shared id (see R37). Consequence: the pin is a **branch-create/registry responsibility**, not a per-query accessor byproduct; validate `fork_ts` against the live horizon at open and reject if below it. (Skeptic B#1 + plan-verification C1, cross-confirmed.) |
| R37 | **Pin double-release: a per-query fork_ts read must not finalize the shared `fork_ts`.** Every `MarkFinished` call site is unconditional and non-refcounted (`inmemory/storage.cpp:1111,1127,1363,1876,1912`). A per-query reconstruction accessor that reuses `start_timestamp = fork_ts` and finalizes on its normal `Abort()`/empty-`Commit()` path (`:1127`) would call `MarkFinished(fork_ts)` and **prematurely release the branch's GC pin** while the branch is still live — a use-after-free of the fork history. | High | The `fork_ts` finished-bit is owned **exclusively** by the one long-lived pin transaction (R16). Per-query reads at `fork_ts` must use a finalization path that **never** touches `commit_log_` for that id. Test: N per-query reads+aborts on an idle branch leave `OldestActive() ≤ fork_ts` unchanged. (Plan-verification 1d.) |
| R38 | **Branch-private overlay has no container + needs a union read-path (missing engine primitive).** There is exactly one `SkipListDb<Vertex> vertices_`/`SkipListDb<Edge> edges_` (`inmemory/storage.hpp:870,898`); `VertexAccessor` wraps a `Vertex*` that must live in that skiplist, and all read paths bind to it. A branch that **modifies** an object `main` moved post-fork cannot replay onto main's live chain — `PrepareForWrite` raises a serialization error (the R35 trap). So the "branch-private overlay (A.1/A.6)" requires **its own delta-scale container** (gid-keyed; created + copy-on-write-modified branch objects — bounded by D5, **not** a main-scale clone, A.2/A.5) **plus a union read-path** (scan / index-candidate / gid-lookup / expansion / degree union of time-traveled-main + overlay). This is real new engine surface, and it is where the replay overlay's **read path** approaches Option-3 cost — the honest additional weight of v1. | High | Build the overlay container + COW + read-path union as an explicit primitive (implementation plan chunks 5a/5b), not "reuse existing scan re-check." The write side never touches main's chain (R35); the read side unions. (Plan-verification B1.) |
| R17 | **Storage-mode switch / `RECOVER SNAPSHOT` strands live branches.** `SetStorageMode(ANALYTICAL)` (`inmemory/storage.cpp:2861-2916`) and snapshot restore have no live-branch awareness; analytical has no MVCC deltas, so the fork-point base becomes unreconstructable, silently. | Med | Reject the switch/restore while branches exist (error in §4.4, §4.6). (Skeptic A#8/B#7.) |
| R18 | **Merge replays the whole change-log as one transaction**, defeating any `PeriodicCommit` the user relied on while building the branch (`interpreter.hpp:767`); the giant-transaction memory/lock cost returns at merge for exactly the branches big enough to have needed chunking. | Low-Med | Document; chunked/periodic merge is future work. (Skeptic B#10.) |
| R19 | **Create-vs-create merge collisions bypass conflict detection — and fail differently for vertices vs edges.** `PrepareForWrite` only guards modify-ops on pre-existing objects; `CreateVertexEx`/`CreateEdgeEx` (`inmemory/storage.cpp:717-745`) never call it. **Vertex** collision = silent `inserted=false` skip (data loss). **Edge** collision = **hard `MG_ASSERT` crash** (`storage.cpp:855`) — a process abort, the same crash class as R11's edge-metadata index. (Mechanically race-free under concurrent main writes — verified — so the exposure is purely logical, not a data race.) | High | Merge needs explicit gid-reservation/remap + a create-collision check for **both** types (ties R11); for edges this is load-bearing against a crash, not just silent loss. The "conflict detection for free" claim (D3) covers only modify-ops. (Skeptic B#4, cross-confirmed.) |
| R20 | **`LoadWal` is not a replay-to-scratch surface.** `LoadWal` (`wal.cpp:1262`) mutates the storage's own live `SkipListDb`/`NameIdMapper`/counts in place; the "replay branch log onto a preview accessor, then `Abort()`" plan (Appendix C) needs a new replay surface, not `LoadWal` as-is. | High | Build a WAL-format replay driver targeting a specific accessor/transaction; reuse `ReadWalDeltaData` decode + the accessor mutation API, not `LoadWal`. (Skeptic B#2.) |
| R21 | **The WAL is one contiguous, sequence-checked stream per storage** (`wal_seq_num_`, `inmemory/storage.cpp:3628`; recovery fatal on seq gap >1, `durability.cpp:738`). A branch's log therefore **reuses the WAL record format but is a separate per-branch stream** — it is not literally "the WAL", and it must not share main's sequence/directory or it corrupts main's recovery continuity. | Med | Per-branch log files reusing `EncodeDelta`/`ReadWalDeltaData` format under the branch's own directory; keep main's WAL stream untouched. (Skeptic B#3.) |
| R22 | **Branch reconstruction contends on the storage-wide access lock.** Index/constraint/enum DDL on the parent takes `UniqueAccess()` (exclusive over the whole `Storage`); heavy branch-read traffic + routine main-DDL can stall each other via a primitive the per-`Database` lock (R2) doesn't cover. | Med | Account for `UniqueAccess`/`SharedAccess` interaction in the design; measure. (Skeptic B#8.) |
| R23 | **Durable reconstruction to `fork_ts` is doubly unsupported.** There is no timestamp-*bounded* recovery (recovery only runs to latest; `LoadWal`'s timestamp is a lower skip-bound), and `FileRetainer` can pin files but has no "pin everything needed to reach ts T" convenience (`file_locker.hpp`). | Med | New recovery machinery for the D9/A.5(b) durable path: a bounded-replay recover-to-`fork_ts` + a retain-to-`fork_ts` helper over `FileRetainer`. (Skeptic B#11.) |
| R24 | **`FOR DATABASE` has no existing privilege-check precedent.** The cited `SHOW STORAGE INFO` path calls `dbms_handler->Get(db_name)` but **does not** call `GetDatabasePrivileges`; copying it inherits a *missing* per-user check, not a proven one. | Med | Implement the per-user DB privilege check explicitly for `SHOW BRANCHES FOR DATABASE`/`FOR ALL DATABASES`; do not assume the precedent exists. (Skeptic B#12.) |
| R25 | **Reconstruction memory spike vs the query memory limit.** A per-query branch reconstruction transiently materializes the branch's whole overlay in the parent DB's arena, tracked by the per-DB query memory tracker (`interpreter.hpp:47-114`); a large branch can exceed the DB/query memory limit — a real ceiling on branch size **independent** of D5's change-count cap. | Med-High | Must fail cleanly (`OutOfMemoryException`, query rejected), never crash or half-apply; document that branch size is bounded by memory as well as by D5; the materialized hot branch (A.5) amortizes this but shifts it to resident memory. (Appendix A.6.) |
| R26 | **Branch *writes* replay too, not just reads.** A write query on a branch (e.g. `MATCH … SET`) must first reconstruct the current branch view to run correctly, so the O(change-log length) cost (R1) applies to every branch operation, read or write — a branch gets steadily slower to write as it grows. | Med | Bounded by D5; the strongest argument for the Beyond-MVP materialized hot branch (removes per-op replay). Sharpens R1. |
| R27 | **`EnumStore` corruption from an aborted branch.** `EnumStore` is eager-into-shared with no abort unwind, and enum value ids are positional (`enum_store.hpp:41`), so a branch that ran enum DDL and then aborted/dropped would permanently shift mainline enum identity — a real correctness bug, not clutter. | High (if enum DDL allowed) | **Prevented by construction:** enum DDL is part of the schema-plane ban (R14/D15). Add a test that enum DDL on a branch is rejected before any `EnumStore` mutation. (Skeptic-follow-up A.7.) |
| R28 | **Vector index pollutes concurrent `main` readers during reconstruction.** The usearch vector index is updated READ_UNCOMMITTED in the write path (`vector_index.hpp:222`) with no `start_timestamp` filter on `SearchNodes` (`vector_index.cpp:432`), so a branch reconstruction's half-built vertices are visible to concurrent vector queries on `main`; there is also no branch-scoped/historical vector view. | Med | D16: a branch query using vector search is **rejected with a clear error** (not silently main-scoped); reconstruction must **not** feed the shared vector index (skip vector maintenance during branch replay). Test concurrent main vector query during a branch reconstruction. |
| R29 | **Full-text (Tantivy) search is not MVCC-filtered.** Text search returns the shared per-DB committed index ignoring `start_timestamp` (`text_index.cpp:286`), so a branch cannot get a fork-point/overlay full-text result. Abort-safe (commit-deferred), but a *merged* branch updates the shared index normally. | Med | D16: a branch query using full-text search is **rejected with a clear error** (not silently answered from `main`'s index). Branch-scoped text search is a large follow-up. |
| R30 | **Unique constraint is validated only at merge, against latest-committed `main`.** Validation runs at `PrepareForCommitPhase` against a fresh commit-ts using a single global skiplist (`unique_constraints.cpp:141`, `storage.cpp:1081`), not the branch's fork-point/overlay; preview never validates. | Med | Correct *for merge* (you are merging into live `main`), but document that uniqueness is enforced only at merge, not while working on a branch; a branch can transiently hold data that would violate a unique constraint until merge rejects it. |
| R31 | **Transient dirty reads of storage counters during reconstruction.** `vertex_count`/`edge_count`/label counts are bumped eagerly and read by `GetInfo` with no MVCC filter (`storage.cpp:3562`), so a concurrent `SHOW STORAGE INFO` on `main` sees the branch's uncommitted objects during reconstruction; self-heals on abort. | Low | Accept for v1 (transient, self-healing); note in docs. Same class as the vector/counter in-flight visibility. |
| R32 | **Backup/restore can silently lose all branches.** A branch's only durable artifact is its WAL-format change-log + registry (no per-branch snapshot, D15). Memgraph's documented backup flow says "if you just made a snapshot there's no need to back up WAL" (`backup-and-restore.mdx:49`), and `RECOVER SNAPSHOT` wholesale-replaces state — so an operator following the standard procedure, or restoring into a fresh instance, loses every branch **with no error**. R17's guard only protects the *live source* (rejects mode-switch/restore while branches exist), not a snapshot-only backup or a fresh restore target that has no branches registered to trigger the guard. | Med | Branch durability must ride the *same* backup unit as the tenant (include branch registry + change-logs in the backup set whenever branches exist; treat a branches-present database as WAL-backup-required), and document that snapshot-only backup drops branches. Product/ops decision + a backup-completeness test. (Skeptic A#5.) |
| R33 | **Per-query index reconciliation cost.** A branch never writes `main`'s indexes (R35); instead every branch index scan reuses `main`'s index read-only (filtered to `fork_ts`) and **reconciles** the result against the branch's overlay — override/remove per candidate by gid + a scan of the (D5-bounded) overlay for the branch's own additions (A.7). O(overlay) work per query, on top of the delta-walk; degrades toward a full scan as the branch grows. | Med | Bounded by D5; removed by a private branch index (Beyond-MVP) or Option 3's native branch-aware index. Include in the perf KPI curve. |
| R34 | **Index-scan divergence penalty.** A branch's index scan iterates the parent's *current* index for the label/property and filters out every entry committed after `fork_ts` via the MVCC re-check (A.7) — so branch scan cost grows with how much `main` has written *since the fork*, independent of the branch's own size. A small but long-lived branch against a busy `main` scans progressively slower (compounds with R13). | Med | Bounded by D5 (branch age); Option 3's branch-aware index removes it. Measure and document in KPIs; a signal to watch in telemetry (§8). |
| R35 | **The reconstruction must NOT write to `main`'s live objects — or it silently becomes Option 1.** The correct design keeps `main` read-only during branch work and applies branch deltas to a **private overlay** (A.1/A.6). If an implementer instead replays branch deltas onto `main`'s live objects via the normal write path on a fork-pinned transaction, they get (a) `PrepareForWrite` serialization errors on the branch's *own* writes to any object `main` moved since the fork, and (b) live uncommitted deltas that block `main` and other branches — i.e. the rejected "live transaction." | High | Make it explicit in the design + code review: branch-work writes target the private overlay only; the only path that writes `main` is `MERGE` (a committing transaction, conflict-checked). Add a test that a branch write to an object `main` changed after the fork succeeds on the branch (no serialization error) and does not block a concurrent `main` write. |
| R36 | **A branch must use the fork-era index/constraint catalog; indexes created after the fork are unusable.** A branch reads `main` as-of `fork_ts`, so it must see `fork_ts`'s *schema*, not current. An index created after the fork was populated from post-fork state and lacks entries for label/property states that ended before it existed — scanning it for a `fork_ts` query returns **incomplete** results (a correctness bug, not just a perf issue). Conversely an index dropped after the fork is gone. | Med-High | Snapshot the fork-era catalog (index/constraint *definitions*, from the metadata-delta log) per branch; branch query planning may use only fork-era indexes and falls back to a scan otherwise. This is a small (definitions-only) snapshot, **not** a clone of index contents. Test: create an index on `main` after a branch forks, then run a branch query on that label — must return the correct fork-state set (via scan), not the post-fork index's partial set. |

---

## Appendix C — Implementation grounding & build order

A layered plan. **Note (post-skeptic correction):** several steps below are *modeled on* existing
components but require **new core-storage engine work**, not reuse of existing entry points — the
new primitives are R16 (historical-timestamp accessor), R20 (WAL-format replay-to-accessor surface),
R21 (per-branch WAL-format log stream), R23 (bounded recover-to-`fork_ts` + retain-to-`fork_ts`), and
R19 (create-collision-safe merge). Each layer builds + tests before the next:

1. **Storage-mode + license + flag gate.** `--versioning-enabled` (`flags/general.*`),
   `MG_ENTERPRISE` + `IsEnterpriseValidFast()`, `IN_MEMORY_TRANSACTIONAL`-only reject (D8).
   Cheapest, unblocks everything.
2. **`versioning::VersionStore` on `Database`** (A.3) — registry (`name → {number, description,
   parent, fork_ts}`) + per-branch **WAL-segment** change-log, durable under `databases/<uuid>/…`,
   restored via the trigger/stream choreography. Per-`Database` lock (R2). Shares the parent's
   `NameIdMapper` (R10).
3. **Change capture into a per-branch WAL-format log** — reuse the WAL *encoder* (`EncodeDelta`,
   `wal.cpp:1118`) to write a branch's writes into its **own** log stream/directory (R21 — not
   main's shared, seq-checked WAL); the op set is the WAL's own, closing the capture-completeness
   gap (R8, R12) by construction.
4. **Reconstruction / replay engine.** *Branch work:* build the R16 historical-timestamp accessor
   (snapshot at `fork_ts`, registered with `commit_log_`) that reads `main` **read-only** via
   time-travel, plus an R20 WAL-format replay surface (decode with `ReadWalDeltaData`; **not**
   `LoadWal`) that applies the branch log into a **branch-private overlay** (its own delta space,
   **not** `main`'s live objects — R35), then discards it. `main` is never written and
   `PrepareForWrite` is never hit during branch work. *Merge (the only path that writes `main`):*
   replay the branch log onto *current* `main` in a committing transaction snapshotted at `fork_ts`;
   native `PrepareForWrite` catches modify-vs-modify collisions (R9/D3), the driver adds a
   create-collision check (R19) and aborts on the *first* serialization error (R5).
5. **Session state + query dispatch** — current-branch pointer on `CurrentDB`
   (`interpreter.hpp:230`); wire `CHECKOUT`/`USING VERSION`/`SHOW BRANCH*`/`MERGE`/`DROP` in the
   interpreter behind the flag with zero off-path cost (R6). Management queries autocommit-only;
   branch data writes allowed in explicit transactions (§4.1).
6. **`FOR DATABASE` reach** — use `SHOW STORAGE INFO`'s tenant-resolution pattern
   (`interpreter.cpp:7298`, `dbms_handler->Get`) but add an explicit per-user DB privilege check —
   that handler does *not* do one (R24).

**Reference map (master `c312db1bc`):** WAL change-log `storage/v2/durability/wal.hpp:439`
(`WalDeltaData`), `:478`(`ReadWalDeltaData`), `:487`(`EncodeDelta`), `:559`/`wal.cpp:1262`(`LoadWal`);
MVCC time-travel/conflict `storage/v2/mvcc.hpp:32`(`ApplyDeltasForRead`), `:111`(`PrepareForWrite`);
rollback `inmemory/storage.cpp:1483`(Abort); replay precedent
`inmemory/storage.hpp:1046`(ReplicationAccessor), `:724,726`(CreateVertexEx/EdgeEx); container
`utils/skip_list.hpp`; objects
`storage/v2/vertex.hpp`/`edge.hpp`/`edge_ref.hpp`/`property_store.hpp`; ids
`storage/v2/name_id_mapper.hpp`; modes `storage/v2/storage_mode.cpp:23`; GC horizon
`inmemory/storage.cpp:2983,3123`; tenant home `dbms/database.hpp:61`, `dbms/dbms_handler.hpp:134`;
session `query/interpreter.hpp:230`.

**POC reference:** `origin/feat/versioning` (Josip Mrđen, PR #4263) — 9 commits (versioning
feature, delta appends, nested branching, merge-to-master, revert-a-commit, per-database
versions). The query surface there already matches this spec 1:1; the *implementation* is the
part being redone correctly. Its core types (`VersionRegistry`, `VersionDeltaStore`,
`OverlayDelta`, `version_overlay.*`, one global `VersioningStoreMutex`, +1171 lines in
`interpreter.cpp`) are the design reference, superseded by the WAL-based, `Database`-hosted,
pinned-fork layering in A.1–A.4 / Appendix C.

---

## Appendix D — MVP Definition of Done & KPIs

### D.1 Definition of Done — MVP query surface

- [ ] `CREATE BRANCH '<name>' [WITH DESCRIPTION '<text>'] FROM '<parent>'` — fork without switching; capture fork point.
- [ ] `CHECKOUT BRANCH '<name>'` — switch session's active version; `CHECKOUT BRANCH 'main'` returns to base.
- [ ] `CHECKOUT BRANCH '<name>' [WITH DESCRIPTION '<text>'] FROM '<parent>'` — combined create-and-switch.
- [ ] Reads and writes against the active version (fork-point reconstruction + WAL-segment capture).
- [ ] `USING VERSION '<name>'` per-query directive (composes with `HOPS_LIMIT` etc.).
- [ ] `SHOW BRANCH` — current session's version.
- [ ] `SHOW BRANCHES [FOR DATABASE <db>]` — list all versions.
- [ ] `SHOW BRANCH DIFF [ '<name>' ] [FORMAT TABLE]` — a version's change-log as a table; the named form needs no checkout (pooled-safe); `query` column = provenance only.
- [ ] `MERGE BRANCH '<name>'` — one-way, conflict-checked merge into parent by explicit name (no checkout required), then drop branch.
- [ ] `DROP BRANCH '<name>'` — delete a branch's change-log + registry entry.

### D.2 Definition of Done — invariants & gates

- [ ] Live `main`/parent (writes still allowed while branches exist); branch pinned to fork point (D2).
- [ ] Merge preconditions: branch named explicitly (no checkout required); branch has no children; conflict-checked (incl. create-vs-create, R19); atomic-durable then drop.
- [ ] Version-name validation (non-empty; not `main`; no leading `.`; no `/` or `\`).
- [ ] Autocommit-only for management queries; explicit transactions allowed for branch data writes (§4.1).
- [ ] Startup-flag + MEL-license + in-memory-transactional-mode enforcement, each with a clear error (§4.4).
- [ ] Branch retention cap enforced with a clear error (D5 / R13).
- [ ] Durability: registry + change-logs survive restart and recover consistently, per database.
- [ ] Community build has **zero** versioning symbols reachable at runtime (enterprise-leak test, R4).
- [ ] **D10 write-routing rail** enforced — a write on a versioning-engaged connection with no resolved version is rejected, not silently applied to `main` (mechanism per the accepted D10 option; post-merge case covered). *[gated on D10 product decision]*
- [ ] **D16** — a branch query using full-text or vector search is rejected with a clear error (§4.4); reconstruction does not feed the shared vector index.
- [ ] **Schema-plane DDL rejected on a branch** (index/constraint/enum), enforced *before* any `EnumStore` mutation (R14/R27 — correctness).
- [ ] **Unique edge gids on replay/merge** (R11/R19) — no `MG_ASSERT` crash on collision.
- [ ] **D12 FGA** applies to branch reconstruction reads (verified) and `MERGE` re-validates the merging user's write privileges. *[D12]*
- [ ] **D11 merge audit record** retained and queryable. *[gated on D11 product decision — if accepted]*
- [ ] **Storage-mode switch / `RECOVER SNAPSHOT` rejected while branches exist** (R17); backup includes branch registry + change-logs when branches exist (R32).

### D.3 Definition of Done — quality gates

- [ ] Unit tests for registry, WAL-segment change-log, fork-point reconstruction, merge conflict-detection, and each invariant.
- [ ] e2e tests for every MVP query + every error path.
- [ ] Concurrency test: parallel sessions on different branches + writes to `main` while a branch is live + a merge that conflicts with concurrent `main` activity.
- [ ] Capture-completeness property/fuzz test (R8): branch-view == merged-result for every op type.
- [ ] Retention test (R13): a long-lived branch pins GC; verify the cap fires and memory is bounded.
- [ ] Flag-off no-regression benchmark vs master (R6).
- [ ] FGA-on-branch test (D12): a user denied a label/property on `main` cannot see it via a branch view; merge re-validates the merging user's privileges.
- [ ] Full-text/vector-search-on-branch returns a clean error, not `main`'s results (D16).
- [ ] Backup/restore test (R32): a snapshot+`RECOVER SNAPSHOT` cycle on a database with branches does not silently lose them.
- [ ] Audit-trail test (D11) — merge record is retained and queryable *[if D11 accepted]*.
- [ ] Docs page published.

### D.4 KPIs

| KPI | Definition | Target (proposal) |
|---|---|---|
| Branch create latency | Time for `CREATE BRANCH` on a graph of N nodes | O(1) — independent of N (empty change-log + a recorded fork timestamp) |
| Branch read overhead | Query latency on a branch vs. the same query on `main`, as a function of change-log length L | Documented curve; ≤ X× at the enforced cap (R1). *Product to set X after engineering supplies the measured curve.* |
| Merge latency | `MERGE BRANCH` time as a function of L | Linear in L; documented |
| Retention footprint | Extra memory retained per live branch as a function of parent churn since fork | Documented; bounded by the D5 cap |
| Correctness | % of e2e + property/fuzz cases where branch-then-merge == direct-write, and conflicts are detected | 100% |
| Enterprise safety | Versioning reachable on a community/expired license | 0 (hard gate) |

---

## Appendix E — Discovery links

- POC PR: https://github.com/memgraph/memgraph/pull/4263
- Demo video: https://drive.google.com/file/d/1leahSTZ7sfDjNlULbRu_KBHIF9tE93AB/view
- Wolters Kluwer — GraphRAG feedback: memgraph.slack.com/archives/C06MB9LSL95/p1781377975533109
- ZF Product Data KG input: memgraph.slack.com/archives/C08VCHZ381L/p1781513459513899
- Adobe architecture call (Fireflies): app.fireflies.ai/view/Adobe-Memgraph-Architecture
- Additional threads: CEVJCLKKL/p1781606184950769, C08EA8ESNKS/p1781618907683029, CEVJCLKKL/p1782910876931759
- **Customer signal (why we are doing it):** several customers were strongly interested at the
  demo; it is a genuine differentiator (no graph vendor offers in-database graph versioning —
  Neo4j differentiation). **Nvidia** — complex use case, specifically needs real *merges*, not
  scratch branches. **Adobe** — needs an *overlay*: projections don't work (graph is separated)
  and cross-database querying doesn't work (can't query across DBs), so an in-instance overlay is
  the shape they need. **Saporo / TFG Bash** — drop-only: never merge, just discard; the value is
  a change that can be reverted.
- **June 16, 2026 sync notes:** durable = yes (else plain BEGIN/ROLLBACK suffices); branch
  ownership = whoever, but sharing/access-control is needed (agent creates, human reviews &
  merges); one-way merge only, redo on conflict.
- **Mrma's spec notes:** single vs collaborative mode; materialization = committed & visible to
  all; Saporo/TFG = drop-only; Nvidia = complex, needs merges; Adobe = overlay; merge only if no
  conflict; must be able to pick a given version; custom permissions required.

*Source materials: Notion export (2026-07-07, in `~/workspace/opencode-work/versioning-v1/spec/`)
+ POC PR #4263 + systematic read of master `c312db1bc` (MVCC, SkipList, Vertex/Edge,
Storage/Database, tenants, WAL). Awaiting product sign-off (§0).*

---

## Appendix F — Implementation strategies for branch isolation

The central architectural fork (D17/D2). The decisive axis is **where a branch's changes live**,
because that determines consistency, whether the branch blocks `main`, read cost, and complexity.
Five points on that axis, from simplest to hardest:

| # | Strategy | Base consistent? | Branch changes live… | Blocks `main` / other branches during work? | Read cost | Impl complexity |
|---|---|---|---|---|---|---|
| 0 | **POC** (auto-rebase) | ❌ — reads *current* `main`, which moves as others merge | applied live onto current `main` per-txn | no (they commit) | cheap | trivial |
| ★ | **Replay overlay** — *v1, this spec (D2)* | ✅ pinned `fork_ts` | in a **branch-private overlay** (durable WAL + per-query scratch); `main` is **read-only** (time-travel), applied in private, then **discarded** | **no** — branch never writes `main`; conflict-checked at **merge only** | O(change-log)/query + retention pin (R1/R13/R33/R34) | moderate–high (new historical-ts accessor R16, replay surface R20, index churn) |
| 1 | **Live durable transaction** | ✅ pinned `fork_ts` | **live in the shared MVCC chains** (branch txn kept uncommitted until merge/drop) | **YES — hard block.** A live uncommitted delta forces `PrepareForWrite` serialization for every other writer, `main` included, until merge/drop | cheap (walk the live chain) | low–moderate ("leverage MVCC as-is") |
| 2 | **Anonymous tenant** | ✅ | in a **full duplicate** storage + indices (tag `main`'s files, don't copy the base) | no | cheap | moderate, but **resource-heavy** (duplicate everything) |
| 3 | **Branch-aware dual chain** — *north-star* | ✅ | in a **separate, `main`-invisible** delta chain | no | cheap | **very high** — rewrite storage + every index to be branch-aware |

**Why the two "simple" ends are disqualified.**

- **POC (Option 0)** fails consistency (D17): a what-if branch's base shifts under it as unrelated
  branches merge, so impact analysis measures the wrong thing. Only viable if product decides
  consistency is not required.
- **Live durable transaction (Option 1)** is the simplest consistency-preserving option — just don't
  commit the branch's transaction — and it was the first proposal. It is **broken for live
  production**, grounded in the engine: `PrepareForWrite` (`mvcc.hpp:112`) admits a write only if the
  object's head delta is the writer's own (`ts == transaction_id`) or committed before the writer's
  snapshot (`ts < start_timestamp`), else it sets `has_serialization_error` (`mvcc.hpp:134`). A live
  branch delta carries the branch's `transaction_id`, which lives in the uncommitted high-bit space
  (`kTransactionInitialId = 1ULL<<63`, `transaction_constants.hpp:19`), while any real transaction's
  `start_timestamp` comes from the separate lower counter (`< 2^63`). So `ts < start_timestamp` can
  **never** be true against a live foreign branch delta → the second writer, **including a `main`
  short-transaction**, is forced into a serialization error, and stays blocked until the branch
  merges (commit) or drops (abort). Net: `main` cannot write any object a live branch has touched,
  for the branch's whole life. A hard block on production, not a slowdown.

**Why Option 2 is not worth it.** Full isolation, but standing up a duplicate storage + indices per
branch is disproportionate for the "small change, test, merge back quickly" use case the feature
targets. Without the base-file sharing it *is* a copy (violating §2); with it, you still duplicate
all in-memory structures — effectively collapsing toward the cost of a tenant for no benefit over
the replay overlay.

**Why the replay overlay (v1) and dual chain (Option 3) are the only two that fit.** Consistency
(D17) + live `main` + concurrent branches together require that **branch changes never sit live on
the shared chain**. Only two designs achieve that: keep the branch's changes in a private overlay,
read `main` in place via time-travel, and reconcile per query (the overlay — buildable now, pays
per-query time-travel + overlay reconciliation + retention, and needs a small fork-era index/constraint
catalog A.7/R36), or give branches their own `main`-invisible chain (Option 3 — cheap and clean, no
reconciliation, but the big rewrite). The replay overlay is the **minimum viable consistent design
short of the Option-3 rewrite** — it does no *main-scale* duplication, but it is not free: the
reconciliation cost (R33) and fork-catalog (R36) are its price.

**Migration path.** The replay overlay and Option 3 share identical *user-facing* semantics
(isolated, consistent, non-blocking branches with the same query surface). So a v1 built on the
replay overlay can be re-architected into Option 3 later as an **internal** change — no breaking
change to the query surface or guarantees. That is a strong argument for shipping the overlay now and
treating the dual chain as a performance-driven v2.
