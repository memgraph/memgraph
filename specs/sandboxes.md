# Sandboxes (isolated, short-lived environments for agents)

**Status:** Draft (proposal for review), Enterprise, experimental
**Author:** Marko Budiselic
**Reviewers:** TBD
**Builds on:** [Graph Versioning](graph-versioning.md) (PR #4373), user profiles (`CREATE PROFILE`),
`IMPERSONATE_USER`

> A **sandbox** gives an agent its own place to work on real data, with limits and an expiry.
> One query creates it: it picks the data (a branch of a database), the user whose permissions
> apply (`ON BEHALF OF`), the resource limits (`PROFILE`) and how long it lasts. It returns a
> **token**. The agent connects with that token and can only reach that branch, with at most that
> user's permissions. When the sandbox expires or is dropped, the token stops working.

---

## 1. Overview

Teams want to point AI agents at production-shaped graph data. Today this means one of the following:

- Give the agent a real user's password. The agent gets everything that user can do, on `main`,
  with no expiry. One bad `DETACH DELETE` changes production.
- Create a new user per agent, grant privileges, set a profile, create a branch, and remember to
  undo all of it afterwards. That takes 5–10 admin queries and the cleanup is often forgotten.

A sandbox puts these pieces together in one object:

| Piece | Existing feature | What the sandbox adds |
|---|---|---|
| Data isolation | Branch (graph versioning v1) | Sandbox sessions can only use this branch. They cannot switch away from it. |
| Permissions | Users, roles, fine-grained access control | Caps the sandbox at the `ON BEHALF OF` user's permissions, minus anything that is not data work |
| Resource limits | User profiles (`sessions`, `transactions_memory`) | The profile applies to the sandbox's sessions |
| Credential | Password / SSO | A token that expires, can be revoked, and is tied to this sandbox only |
| Lifetime | none | Every sandbox expires. Expiry is required. |

The v1 use case: **give an agent a disposable copy of production to explore or edit, then a human
looks at the result and discards it.** Agents are expected to live minutes to hours, not weeks.

## 2. Concepts

| Term | Meaning |
|---|---|
| Sandbox | A named object in one database. It ties together a branch, an owner user, an optional profile, an expiry and one token. |
| Owner | The `ON BEHALF OF` user. The sandbox never has more permissions than the owner. If the owner's permissions change, the sandbox's permissions change with them right away. |
| Creator | The user who ran `CREATE SANDBOX`. Recorded for audit. Defaults to the owner. |
| Sandbox branch | The branch that sandbox sessions run on. Either the sandbox creates it and **owns** it (default), or it **attaches** to an existing branch (`BRANCH <b>`). |
| Token | An opaque bearer secret, returned once by `CREATE SANDBOX`. Memgraph stores only a hash of it. |
| Sandbox session | A Bolt session that logged in with a sandbox token. |

## 3. Prerequisites

- Enterprise build and license (same checks and messages as graph versioning §3).
- `--experimental-enabled=sandboxes`. It requires `versioning` too. Without it:
  `Sandboxes are disabled. Start Memgraph with --experimental-enabled=versioning,sandboxes.`
- All graph-versioning prerequisites (`IN_MEMORY_TRANSACTIONAL`, WAL + recovery, MAIN instance),
  because every sandbox sits on a branch.

## 4. Queries (MVP)

All management queries are autocommit only and run on MAIN. `<name>` and `<b>` follow branch
naming rules (non-empty, no `.` prefix, no `/`, `\`, `@`). `<duration>` is a string literal
(`'30m'`, `'2h'`, `'1d'`).

### 4.1 `CREATE SANDBOX`

```cypher
CREATE SANDBOX <name>
  [ON BEHALF OF <user>]
  [PROFILE <profile>]
  [BRANCH <b>]
  [READ ONLY]
  [EXPIRES IN <duration>];
```

| Clause | Default | Meaning |
|---|---|---|
| `ON BEHALF OF <user>` | the creator | Whose permissions cap the sandbox. If it is another user, the creator needs `IMPERSONATE_USER` on that user. Roles are not allowed: the owner must be a user. |
| `PROFILE <profile>` | the owner's profile, if any | An existing user profile. Its limits apply to the sandbox's sessions (§6.2). |
| `BRANCH <b>` | create a new branch named `<name>`, owned by the sandbox | Attach to an existing branch `<b>` of the current database instead. The sandbox never drops an attached branch. `BRANCH main` is allowed only with `READ ONLY`. |
| `READ ONLY` | off | Removes all write permissions from sandbox sessions, even if the owner has them. |
| `EXPIRES IN <duration>` | `--sandbox-default-ttl` (default `1h`) | Must be > 0 and ≤ `--sandbox-max-ttl` (default `24h`). |

The sandbox belongs to the session's current database (on `main`).

**Result:** one row, and this is the only time the token is ever shown:

| name | database | branch | owner | profile | read_only | expires_at | token |
|---|---|---|---|---|---|---|---|
| `"triage"` | `"shop"` | `"triage"` | `"alice"` | `"agent_small"` | `false` | `"2026-10-29T11:15:02.418305+0000"` | `"mgsbx_3kQ…"` |

**Privileges:** `SANDBOX_EDIT` (new). Also `BRANCH_EDIT` when the sandbox creates its own branch,
or `BRANCH_USE` when it attaches with `BRANCH <b>`. Also `IMPERSONATE_USER` on the owner when the
owner is not the creator.

### 4.2 `DROP SANDBOX`

```cypher
DROP SANDBOX <name> [KEEP BRANCH];
```

Revokes the token, terminates every sandbox session, and removes the sandbox. If the sandbox owns
its branch, the branch is dropped too (with `DROP BRANCH … FORCE` semantics), unless the query
says `KEEP BRANCH`. With `KEEP BRANCH`, the branch stays as an ordinary branch so a human can
inspect what the agent did. An attached branch is never dropped.

Privileges: `SANDBOX_EDIT`.

### 4.3 `ALTER SANDBOX … EXPIRES IN`

```cypher
ALTER SANDBOX <name> EXPIRES IN <duration>;
```

Sets the expiry to now + `<duration>`. The same bounds apply. This is the only way to extend an
agent's lifetime, and it does not issue a new token. It is refused on an expired sandbox:
`Sandbox '<name>' has expired.`

Privileges: `SANDBOX_EDIT`.

### 4.4 `SHOW SANDBOXES` / `SHOW SANDBOX`

```cypher
SHOW SANDBOXES [FOR DATABASE <db>];
SHOW SANDBOX <name>;
```

Columns: `name`, `database`, `branch`, `branch_owned` (bool), `owner`, `creator`, `profile`,
`read_only`, `created_at`, `expires_at`, `status` (`ACTIVE` | `EXPIRED`), `sessions`. The token
is never shown.

Privileges: `SANDBOX_EDIT`.

### 4.5 Keywords

New keywords are `SANDBOX`, `SANDBOXES`, `BEHALF` and `EXPIRES`. Every new rule starts with a
fixed prefix (`CREATE SANDBOX`, `DROP SANDBOX`, …), so all of them go into the non-reserved list
(`symbolicName`). `PROFILE` inside `CREATE SANDBOX` cannot be confused with the `PROFILE <query>`
prefix, because it is never the first token.

## 5. Connecting as an agent

The agent authenticates over Bolt with the **bearer** scheme. Neo4j-compatible drivers already
support it:

```python
from neo4j import GraphDatabase, bearer_auth

driver = GraphDatabase.driver("bolt://memgraph:7687", auth=bearer_auth(token))
with driver.session() as s:            # no database needed; the sandbox pins it
    s.run("MATCH (o:Order {status: 'stuck'}) RETURN count(o)")
```

- Memgraph handles tokens that start with `mgsbx_` itself, before auth modules and SSO are asked.
  This is the same place where `memgraph_internal` is handled. A wrong or expired token is
  rejected and is not passed on to SSO.
- The session is pinned to `<database>@<branch>`. If the driver sends no `db`, or sends
  `db="<database>"`, the session goes to the sandbox branch. Any other `db` value is refused:
  `This sandbox can only access '<database>@<branch>'.`
- `ROUTE` returns only the MAIN instance.

## 6. What a sandbox session can do

### 6.1 Permissions

Effective permissions = **owner's current permissions** ∩ **sandbox allowlist**, minus write
permissions when `READ ONLY` is set. Memgraph checks them on every query, so if a
`REVOKE` hits the owner, the sandbox loses that permission right away. Fine-grained (label/property)
rules of the owner apply unchanged.

Sandbox allowlist (MVP), and everything else is refused:

| Allowed | Notes |
|---|---|
| Cypher read/write queries (`MATCH`, `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, `CALL` subqueries) | They run on the sandbox branch. Writes never reach `main`. |
| `CALL <procedure>` | Only if the owner may run it. See risk §10 about procedures with external side effects. |
| `EXPLAIN`, `PROFILE` | |
| `SHOW INDEX INFO`, `SHOW CONSTRAINT INFO`, `SHOW STORAGE INFO`, `SHOW SCHEMA INFO` | As on a branch (graph versioning §6) |
| `SHOW CURRENT USER`, `SHOW SANDBOX` (no name: its own sandbox) | |
| Explicit transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) | |

Refused, even if the owner has the privilege: `USE DATABASE`, `USE BRANCH`, `USING BRANCH`, any
auth/user/role/profile query, any branch or sandbox management, database management, replication
and coordinator queries, schema DDL, triggers, streams, TTL, `SET DATABASE SETTING`, `TERMINATE`,
`SHOW SESSIONS`/`SHOW TRANSACTIONS` of others, snapshot and durability queries, and module file
management. Error:
`Query is not allowed in a sandbox session.`

### 6.2 Limits

- If there is a `PROFILE`, its `sessions` limit counts the sandbox's own sessions (all
  connections that use this token). Its `transactions_memory` limit applies to each sandbox
  transaction.
- Sandbox sessions do **not** count against the owner's own profile limits. Otherwise an agent
  could lock its owner out (see open question Q2).
- The tenant profile of the database still applies on top of this, as it does for every session.

### 6.3 Identity and audit

`SHOW CURRENT USER` returns the owner. `SHOW SESSIONS`, the audit log, and the slow- and
failed-query logs get a new `sandbox` field (the sandbox name, null otherwise). This way every
action can be traced to *owner + sandbox*.

## 7. Lifecycle

```
CREATE ──► ACTIVE ──(expires_at passes)──► EXPIRED ──► DROP SANDBOX ──► gone
              │                                ▲
              └──(owner dropped / branch dropped)┘
```

- **Expiry**: the token is rejected, open sandbox sessions are terminated, and their open
  transactions are rolled back. The sandbox and its branch stay in place so a human can inspect
  the result, until `DROP SANDBOX`.
- The sandbox expires right away if its owner is dropped, or if its branch is dropped (for
  example with `DROP BRANCH … FORCE` on an attached branch). `DROP BRANCH` without `FORCE` is refused
  while an active sandbox uses the branch:
  `Cannot drop branch '<b>': it is used by sandbox '<name>'.`
- **Restart**: sandboxes are durable, because their branches are durable. Expiry is wall-clock
  time, so a sandbox can expire while Memgraph is down.
- **Replication / HA**: same as branches in v1. Sandboxes live only on MAIN and are not
  replicated. Demotion or failover drops them.

## 8. Errors

| Situation | Message |
|---|---|
| Name taken | `Sandbox '<name>' already exists.` |
| Unknown sandbox | `Sandbox '<name>' does not exist.` |
| Default branch name taken | `Branch '<name>' already exists. Use BRANCH <b> to attach to it, or pick another sandbox name.` |
| Unknown owner / profile / branch | `User '<u>' does not exist.` / `Profile '<p>' does not exist.` / `Branch '<b>' does not exist.` |
| Impersonation not allowed | `You are not allowed to create a sandbox on behalf of '<u>'.` |
| `BRANCH main` without `READ ONLY` | `A sandbox on 'main' must be READ ONLY.` |
| Bad duration | `EXPIRES IN must be between 1s and <sandbox-max-ttl>.` |
| Expired/unknown token at login | `Authentication failure` (same as a wrong password, so nothing about the sandbox is revealed) |
| Disallowed query in a sandbox session | `Query is not allowed in a sandbox session.` |
| Wrong `db` from a sandbox session | `This sandbox can only access '<database>@<branch>'.` |

## 9. Worked example

An admin prepares a profile once, then a person or an orchestrator creates a sandbox for each agent run.

```cypher
// once
CREATE PROFILE agent_small LIMIT sessions 2, transactions_memory 512MB;
GRANT SANDBOX_EDIT, BRANCH_EDIT TO orchestrator;
GRANT IMPERSONATE_USER alice TO orchestrator;

// per agent run (as orchestrator, on database shop)
CREATE SANDBOX triage ON BEHALF OF alice PROFILE agent_small EXPIRES IN '2h';
// → name: "triage", database: "shop", branch: "triage", owner: "alice", profile: "agent_small",
//   read_only: false, expires_at: "2026-10-29T11:15:02.418305+0000", token: "mgsbx_3kQ…"
```

The agent connects with `bearer_auth("mgsbx_3kQ…")`:

```cypher
MATCH (o:Order {status: 'stuck'})-[:WAITS_ON]->(s:Shipment) SET o.status = 'retry';  // branch only
MATCH (o:Order {status: 'retry'}) RETURN count(o);                                     // 37
USE DATABASE billing;     // Query is not allowed in a sandbox session.
DROP INDEX ON :Order;     // Query is not allowed in a sandbox session.
```

A human reviews the result and cleans up:

```cypher
SHOW SANDBOXES;
//   name: "triage", branch: "triage", owner: "alice", creator: "orchestrator",
//   status: "ACTIVE", sessions: 1, expires_at: "2026-10-29T11:15:02.418305+0000", …
USING BRANCH triage MATCH (o:Order {status: 'retry'}) RETURN count(o);   // 37
MATCH (o:Order {status: 'retry'}) RETURN count(o);                       // 0 on main

DROP SANDBOX triage KEEP BRANCH;   // agent cut off; the branch stays for further review
DROP BRANCH triage;
```

A read-only analysis agent with no branch cost:

```cypher
CREATE SANDBOX analyst ON BEHALF OF alice BRANCH main READ ONLY EXPIRES IN '30m';
```

## 10. Definition of done, risks, open questions

**Definition of done (MVP)**
- [ ] `CREATE` / `DROP` / `ALTER … EXPIRES IN` / `SHOW SANDBOX[ES]` as specified, with `SANDBOX_EDIT`
- [ ] Bearer token login, session pinned to `<db>@<branch>`, permission check owner ∩ allowlist
- [ ] Expiry enforced at login and on live sessions, and across restarts
- [ ] Profile limits applied to sandbox sessions
- [ ] `sandbox` field in `SHOW SESSIONS` and the audit log
- [ ] E2E tests: privilege escalation attempts, expiry during a transaction, owner revoke/drop, restart

**Config flags (new):** `--sandbox-default-ttl` (default `1h`), `--sandbox-max-ttl` (default `24h`).

**Metrics:** `memgraph_sandboxes_active` (gauge), `memgraph_sandboxes_created_total`,
`memgraph_sandboxes_expired_total`, `memgraph_sandbox_sessions_active` (gauge).

**Risks**
- *Cost on `main`.* Every owned branch pins main's GC and WAL retention (graph versioning §7).
  Many agents, or forgotten `EXPIRED` sandboxes, retain memory on `main`. The maximum TTL limits how
  long a sandbox stays active, but expired sandboxes keep their branch until dropped (Q1).
- *Procedures with external side effects* (`migrate.*`, exporting to files, HTTP calls) escape
  the branch isolation. In the MVP they are only gated by the owner's permissions.
- *Branch gaps* (graph versioning §12): agents use vector and text search a lot, and both are not in v1 on
  branches. Sandboxes on a branch get errors for them. `BRANCH main READ ONLY` is the workaround.
- *Token leakage.* Tokens are bearer secrets. The mitigations are a short TTL, hash-only storage,
  and a recognizable `mgsbx_` prefix so secret scanners can find them.

**Open questions**
1. Should `EXPIRED` sandboxes be auto-dropped after a grace period (`--sandbox-expired-retention`)?
   This would free main's retention without manual cleanup.
2. Should sandbox sessions also count against the owner's profile, so all of a user's agents share
   one budget?
3. Should there be a procedure allowlist for sandboxes (a flag, or a profile key) in the MVP, or later?
4. Should `ON BEHALF OF` accept a role, so permissions come from a role and not from a person?
5. Should the sandbox also cap its branch size (a `branch_changes` profile key)?

## 11. Not in MVP

| Feature | Notes |
|---|---|
| Token rotation (`ALTER SANDBOX … ROTATE TOKEN`) | Workaround: `DROP SANDBOX … KEEP BRANCH`, then `CREATE SANDBOX … BRANCH <b>` |
| Several tokens per sandbox (one per sub-agent) | |
| Sandbox-specific limits: query timeout, rows/sec, total writes | Natural extension of user profile limit keys |
| Sandbox over several databases | |
| Merge sandbox results into `main` | Depends on branch merge (graph versioning §12) |
| HTTP / MCP server login with a sandbox token | |
| Replication / HA | Depends on branch replication |
