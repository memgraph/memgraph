# PRD: SSO Authentication on Coordinators

## Problem Statement

Today, Memgraph coordinators (the Raft-based HA coordinator role) skip authentication
entirely: any Bolt client that connects is accepted with an empty user, and
username/password are neither validated nor meaningful. There is no way to require that
a client proves its identity against a corporate identity provider (IdP) before talking
to a coordinator, even though the same organizations that run SSO against data instances
expect their tooling (e.g. Memgraph Lab, admin scripts) to authenticate the same way
against coordinators.

At the same time, coordinators have no concept of roles, so there is nothing for an SSO
identity to map onto. And two pieces of dead surface — the `COORDINATOR` privilege and
the `EnableWritingOnMainRpc` — add confusion without providing behavior.

## Solution

Enable SSO (OIDC, SAML, Kerberos) authentication on coordinators, backed by a small,
Raft-replicated set of roles, each carrying a coordinator privilege (`COORDINATOR_READ` or
`COORDINATOR_WRITE`).

From the user's perspective:

- Connecting to a coordinator with **basic auth (username + password) succeeds as a
  passthrough** whenever SSO is not in effect — credentials are ignored, and the session has
  full `COORDINATOR_WRITE` access. Nothing about existing tooling breaks. Once SSO is
  configured (`--auth-module-mappings` non-empty) **and** the enterprise license is valid,
  basic/none is denied so the credential-less passthrough cannot bypass the SSO privilege
  model. If the license is missing, expired, or invalid, SSO itself rejects every login, so
  basic/none **falls back to the passthrough** — a license transition never locks every Bolt
  session out of a coordinator, and the break-glass session can re-install the license over
  Bolt (`SET DATABASE SETTING "enterprise.license" TO ...`).
- Connecting with an **SSO scheme** (one listed in `--auth-module-mappings`) now runs the
  corresponding auth module and **actually authenticates**: the connection is accepted
  only if the IdP token is valid and every role the module returns already exists on the
  coordinator; otherwise the connection is rejected. SSO on coordinators works the same
  way for SAML and Kerberos as it does for OIDC.
- An authenticated SSO session is **restricted by its role privileges**. Coordinators have
  exactly two privileges: **`COORDINATOR_READ`** (routing-table read, `SHOW COORDINATOR
  SETTINGS`, `SHOW INSTANCE`, `SHOW INSTANCES`, `SHOW REPLICATION LAG`, and other read-only
  introspection) and **`COORDINATOR_WRITE`** (everything runnable on a coordinator;
  `COORDINATOR_WRITE` is a superset of `COORDINATOR_READ`). A session's effective privilege
  is the union of its roles' privileges. (These are new privilege names chosen because bare
  `READ`/`WRITE` already exist as data-instance fine-grained privileges.)
- Administrators manage roles with **`CREATE ROLE`**, **`DROP ROLE`**, **`SHOW ROLES`**,
  assign privileges with **`GRANT COORDINATOR_READ|COORDINATOR_WRITE TO <role>`** /
  **`REVOKE`**, and inspect them with **`SHOW PRIVILEGES FOR ROLE <role>`**.
  **`GRANT ALL PRIVILEGES TO <role>`** grants both coordinator privileges and
  **`REVOKE ALL PRIVILEGES FROM <role>`** removes both. These queries work on any
  coordinator: if run on a follower they are transparently forwarded to the leader. The role
  set and its privileges are persisted through the Raft log, so they survive restarts,
  follower catch-up, and leader failover.
- All other auth queries remain **rejected** on coordinators: users/passwords, `DENY` in any
  form, `GRANT DATABASE` / multi-tenancy database access, fine-grained access control
  (label/edge-type entity privileges), and property-permission grants.
- SSO, role management, privilege grants, and privilege enforcement on coordinators are
  **enterprise features** (require a valid license); basic-auth passthrough remains free
  and retains full `COORDINATOR_WRITE` access, so community/unlicensed deployments are
  unaffected. Because basic auth is license-free, the deny-basic-under-SSO rule above is
  itself conditioned on a valid license: with SSO configured but no valid license, basic
  auth still works (break-glass) while SSO logins are rejected with a license error.

Separately, this work removes two unused constructs — the `COORDINATOR` privilege and
`EnableWritingOnMainRpc` — to reduce confusion.

## User Stories

1. As an operator, I want to connect to a coordinator with a username and password and
   succeed whenever SSO is not in effect (not configured, or configured without a valid
   license), so that my existing admin tooling keeps working unchanged.
2. As a security engineer, I want SSO clients to be rejected on a coordinator when they
   present an invalid IdP token, so that only authenticated identities reach the cluster
   control plane.
3. As a security engineer, I want an SSO client to be rejected when the IdP returns a role
   that does not exist on the coordinator, so that misconfigured IdP group mappings fail
   loudly instead of silently granting access.
4. As a security engineer, I want an SSO client whose IdP response lists multiple roles to
   be accepted only when all of those roles exist on the coordinator, so that partial
   misconfiguration is caught.
5. As an operator using OIDC, I want to authenticate to a coordinator with my OIDC token,
   so that I use the same corporate identity I use for data instances.
6. As an operator using SAML, I want SAML authentication on coordinators to behave exactly
   like OIDC, so that my SAML deployment is a first-class citizen.
7. As an operator using Kerberos, I want Kerberos authentication on coordinators to behave
   exactly like OIDC, so that my Kerberos deployment is a first-class citizen.
8. As an administrator, I want to run `CREATE ROLE <name>` on a coordinator, so that an SSO
   identity mapping to that role can authenticate.
9. As an administrator, I want to run `DROP ROLE <name>` on a coordinator, so that I can
   revoke a role that SSO identities map to.
10. As an administrator, I want to run `SHOW ROLES` on a coordinator, so that I can see the
    exact set of roles the coordinator will accept from SSO.
11. As an administrator, I want `CREATE ROLE`/`DROP ROLE`/`SHOW ROLES` run on a follower
    coordinator to be automatically forwarded to the leader, so that I don't have to find
    and reconnect to the leader myself.
12. As an administrator, I want `CREATE ROLE` on an already-existing role to error (unless I
    pass `IF NOT EXISTS`), so that role management behaves the same as on data instances.
13. As an administrator, I want `DROP ROLE` on a non-existent role to error, so that role
    management behaves the same as on data instances.
14. As an administrator, I want the coordinator role list to survive a full-cluster restart,
    so that authentication configuration is durable.
15. As an administrator, I want a coordinator that was down while roles changed to catch up
    to the exact role set when it rejoins, so that all coordinators agree on who can
    authenticate.
16. As an administrator, I want the role list to survive a leader failover, so that a change
    of leadership does not lose authentication configuration.
17. As an administrator, I want all non-role auth queries (`CREATE USER`, `SET PASSWORD`,
    `SHOW USERS`, `SET ROLE`, `DENY` in any form, `GRANT DATABASE`, fine-grained access
    control, property-permission grants, …) to be rejected on coordinators, so that the
    coordinator's auth surface stays minimal and predictable.
18. As an administrator, I want to grant a coordinator role either `COORDINATOR_READ` or
    `COORDINATOR_WRITE` with `GRANT COORDINATOR_READ|COORDINATOR_WRITE TO <role>`, so that I
    can control what an SSO identity mapping to that role may do.
19. As an administrator, I want `GRANT ALL PRIVILEGES TO <role>` to grant both coordinator
    privileges and `REVOKE ALL PRIVILEGES FROM <role>` to remove both, so that I can set a
    role to full access (or clear it) in one statement.
20. As an administrator, I want to remove a granted privilege with `REVOKE
    COORDINATOR_READ|COORDINATOR_WRITE FROM <role>`, so that I can tighten access without
    dropping the role.
21. As an administrator, I want `SHOW PRIVILEGES FOR ROLE <role>` to report a coordinator
    role's granted privilege, so that I can audit and debug access.
22. As a security engineer, I want a `COORDINATOR_READ`-only SSO session to read the routing
    table and run the read/introspection queries but be denied every mutating query, so that
    least-privilege access to the control plane is enforced.
23. As a security engineer, I want a `COORDINATOR_WRITE` SSO session to run every coordinator
    query (`COORDINATOR_WRITE` being a superset of `COORDINATOR_READ`), so that operators
    with write access are not additionally blocked from reads.
24. As a security engineer, I want an SSO session whose role has no privilege granted to be
    denied everything (including the routing table) until an admin grants a coordinator
    privilege, so that roles never confer implicit access.
25. As a security engineer, I want a multi-role SSO session to receive the union of its
    roles' privileges, so that combined roles behave predictably.
26. As an operator, I want basic-auth sessions to keep full `COORDINATOR_WRITE` access, so
    that existing admin tooling that connects with username/password is never locked out by
    the new privilege model.
27. As a security engineer, I want fine-grained access control queries (label/edge-type
    entity privileges, property permissions) and `GRANT DATABASE` to be rejected on
    coordinators, so that coordinators never appear to accept data-plane access control they
    cannot honor.
28. As an administrator, I want SSO login for a given role to be rejected before I create
    that role and to succeed after I create it, so that role provisioning has an observable,
    verifiable effect.
29. As an operator, I want the coordinator to inherit my `MEMGRAPH_SSO_*` environment
    variables into the auth module, so that I configure SSO on coordinators exactly as I do
    on data instances.
30. As an operator, I want to configure the coordinator's SSO scheme-to-module mappings with
    `--auth-module-mappings`, so that I control which schemes and modules the coordinator uses.
31. As an operator running a rolling upgrade, I want a role or privilege query forwarded to a
    not-yet-upgraded leader to fail with an error rather than crash the coordinator, so that
    a mixed-version window degrades safely.
32. As an operator without an enterprise license, I want SSO auth, role queries, privilege
    grants, and enforcement on coordinators to be gated by a license error while basic-auth
    still works with full access — **even when SSO is configured** — so that licensing is
    consistent with the rest of auth and a license expiry never locks every Bolt session
    out of a coordinator (basic auth is the break-glass through which the license can be
    re-installed).
33. As a developer, I want the unused `COORDINATOR` privilege removed, so that the privilege
    model does not advertise a permission that gates nothing.
34. As a developer, I want the unused `EnableWritingOnMainRpc` removed, so that the RPC
    surface reflects what is actually called.
35. As a developer, I want a dedicated e2e suite for coordinator authentication, so that
    every scenario above is protected against regressions.
36. As a developer, I want Kerberos exercised through the real coordinator Bolt handshake
    (not just as an isolated Python unit test), so that the coordinator SSO path has genuine
    Kerberos coverage.

## Implementation Decisions

### Auth semantics on coordinators

- **Basic auth is a passthrough**: `basic`/`none` schemes succeed on a coordinator with
  credentials ignored (current behavior preserved), **except** when SSO is configured
  (`--auth-module-mappings` non-empty) and the enterprise license is valid, in which case
  they are denied. No license required. The deny condition deliberately mirrors the SSO
  path's own license gate: basic/none is denied only while SSO can actually authenticate
  someone, so there is always at least one working auth path on a coordinator.
- **SSO authenticates**: for a scheme present in `--auth-module-mappings`, run the auth
  module for that scheme, then validate the returned roles. Authentication **succeeds only
  if every returned role exists** in the coordinator's role list. Invalid token, any
  missing role, or an unknown scheme (not in mappings) → **connection rejected**.
- **Privilege gating (supersedes the earlier no-gating decision)**: coordinators have
  exactly two privileges, **`COORDINATOR_READ`** and **`COORDINATOR_WRITE`**, where
  `COORDINATOR_WRITE` is a superset of `COORDINATOR_READ`. A READ-classified query requires
  `COORDINATOR_READ` or `COORDINATOR_WRITE`; a WRITE-classified query requires
  `COORDINATOR_WRITE`. A basic-auth passthrough session has full `COORDINATOR_WRITE`
  (backward compatible). An SSO session's effective privilege is the **union** of the masks
  of its matched roles; a role with no grant confers nothing, so such a session is denied
  everything (including the routing table) until granted.
- **Query classification**: READ = routing-table read, `SHOW COORDINATOR SETTINGS`,
  `SHOW INSTANCE`, `SHOW INSTANCES`, `SHOW REPLICATION LAG`, and all other read-only
  introspection (`SHOW ROLES`, `SHOW PRIVILEGES FOR ROLE`, `SHOW CONFIG`, `SYSTEM INFO`, the
  SHOW form of settings). WRITE = every mutating/admin query (`REGISTER`/`UNREGISTER
  INSTANCE`, `SET INSTANCE TO MAIN`, `DEMOTE INSTANCE`, `ADD`/`REMOVE COORDINATOR`,
  `YIELD LEADERSHIP`, `SET COORDINATOR SETTING`, `UPDATE CONFIG`, `FORCE RESET CLUSTER
  STATE`, `RELOAD SSL`, the SET form of settings, `CREATE ROLE`, `DROP ROLE`, `GRANT`,
  `REVOKE`).
- **Enterprise-gated**: SSO authentication, the role queries, privilege grants
  (`GRANT`/`REVOKE`), `SHOW PRIVILEGES FOR ROLE`, and privilege enforcement require
  `MG_ENTERPRISE` + a valid license; without it they are rejected with a license error.
  Basic-auth passthrough works in any build and retains full `COORDINATOR_WRITE` — including
  when SSO is configured but the license is missing/expired/invalid (break-glass): denying
  basic there too would leave no successful auth path on any coordinator Bolt session,
  recoverable only by restarting with `--auth-module-mappings` cleared.
- The coordinator branch of the Bolt handshake's `AuthenticateUser` is changed from
  "ignore auth on coordinators" to: basic/none → passthrough, unless SSO is configured
  **and** the license is valid, in which case deny; SSO scheme in mappings → the dedicated
  coordinator SSO path; otherwise reject.

### Coordinator SSO authenticator (deep module)

- A **dedicated coordinator SSO path** lives in the session/glue layer and shares as little
  as possible with the data-instance `Auth` path. It reuses the existing auth-module
  subprocess machinery (scheme→path mappings, pipe protocol) to run the module, but the
  role-existence check is performed against `CoordinatorState`'s committed role list rather
  than the auth kvstore.
- The authenticator is shaped as a testable unit: given a scheme and an IdP response, it
  produces an authenticated/rejected result plus an **effective privilege mask** (the union
  of the matched roles' masks), using an injected module-runner and an injected role/mask
  provider over the coordinator role set.
- On success, the session carries that effective mask; the coordinator privilege check
  consults the session mask, **not** the auth kvstore (coordinator roles live only in Raft).
- `MEMGRAPH_SSO_*` variables are the module's contract and are inherited by the spawned
  module subprocess. **No new C++ env parsing** is added on coordinators.

### Coordinator privilege model & enforcement

- Two new privileges are introduced (an `AuthQuery::Privilege` value and a `Permission` bit
  each): **`COORDINATOR_READ`** and **`COORDINATOR_WRITE`**, added to the grammar `privilege`
  rule. These names are required because bare `READ`/`WRITE` already exist as data-instance
  fine-grained privileges. The previously-removed `COORDINATOR` privilege stays removed (its
  reserved bit is not reused). Granting them is a coordinator-only carve-out and they are not
  meaningful on data instances.
- Roles carry a permission bitmask. The coordinator carve-out reuses the existing
  `grantPrivilege`/`revokePrivilege` grammar but **inspects the content**: it accepts only
  `ALL PRIVILEGES` or a privilege list containing solely `COORDINATOR_READ`/
  `COORDINATOR_WRITE`. `GRANT ALL PRIVILEGES TO ROLE <role>` grants **both** coordinator
  privileges; `REVOKE ALL PRIVILEGES FROM ROLE <role>` removes **both**. `SHOW PRIVILEGES FOR
  ROLE <role>` reports the role's privilege (the trailing `ON MAIN|CURRENT|DATABASE` clause
  is rejected — coordinators have no database). `SHOW ROLES` stays name-only.
- **Explicitly rejected** on coordinators, even though they share grammar with the accepted
  forms: `DENY` in any form; a privilege list containing any non-coordinator privilege;
  fine-grained access control (the `entityPrivilegeList` label/edge-type forms); the
  property-permission grants; and `GRANT/DENY/REVOKE DATABASE` (multi-tenancy database
  access). These raise the coordinator-not-allowed error.
- Enforcement of Cypher coordinator queries reuses the existing required-privilege
  extraction + `CheckAuthorized` path, comparing the query's required privilege against the
  session's effective mask (`COORDINATOR_WRITE` satisfies a `COORDINATOR_READ` requirement).
- The **routing table** is a Bolt `ROUTE` message that bypasses the query privilege path, so
  an explicit `COORDINATOR_READ` check is added in the ROUTE handler against the session's
  effective mask.

### Roles as Raft-replicated state

- A new field holding a **`vector<{name, privilege-mask}>`** (a small role struct: role name
  plus its coordinator permission bitmask) is added to `CoordinatorClusterState` and is the
  **sole source of truth** for coordinator roles and their privileges. There are no user
  records stored on coordinators; the auth kvstore is not used for coordinator roles.
- The field is added through the full established pattern: member + default, all special
  member functions, `operator==` tie-lists, a new optional in `CoordinatorClusterStateDelta`,
  getter/setter, `to_json`/`from_json` (full-state snapshot), `DoAction`,
  `SerializeUpdateClusterState`/`DecodeLog` (per-log delta), and a new key constant.
- **No `LogStoreVersion` bump.** Backward/forward compatibility is achieved the same way as
  the most recent fields: `add_if_set` when serializing the delta, a `contains`-guard in
  `DecodeLog`, and `j.value(key, {})` (default empty) in `from_json`. An older coordinator
  ignores the unknown key; a newer coordinator reading an older log/snapshot sees an empty
  role set.

### Role & privilege query handling and forwarding

- The interpreter's coordinator query gate is extended to permit exactly `CREATE ROLE`,
  `DROP ROLE`, `SHOW ROLES`, `GRANT (COORDINATOR_READ|COORDINATOR_WRITE|ALL PRIVILEGES) TO
  ROLE <role>`, `REVOKE (COORDINATOR_READ|COORDINATOR_WRITE|ALL PRIVILEGES) FROM ROLE
  <role>`, and `SHOW PRIVILEGES FOR ROLE <role>` (carved out of the auth-query rejection by
  inspecting the auth-query sub-action and privilege content). All other auth queries —
  including `DENY`, FGAC/entity privileges, property permissions, and `GRANT DATABASE` —
  remain rejected on coordinators with the existing coordinator-only-queries error.
- Semantics **match data instances**: `CREATE ROLE x` errors if `x` already exists, is a
  no-op with `IF NOT EXISTS`; `DROP ROLE x` errors if `x` does not exist.
- New coordinator→coordinator RPCs are introduced for the write operations (create role,
  drop role, grant, revoke) and read operations (get roles, get role privileges).
- **All role/privilege writes forward to the leader** via the existing `ForwardToLeader`
  mechanism. On the leader, each write builds a `CoordinatorClusterStateDelta` carrying the
  updated role set and commits it through `AppendLogAndWaitForCommit`. `SHOW ROLES` and
  `SHOW PRIVILEGES FOR` are also forwarded to the leader (strong read).

### Mixed-version behavior (rolling upgrade)

- **No RPC version negotiation.** If a new follower forwards a role/privilege RPC to a
  not-yet-upgraded leader that has no handler for it, the RPC **fails and surfaces an error**
  to the client.
- This path must **not** trigger `MG_ASSERT` or otherwise crash the coordinator; the
  receive/dispatch and forwarding code must handle a missing handler / RPC failure by
  returning an error.
- The cluster-state field remains safe across versions via the serialization pattern above.

### Removals

- **`COORDINATOR` privilege** is removed from the `Permission` enum (its bit is left reserved
  and not reused), from `kPermissionsAll`, from the permission stringification, from the
  grammar **privilege rule** (the `COORDINATOR` lexer keyword is kept — it is used by
  `ADD COORDINATOR`, `SHOW COORDINATOR SETTINGS`, etc.), from the visitor mapping, from the
  AST privilege list, from the query→permission mapping, and from the required-privileges
  attachment for `CoordinatorQuery`. **No migration** is added; a stale persisted bit in an
  existing deployment is simply never reported and never checked.
- **`EnableWritingOnMainRpc`** is removed entirely: type alias, request/response structs and
  their Save/Load, the server-handler registration and its handler implementation, the
  `RpcInfoSpecialize` entry, and the Prometheus counters. The underlying
  `ReplicationState::EnableWritingOnMain()` method is **kept** (still used; writing on a
  promoted main is enabled via the `writing_enabled` flag inside `PromoteToMainRpc`).

### Out-of-scope confirmations baked into the design

- Password masking behavior is unchanged (see Out of Scope).
- SSO-driven auto-creation of roles (readonly/readwrite/admin) is not implemented on
  coordinators — roles must be created explicitly.

## Testing Decisions

Good tests here assert **external behavior** — the outcome a client or operator observes
(connection accepted/rejected, `SHOW ROLES` output, query error, role set after
restart/failover) — not internal call sequences or private data structures. Tests should
remain valid if the implementation is refactored, as long as behavior holds.

### Unit tests

- **Coordinator role store**: add/drop/list operations on `CoordinatorClusterState`, plus a
  serialization roundtrip for both the per-log delta and the full-state snapshot (encode →
  decode → identical role set **including each role's privilege mask**, and the empty /
  backward-compatible-missing-key cases). This directly protects the Raft-persisted
  `vector<{name, mask}>` code path in isolation. Prior art: existing
  `CoordinatorClusterState` / coordinator state-machine unit tests.
- **Auth-query gate**: assert that the coordinator gate permits exactly `CREATE ROLE`,
  `DROP ROLE`, `SHOW ROLES`, `GRANT`/`REVOKE COORDINATOR_READ|COORDINATOR_WRITE|ALL
  PRIVILEGES`, and `SHOW PRIVILEGES FOR ROLE`, and rejects all other auth queries — including
  `DENY`, `GRANT DATABASE`, FGAC/entity privileges, and property permissions — at unit level
  where feasible. Prior art: existing interpreter/required-privileges tests.
- **Effective-privilege computation**: union of masks across multiple roles; the
  authorization check semantics (`COORDINATOR_READ` requirement satisfied by either
  privilege; `COORDINATOR_WRITE` requirement needs `COORDINATOR_WRITE`; a bare role confers
  nothing); and that `ALL PRIVILEGES` maps to both coordinator privileges.

The role/privilege RPCs and the coordinator SSO authenticator are covered by the e2e suite
rather than dedicated unit tests.

### End-to-end tests — `tests/e2e/high_availability/auth.py`

Shaped like `distributed_coords.py` (driven by `interactive_mg_runner`, `ADD COORDINATOR`
wiring) but with **3 coordinators and no data instances**. A dedicated dummy auth module
covering OIDC, SAML, and Kerberos schemes (canned success roles + error cases) is driven
through the **real coordinator Bolt handshake**. The new test is registered in the
directory's `workloads.yaml` and `CMakeLists.txt`.

Functional buckets:

- **Basic-auth passthrough**: with no SSO module configured, username/password succeeds and
  the session can run coordinator queries; an unknown/SSO scheme not in mappings is rejected.
  With SSO configured and a valid license, basic/none is denied. With SSO configured but the
  license revoked at runtime, basic/none falls back to the passthrough (break-glass): the
  session retains full `COORDINATOR_WRITE`, can re-install the license over Bolt, and the
  cluster returns to SSO-only access — no restart, no total lockout.
- **SSO across all three schemes + failures**: OIDC, SAML, Kerberos each succeed when the
  returned role(s) exist; rejection on invalid token, on a missing role, and multi-role
  where all-exist succeeds and any-missing rejects. Kerberos is exercised through the Bolt
  handshake (closing the "no server-path coverage" gap).
- **Role CRUD + follower forwarding**: `CREATE`/`DROP`/`SHOW ROLE` on the leader; the same
  run on a follower and transparently forwarded; duplicate → error, `IF NOT EXISTS` →
  no-op, `DROP` missing → error; forwarded `SHOW ROLES`.
- **Privilege management**: `GRANT COORDINATOR_READ|COORDINATOR_WRITE TO ROLE <role>`
  reflected in `SHOW PRIVILEGES FOR ROLE <role>`; `GRANT ALL PRIVILEGES` grants both and
  `REVOKE ALL PRIVILEGES` removes both; `REVOKE` clears a single privilege; a bare role shows
  no privilege; grants/revokes forwarded from a follower; `GRANT`/`REVOKE`/`SHOW PRIVILEGES`
  rejected for non-role auth targets.
- **Privilege enforcement (via SSO)**: a `COORDINATOR_READ`-only SSO session can read the
  routing table and run the read/introspection queries but is denied every mutating query; a
  `COORDINATOR_WRITE` SSO session can run everything; a bare-role SSO session is denied
  everything including the routing table; basic-auth retains full `COORDINATOR_WRITE`.
- **Disallowed queries + SSO/role tie-in**: `CREATE USER`, `SET PASSWORD`, `SHOW USERS`,
  `SET ROLE`, `DENY` (any form), `GRANT DATABASE` / `SET MAIN DATABASE`, fine-grained access
  control (`GRANT ... ON NODES/EDGES ...`), and property-permission grants are all rejected
  on coordinators; SSO login for a role rejected before `CREATE ROLE` and succeeding after.

Persistence / recovery:

- **Full-cluster restart**: create roles, restart all coordinators, `SHOW ROLES` still
  returns them (durable log/snapshot replay).
- **Lagging-follower catch-up**: kill a follower, mutate roles on the leader, restart the
  follower, assert it reconstructs the exact role set (replication + apply on rejoin).
- **Leader failover**: create roles, kill the leader, let a new leader be elected, assert
  the new leader's role set matches (committed state survives leadership change).

Prior art: `tests/e2e/high_availability/distributed_coords.py` for cluster shape and
lifecycle; `tests/e2e/sso/` (`test_sso.py`, `test_oidc.py`, `test_saml_sso_module.py`,
`dummy_sso_module.py`) for the mock-module structure and IdP-response fixtures.

## Out of Scope

- **Password masking changes**: current behavior is intentionally left as-is — enterprise +
  valid license masks the password in the query log; community and unlicensed-enterprise
  builds are unchanged. No masking is added to failed/slow/session-trace/audit sinks, and
  the syntax-error/client-facing path is untouched.
- **Privileges beyond `COORDINATOR_READ`/`COORDINATOR_WRITE`**: coordinators support only the
  two coarse privileges;
  the fine-grained data-instance privilege set is not modeled on coordinators. (This
  supersedes the earlier "no privilege gating on coordinators" decision.)
- **SSO auto-provisioning of roles**: the data-instance-style scenario where SSO implies
  built-in roles (readonly/readwrite/admin) is explicitly not implemented on coordinators.
- **User management on coordinators**: no users or passwords are stored on coordinators;
  the only persisted auth state is the role set and each role's READ/WRITE mask.
- **Real KDC/SPNEGO Kerberos integration**: Kerberos is tested through the coordinator Bolt
  handshake with a dummy module, not against a live KDC.
- **RPC version negotiation / capability detection** for the new role RPCs.

## Further Notes

- Two items are deliberately deferred to implementation because they are mechanism-reuse
  questions, not product decisions: the exact reuse of the existing coordinator RPC
  registration/dispatch for the new role RPCs (ensuring a missing handler returns an error
  rather than asserting), and the precise driver/connection mechanism the e2e uses to send
  SSO tokens over Bolt (mirroring `tests/e2e/sso`).
- No new gflags are introduced (`--auth-module-mappings` already exists), so
  `tests/e2e/configuration/default_config.py` does not need updating.
- Lock ordering and the `CoordinatorClusterState` update checklist from the repo guidelines
  must be respected when adding the `roles_` field.
