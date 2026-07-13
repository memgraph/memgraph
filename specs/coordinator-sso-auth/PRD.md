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
Raft-replicated list of role names.

From the user's perspective:

- Connecting to a coordinator with **basic auth (username + password) always succeeds**,
  exactly as today — credentials are ignored. Nothing about existing tooling breaks.
- Connecting with an **SSO scheme** (one listed in `--auth-module-mappings`) now runs the
  corresponding auth module and **actually authenticates**: the connection is accepted
  only if the IdP token is valid and every role the module returns already exists on the
  coordinator; otherwise the connection is rejected. SSO on coordinators works the same
  way for SAML and Kerberos as it does for OIDC.
- Administrators manage the coordinator's role list with **`CREATE ROLE`**, **`DROP ROLE`**,
  and **`SHOW ROLES`**. These queries work on any coordinator: if run on a follower they
  are transparently forwarded to the leader. The role list is persisted through the Raft
  log, so it survives restarts, follower catch-up, and leader failover.
- All other auth queries (users, passwords, privileges, grants) remain **rejected** on
  coordinators, as today.
- SSO and role management on coordinators are **enterprise features** (require a valid
  license); basic-auth passthrough remains free.

Separately, this work removes two unused constructs — the `COORDINATOR` privilege and
`EnableWritingOnMainRpc` — to reduce confusion.

## User Stories

1. As an operator, I want to connect to a coordinator with a username and password and
   always succeed, so that my existing admin tooling keeps working unchanged.
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
    `GRANT`/`DENY`/`REVOKE`, `SHOW USERS`, `SET ROLE`, `SHOW PRIVILEGES`, …) to be rejected
    on coordinators, so that the coordinator's auth surface stays minimal and predictable.
18. As an administrator, I want SSO login for a given role to be rejected before I create
    that role and to succeed after I create it, so that role provisioning has an observable,
    verifiable effect.
19. As an operator, I want the coordinator to inherit my `MEMGRAPH_SSO_*` environment
    variables into the auth module, so that I configure SSO on coordinators exactly as I do
    on data instances.
20. As an operator, I want to configure the coordinator's SSO scheme-to-module mappings with
    `--auth-module-mappings`, so that I control which schemes and modules the coordinator uses.
21. As an operator running a rolling upgrade, I want a role query forwarded to a
    not-yet-upgraded leader to fail with an error rather than crash the coordinator, so that
    a mixed-version window degrades safely.
22. As an operator without an enterprise license, I want SSO auth and role queries on
    coordinators to be rejected with a license error while basic-auth still works, so that
    licensing is consistent with the rest of auth.
23. As a developer, I want the unused `COORDINATOR` privilege removed, so that the privilege
    model does not advertise a permission that gates nothing.
24. As a developer, I want the unused `EnableWritingOnMainRpc` removed, so that the RPC
    surface reflects what is actually called.
25. As a developer, I want a dedicated e2e suite for coordinator authentication, so that
    every scenario above is protected against regressions.
26. As a developer, I want Kerberos exercised through the real coordinator Bolt handshake
    (not just as an isolated Python unit test), so that the coordinator SSO path has genuine
    Kerberos coverage.

## Implementation Decisions

### Auth semantics on coordinators

- **Basic auth is a passthrough**: `basic`/`none` schemes always succeed on a coordinator,
  credentials ignored (current behavior preserved). No license required.
- **SSO authenticates**: for a scheme present in `--auth-module-mappings`, run the auth
  module for that scheme, then validate the returned roles. Authentication **succeeds only
  if every returned role exists** in the coordinator's role list. Invalid token, any
  missing role, or an unknown scheme (not in mappings) → **connection rejected**.
- **No privilege gating**: once authenticated (basic or SSO), a session may run any query
  that coordinators otherwise allow. Roles exist to satisfy the SSO module contract and
  identity, not to restrict which coordinator queries run.
- **Enterprise-gated**: SSO authentication and the role queries require `MG_ENTERPRISE` +
  a valid license; without it they are rejected with a license error. Basic-auth
  passthrough works in any build.
- The coordinator branch of the Bolt handshake's `AuthenticateUser` is changed from
  "ignore auth on coordinators" to: basic/none → passthrough; SSO scheme in mappings →
  the dedicated coordinator SSO path; otherwise reject.

### Coordinator SSO authenticator (deep module)

- A **dedicated coordinator SSO path** lives in the session/glue layer and shares as little
  as possible with the data-instance `Auth` path. It reuses the existing auth-module
  subprocess machinery (scheme→path mappings, pipe protocol) to run the module, but the
  role-existence check is performed against `CoordinatorState`'s committed role list rather
  than the auth kvstore.
- The authenticator is shaped as a testable unit: given a scheme and an IdP response, it
  produces an authenticated/rejected result, using an injected module-runner and an injected
  role-existence provider (predicate over the coordinator role list).
- `MEMGRAPH_SSO_*` variables are the module's contract and are inherited by the spawned
  module subprocess. **No new C++ env parsing** is added on coordinators.

### Roles as Raft-replicated state

- A new field `std::vector<std::string> roles_` is added to `CoordinatorClusterState` and is
  the **sole source of truth** for coordinator roles. There are no user records and no
  privileges stored on coordinators; the auth kvstore is not used for coordinator roles.
- The field is added through the full established pattern: member + default, all special
  member functions, `operator==` tie-lists, a new optional in `CoordinatorClusterStateDelta`,
  getter/setter, `to_json`/`from_json` (full-state snapshot), `DoAction`,
  `SerializeUpdateClusterState`/`DecodeLog` (per-log delta), and a new key constant.
- **No `LogStoreVersion` bump.** Backward/forward compatibility is achieved the same way as
  the most recent fields: `add_if_set` when serializing the delta, a `contains`-guard in
  `DecodeLog`, and `j.value(key, {})` (default empty) in `from_json`. An older coordinator
  ignores the unknown `roles` key; a newer coordinator reading an older log/snapshot sees an
  empty role set.

### Role query handling and forwarding

- The interpreter's coordinator query gate is extended to permit exactly `CREATE ROLE`,
  `DROP ROLE`, and `SHOW ROLES` (carved out of the auth-query rejection by inspecting the
  auth-query sub-action). All other auth queries remain rejected on coordinators with the
  existing coordinator-only-queries error.
- Semantics **match data instances**: `CREATE ROLE x` errors if `x` already exists, is a
  no-op with `IF NOT EXISTS`; `DROP ROLE x` errors if `x` does not exist.
- New coordinator→coordinator RPCs are introduced: `CreateRoleRpc`, `DropRoleRpc`, and a
  read RPC (`GetRolesRpc`) for `SHOW ROLES`.
- **All three role operations forward to the leader** via the existing `ForwardToLeader`
  mechanism. On the leader, `CREATE`/`DROP` build a `CoordinatorClusterStateDelta` carrying
  the updated roles vector and commit it through `AppendLogAndWaitForCommit`. `SHOW ROLES`
  is also forwarded to the leader (strong read) and returns the leader's committed list.

### Mixed-version behavior (rolling upgrade)

- **No RPC version negotiation.** If a new follower forwards a role RPC to a
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
  decode → identical role set, including the empty and backward-compatible-missing-key
  cases). This directly protects the Raft-persisted `vector<string>` code path in isolation.
  Prior art: existing `CoordinatorClusterState` / coordinator state-machine unit tests.
- **Auth-query gate**: assert that the coordinator gate permits exactly `CREATE ROLE`,
  `DROP ROLE`, and `SHOW ROLES` and rejects all other auth queries, at unit level where
  feasible without standing up a full cluster. Prior art: existing interpreter/required-
  privileges tests.

The role RPCs and the coordinator SSO authenticator are covered by the e2e suite rather
than dedicated unit tests.

### End-to-end tests — `tests/e2e/high_availability/auth.py`

Shaped like `distributed_coords.py` (driven by `interactive_mg_runner`, `ADD COORDINATOR`
wiring) but with **3 coordinators and no data instances**. A dedicated dummy auth module
covering OIDC, SAML, and Kerberos schemes (canned success roles + error cases) is driven
through the **real coordinator Bolt handshake**. The new test is registered in the
directory's `workloads.yaml` and `CMakeLists.txt`.

Functional buckets:

- **Basic-auth passthrough**: username/password always succeeds and the session can run
  coordinator queries; an unknown/SSO scheme not in mappings is rejected.
- **SSO across all three schemes + failures**: OIDC, SAML, Kerberos each succeed when the
  returned role(s) exist; rejection on invalid token, on a missing role, and multi-role
  where all-exist succeeds and any-missing rejects. Kerberos is exercised through the Bolt
  handshake (closing the "no server-path coverage" gap).
- **Role CRUD + follower forwarding**: `CREATE`/`DROP`/`SHOW ROLE` on the leader; the same
  run on a follower and transparently forwarded; duplicate → error, `IF NOT EXISTS` →
  no-op, `DROP` missing → error; forwarded `SHOW ROLES`.
- **Disallowed queries + SSO/role tie-in**: `CREATE USER`, `SET PASSWORD`,
  `GRANT`/`DENY`/`REVOKE`, `SHOW USERS`, `SET ROLE`, `SHOW PRIVILEGES`, etc. rejected on
  coordinators; SSO login for a role rejected before `CREATE ROLE` and succeeding after.

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
- **Privilege enforcement on coordinators**: authenticated sessions are not restricted by
  role; there is no per-role gating of coordinator queries.
- **SSO auto-provisioning of roles**: the data-instance-style scenario where SSO implies
  built-in roles (readonly/readwrite/admin) is explicitly not implemented on coordinators.
- **User management on coordinators**: no users, passwords, or privileges are stored on
  coordinators.
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
