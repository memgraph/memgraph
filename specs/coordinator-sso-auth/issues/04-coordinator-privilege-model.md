## Parent

`specs/coordinator-sso-auth/PRD.md`

## What to build

Give coordinators a two-privilege access-control model — **`COORDINATOR_READ`** and
**`COORDINATOR_WRITE`** — attached to roles, with enforcement on every coordinator query and
on the routing table. This slice is leader-only for the GRANT/REVOKE writes (follower
forwarding is generalized in the next slice) but the enforcement and grammar are complete.

The privilege names are deliberately prefixed: bare `READ`/`WRITE` already exist in the
grammar as data-instance fine-grained privileges, so the coordinator privileges must be
distinct tokens.

End-to-end behavior:

- Introduce two coordinator privileges, `COORDINATOR_READ` and `COORDINATOR_WRITE`, where
  `COORDINATOR_WRITE` is a superset of `COORDINATOR_READ`. Each is a new `AuthQuery::Privilege`
  value + `Permission` bit, added to the grammar `privilege` rule. The previously-removed
  `COORDINATOR` privilege stays removed and its bit is not reused.
- The coordinator carve-out reuses the existing `grantPrivilege`/`revokePrivilege` grammar
  but inspects the content:
  - `GRANT COORDINATOR_READ|COORDINATOR_WRITE TO ROLE <role>` / `REVOKE ... FROM ROLE <role>`
    set/clear the role's mask (stored in the Raft role struct from slice 03).
  - `GRANT ALL PRIVILEGES TO ROLE <role>` grants **both** coordinator privileges;
    `REVOKE ALL PRIVILEGES FROM ROLE <role>` removes **both**.
  - `SHOW PRIVILEGES FOR ROLE <role>` reports the role's privilege; the trailing
    `ON MAIN|CURRENT|DATABASE` clause is rejected (coordinators have no database).
  - `SHOW ROLES` stays name-only.
- Explicitly rejected on coordinators (they share grammar with the accepted forms):
  `DENY` in any form; a privilege list containing any non-coordinator privilege;
  fine-grained access control (`GRANT ... ON NODES CONTAINING LABELS ...` / `... ON EDGES OF
  TYPE ...`); property-permission grants; and `GRANT/DENY/REVOKE DATABASE` and
  `SET MAIN DATABASE`. All raise the coordinator-not-allowed error.
- Every coordinator query is classified READ or WRITE and enforced via the existing
  required-privilege extraction + `CheckAuthorized` path: a READ query requires
  `COORDINATOR_READ` or `COORDINATOR_WRITE`; a WRITE query requires `COORDINATOR_WRITE`.
  Classification — READ: routing-table read, `SHOW COORDINATOR SETTINGS`, `SHOW INSTANCE`,
  `SHOW INSTANCES`, `SHOW REPLICATION LAG`, and read-only introspection (`SHOW ROLES`,
  `SHOW PRIVILEGES FOR ROLE`, `SHOW CONFIG`, `SYSTEM INFO`, SHOW-form settings). WRITE: all
  mutating/admin queries plus `CREATE ROLE`, `DROP ROLE`, `GRANT`, `REVOKE`, SET-form
  settings, `RELOAD SSL`.
- The routing table (a Bolt `ROUTE` message that bypasses the query privilege path) is gated
  by an explicit `COORDINATOR_READ` check added in the ROUTE handler against the session mask.
- A basic-auth passthrough session carries full `COORDINATOR_WRITE` (backward compatible), so
  all existing basic-auth flows continue to work and are the vehicle for demoing enforcement
  in this slice. A role with no grant confers nothing.
- SSO, role queries, `GRANT`/`REVOKE`, `SHOW PRIVILEGES FOR ROLE`, and enforcement are
  enterprise-gated; basic-auth full-`COORDINATOR_WRITE` keeps community/unlicensed unaffected.

The READ-vs-WRITE *denial* (a non-WRITE session blocked from mutating queries) is proven in
the SSO slice, since only an SSO session yields a non-WRITE effective mask; this slice
verifies grammar, persistence of grants, and that basic-auth `COORDINATOR_WRITE` runs
everything.

## Acceptance criteria

- [ ] `COORDINATOR_READ` and `COORDINATOR_WRITE` exist as `AuthQuery::Privilege` values + `Permission` bits and grammar `privilege` tokens; the removed `COORDINATOR` bit is not reused.
- [ ] `GRANT`/`REVOKE COORDINATOR_READ|COORDINATOR_WRITE TO/FROM ROLE <role>` update the role's persisted mask on the leader; `SHOW PRIVILEGES FOR ROLE <role>` reports it; a bare role shows no privilege.
- [ ] `GRANT ALL PRIVILEGES TO ROLE <role>` grants both coordinator privileges; `REVOKE ALL PRIVILEGES FROM ROLE <role>` removes both.
- [ ] `DENY` (any form), FGAC/entity privileges, property-permission grants, `GRANT DATABASE`/`SET MAIN DATABASE`, a privilege list with any non-coordinator privilege, and a trailing `ON ...` on `SHOW PRIVILEGES FOR ROLE` are all rejected with the coordinator-not-allowed error.
- [ ] Every coordinator query carries a READ or WRITE required-privilege per the classification above; the routing-table ROUTE handler performs an explicit `COORDINATOR_READ` check against the session mask.
- [ ] A basic-auth session (full `COORDINATOR_WRITE`) can run every coordinator query and read the routing table.
- [ ] Enforcement, `GRANT`/`REVOKE`, and `SHOW PRIVILEGES FOR ROLE` require a valid enterprise license; without it they are rejected while basic-auth still works.
- [ ] Unit test: effective-privilege union across masks; the authorization check (`COORDINATOR_READ` satisfied by either; `COORDINATOR_WRITE` needs `COORDINATOR_WRITE`; bare role confers nothing); `ALL PRIVILEGES` maps to both.
- [ ] Unit test (role store): the privilege mask survives the delta + snapshot serialization roundtrip (extends slice 03's roundtrip test).
- [ ] E2E: `GRANT`/`REVOKE`/`GRANT ALL`/`SHOW PRIVILEGES FOR ROLE` round-trip on the leader; bare-role shows none; and `DENY`, `GRANT DATABASE`, and fine-grained access control queries are rejected on coordinators.

## Blocked by

- `03-coordinator-role-store.md`
