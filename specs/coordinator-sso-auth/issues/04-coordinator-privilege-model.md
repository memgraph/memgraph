## Parent

`specs/coordinator-sso-auth/PRD.md`

## What to build

Give coordinators a two-privilege access-control model — **READ** and **WRITE** — attached
to roles, with enforcement on every coordinator query and on the routing table. This slice
is leader-only for the GRANT/REVOKE writes (follower forwarding is generalized in the next
slice) but the enforcement and grammar are complete.

End-to-end behavior:

- Introduce two coordinator privileges: **READ** and **WRITE**, where **WRITE is a superset
  of READ**. Implemented as a new `AuthQuery::Privilege` value and a `Permission` bit each.
  The previously-removed `COORDINATOR` privilege stays removed and its bit is not reused.
- `GRANT READ|WRITE TO <role>` and `REVOKE READ|WRITE FROM <role>` set/clear the role's mask
  (stored in the Raft role struct from slice 03); `SHOW PRIVILEGES FOR <role>` reports it.
  These are carved into the coordinator auth allowlist (GRANT/REVOKE restricted to the
  READ/WRITE privileges); all other privilege/user auth queries stay rejected.
- Every coordinator query is classified READ or WRITE and enforced: a READ query requires
  READ or WRITE; a WRITE query requires WRITE. Classification — READ: routing-table read,
  `SHOW COORDINATOR SETTINGS`, `SHOW INSTANCE`, `SHOW INSTANCES`, `SHOW REPLICATION LAG`,
  and read-only introspection (`SHOW ROLES`, `SHOW PRIVILEGES FOR`, `SHOW CONFIG`,
  `SYSTEM INFO`, SHOW-form settings). WRITE: all mutating/admin queries plus `CREATE ROLE`,
  `DROP ROLE`, `GRANT`, `REVOKE`, `SET`-form settings, `RELOAD SSL`.
- Enforcement reuses the existing required-privilege extraction + `CheckAuthorized` path,
  comparing the query's required privilege against the session's effective mask. The
  **routing table** (a Bolt `ROUTE` message that bypasses the query privilege path) is gated
  by an explicit READ check added in the ROUTE handler against the session's mask.
- A basic-auth passthrough session carries full WRITE (backward compatible), so all existing
  basic-auth flows continue to work and are the vehicle for demoing enforcement in this
  slice. A role with no grant confers nothing.
- SSO, role queries, `GRANT`/`REVOKE`, `SHOW PRIVILEGES FOR`, and enforcement are
  enterprise-gated; basic-auth full-WRITE keeps community/unlicensed unaffected.

The READ-vs-WRITE *denial* (a non-WRITE session blocked from mutating queries) is proven in
the SSO slice, since only an SSO session yields a non-WRITE effective mask; this slice
verifies grammar, persistence of grants, and that basic-auth WRITE runs everything.

## Acceptance criteria

- [ ] Two new coordinator privileges (READ, WRITE) exist as `AuthQuery::Privilege` values + `Permission` bits; the removed `COORDINATOR` bit is not reused.
- [ ] `GRANT READ|WRITE TO <role>` and `REVOKE ... FROM <role>` update the role's persisted mask on the leader; `SHOW PRIVILEGES FOR <role>` reports it; a bare role shows no privilege.
- [ ] `GRANT`/`REVOKE`/`SHOW PRIVILEGES` are accepted only for coordinator READ/WRITE on roles and rejected for any other auth target.
- [ ] Every coordinator query carries a READ or WRITE required-privilege per the classification above; the routing-table ROUTE handler performs an explicit READ check against the session mask.
- [ ] A basic-auth session (full WRITE) can run every coordinator query and read the routing table.
- [ ] Enforcement, `GRANT`/`REVOKE`, and `SHOW PRIVILEGES FOR` require a valid enterprise license; without it they are rejected while basic-auth still works.
- [ ] Unit test: effective-privilege union across multiple masks and the authorization check (READ satisfied by READ or WRITE; WRITE needs WRITE; bare role confers nothing).
- [ ] Unit test (role store): the privilege mask survives the delta + snapshot serialization roundtrip (extends slice 03's roundtrip test).
- [ ] E2E: `GRANT`/`REVOKE`/`SHOW PRIVILEGES FOR` round-trip on the leader; bare-role shows none; rejected for non-role targets.

## Blocked by

- `03-coordinator-role-store.md`
