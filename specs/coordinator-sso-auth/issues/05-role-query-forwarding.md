## Parent

`specs/coordinator-sso-auth/PRD.md`

## What to build

Make every coordinator role and privilege query work from any coordinator by transparently
forwarding to the leader, instead of erroring with not-leader. This generalizes the
leader-only writes from slices 03 and 04.

End-to-end behavior:

- Introduce coordinator→coordinator RPCs for the write operations (create role, drop role,
  grant, revoke) and the read operations (get roles for `SHOW ROLES`, get role privileges
  for `SHOW PRIVILEGES FOR`).
- When any of these queries runs on a follower, it is forwarded to the leader via the
  existing forward-to-leader mechanism. The leader applies writes through the Raft log and
  returns committed data for reads; the client observes the same result regardless of which
  coordinator it connected to.
- Mixed-version safety: if a forwarded role/privilege RPC reaches a coordinator that has no
  handler for it (e.g. a not-yet-upgraded leader during a rolling upgrade), the operation
  fails and surfaces an error to the client. This path must never trigger `MG_ASSERT` or
  crash the coordinator. No RPC version negotiation is added.

This slice extends `auth.py` with follower-run role and privilege management and the
persistence scenarios that require multiple coordinators.

## Acceptance criteria

- [ ] Coordinator→coordinator RPCs exist for create-role, drop-role, grant, revoke, get-roles, and get-role-privileges, registered and dispatched.
- [ ] `CREATE`/`DROP`/`SHOW ROLE`, `GRANT`/`REVOKE`, and `SHOW PRIVILEGES FOR` run on a follower produce the same result as on the leader (writes forwarded and committed; reads reflect committed state).
- [ ] A forwarded role/privilege RPC with no handler on the receiver returns an error to the client and does not assert/crash the coordinator.
- [ ] E2E: follower-run role CRUD, follower-run `GRANT`/`REVOKE`, and forwarded `SHOW ROLES` / `SHOW PRIVILEGES FOR`.
- [ ] E2E: lagging-follower catch-up — a coordinator down during role/privilege changes reconstructs the exact role set and masks on rejoin.
- [ ] E2E: leader failover — after the leader is killed and a new leader elected, the role set and masks are preserved.

## Blocked by

- `04-coordinator-privilege-model.md`
