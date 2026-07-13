## Parent

`specs/coordinator-sso-auth/PRD.md`

## What to build

Make `CREATE ROLE`, `DROP ROLE`, and `SHOW ROLES` work from any coordinator by transparently
forwarding to the leader, instead of erroring with not-leader.

End-to-end behavior:

- Introduce coordinator→coordinator RPCs for the three role operations (create, drop, and a
  read for `SHOW ROLES`).
- When a role query runs on a follower, it is forwarded to the leader via the existing
  forward-to-leader mechanism. The leader applies writes through the Raft log and returns
  the committed role list for reads; the client observes the same result regardless of which
  coordinator it connected to.
- Mixed-version safety: if a forwarded role RPC reaches a coordinator that has no handler for
  it (e.g. a not-yet-upgraded leader during a rolling upgrade), the operation fails and
  surfaces an error to the client. This path must never trigger `MG_ASSERT` or crash the
  coordinator. No RPC version negotiation is added.

This slice extends `auth.py` with follower-run role management and the remaining persistence
scenarios that require multiple coordinators.

## Acceptance criteria

- [ ] New coordinator→coordinator RPCs exist for create-role, drop-role, and get-roles, registered and dispatched.
- [ ] `CREATE`/`DROP`/`SHOW ROLE` run on a follower produce the same result as on the leader (writes forwarded and committed; reads reflect committed state).
- [ ] A forwarded role RPC with no handler on the receiver returns an error to the client and does not assert/crash the coordinator.
- [ ] E2E: follower-run CRUD + forwarded `SHOW ROLES`.
- [ ] E2E: lagging-follower catch-up — a coordinator that was down during role changes reconstructs the exact role set on rejoin.
- [ ] E2E: leader failover — after the leader is killed and a new leader elected, the role set is preserved.

## Blocked by

- `03-coordinator-role-store.md`
