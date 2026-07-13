## Parent

`specs/coordinator-sso-auth/PRD.md`

## What to build

The foundational roles slice. Give coordinators a Raft-replicated list of role names and
the ability to manage it with `CREATE ROLE`, `DROP ROLE`, and `SHOW ROLES` — initially only
when the query runs on the leader (follower forwarding is a later slice).

End-to-end behavior:

- A new `roles_` role-name list becomes part of the coordinator's replicated cluster state
  and is the sole source of truth for coordinator roles (no user records, no privileges, no
  auth kvstore roles).
- The coordinator's auth-query gate is carved out to permit exactly `CREATE ROLE`,
  `DROP ROLE`, and `SHOW ROLES`; every other auth query (users, passwords, privileges,
  grants) is rejected on coordinators as before.
- On the leader, `CREATE ROLE`/`DROP ROLE` commit a change to the role list through the Raft
  log; `SHOW ROLES` returns the committed list. Semantics match data instances: duplicate
  `CREATE` errors, `IF NOT EXISTS` is a no-op, `DROP` of a missing role errors.
- Run on a follower in this slice, role queries error with the existing not-leader behavior.
- The role list is durable: it survives a full-cluster restart via log/snapshot replay.
- The new state field follows the established add-a-field pattern and stays
  backward/forward compatible without a log-store version bump (delta emitted only when set,
  decode guarded by key presence, snapshot decode defaults to an empty list).

This slice also stands up the e2e suite `tests/e2e/high_availability/auth.py` (3
coordinators, no data instances), registered in that directory's `workloads.yaml` and
`CMakeLists.txt`, and uses it to cover basic-auth passthrough, disallowed-query rejection,
on-leader role CRUD, and restart persistence.

## Acceptance criteria

- [ ] `roles_` is added to the coordinator cluster state through every layer (member/defaults, special members, equality, delta optional, getter/setter, snapshot to/from JSON, apply-action, per-log delta serialize/decode, key constant).
- [ ] No log-store version bump; an older log/snapshot decodes to an empty role list and an unknown `roles` key is ignored.
- [ ] The coordinator gate permits exactly CREATE/DROP/SHOW ROLE and rejects all other auth queries with the coordinator-only-queries error.
- [ ] On the leader: `CREATE ROLE x` adds x and appears in `SHOW ROLES`; duplicate errors; `IF NOT EXISTS` is a no-op; `DROP ROLE x` removes x; `DROP` of a missing role errors.
- [ ] The role list survives a full-cluster restart (created roles still returned by `SHOW ROLES`).
- [ ] Unit tests: role-store add/drop/list and delta + full-snapshot serialization roundtrip (including empty and missing-key cases).
- [ ] Unit test: the coordinator auth-query gate admits only the three role queries and rejects others.
- [ ] `auth.py` exists (3 coordinators, no data instances), registered in `workloads.yaml` and `CMakeLists.txt`, and covers basic-auth passthrough, disallowed-query rejection, on-leader role CRUD, and full-cluster-restart persistence.

## Blocked by

None - can start immediately
