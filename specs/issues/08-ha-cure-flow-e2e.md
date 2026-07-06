# Slice 8 — HA cure-flow e2e coverage

**Type:** AFK
**Triage:** ready-for-agent

## Parent

`specs/storage-allow-recovery-failure.md`

## What to build

Coordinator-based e2e coverage for the operator cure flows in a clustered
deployment. Styled after `tests/e2e/high_availability/distributed_coords.py`
(coordinators + data instances; `test_name` fixture, `file = ...`, fixtures,
`interactive_mg_runner` instance-description dicts), placed under
`tests/e2e/durability/`.

Scenarios:
1. The **main** boots with a corrupted tenant → the admin cures it with `RECOVER
   SNAPSHOT`; the cluster returns to healthy.
2. **Both** main and replica boot with the corrupted tenant → verify the
   `defunct` / `ready` status via **both** `SHOW STORAGE INFO` and `SHOW
   DATABASES` on **both** instances, then cure and verify recovery.
3. The **`RESET DATABASE` + import-queries** cure path in the HA setting (not
   only `RECOVER SNAPSHOT`).

No new production code is expected beyond what slices 4, 5, and 7 deliver; this
slice is the HA verification surface.

## Acceptance criteria

- [ ] Scenario 1 passes: main corrupt → `RECOVER SNAPSHOT` on main → cluster healthy, data served.
- [ ] Scenario 2 passes: both corrupt → status verified as `defunct` on both via `SHOW STORAGE INFO` and `SHOW DATABASES`; after cure both report `ready`.
- [ ] Scenario 3 passes: `RESET DATABASE` on main + import → tenant healthy and queryable across the cluster.

### Tests (verification gate)

- [ ] The three coordinator-based e2e scenarios above, under `tests/e2e/durability/`.

## Blocked by

- Slice 4 (`specs/issues/04-cure-recover-snapshot.md`)
- Slice 5 (`specs/issues/05-reset-database-query.md`)
- Slice 7 (`specs/issues/07-replica-self-heal.md`)
