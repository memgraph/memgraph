# Slice 5 — REPAIR DATABASE query (end-to-end)

**Type:** AFK
**Triage:** ready-for-agent

## Parent

`specs/storage-allow-recovery-failure.md`

## What to build

A new `REPAIR DATABASE` query that resets a defunct tenant to an empty working
state:

- Grammar + AST node + visitor + interpreter handler. Operates on the **current**
  database (no name argument), mirroring `RECOVER SNAPSHOT`. Available in both
  Community and Enterprise.
- **Main-only** in a cluster (reject on a replica, like `RECOVER SNAPSHOT`).
- **Defunct-only:** throw on a healthy (non-defunct) database to prevent
  accidental data loss.
- Takes UNIQUE access. Moves the tenant's corrupt `snapshots/`/`wal/` files to
  `.old` when `--storage-backup-dir-enabled` is true, otherwise deletes them;
  resets the tenant to empty; clears the `defunct` flag; resumes background
  durability.
- On success, emits an INFO notification (new `REPAIR_DATABASE` notification
  code): title "Database '<name>' repaired."; description stating the database is
  now empty and import queries can be run.

## Acceptance criteria

- [ ] `REPAIR DATABASE` parses to its AST node and is rejected inside a multicommand transaction (consistent with `RECOVER SNAPSHOT`).
- [ ] On a defunct tenant: resets to empty, clears defunct, emits the repair notification; subsequent import queries succeed.
- [ ] On a healthy tenant: throws (defunct-only gate).
- [ ] On a replica: rejected (main-only).
- [ ] Corrupt files moved to `.old` when backup dirs enabled, deleted otherwise; a restart after repair recovers cleanly (no re-defunct).

### Tests (verification gate)

- [ ] **Unit (`cypher_main_visitor`):** `REPAIR DATABASE` produces the `RepairDatabaseQuery` AST node.
- [ ] **Unit:** repair on a defunct storage resets to empty + clears defunct + moves files to `.old`; repair on a healthy storage throws.
- [ ] **E2E (`tests/e2e/durability/`):** boot with a defunct tenant; `USE DATABASE <defunct>` + `REPAIR DATABASE`; assert the success notification is present; run import queries and assert data is queryable; restart and assert `ready`.

## Blocked by

- Slice 1 (`specs/issues/01-walking-skeleton-flag-defunct-gate.md`)
