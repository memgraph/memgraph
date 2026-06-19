# Slice 6 — DROP DATABASE on a defunct tenant

**Type:** AFK
**Triage:** ready-for-agent

## Parent

`specs/storage-allow-recovery-failure.md`

## What to build

Ensure an operator can abandon a defunct tenant via `DROP DATABASE`:

- `DROP DATABASE <defunct>` is not caught by the defunct query-gate (it targets a
  named database, not the current tenant's graph) and operates on the defunct
  placeholder like any other database.
- The deferred storage-directory removal also wipes the corrupt on-disk files.

This slice is primarily verification + any small fix needed to confirm the path
works; no new data model.

## Acceptance criteria

- [ ] `DROP DATABASE <defunct>` succeeds and the tenant disappears from `SHOW DATABASES`.
- [ ] The tenant's on-disk directory (including the corrupt files) is removed.
- [ ] Dropping a defunct tenant does not affect other tenants.

### Tests (verification gate)

- [ ] **E2E (`tests/e2e/durability/`):** boot with a defunct tenant alongside a healthy one; `DROP DATABASE <defunct>`; assert it is gone from `SHOW DATABASES`, its directory is removed, and the healthy tenant is unaffected.

## Blocked by

- Slice 1 (`specs/issues/01-walking-skeleton-flag-defunct-gate.md`)
