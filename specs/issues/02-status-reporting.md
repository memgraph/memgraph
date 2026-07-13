# Slice 2 — Broken/ready health reporting

**Type:** AFK
**Triage:** ready-for-agent

## Parent

`specs/storage-allow-recovery-failure.md`

## What to build

Surface a tenant's broken/ready state through the two operator-facing queries:

- `SHOW DATABASES` gains a `Health` column, with values `ready` or
  `broken`, populated by reading each database's broken flag.
- `SHOW STORAGE INFO` gains an always-present key/value row `health` =
  `ready` / `broken` (for both healthy and broken databases).

## Acceptance criteria

- [ ] `SHOW DATABASES` returns a `Health` column (alongside `Name`, `State`); broken tenants show `broken`, healthy show `ready`.
- [ ] `SHOW STORAGE INFO` (current DB and `ON DATABASE <name>`) includes a `health` row with `ready`/`broken`.
- [ ] Both queries work when run against / listing a broken tenant (they are on the gate allowlist).

### Tests (verification gate)

- [ ] **E2E (`tests/e2e/durability/`):** with one broken and one healthy tenant, assert `SHOW DATABASES` reports `broken` and `ready` for the respective rows.
- [ ] **E2E:** `SHOW STORAGE INFO ON DATABASE <broken>` contains `health = broken`; for a healthy DB it contains `health = ready`.

## Blocked by

- Slice 1 (`specs/issues/01-walking-skeleton-flag-broken-gate.md`)
