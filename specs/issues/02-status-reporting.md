# Slice 2 — Defunct/ready status reporting

**Type:** AFK
**Triage:** ready-for-agent

## Parent

`specs/storage-allow-recovery-failure.md`

## What to build

Surface a tenant's defunct/ready state through the two operator-facing queries:

- `SHOW DATABASES` gains a second column, `Status`, with values `ready` or
  `defunct`, populated by reading each database's defunct flag.
- `SHOW STORAGE INFO` gains an always-present key/value row `status` =
  `ready` / `defunct` (for both healthy and defunct databases).

## Acceptance criteria

- [ ] `SHOW DATABASES` returns columns `Name`, `Status`; defunct tenants show `defunct`, healthy show `ready`.
- [ ] `SHOW STORAGE INFO` (current DB and `ON DATABASE <name>`) includes a `status` row with `ready`/`defunct`.
- [ ] Both queries work when run against / listing a defunct tenant (they are on the gate allowlist).

### Tests (verification gate)

- [ ] **E2E (`tests/e2e/durability/`):** with one defunct and one healthy tenant, assert `SHOW DATABASES` reports `defunct` and `ready` for the respective rows.
- [ ] **E2E:** `SHOW STORAGE INFO ON DATABASE <defunct>` contains `status = defunct`; for a healthy DB it contains `status = ready`.

## Blocked by

- Slice 1 (`specs/issues/01-walking-skeleton-flag-defunct-gate.md`)
