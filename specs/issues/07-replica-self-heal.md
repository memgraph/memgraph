# Slice 7 — Replica self-heal (HA)

**Type:** AFK
**Triage:** ready-for-agent

## Parent

`specs/storage-allow-recovery-failure.md`

## What to build

A defunct tenant on a replica heals automatically from the main:

- A defunct replica tenant reports an initial commit timestamp with a fresh
  epoch (a consequence of `Clear()`), so the main drives it into the `RECOVERY`
  state and sends a full snapshot before any incremental delta.
- The replica-side snapshot-receive handler clears the `defunct` flag on a
  successful snapshot load, after which the tenant resumes normal replication.
- No defunct check is added to the incremental commit handler — add a comment
  documenting the invariant: a defunct tenant is always routed through `RECOVERY`
  → snapshot (which clears defunct) before any incremental delta can arrive, so
  the handler is never reached while defunct.

## Acceptance criteria

- [ ] A replica that boots with a corrupted tenant (flag on) while the main is healthy self-heals: the main full-syncs the tenant and the replica clears defunct.
- [ ] After self-heal, the replica serves reads of that tenant and shows `ready`.
- [ ] A defunct tenant on one instance does not disrupt replication of other healthy tenants.

### Tests (verification gate)

- [ ] **E2E (`tests/e2e/durability/`, coordinator-based, styled after `tests/e2e/high_availability/distributed_coords.py`):** bring up a coordinator-managed cluster; the replica boots with a corrupted tenant while the main is healthy; assert the replica transitions from `defunct` to `ready` (via `SHOW DATABASES` / `SHOW STORAGE INFO`) and serves the tenant's data.

## Blocked by

- Slice 1 (`specs/issues/01-walking-skeleton-flag-defunct-gate.md`)
- Slice 2 (`specs/issues/02-status-reporting.md`)
