# Slice 4 — Cure via RECOVER SNAPSHOT

**Type:** AFK
**Triage:** ready-for-agent

## Parent

`specs/storage-allow-recovery-failure.md`

## What to build

Make `RECOVER SNAPSHOT` a working cure for a defunct tenant:

- On successful `RecoverSnapshot`, clear the in-memory `defunct` flag (under the
  query's existing UNIQUE access) and resume normal background durability.
- Rely on the existing `RecoverSnapshot` behavior that moves all prior/corrupt
  snapshots and WAL files to `.old` (or deletes them when backup directories are
  disabled), leaving a restart-clean single-snapshot directory.
- No defunct-only restriction: `RECOVER SNAPSHOT` remains usable on ordinary
  empty databases. The defunct placeholder is empty, so it passes the existing
  non-empty-storage precondition.

## Acceptance criteria

- [ ] Running `RECOVER SNAPSHOT` against a defunct tenant clears defunct; subsequent queries succeed and return the recovered data.
- [ ] After the cure, the durability directory contains only the recovered snapshot (prior/corrupt files moved to `.old` or deleted per `--storage-backup-dir-enabled`).
- [ ] A restart after the cure recovers the tenant normally (it does not re-enter defunct).

### Tests (verification gate)

- [ ] **E2E (`tests/e2e/durability/`):** boot with a defunct tenant; `USE DATABASE <defunct>` + `RECOVER SNAPSHOT <good-snapshot>`; assert queries return expected data; restart the instance and assert the tenant is `ready` with data intact.
- [ ] **Unit:** `RecoverSnapshot` on a defunct placeholder clears defunct and leaves a clean single-snapshot directory.

## Blocked by

- Slice 1 (`specs/issues/01-walking-skeleton-flag-defunct-gate.md`)
