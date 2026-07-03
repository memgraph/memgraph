# Global read-only: enforce read-only on a running main

## Parent

`specs/read_only/global-read-only-ha.md`

## What to build

Make toggling the `global_read_only` coordinator setting actually take effect, online, on the current main — without any restart or re-promotion. When the operator sets it to `true`, the running main stops accepting write queries; when set back to `false`, writes resume.

End-to-end behavior:
- With a healthy cluster, `SET COORDINATOR SETTING "global_read_only" TO "true"` causes subsequent write queries on the main to be rejected within a reconciliation cycle.
- `SET COORDINATOR SETTING "global_read_only" TO "false"` causes writes to be accepted again.
- Enforcement covers all write sources on the main (user Cypher writes, TTL background expiry, stream-driven writes, trigger-driven writes) because they all key off the same `IsMainWriteable()` gate.
- The propagation is declarative and self-healing: a transient RPC failure is retried on the next reconciliation cycle, so the setting is never silently dropped.

Implementation notes (from the resolved design in the parent PRD):
- Reuse the existing `RoleMainData::writing_enabled_` bit — the coordinator's `global_read_only` is the source of truth, the main's `writing_enabled_` is its projection. No second boolean.
- Add `ReplicationState::DisableWritingOnMain()` alongside the existing `EnableWritingOnMain()`.
- Bump `UpdateDataInstanceConfigRpc` to v2: combined payload `{ deltas_batch_progress_size, disable_writing }` with a `Downgrade()` that drops `disable_writing`. The two config items now travel together (every send carries both desired values). No `StateCheck`/`InstanceState` change — the main already reports `is_writing_enabled`.
- The v2 handler applies `deltas_batch_progress_size` unconditionally, but applies the writing flag (`writing_enabled = !disable_writing`) **only under a hard `IsMain()` guard** — accessing the main-role state on a replica asserts/crashes.
- Reconciliation loop:
  1. Gate the existing "re-promote a non-writeable main" trigger so it only fires when NOT read-only (`!is_writing_enabled AND NOT global_read_only`), so a deliberately read-only main is not re-promoted every cycle. This branch still covers restart recovery and the clear-read-only re-enable direction via the idempotent promotion path.
  2. Add a reconciliation block in the current-main branch: if reported `is_writing_enabled` diverges from `!global_read_only`, send `UpdateDataInstanceConfigRpc` with `disable_writing = global_read_only`.

## Acceptance criteria

- [ ] Enabling `global_read_only` on a running cluster causes write queries on the main to be rejected (converges within a reconciliation cycle).
- [ ] Disabling `global_read_only` causes writes to be accepted again.
- [ ] `SHOW COORDINATOR SETTINGS` reflects the toggled value.
- [ ] A read-only main is NOT continuously re-promoted by the reconciliation loop.
- [ ] The `disable_writing` handler is a no-op on a replica (hard `IsMain()` guard; no crash).
- [ ] Unit test: `UpdateDataInstanceConfig` v2 SLK round-trip + `Downgrade`/v1 compatibility.
- [ ] Unit test: `ReplicationState` `IsMainWriteable`/`EnableWritingOnMain`/`DisableWritingOnMain` transitions for main and replica roles.
- [ ] E2E test (new file under `tests/e2e/high_availability/`, registered in that directory's `workloads.yaml` + `CMakeLists.txt`): enable -> writes rejected; disable -> writes pass; `SHOW COORDINATOR SETTINGS` reflects the value.

## Blocked by

- `01-coordinator-setting.md`
