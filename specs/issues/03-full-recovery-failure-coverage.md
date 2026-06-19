# Slice 3 — Full recovery-failure coverage (WAL + enumeration + structural checks)

**Type:** AFK
**Triage:** ready-for-agent

## Parent

`specs/storage-allow-recovery-failure.md`

## What to build

Extend the flag-gated `RecoveryFailure` conversion (slice 1 covered only the
"no usable snapshot" path) to all remaining **data-driven** failure points in the
`RecoverData` call tree, so any corruption type produces a defunct tenant rather
than a crash:

- Snapshot/WAL file-enumeration failures.
- Edges-metadata size mismatch.
- Structural WAL-chain checks: missing prefix WAL (no snapshot); snapshot with no
  WAL covering the pre-snapshot boundary; WAL sequence-number gap.
- Delta-level WAL load failure.

Keep **always fatal** the genuine code-logic invariants (an edge index present
while `properties_on_edges` is disabled) — these are not flag-gated.

Dividing line: a malformed/truncated/partial on-disk file → `RecoveryFailure`
(flag-gated); only an internal code bug can trigger it → keep the assert.

## Acceptance criteria

- [ ] With the flag on, each listed corruption type results in a defunct tenant and a successful boot.
- [ ] With the flag off, each listed corruption type retains today's fatal behavior.
- [ ] The `properties_on_edges` invariants remain always-fatal regardless of the flag.

### Tests (verification gate)

- [ ] **Unit (byte-flip):** copy a real WAL file; for each byte offset, flip the byte, attempt recovery, and assert corruption is detected — with the flag on it yields a defunct/empty storage (no crash); pattern per the helper in the spec's Testing Decisions.
- [ ] **Unit:** a missing prefix WAL file (no snapshot) and a WAL sequence-number gap each produce a defunct tenant with the flag on, and remain fatal with the flag off.
- [ ] **Unit:** an `properties_on_edges`-disabled-with-edge-index directory remains fatal with the flag on.

## Blocked by

- Slice 1 (`specs/issues/01-walking-skeleton-flag-defunct-gate.md`)
