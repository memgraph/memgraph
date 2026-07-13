# Slice 1 — Walking skeleton: flag + broken-on-snapshot-failure + query gate

**Type:** HITL (foundation review before fan-out)
**Triage:** ready-for-human

## Parent

`specs/storage-allow-recovery-failure.md`

## What to build

The end-to-end spine of the feature, exercised through the single
"no usable snapshot" recovery failure:

- Add the `--storage-allow-recovery-failure` gflag (default `false`) and carry it
  in `storage::Config` (durability sub-config); thread it into the recovery path.
- Register the new flag in the configuration e2e test so `SHOW CONFIG` stays in
  sync (`tests/e2e/configuration/default_config.py` `startup_config_dict`, and
  `workloads.yaml` if the flag is exercised there).
- When the flag is on, the "no usable snapshot could be recovered" failure throws
  `RecoveryFailure` instead of being fatal. When off, behavior is unchanged.
- The `InMemoryStorage` constructor catches `RecoveryFailure` (flag on): scrubs
  partial state via the existing `Clear()` reset-to-empty path (plus reset of the
  name-id mapper and description store), sets an **in-memory** `broken` flag, and
  **skips the file-move-to-`.backup` branch** so the corrupt `snapshots/`/`wal/`
  files are left byte-for-byte untouched. Construction then succeeds, yielding a
  valid-but-broken storage (no `DbmsHandler` try/catch needed).
- Broken state is surfaced from `Storage` → `Database`.
- Background snapshot writes are guarded: the periodic snapshot handler and the
  exit-snapshot path early-return while broken (no empty snapshot ever
  overwrites the corrupt files). WAL is never written because commits are
  rejected.
- A broad, fail-closed query gate: any query that would operate on a broken
  current database throws the verbatim exception below. Allowlist: the cure query
  `RECOVER SNAPSHOT` plus meta / session / admin queries that operate on
  instance-level or system state rather than the tenant graph —
  `RecoverSnapshotQuery`, `SystemInfoQuery`, `ReplicationInfoQuery`,
  `ShowConfigQuery`, `ShowQueryCallableMappingsQuery`, `SettingQuery`,
  `VersionQuery`, `UseDatabaseQuery`, `MultiDatabaseQuery`, `ShowDatabaseQuery`,
  `ShowDatabasesQuery`, `ShowMemoryInfoQuery`, `SessionTraceQuery`,
  `SessionSettingQuery`, `AuthQuery`, `ReplicationQuery`, `UserProfileQuery`,
  `TenantProfileQuery`, `ParameterQuery`, `TransactionQueueQuery`,
  `LockPathQuery`, `FreeMemoryQuery`, `CoordinatorQuery`, `ReloadSSLQuery`.
  `DatabaseInfoQuery` is deliberately excluded — `SHOW INDEX/CONSTRAINT/NODE
  LABELS/EDGE TYPES/METRICS INFO` would read the empty post-failure storage and
  return a misleading 0-row result instead of surfacing the broken health.
  `RECOVER SNAPSHOT` requires UNIQUE access and must be explicitly exempted.

Exception text (exact):
> Database is in the broken state because the recovery process failed. Please recover your database using the RECOVER SNAPSHOT query. If you have a backup of the whole data directory, please replace the current data directory with the backup one and restart the process.

In-memory storage only.

## Acceptance criteria

- [ ] `--storage-allow-recovery-failure` exists, defaults to `false`, lives in `Config.durability`.
- [ ] The flag is registered in `tests/e2e/configuration/default_config.py` and the configuration check passes.
- [ ] With the flag off, a tenant that fails snapshot recovery still aborts startup (unchanged behavior).
- [ ] With the flag on, an instance with a corrupt/unusable snapshot for one tenant boots successfully.
- [ ] The failed tenant is empty in memory and its on-disk `snapshots/`/`wal/` are byte-for-byte unchanged after boot.
- [ ] A data query against the broken tenant throws the exact exception text above.
- [ ] No periodic or exit snapshot is written for a broken tenant.

### Tests (verification gate)

- [ ] **Unit:** constructing `InMemoryStorage` over a directory whose only snapshot is corrupt, with the flag on, yields a storage reporting `broken`, with zero vertices/edges, and with the on-disk files unchanged (compare directory contents/bytes before and after).
- [ ] **Unit:** same construction with the flag off throws/aborts as today.
- [ ] **E2E (`tests/e2e/configuration/`):** the existing configuration check passes with `--storage-allow-recovery-failure` present in `SHOW CONFIG`.
- [ ] **E2E (`tests/e2e/durability/`):** boot a single instance with a corrupted snapshot for one tenant + flag on; assert the instance is up; assert a `MATCH`/`CREATE` against that tenant returns the exact broken error.

## Blocked by

- None — can start immediately.
