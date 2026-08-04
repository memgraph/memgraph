# HA analytical import — design

**Status**: design interview complete; all ten questions resolved. Ready to implement.
**Started**: 2026-08-03. **Completed**: 2026-08-04.

## Target workload

An HA cluster whose single data instance:

1. switches to `IN_MEMORY_ANALYTICAL`,
2. performs a bulk import,
3. switches back to `IN_MEMORY_TRANSACTIONAL` — this writes the snapshot,
4. has replicas registered back.

A manual `CREATE SNAPSHOT` between the import and the switch back was in the original
workload and was dropped by decision 8.

Operator constraint stated up front: replica registration **and** unregistration must
happen only while in transactional mode.

## Decisions taken

| # | Decision | Choice |
|---|---|---|
| 1 | Precondition for an HA data instance to enter analytical | Instance holds the MAIN role **and** has zero registered replication clients |
| 2 | Enforcement of "register only in transactional" | Reject up-front, before any replication state is mutated — both the coordinator RPC path and the user-facing `REGISTER REPLICA`; same for unregister |
| 3 | Multi-tenancy scope of both gates | Instance-wide, all-or-nothing: any database in analytical blocks registration; entering analytical anywhere requires zero replicas instance-wide |
| 4 | Replica state on re-registration | Originally **assumed empty**, with no recovery-path changes — the existing path is correct for an empty replica (finding 5). **Superseded by decision 9**: the assumption does not hold, because unregistering never wipes the replica (finding 7b), so recovery is fixed instead of assumed |
| 5 | `--storage-mode=IN_MEMORY_ANALYTICAL` at startup for data instances | Stays forbidden; analytical is reachable only via the runtime query from a transactional start |
| 6 | Observability when `REGISTER INSTANCE` lands while the main is analytical | Keep returning `SUCCESS`; the reconciliation loop heals it once the main is transactional again. The data instance logs the cause on reject. No `StateCheckRes` version bump, no new `SHOW INSTANCES` column. Operator docs state that registration belongs in transactional mode only |
| 7 | Serialization of the two gates | Defence in depth **plus** a strict window close: friendly instance-wide check in `PrepareStorageModeQuery`, hard invariant inside `SetStorageMode` under UNIQUE, register-side check under the `repl_state_` lock — and both the final mode store and the client insert re-check under `replication_storage_clients_`' own lock, making "analytical with a live client" unrepresentable |
| 8 | `CREATE SNAPSHOT` while analytical (step 3 of the workload) | Dropped from the recipe. The analytical→transactional switch already writes a correctly stamped snapshot; a manual one taken while analytical skips the digest check and is stamped with a pre-import timestamp |
| 9 | Non-empty replica on re-registration | **Make recovery self-correct**, derived entirely from the durability files so it survives a main restart. Three parts: finalize the WAL when *entering* analytical; in `GetRecoverySteps`, treat "the latest snapshot's `durable_timestamp` lies inside no WAL file's `[from, to]` range" as proof the WAL chain cannot reproduce that data and force the snapshot path; keep a `snapshot_durable_ts > replica_commit` guard on that override. No refusal, no operator wipe, no in-memory flag, no persisted boundary, no snapshot format change. Supersedes an earlier "detect and refuse" choice, which was abandoned because its flag was in-memory and a main restart forgot it |
| 10 | Test coverage | One new e2e file, `analytical_import.py`, with four scenarios: **A** nothing ingested anywhere before the switch, **B** data ingested on main *and* replica beforehand with the replica left strictly behind, **B2** B with a main restart before re-registration, **C** deferred registration. Plus unit tests pinning each gate, and a fires/does-not-fire pair for decision 9's override |

## Work to do

### Gates

1. `PrepareStorageModeQuery` — replace the blanket `IsDataInstance()` rejection with the
   decision-1 gate: MAIN role and zero registered replicas, instance-wide, with an error
   message naming what blocks the switch. This is the operator-facing check; it is
   advisory, since the state can change before the mode store lands.
2. `InMemoryStorage::SetStorageMode` — under the UNIQUE hold it already takes, refuse the
   transition to analytical when this storage has replication clients. It already throws
   `BasicException` for active constraints and a non-idle async indexer, so this joins an
   established rejection point.
3. `RegisterReplica_` — reject before any replication state is mutated when any database is
   analytical, under the `repl_state_` write lock it already holds. This is what makes the
   permanent-`NAME_EXISTS` trap of finding 4 unreachable.
4. The strict window close from decision 7: the `storage_mode_` store in `SetStorageMode`
   and the client insert in `RegisterReplica_` both do their final check inside
   `replication_storage_clients_`' own lock. Note the store currently sits *after* the
   switch-back snapshot, at the end of the `if (storage_mode_ != new_storage_mode)` block.
5. The same up-front rejection for the unregister paths.

### Self-correcting recovery for a stale replica (decision 9)

6. `InMemoryStorage::SetStorageMode`, transactional→analytical branch — finalize the WAL
   (`wal_file_->FinalizeWal()`, `wal_file_.reset()`), placed **after** the two checks that
   throw (active constraints, non-idle async indexer) so a rejected switch has no side
   effect, and **under `engine_lock_`**: `GetRecoverySteps` holds that lock specifically to
   read `wal_file_`, so finalizing without it races a replica-recovery reader. Lock order
   `main_lock_ → engine_lock_` is respected, since the UNIQUE hold is `main_lock_`.
   `InitializeWalFile` returns false in analytical mode, so no WAL is created during the
   import, and the first post-switch commit opens a fresh file. Sequence numbering stays
   contiguous — only *gaps* break the main's restart recovery, not rotations.
7. `GetRecoverySteps` — override the WAL-only decision when the snapshot holds data no WAL
   does:

   ```cpp
   // A snapshot timestamp inside no WAL file's range means its data was never written to a
   // WAL (analytical-mode writes bypass it), so WAL-only recovery would silently skip it.
   if (latest_snapshot && latest_snapshot->durable_timestamp > replica_commit &&
       !SnapshotTsCoveredByAnyWal(wal_files, current_wal_from, current_wal_to,
                                  latest_snapshot->durable_timestamp)) {
     wal_chain_info.covered_by_wals = false;
   }
   ```

   The check must consider the currently-open WAL as well as the finalized ones —
   `WalFile::FromTimestamp()`/`ToTimestamp()` and `WalDurabilityInfo`'s from/to. The
   `> replica_commit` guard is load-bearing, not decoration: without it a replica already
   past the import takes the not-covered branch with `first_useful_wal` pointing at the
   post-import file, and `MG_ASSERT(wal.from_timestamp <= snap_durable_ts || wal.seq_num == 0,
   "Broken data chain.")` fires — an always-on assert, i.e. a crash.

### Docs

Operator-facing documentation must state that replica registration and unregistration
belong in transactional mode only. A wiped data directory is **not** required —
decision 9 makes a re-registered replica with stale data recover correctly.

### Tests

#### e2e — `tests/e2e/high_availability/analytical_import.py`

New file, following the conventions of the existing HA tests (`interactive_mg_runner`,
`MEMGRAPH_INSTANCES_DESCRIPTION`, `mg_sleep_and_assert`, `get_data_path`/`get_logs_path`
from `common.py`). Register it in **both** `tests/e2e/high_availability/CMakeLists.txt`
(`copy_e2e_python_files(high_availability analytical_import.py)`) and `workloads.yaml`.

Replicas serve reads, so every assertion about replica contents is a plain
`MATCH (n) RETURN count(n)` on the replica's own bolt cursor, wrapped in
`mg_sleep_and_assert` because recovery is asynchronous. Assert the count *equals main's*,
not just that it is non-zero.

**Scenario A — nothing ingested anywhere before the switch.**

1. Start a cluster with a coordinator, a main, and one replica. Write nothing.
2. `UNREGISTER INSTANCE` the replica — required by decision 1's gate.
3. On main: `STORAGE MODE IN_MEMORY_ANALYTICAL`, import N nodes,
   `STORAGE MODE IN_MEMORY_TRANSACTIONAL`.
4. `REGISTER INSTANCE` the replica back.
5. Assert the replica holds N nodes and `SHOW INSTANCES` is healthy with the original main
   still MAIN.

This is the already-correct path from finding 5 — an empty replica reports timestamp 0 and
takes the `add_snapshot()` branch. Its job is to prove the decision-1/2/3 gates work
end-to-end and, critically, that the decision-9 override does **not** regress the empty
case. It would pass without decision 9, so it must not be mistaken for that fix's guard.

**Scenario B — data ingested on main *and* replica before the switch.**

1. Start the same cluster. Write M nodes on main and wait until the replica reports M, so
   it genuinely holds data.
2. `UNREGISTER INSTANCE` the replica. It keeps its M nodes and its timestamp — see
   finding 7b, nothing wipes it.
3. **Write K more nodes on main.** See below; without this step the test does not exercise
   the bug at all.
4. main → analytical, import N nodes, → transactional.
5. Re-register the replica **without wiping its data directory** — do not restart it clean,
   and keep `keep_directories` semantics in mind when starting/stopping instances.
6. Assert the replica holds M + K + N nodes and that this equals main's count.

**Why step 3 is mandatory.** The bug needs `covered_by_wals == true`, which requires a
finalized WAL chain that both extends past the replica's timestamp and reaches back to it.
If the replica is re-registered while exactly in sync, `wal_files.back().to_timestamp ==
replica_commit`, the WAL branch is skipped, the `else if` fires, and the snapshot is shipped
anyway — the test would pass with the bug fully present. Writing K more nodes after the
unregister leaves the replica **strictly behind** the end of the chain, which is the
condition from the corrected reachability analysis in finding 6.

**Why this is a genuine guard — and an implementation-ordering warning.** Part 1 of
decision 9 (finalize the WAL when entering analytical) is what makes scenario B
*deterministic*: it puts the pre-analytical file into `wal_files` with
`to_timestamp > replica_commit`, so `covered_by_wals` becomes reliably true and part 2 is
what saves the import. Remove part 2 and scenario B fails — which is exactly what a
regression guard should do.

The corollary matters: **part 1 alone makes things worse and the two must land together.**
Today the pre-analytical WAL stays open, so it is excluded from `wal_files` and many layouts
fall into the `else if` branch and ship the snapshot correctly *by accident*. Finalizing
that file removes the accident. Do not merge part 1 without part 2.

To reproduce the loss on a **pre-fix** build (useful to confirm the test is really testing
something), force WAL rotation with a very small `--storage-wal-file-size-kib` so a
finalized file covers the replica's position without part 1 in place.

**Scenario B2 — main restart between the import and the re-registration.** Scenario B with
the main restarted after step 4 and before step 5. This pins the requirement that rejected
every in-memory-state design, and it is cheap: the assertion is identical to B.

**Scenario C — deferred registration.** `REGISTER INSTANCE` while the main is analytical:
assert it returns success, that the replica is not attached yet, and that it attaches by
itself once the main is back in transactional mode — the reconciliation-loop behaviour of
decision 6.

#### Unit

- `tests/unit/storage_v2_storage_mode.cpp` — switching to analytical with a replication
  client attached is refused.
- `tests/unit/storage_v2_replication.cpp` — registering while a database is analytical is
  refused.
- Decision 9 needs its own pair, because a false positive costs a needless full snapshot
  transfer on every lagging replica: assert the override **does** fire after an analytical
  episode, and **does not** fire for an ordinary periodic snapshot (finalized WALs ending
  before the snapshot, whose timestamp the open WAL's range contains).
- No race test for the decision-7 window: it is a few instructions wide, so a thread test
  would pass with or without the fix.

### Cleanup

`storage/v2/replication/replication_client.cpp` justifies a guardrail with the comment
"Data instances are barred from analytical at query time", which decision 1 makes false.

## Deliberately out of scope

- **Correctly stamped manual snapshots in analytical mode** (finding 3). Worth having — a
  multi-hour import wants a mid-import checkpoint — but it is a durability fix affecting
  any analytical instance, HA or not. File separately.
Note that self-correcting recovery for a stale replica is **no longer** out of scope — it
became decision 9. The hole is pre-existing and not HA-specific (nothing today stops a
non-HA main with replicas from going analytical), so the fix benefits plain replication too.

## Approaches ruled out on evidence

Recorded so they are not re-proposed.

- **Deliberate WAL sequence-number skip on switch-back.** Not merely risky — unusable. The
  main's own restart recovery throws on *any* mid-chain gap, unconditionally, regardless of
  snapshot coverage: `throw RecoveryFailure("You are missing a WAL file with the sequence
  number {}!")` when `wal_file.seq_num - previous_seq_num > 1`. A deliberate skip means the
  main will not restart (or, with `--storage-allow-recovery-failure`, the tenant goes
  defunct).
- **Deleting the pre-import WAL files instead.** Then `wal_files[0].seq_num != 0` and the
  first WAL's `from_timestamp` exceeds the snapshot's, tripping the other check:
  `RecoveryFailure("You must have at least one WAL file that contains at least one delta
  that was created before the snapshot file!")`. The retention rule that always keeps one
  pre-snapshot WAL exists to keep that check satisfiable.
- **Rotating the epoch on a storage-mode switch.** The branching-point test fires only when
  `recorded_ldt_for_epoch < replica_timestamp` — i.e. *"does the replica hold data I do
  not?"* Our replica is **behind**, not divergent, so the test is false whether the epoch is
  rotated before or after the switch-back's ldt bump. Recovery-step selection never consults
  the epoch at all; its only influence is that one boolean. Rotating would also spend entries
  in a bounded history (`kEpochHistoryRetention = 1000`, `pop_front`) and assert a lineage
  change that did not happen — and `SaveLatestHistory()` early-returns when the ldt is
  unchanged, so rotating before the bump would sometimes record nothing at all. Epochs
  express *lineage*; the problem is *WAL coverage*, and no lineage marker can encode it.
- **A boundary timestamp held in memory** (with or without a "has been analytical" flag).
  Lost on a main restart, which is precisely the case that must work.
- **Deriving the hole from WAL files without the forced rotation.** Nothing in
  `SetStorageMode` finalizes the WAL today, so the pre-import file stays open across the
  whole analytical episode and post-import commits append to it. Its `[from, to]` then spans
  the gap and hides it inside a single file, so no file-level signature exists. Whether the
  gap is even visible would otherwise depend on incidental size-based rotation.
- **Comparing the snapshot against the end of the WAL chain** (`snapshot_ldt > max WAL
  to_timestamp`). The open WAL's `ToTimestamp()` advances on *every* commit, so the first
  post-import commit silences it; excluding the open WAL instead makes it fire for ordinary
  periodic snapshots and forces needless full snapshot transfers. Containment — "is the
  snapshot's timestamp inside some WAL's range?" — is the check that survives both.

## Findings from the codebase

Each of these was verified by reading the code, and several materially shaped the
decisions above.

### 1. There are two blockers, not one

- `query/interpreter.cpp:6073-6078` throws `"Data instances cannot use analytical mode"`
  for any `coordinator_state_->IsDataInstance()`.
- `memgraph.cpp:613-615` `MG_ASSERT`s the same thing at startup, aborting the process.

### 2. Startup mode permanently determines WAL capability

`memgraph.cpp:606-608` sets `snapshot_wal_mode = DISABLED` for any non-transactional
startup mode, and `InitializeWalFile` (`storage/v2/inmemory/storage.cpp:3635`) returns
false unless the mode is `PERIODIC_SNAPSHOT_WITH_WAL`. That is creation-time config which
`SetStorageMode` never touches, so an instance *started* analytical could never emit WAL
even after switching to transactional — permanently breaking replication. This is the
reason decision 5 keeps the startup assertion.

Conversely the runtime mode is **not** persisted: `SetStorageMode` writes only the
`storage_mode_` member, never `config_.salient.storage_mode` (confirmed by the comment at
`dbms/dbms_handler.cpp:1067`). A restart mid-import therefore returns a healthy
transactional instance with WAL config intact.

### 3. The switch-back already writes a snapshot; the manual one is stale

`SetStorageMode` on analytical→transactional (`storage/v2/inmemory/storage.cpp:2895-2917`)
sets `txn->last_durable_ts_ = txn->start_timestamp` and then writes a snapshot with
trigger `"storage_mode_change"`, synchronously, before `storage_mode_` is updated.

A manual `CREATE SNAPSHOT` taken *while* analytical also **skips the digest check
entirely** — the `last_snapshot_digest_` comparison is guarded on
`transaction->storage_mode == IN_MEMORY_TRANSACTIONAL` — so it always writes, and it gets a
**stale** durable timestamp:
`storage.cpp:4263` states "In memory analytical doesn't update last_durable_ts so digest
isn't valid", so the snapshot's `durable_timestamp` is the pre-analytical ldt and does not
reflect the imported data. The later mode-change snapshot supersedes it. So step 3 of the
workload is redundant and mildly misleading.

`CREATE SNAPSHOT` itself is legal in analytical mode — only on-disk storage rejects it
(`interpreter.cpp:6173`).

### 4. The permanent-NAME_EXISTS trap (motivates decision 2)

If `RegisterReplicaOnMainRpc` arrives while a database is analytical:

- `ReplicationState::RegisterReplica` succeeds and **persists** the instance-level client, but
- `RegisterReplica_` (`replication_handler/include/replication_handler/replication_handler.hpp:281`)
  does `if (storage->storage_mode_ != IN_MEMORY_TRANSACTIONAL) return;` and creates no
  per-database `ReplicationStorageClient`.

Then `GetNumCommittedTxns` (`replication_handler/replication_handler.cpp:471`) enumerates
*per-storage* clients, so the replica never appears in `replicas_num_txns`;
`InstanceSuccessCallback` (`coordination/coordinator_instance.cpp:1188`) sees the gap and
re-sends the RPC every ping; every retry now hits `NAME_EXISTS` → `DoRegisterReplica`
returns false → `spdlog::warn` only. **The storage client is never created even after the
switch back to transactional** — the replica is permanently dead with no error surfaced.

Rejecting before mutating state makes this unreachable via this path.

### 5. Empty replicas recover correctly through the existing path

For `replica_commit = 0`, `GetWalChainInfo`
(`storage/v2/inmemory/replication/recovery.cpp:199`) returns `covered_by_wals = false`
because the oldest WAL's `from_timestamp > 1`, so `GetRecoverySteps` takes the
`add_snapshot()` branch and the replica receives the mode-change snapshot. No changes
needed. This is what makes decision 4 safe.

### 6. Non-empty replicas would silently lose the import (why decision 4 is an assumption)

`GetWalChainInfo` decides `covered_by_wals` from **seq-num contiguity** plus
`from_timestamp`. The analytical import writes no WAL at all but does not disturb seq
numbering, so it punches a *timestamp* hole into a chain that still looks seq-contiguous.
A replica whose data predates the import gets recovered from WALs alone — pre-import WALs
it already has, then post-import WALs — never the snapshot containing the import, and is
then declared in sync.

Two fixes were ruled out on evidence:

- **Timestamp-gap-aware chain check is not viable**: `CreateTransaction`
  (`storage.cpp:2835`) does `start_timestamp = timestamp_++` for *every* transaction
  including read-only ones, so timestamp gaps between consecutive WAL files are normal and
  the check would fire constantly.
- **Snapshot retention will not clean up the stale WALs**: `DeleteOldWalFiles`
  (`storage/v2/durability/snapshot.cpp:13827`) keys deletion on the *oldest retained*
  snapshot's timestamp and always keeps one pre-snapshot WAL.

Note this hole is pre-existing and not HA-specific: nothing today stops a non-HA MAIN with
registered replicas from switching to analytical.

**Corrected reachability.** An earlier draft of this finding claimed WAL-only recovery is
unconditional. It is not — it depends on the WAL file layout at re-registration, because
`GetWalFiles` **excludes the currently-open WAL** and the branch is entered only when
`wal_files.back().to_timestamp > replica_commit`. Trace with a replica at 100 and a
mode-change snapshot at 5000:

- Finalized WALs end before the replica's position → the branch is skipped, the `else if`
  sees `snapshot_durable_ts > replica_commit`, and the **snapshot is sent. Correct.** This
  is the case of an exactly-in-sync replica re-registered with no post-import writes.
- Replica at 90, last finalized WAL covers `[80,100]` → `100 > 90`, and that file's
  `from_timestamp 80 <= 91` → covered → **snapshot skipped, import lost.** No post-import
  traffic needed; a briefly-lagging or ASYNC replica suffices.
- Any WAL holding post-switch commits gets finalized → `back().to_timestamp` exceeds every
  replica's timestamp while the walk back still reaches `80 <= 101` → covered → **import
  lost even for a replica that was perfectly in sync.**

So correctness depended on incidental file layout rather than on whether the WALs hold the
data — which is why the test in decision 10 must use a replica that is *behind*, and why a
naive e2e test where nothing rotates would pass without the fix.

### 6b. The main's ldt is not advanced by the switch-back snapshot

`CreateSnapshot` writes `transaction->last_durable_ts_` into the snapshot header and never
touches the storage's ldt. The ldt advances only on the transactional commit path
(`ldt_ = durability_commit_timestamp`, with `DMG_ASSERT(durability_commit_timestamp >= prev,
"LDT not monotonically increasing")`). Analytical commits never reach it, and the mode-change
unique transaction is not committed through it either — `SetStorageMode` releases its guard
via `FreeMemory`.

So immediately after the switch back, **main's ldt is still the pre-analytical value while
the newest snapshot's header carries the post-import timestamp**. A replica that loads that
snapshot reports a timestamp *ahead of main*, and the `READY` check
`replica_ts >= main_ldt` passes. This is what the three comments in
`storage/v2/replication/replication_client.cpp` are documenting — *"ldt can be larger on
replica due to snapshots"* — i.e. the anomaly has already been observed in practice. It is
also why decision 9 keys on WAL containment rather than on any comparison against main's ldt.

### 7. Epoch rotation alone would not help

The HA branching-point path (`storage/v2/replication/replication_client.cpp:188-232`) does
force a full reset — `RecoverReplica(0, ..., reset_needed=true)` — but only when the
replica's epoch is absent from the main's history **or** the replica's timestamp for that
epoch exceeds the main's recorded value. Rotating the epoch on switch-back would leave the
history continuous, so `branching_point` stays false and the WAL-only path is taken again.
See "Approaches ruled out on evidence" for the full walkthrough of both orderings.

Note what the two roles do with a branching point, since it shaped decision 9: **non-HA**
sets `DIVERGED_FROM_MAIN` and `RegisterReplica_` then unregisters the replica — registration
fails and the operator must wipe. **HA** does not fail; it sets `RECOVERY` and calls
`RecoverReplica(0, ..., reset_needed=true)`, healing itself. So refusing a registration would
have been the only place in HA where an operator must manually wipe a data directory, which
is why the self-correcting shape of decision 9 fits the surrounding design.

### 7b. Unregistering a replica does not touch its data

`UnregisterReplicationInstance` does exactly three things: removes the instance from the Raft
cluster state, sends `UnregisterReplicaRpc` to the **main** so it drops its client, and erases
the coordinator's in-memory connector. It **sends nothing to the instance being removed** —
that process keeps running with its full data directory. On the way back in,
`DemoteMainToReplicaHandler` only calls `SetReplicationRoleReplica`; no wipe, no reset.

So "dropped the replica" means removed from the cluster, not emptied. Since decision 1's gate
is what forces the operator to unregister before entering analytical, a re-registered replica
carrying pre-import data is the natural path through the workload, not a corner case.

### 8. HA lifecycle interactions

- Replication topology is instance-wide: "NOTE Currently all databases are connected to
  each replica" (`replication_handler.hpp:280`), and registered replicas live in
  `RoleMainData::registered_replicas_`. `STORAGE MODE` is per-database. This mismatch is
  what decision 3 resolves.
- `UNREGISTER INSTANCE` refuses the current MAIN (`coordinator_instance.cpp:862`), so the
  surviving single data instance must be the MAIN.
- `REGISTER INSTANCE` returns `SUCCESS` even when `RegisterReplicaOnMainRpc` fails
  (`coordinator_instance.cpp:817-825`) — the Raft commit is the success criterion, the RPC
  is best-effort with reconciliation retry. This is why Q6 is about observability rather
  than error propagation.
- `PromoteToMainHandler` (`coordination/data_instance_management_server_handlers.cpp:315`)
  calls `DoRegisterReplica` per replica and a failure there makes
  `InstanceSuccessCallback` attempt `TryFailover()`. A healthy analytical main is not sent
  `PromoteToMainRpc` in steady state (it is not a replica, writing is enabled, uuid
  matches), so this is not triggered by the target workload — but it is the reason the
  gates must not make promotion fail spuriously.
- `StateCheckHandler` does not take `main_lock_` or `engine_lock_`, so a long import or the
  UNIQUE-access mode switch cannot stall the coordinator's health checks into a false
  failover.
