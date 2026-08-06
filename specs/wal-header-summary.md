# WAL header transaction summary

**Status:** ready-for-agent
**Area:** storage/v2 (durability)
**User-facing change:** none — internal I/O optimisation, no observable behaviour change.

## Problem Statement

`ReadWalInfo` parses a WAL file's header *and* then walks every delta in it,
skipping each frame and accumulating CRC over every byte. With the default
`--storage-wal-file-size-kib` of 20 MiB, one call is a full-file parse. It is the
only way to learn a file's `from_timestamp`, `to_timestamp` and whether it holds
anything recoverable, because none of that is stored in the file.

That makes WAL *discovery* cost the same as WAL *reading*, on paths that never
read a delta:

| Call site | Runs | Needs |
|---|---|---|
| `GetWalFiles` | every recovery | seq_num, uuid, epoch_id, from/to |
| `EnsureNecessaryWalFilesExist` | after every snapshot, default 300 s | uuid, seq_num, from_timestamp |
| `GetRecoverySteps` | every replica recovery | seq_num, from/to, path |
| `LoadWal` | per file applied | offsets, num_deltas, to_timestamp |

Only the last genuinely needs the deltas parsed. `GetRecoverySteps` reaches them
through `GetWalFiles`; `EnsureNecessaryWalFilesExist` has its own directory loop.

`RecoverData` was worse still: in the snapshot-less case it parsed the whole
directory twice, once in a pre-pass that only wanted the newest file's uuid and
epoch id, and again in `GetWalFiles`.

## Solution

A WAL file records its own summary. `WalFile::FinalizeWal` back-patches
`from_timestamp`, `to_timestamp` and the delta count into the header, so a reader
gets them for one page instead of the whole file.

`ReadWalHeader` stops after the offsets and metadata sections. `GetWalFiles` and
`EnsureNecessaryWalFilesExist` both use it and consult the summary; only a file
without one falls back to `ReadWalInfo`. `RecoverData`'s pre-pass is deleted — with
no uuid known yet it reads every header, adopts the newest file's identity, and
filters the rest in memory.

`LoadWal` and `InMemoryReplicationHandlers::LoadWal` read every file twice today,
once to scan and once to apply. With `num_deltas` in the header they read it once.
That required moving CRC verification into `LoadWal`'s apply loop - see decision 9.

## Decisions

### 1. Three values, not two

Caching only the timestamps leaves a hole. `WalFile::count_` counts frames, not
frames belonging to *completed* transactions, and while the commit path finalizes
right after `AppendTransactionEnd`, the shutdown path and `PrepareForNewEpoch` do
not visibly exclude a transaction being mid-append. Such a file would advertise
perfectly good timestamps while holding a partial transaction, and a reader
trusting them would replay it.

`num_deltas` - `count_` snapshotted at each `AppendTransactionEnd` - is exactly
what `ReadWalInfo` derives by parsing. It does three jobs: it marks the unwritten
placeholder, it lets discovery drop a file with nothing recoverable, and it bounds
the replay so a torn trailing transaction is never applied.

### 2. A zero delta count marks the placeholder

The three values are written twice: as zeros by the constructor, which has nothing
to describe yet, and with real values by `FinalizeWal`. A reader must tell those
apart, since placeholders mean "scan this file" and real values mean "trust these".

`num_deltas == 0` is what marks them as unwritten. It works because `FinalizeWal`
only ever writes a positive count, so no file whose summary is worth trusting
carries a zero. It also reads honestly: a file with no complete transaction has
nothing to summarize, so having no summary is the correct encoding rather than a
special case.

`from_timestamp == 0` would *not* work as the marker: `kTimestampInitialId` is 0,
so zero is a legitimate commit timestamp rather than a reserved value.

The two states that collapse into "no summary" — never finalized, and finalized
mid-transaction with nothing committed — both fall back to `ReadWalInfo`, and both
come out right: the first gets its timestamps derived, the second throws and is
dropped, exactly as before this change. The only cost is that the second case is
scanned rather than rejected outright, and it is a crash artifact affecting at most
one file.

An explicit boolean flag was considered and dropped. It would have let the second
case be rejected without a scan, but that is not worth a byte in the format and an
extra field to keep consistent.

### 9. CRC verification moves into `LoadWal`'s apply loop

`num_deltas` is not what stopped `LoadWal` from using the header — CRC was. Its
apply loop verified nothing; the pre-scan was the only place v36's protection
happened on the disk-recovery path, which is what the old comment above `LoadWal`
meant. Caching the count and deleting the scan without more would have silently
dropped that protection.

So `LoadWal` now verifies each transaction's CRC as it replays it, resetting the
accumulator at every transaction end exactly as the write side and `ReadWalInfo`
do. `InMemoryReplicationHandlers` already worked this way, so this aligns the two
rather than inventing anything. Verification is retained and the second pass is
gone.

### 3. Back-patch the header rather than append a trailer

The constructor already back-patches the offsets, and `WriteUint` is a marker plus
8 little-endian bytes, so patching in place is exact. Readers keep a single
sequential header parse instead of a second seek to EOF.

`WriteSummary` rewrites the whole metadata section rather than patching the three
values, so its CRC comes from the same code path as the constructor's instead of
from byte-level CRC arithmetic. The constructor stores where the section begins,
where its CRC trailer sits, and the CRC of everything preceding it. An assertion
checks the rewrite lands exactly on the reserved trailer.

### 4. No new durability version

v37 (`k37`) landed after `v3.12.0` and is unreleased, so no WAL in
the wild claims v37 without a summary and the change can ride on it. That avoids an
ISSU-visible bump — an older replica would not be able to read a newer WAL streamed
from a newer main. Sharing one version between two features already happens in this
file (`kNumCommittedTxns`/`kTtlSupport` both 30, `kCompositeIndicesForLabelProperties`/
`kEdgePropIndex` both 24).

A version guard is still required: pre-v37 files genuinely have no bytes there, and
without it the reader would consume the CRC trailer as the summary.

### 5. Fall back to a scan, don't require the summary

A file has no summary when its writer never finalized it — the `_current` file a
crash leaves behind — or when it predates v37. Those get `ReadWalInfo`, which is
exactly today's behaviour for them. There is normally at most one such file.

This is what keeps the change semantics-preserving. An earlier attempt made
discovery header-only *without* a summary, which could not detect a file holding no
complete transaction; that needed a dedicated exception and a skip in the load loop,
and still left the chain-start check able to throw where it previously could not.
Recording the count removes the reason that machinery existed.

### 6. Pick the identity by path, not by `back()`

The deleted pre-pass sorted by **path**; `GetWalFiles` returns a vector sorted by
**seq_num**, so `back()` would be a silent behaviour change. They disagree when one
directory holds more than one uuid — a force-reset replica whose fresh WAL restarts
at seq_num 0 while stale files carry high seq_nums. Newest-by-path picks the fresh
file; newest-by-seq_num picks a stale one, and that uuid then filters out every real
WAL. WAL filenames are prefixed with a zero-padded microsecond timestamp, so
lexicographic path order is chronological and survives seq_num resets.

### 7. `epoch_id` must be copied, not moved

The old `SetEpoch(std::move(...))` moved out of a local vector that was discarded
immediately. That element now belongs to the vector that is retained and handed to
the load loop, which reads `wal_file.epoch_id` from it to build epoch history.
Moving would leave an empty string there and silently corrupt the epoch chain.

### 8. The dead reopen constructor is removed

`WalFile`'s second constructor, which reopened an existing `_current` file, had no
callers anywhere in `src/` or `tests/`. It never learned the offsets `WriteSummary`
needs, so a file finalized through it would have had its header written at offset 0.
Deleting it removes the hazard rather than guarding against a caller that doesn't
exist.

## Invariants preserved

- **Which files are excluded.** `num_deltas == 0` reproduces `ReadWalInfo` throwing.
  A corrupt header still throws from `ReadWalHeader`.
- **CRC verification.** Untouched — it happens in `ReadWalInfo`, which `LoadWal`
  still calls on every file it applies.
- **Ordering.** `GetWalFiles` still sorts by seq_num, and `erase_if` is stable, so
  the ordering the gap check and the load loop depend on survives filtering.
- **`wal_files[0]` is the oldest loadable file**, so the chain-start check still
  reads a `from_timestamp` that is already in hand and cannot throw.
- **No new early return.** `erase_if` cannot empty a non-empty list: the
  newest-by-path element is by construction the one whose uuid was adopted.

## Deliberate behaviour deltas

- The corrupt-file report in the snapshot-less branch moves from `spdlog::error` to
  `spdlog::warn`, but gains the filename — the old message formatted `e.what()` into
  a slot that read as if it were the path, so the file was never named.
- Unreadable files are skipped by `ValidateDurabilityFile` with a clear message
  rather than attempted and thrown, because `GetWalFiles` checks read access where
  the old pre-pass only checked `is_regular_file`.
- Corruption in the delta region of a *finalized* file — bit rot, not a crash — is
  no longer noticed during discovery, since discovery no longer reads deltas. Such a
  file used to be reported with a `to_timestamp` truncated at the last valid
  transaction; it now reports what the writer recorded. The summary itself is covered
  by the header CRC, so a corrupted summary is still caught.

  Replay behaviour changes with it: a finalized file corrupted mid-way used to have
  its `num_deltas` truncated at the corruption by the pre-scan, so `LoadWal` applied
  the valid prefix and recovery *succeeded with partial data*. The header states the
  full count, so replay now reaches the damage, the apply loop's CRC check fires, and
  recovery fails loudly (or yields a broken tenant under
  `--storage-allow-recovery-failure`). That is the better outcome — silently
  truncating recovery at a corruption point drops committed data without telling
  anyone, and if the damaged file is mid-chain the old behaviour replayed later
  transactions on top of a hole.

  For discovery the difference reaches only `GetRecoverySteps`, and only as a
  liveness concern.
  `RecoverData` never reads `to_timestamp`, and `LoadWal` re-scans and applies just
  the valid transactions, so recovered data is identical. On the replication path the
  replica calls `ReadWalInfo` on every file it receives before applying anything, so
  an overstated summary cannot make it apply bad data — and such a file was shipped
  to the replica before this change too, because `ReadWalInfo` does not throw for a
  file holding at least one valid transaction. The outcome either way is a replica
  that cannot get past the corruption and keeps retrying.

## Verification

`FinalizeWal` is never called anywhere in `storage_v2_wal_file.cpp`, so every
existing test there leaves a `_current` file — they now all exercise the fallback
path, unchanged. Two tests added there:

- `FinalizedHeaderSummaryMatchesScan` — the back-patched summary must say exactly
  what parsing every delta concludes. This is the invariant the whole change rests
  on.
- `UnfinalizedFileHasNoSummary` — a file the writer never finished carries no
  summary and is still readable by scanning.

In `storage_v2_durability_inmemory.cpp`, `WalMixedUUID` covers decision 6, which
nothing currently guards: `SnapshotAndWalMixedUUID` always has snapshots present, so
it takes the other branch. Session 1 at a 20-minute snapshot interval yields WALs
under uuid A and zero snapshots; session 2 without `recover_on_startup` moves those
to `.backup` and writes its own under uuid B; `RestoreBackups()` merges both;
session 3 must adopt B and ignore A.

Existing coverage of the snapshot-less branch stays green: `WalBasic` explicitly
asserts zero snapshot files, and `WalAppendToExisting`, `WalMissingSecond`,
`WalCorruptSecond`, `WalCorruptLastTransaction`, `WalDeathResilience` and
`ParallelWalRecovery` all run through it.

Build `memgraph__unit`, then
`ctest --test-dir build -R "storage_v2_durability_inmemory|storage_v2_wal_file"`.

## Deferred work

`InMemoryReplicationHandlers::LoadWal` is the one remaining `ReadWalInfo` caller
that could in principle use the header, for its `to_timestamp <= ldt` early exit —
skipping a file the replica already has, without parsing it. It was left alone
deliberately, because the exit almost never fires: `GetWalChainInfo` picks
`first_useful_wal` as the file straddling `replica_commit`, and
`FirstWalAfterSnapshot` drops anything the snapshot already covers, so main does not
normally ship a fully-redundant file. The one case that would benefit — the current
WAL — has no summary anyway. Low value against a change in a sensitive path.

Its real cost is elsewhere: it scans the file with `ReadWalInfo`, then reads it again
in its own apply loop. Collapsing those is the same problem as `LoadWal` and is out
of scope for the same reason.

`InMemoryReplicationHandlers::LoadWal` still calls `ReadWalInfo` for uuid, epoch_id
and `to_timestamp`, then applies the deltas in its own loop — two passes over a file
the replica just received. Worth revisiting separately.
