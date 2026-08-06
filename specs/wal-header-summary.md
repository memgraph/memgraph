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
| `EnsureNecessaryWalFilesExist` | after every snapshot, default 300 s | uuid, seq_num, from/to |
| `GetRecoverySteps` | every replica recovery | seq_num, from/to, path |
| `LoadWal` | per file applied | offsets, num_deltas, to_timestamp |

`RecoverData` was worse still: in the snapshot-less case it parsed the whole
directory twice, once in a pre-pass that only wanted the newest file's uuid and
epoch id, and again in `GetWalFiles`.

## Solution

A WAL file records its own summary. `WalFile::FinalizeWal` back-patches
`from_timestamp`, `to_timestamp` and the count of completed transactions into the
header, so discovery reads one page per file instead of the whole file.

`ReadWalHeader` stops after the offsets and metadata sections. `GetWalFiles` uses
it and consults the summary; only a file without one falls back to `ReadWalInfo`.
`RecoverData`'s pre-pass is deleted — with no uuid known yet it reads every
header, adopts the newest file's identity, and filters the rest in memory.

`LoadWal` is untouched and still parses every delta. Its loop is bounded by
`num_deltas`, which is also how an incomplete trailing transaction gets excluded,
and per-transaction CRC verification happens only inside `ReadWalInfo`.

## Decisions

### 1. Three values, not two

Caching only the timestamps leaves a hole. `WalFile::count_` counts frames, not
completed transactions, and while the commit path finalizes right after
`AppendTransactionEnd`, the shutdown path and `PrepareForNewEpoch` don't visibly
exclude a transaction being mid-append. Such a file would advertise perfectly good
timestamps while holding nothing loadable, and a reader trusting them would try to
load it.

`num_txns`, incremented in `AppendTransactionEnd`, is what reproduces today's
include/exclude decision without a scan: `ReadWalInfo` throws for a file with no
complete transaction, which is how `GetWalFiles` used to drop it, and
`num_txns == 0` now means the same thing.

### 2. A zero transaction count marks the placeholder

The three values are written twice: as zeros by the constructor, which has nothing
to describe yet, and with real values by `FinalizeWal`. A reader must tell those
apart, since placeholders mean "scan this file" and real values mean "trust these".

`num_txns == 0` is what marks them as unwritten. It works because `FinalizeWal`
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

- **Which files are excluded.** `num_txns == 0` reproduces `ReadWalInfo` throwing.
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

`EnsureNecessaryWalFilesExist` and `GetRecoverySteps` both go through `GetWalFiles`,
so both get the header-only path for free. Neither was re-read as part of this
change; `GetRecoverySteps` in particular never touches `epoch_id`, `num_deltas` or
the offsets, and already reads the current WAL's timestamps from memory rather than
from the file, so it should now be fully header-only.

`InMemoryReplicationHandlers::LoadWal` still calls `ReadWalInfo` for uuid, epoch_id
and `to_timestamp`, then applies the deltas in its own loop — two passes over a file
the replica just received. Worth revisiting separately.
