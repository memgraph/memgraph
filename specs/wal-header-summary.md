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

The two replay paths - `LoadWal` and `InMemoryReplicationHandlers::LoadWal` - keep
parsing. They read every file twice, once to scan and once to apply, and that cannot
be collapsed; see decision 9.

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

### 9. Replay cannot use the summary, so it still parses

Both replay paths were converted to read only the header and then reverted. The
attempt is recorded because the reason is not obvious and the count in the header
invites trying again.

The pre-scan does three things, not two. Beyond verifying CRC and bounding the
loop, it establishes *where the valid data ends before anything is applied*.
`LoadWal` applies each delta to the skip lists as it reads it, so replaying to the
writer's own count means a damaged trailing transaction is half-applied before the
damage is found — strictly worse than not applying it, and unrecoverable, since
there is nothing to roll back to.

`WalCorruptLastTransaction` pins this as required behaviour: it zeroes the last 100
bytes of a *finalized* WAL file and asserts recovery still comes up with everything
before the damaged transaction. The scan stops at the first transaction that fails
to parse or fails its CRC, so `num_deltas` spans only transactions that can be
replayed whole. The summary states what the writer wrote, which is a different
number once a file has rotted.

Making replay single-pass would mean buffering a transaction's deltas before
applying them, which is a much larger change than this one.

CRC is a red herring here. `LoadWal`'s apply loop verifies nothing, so the pre-scan
is the only place v36's protection happens on the disk-recovery path — but adding
verification to the apply loop, which was tried, does not help: it detects the
damage no earlier than the parse failure does, and by then part of the transaction
is applied.

### 3. Back-patch, but outside the metadata's CRC region

The constructor already back-patches the offsets, and `WriteUint` is a marker plus
8 little-endian bytes, so patching in place is exact. Readers keep a single
sequential header parse instead of a second seek to EOF.

The summary is *not* part of the metadata section, though. It sits after the
metadata's CRC trailer and carries a CRC of its own:

```
SECTION_METADATA marker, uuid, epoch_id, seq_num
metadata CRC trailer        <- written once by the constructor, never again
from_timestamp, to_timestamp, num_deltas
summary CRC trailer
offset_deltas -> first delta
```

The first version put the summary inside the metadata section and had
`WriteSummary` rewrite the whole thing to recompute the CRC. That created a window
where a crash could lose an entire WAL file. Every `GetPosition()` and
`SetPosition()` reaches `OutputFile::SetPosition`, which flushes, so the new values
reached the page cache several writes before the new CRC did; a crash between them
left the header carrying new values with a stale CRC, `ReadWalHeader` threw, and
`GetWalFiles` dropped the file - taking up to `--storage-wal-file-size-kib` of
committed transactions with it. Before this work `FinalizeWal` never touched the
header at all, so a crash during rotation cost nothing, and WAL rotation runs
routinely under write load.

Separating the regions makes it fail-safe. A torn summary write cannot invalidate
uuid/epoch/seq or their CRC, so the identity still parses; the summary's own CRC
catches the damage, it reads as absent, and the file falls back to `ReadWalInfo` -
the same path an unfinalized file takes. The worst case is losing the optimisation
for one file rather than the file.

`WriteSummary` is also now seek-once-and-write-forward: `WriteCrc()` emits the
marker and value at the current position, so nothing between the seek and the CRC
flushes and the whole region reaches the file in one write. And it no longer needs
the uuid, epoch id, metadata offset, CRC offset or prefix CRC as members - only the
summary offset.

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
- **Finalizing cannot damage a file.** The metadata section and its CRC are written
  once, by the constructor. A crash during `FinalizeWal` can only leave the summary
  torn, which reads as absent.
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

  Replay is unaffected, because it still parses - see decision 9. The difference
  reaches only `GetRecoverySteps`, and only as a liveness concern.
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

Both replay paths still read each file twice: `ReadWalInfo` to find where the valid
data ends, then again to apply. Collapsing that needs per-transaction buffering so a
damaged transaction can be discarded rather than half-applied - see decision 9. It is
the largest remaining win and the one with the most exposure.

A smaller one, deliberately skipped: the replica's `to_timestamp <= ldt` early exit
could come from the header, skipping a file the replica already has without parsing
it. The exit almost never fires, though - `GetWalChainInfo` picks `first_useful_wal`
as the file straddling `replica_commit` and `FirstWalAfterSnapshot` drops what the
snapshot already covers, so main does not normally ship a fully-redundant file - and
the one case that would benefit, the current WAL, has no summary anyway.

`InMemoryReplicationHandlers::LoadWal` still calls `ReadWalInfo` for uuid, epoch_id
and `to_timestamp`, then applies the deltas in its own loop — two passes over a file
the replica just received. Worth revisiting separately.
