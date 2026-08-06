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

Both replay paths - `LoadWal` and `InMemoryReplicationHandlers::LoadWal` - take a
finalized file's extent from its header too, so they read it once instead of twice.
Only a file with no summary is parsed first. Replay verifies each transaction's CRC
as it applies it, and damage in a finalized file is fatal; see decision 9.

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

### 9. Replay uses the summary, and damage there is fatal

A finalized file is fsynced before it is renamed, so its transactions are durable and
were acknowledged. If replay comes up short of what its header states, the bytes
rotted - it is not an interrupted write. A file with no summary was never finalized
and its tail may legitimately be torn, so that one is still parsed by the dry run,
which stops at the last whole transaction.

That split is the whole design:

| File | Extent from | Damage means |
|---|---|---|
| finalized (has summary) | the header | media rot -> fail |
| unfinalized, or pre-v37 | `ReadWalInfo` dry run | interrupted write -> truncate |

Replay therefore verifies each transaction's CRC as it applies it, and lets a parse
failure or a CRC mismatch propagate.

**This also fixes a pre-existing bug.** Recovering a prefix is only sound for the
*last* file in the chain. The gap check catches a missing file, never a truncated
one, so a truncated file N left `last_loaded_timestamp` at the last transaction it
applied, and file N+1 then applied in full on top - a hole. If N+1 touched anything
from N's lost tail the apply threw, but if its transactions were independent
recovery succeeded silently with an incomplete dataset. On a replica it was worse:
the replica reported the later ldt, so main considered it caught up and the
divergence never healed. Failing on damage in a finalized file removes the case
entirely, and a mid-chain file cannot be unfinalized in a way that matters, because
whichever session recovered its prefix already continued from there.

**An earlier revision got this wrong** by using the summary for replay while keeping
graceful truncation, which half-applied a damaged transaction before noticing. That
is only a problem if recovery then continues: with a hard failure the half-applied
state is discarded along with everything else, and on a replica the accessor is
destroyed while unwinding, which aborts the transaction, so nothing partial commits.

### 10. The replica stops the chain rather than failing the process

Damage cannot be fatal on the replica the way it is on restart - there is no process
to abort and no operator watching. So `InMemoryReplicationHandlers::LoadWal` catches
it, reports failure, and the caller stops before the remaining files. Those files
build on the transactions that just went missing, so applying them is exactly the
hole described above.

The response carries no commit timestamp, so main does not advance its view of the
replica, retries later, and the replica skips whatever it already has via
`to_timestamp <= ldt`. Progress already committed is kept; nothing partial is.

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

  Replay now treats that as fatal for a finalized file rather than truncating - see
  decision 9, which explains why silently dropping acknowledged data is the worse
  option. `WalCorruptLastTransaction` was renamed to
  `WalFinalizedFileCorruptLastTransactionCrashes` and its expectation inverted.
  Operationally this means bit rot in a WAL file after the newest snapshot stops the
  tenant instead of bringing it up stale, so recovery is via
  `--storage-allow-recovery-failure` plus `RECOVER SNAPSHOT`, or a backup. Files the
  newest snapshot already covers are skipped without being read, so rot in those is
  still harmless.
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

A deliberately skipped one: the replica's `to_timestamp <= ldt` early exit
could come from the header, skipping a file the replica already has without parsing
it. The exit almost never fires, though - `GetWalChainInfo` picks `first_useful_wal`
as the file straddling `replica_commit` and `FirstWalAfterSnapshot` drops what the
snapshot already covers, so main does not normally ship a fully-redundant file - and
the one case that would benefit, the current WAL, has no summary anyway.

`InMemoryReplicationHandlers::LoadWal` still calls `ReadWalInfo` for uuid, epoch_id
and `to_timestamp`, then applies the deltas in its own loop — two passes over a file
the replica just received. Worth revisiting separately.
