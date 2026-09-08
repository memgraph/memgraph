# Pipelined commit: base and citation notes

The implementation plan this series followed cites `U file:line` on the head of `feat/adaptive-commit-lock-scheduling` at `cd7a8f8814617eab2357da203cd81a28a090908b`.
The branch was first cut from `master` at `be32bab6bebef32c8d1eafb4420fe8f6221dd5ba` merged with that head
(merge base `cdd8b5e1285f2b4a8ee4c29710aecb7619f3e98d`, clean, 45 ahead / 22 behind). On 2026-09-08 the upstream
branch was rewritten (its old head is no longer an ancestor of it), so the series was rebased onto the current head;
`master` is not merged in, so a pull request against that branch shows only this series:

| Item | Value |
|---|---|
| Base | `d22a37bb0b9186dc31c71a093f25e75e28bdec80`, head of `feat/adaptive-commit-lock-scheduling` (memgraph#4777) on 2026-09-08 |
| `master` at that time | `9e70eab5e`; 24 commits not in the base, none needed by this series (a variant with `master` merged in builds and passes the same suites and the full storage sweep) |
| Rebase conflicts | three files, all one change: upstream renamed the serializer's lock to `CommitLock` (`std::unique_lock<std::timed_mutex>`), which `QuiesceCommits`, `CommitWithTicket` and `OrderedLegacyCommit` now take and return |
| Upstream changes absorbed | `SeedReadSnapshotWatermarkFromLocalCounter` (recovery watermark), the strictly-increasing watermark `DMG_ASSERT` in `FinalizeCommitPhase` (holds: tickets publish in mint order), `TryAccessFor` and the park stack; none touch the encode or ordered stages |
| Toolchain | v8 (clang 22.1.8); required by `master` and by the merged head alike |
| Durability format | `kVersion = 37` (`kWalHeader`); v3.12.0 writes 36. A downgrade after this binary has written is not possible without a backup. |

## Citation revalidation

Every cited site was revalidated by content on the merged tree before use. The 22 `master` commits past the merge base
did not touch the cited regions, so the line numbers the plan quotes for `src/storage/v2/**`, `src/memgraph.cpp`,
`src/flags/experimental.*`, `src/dbms/inmemory/replication_handlers.*`, `src/utils/**` and `tests/unit/**` held
exactly as quoted. In particular:

| Plan citation | Location on this branch (before this work) |
|---|---|
| `inmemory/storage.cpp:1091` `PrepareForCommitPhase` | 1091 |
| `inmemory/storage.cpp:1152-1156` mint + unique validation | 1152-1156 |
| `inmemory/storage.cpp:1186` `InitializeWalFile` | 1186 |
| `inmemory/storage.cpp:1273` `FinalizeCommitPhase` | 1273 |
| `inmemory/storage.cpp:3905, 3924` `InitializeWalFile`, `FinalizeWalFile` | 3905, 3924 |
| `inmemory/storage.cpp:4006-4019` anonymous `TxnDataCommand`/`TxnCommands` | 4006-4019 (replaced by `inmemory/txn_commands.hpp`) |
| `inmemory/storage.cpp:4231` `HandleDurabilityAndReplicate` | 4231 |
| `inmemory/storage.cpp:5028-5030` `GetCommitTimestamp`, `PrepareForNewEpoch` | 5028-5030 |
| `inmemory/storage.cpp:578` shutdown ordering | 578-585 |
| `inmemory/replication/recovery.cpp:89` recovery-step commit serializer | 89-94 |
| `replication/replication_client.cpp:245, 894, 992` quiesce sites | 245-249, 894-899, 992-996 |
| `replication/replication_transaction.cpp:274-300` constructor | 274-300 |
| `durability/wal.cpp:1346, 1358` transaction start/end encoding | 1346, 1358 |
| `durability/wal.cpp:2402, 2431-2448` commit patch, end bookkeeping | 2402, 2431-2448 |
| `durability/serialization.cpp:108` `WriteCrc` | 108 |
| `src/memgraph.cpp:293` environment append | 293-298 |
| `src/memgraph.cpp:557` TEMP forced lock-free default | 557-560 (reverted in the first commit) |
| `config.hpp:129` TEMP default | 129-130 (reverted) |
| `src/flags/experimental.hpp:32` `enum class Experiments` | 30-34 |
| `src/utils/on_scope_exit.hpp:51-52` | 51-52 |
| `src/utils/logging.hpp:68-74` `DMG_ASSERT` | 68-74 |
| `dbms/inmemory/replication_handlers.cpp:512-518, 585-596` | 512-518, 585-596 |
| `tests/unit/storage_v2_wal_file.cpp:287, 813` `DeltaGenerator` ctor, `WalFileTest` | 287, 813 |
| `tests/unit/storage_v2_replication.cpp:140-188` `MinMemgraph` | 140-188 |
| `tests/unit/storage_v2_durability_inmemory.cpp:3106` `WalTransactionOrdering` | 3106 |

## Deviations from the plan

- The build uses toolchain v8, which the plan's build recipe (written for a v7 container) did not mention; the
  container was given v8 from `deps.memgraph.io`.
- `Task 9` produces `docs/pipelined-commit-upstream-pr.md` instead of a GitHub pull request, as the handoff
  requires.
