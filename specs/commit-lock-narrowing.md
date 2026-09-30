# Commit lock narrowing (`--experimental-enabled=commit-lock-narrowing`)

**Status:** Experimental (opt-in, off by default)
**Area:** storage/v2 (MVCC, durability, garbage collection), replication

One-line summary: an experimental storage mode that stops new transactions from
stalling behind a slow commit's durability/replication wait, so read throughput no
longer collapses when writes are slow — without changing what any transaction sees.

## Problem Statement

Under normal load Memgraph serves reads quickly. But when commits become slow — most
commonly with **SYNC or STRICT_SYNC replication** (a commit waits for a replica round
trip) or slow disk durability — read throughput can fall off a cliff even though the
reads themselves are cheap.

The reason is observable only as a symptom: while a write commit is waiting for its
durability/replication to finish, **every new transaction that tries to start is blocked**,
regardless of whether it is a read or a write. A single in-flight slow commit serializes
all incoming `BEGIN`s behind it. So a workload that mixes fast reads with occasional slow
commits sees its reads periodically freeze for the duration of each commit's wait, and
aggregate read throughput drops far below what the hardware can do.

## Solution

An opt-in, startup-only flag, `--experimental-enabled=commit-lock-narrowing`
(default off). When enabled, a new transaction no longer has to wait for an in-flight
commit's durability/replication round trip in order to start. New `BEGIN`s no longer wait
for a commit's WAL or replication round trip; a reader is given a consistent view of the database **as of the last commit
that has fully completed** at the moment it starts.

Concretely, with the flag on:

- Reads and read-heavy workloads keep running at full speed while a slow write commit is
  in flight, instead of stalling for the length of that commit's replication/durability
  wait. This is the whole point of the feature and applies to transactions at **every
  isolation level**.
- A `SNAPSHOT` transaction that starts while a commit is mid-flight is ordered **before**
  that commit: it does not see that commit's changes, exactly as if it had started an instant
  earlier. It never sees a half-finished commit (no dirty reads of not-yet-durable data).
- Commits still complete one at a time; this feature stops new transactions from waiting on
  them, it does not make commits faster.

## Guarantees

- **Snapshot Isolation is preserved.** A transaction sees a single consistent snapshot and
  is protected against lost updates, exactly as without the flag. The only observable
  semantic difference is timing (see "First-updater-wins" below).
- **Off behaves as today.** With the flag off (the default), behavior is the same as the
  current release on every path — reads, writes, GC, durability, and
  replication. Turning the flag on is the only thing that changes behavior.
- **Durable data does not depend on the flag.** Snapshot and WAL formats are unchanged, and
  files written with the flag on or off are interchangeable. You can start with the flag
  on, restart with it off (or the reverse), and recover the exact same data. The flag
  affects only in-memory scheduling, never what is persisted.
- **Opt-in and immutable for the process lifetime.** The flag is a startup argument. It
  cannot be changed at runtime, so a running instance has one consistent behavior.

## Timestamps

A reader's snapshot is taken from a new in-memory watermark: the last fully published
**local** commit timestamp, on the instance's own logical clock. It is separate from the
last durable timestamp (`ldt`), which on a replica is MAIN's timestamp and is used to keep
the replica in sync with MAIN; a replica stamps its own versions with local timestamps, so
the two differ there. The watermark is never persisted.

## Configuration

| | |
|---|---|
| Flag | `--experimental-enabled=commit-lock-narrowing` |
| Default | off |
| Scope | per instance, set at startup, immutable while running |
| Combine with other experiments | yes — `--experimental-enabled` takes a comma-separated list |

## Applicability

- **Isolation levels.** The *unblocking* (new transactions no longer wait behind commits)
  applies to all isolation levels. The change to *what a transaction sees* applies only to
  **`SNAPSHOT`** isolation, the only level that reads from a fixed snapshot: a `SNAPSHOT`
  transaction that starts while a commit is in flight is ordered before that commit.
  `READ COMMITTED` and `READ UNCOMMITTED` still see a commit as soon as it completes.
- **Storage mode.** In-memory transactional storage only. **On-disk** storage
  (`--storage-mode=ON_DISK_TRANSACTIONAL`) is unaffected — the flag is inert there.
  **Analytical** in-memory mode is likewise unaffected (it keeps no version history to
  snapshot).

## Costs and trade-offs

Each cost below comes from the same source: a transaction can now start while one commit
is still in flight, and it is ordered before that commit. At most one commit is ever in
flight, because commits still complete one at a time.

- **More retryable write conflicts.** A transaction that starts during a commit's
  durability/replication wait and then writes an object that commit touched fails with a
  serialization error (first-updater-wins); with the flag off it would have waited at
  `BEGIN` and then succeeded. This applies at every isolation level. It never produces a
  wrong result, and clients already retry serialization errors.
- **Concurrent edge creation takes the slower path.** Creating an edge on a vertex that the
  in-flight commit also added edges to goes through the non-sequential write path, and
  that version history is retained as a group until every contributing transaction
  finishes.
- **Slightly more version history retained.** While a transaction that started during a
  commit's durability/replication wait is the oldest one running, garbage collection keeps
  that one commit's previous versions (the transaction must not see it). This is at most one
  commit's worth of history, released as soon as those transactions finish.

## Limitations and status

- **Experimental.** The flag is off by default and intended for evaluation, not yet for
  production reliance.
- **Replication.** SYNC and ASYNC replicas behave the same with the flag on or off. For
  STRICT_SYNC (2PC), a commit becomes visible on MAIN only after replicas have finalized
  it, the same as with the flag off, so no reader on MAIN sees a commit that a failover
  could lose.
- **Performance.** On the HA benchmark (1 main, 2 SYNC replicas, ~1 ms range reads mixed
  with 5000-node writes), reads rise from 32% to 66% of the read-only ceiling when all
  traffic goes to MAIN, and from 66% to 88% with routing; writes rise ~22-25%. Read-only
  workloads are unchanged within noise. The remaining gap is Bolt workers held by writers
  during the replication wait, which this feature does not address.

## Out of scope

- Making commits themselves faster (this feature only stops readers from waiting on them).
- Any change to on-disk or analytical storage.
- Any new user-facing query surface — there is no new Cypher syntax; the only surface is
  the startup flag.

## References

- Implementation and its correctness argument live on the PR that introduces the flag
  (branch `experiment/commit-lock-narrowing`); a design-level walk-through of the mechanism
  (the three-phase commit, the read snapshot, and the garbage-collection horizon split) and
  the deterministic interleaving tests accompany it there.
- Motivating problem: reads collapsing under slow SYNC/STRICT_SYNC commits because every
  `BEGIN` serializes behind the commit's durability/replication wait.
