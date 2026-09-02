# Lock-free read snapshot (`--experimental-enabled=lockfree-read-snapshot`)

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

An opt-in, startup-only flag, `--experimental-enabled=lockfree-read-snapshot`
(default off). When enabled, a new transaction no longer has to wait for an in-flight
commit's durability/replication round trip in order to start. New `BEGIN`s proceed
immediately; a reader is given a consistent view of the database **as of the last commit
that has fully completed** at the moment it starts.

Concretely, with the flag on:

- Reads and read-heavy workloads keep running at full speed while a slow write commit is
  in flight, instead of stalling for the length of that commit's replication/durability
  wait. This is the whole point of the feature and applies to transactions at **every
  isolation level**.
- A transaction that starts while a commit is mid-flight is ordered **before** that
  commit: it does not see that commit's changes, exactly as if it had started an instant
  earlier. It never sees a half-finished commit (no dirty reads of not-yet-durable data).
- Write throughput is unchanged. Commits still complete one at a time; this feature makes
  readers stop waiting on them, it does not make commits faster.

## Guarantees

- **Snapshot Isolation is preserved.** A transaction sees a single consistent snapshot and
  is protected against lost updates, exactly as without the flag. The only observable
  semantic difference is timing (see "First-updater-wins" below).
- **Off is identical to today.** With the flag off (the default), behavior is byte-for-byte
  the same as the current release on every path — reads, writes, GC, durability, and
  replication. Turning the flag on is the only thing that changes behavior.
- **Durable data does not depend on the flag.** Snapshots and WAL written with the flag on
  are identical to those written with it off, and vice versa. You can start with the flag
  on, restart with it off (or the reverse), and recover the exact same data. The flag
  affects only in-memory scheduling, never what is persisted.
- **Opt-in and immutable for the process lifetime.** The flag is a startup argument. It
  cannot be changed at runtime, so a running instance has one consistent behavior.

## Configuration

| | |
|---|---|
| Flag | `--experimental-enabled=lockfree-read-snapshot` |
| Default | off |
| Scope | per instance, set at startup, immutable while running |
| Combine with other experiments | yes — `--experimental-enabled` takes a comma-separated list |

## Applicability

- **Isolation levels.** The *unblocking* (readers no longer wait behind commits) applies to
  all isolation levels. The change to *what a transaction sees* applies only to
  **`SNAPSHOT`** isolation, because that is the only level that reads from a fixed snapshot;
  `READ COMMITTED` and `READ UNCOMMITTED` observe no behavioral change beyond no longer
  being blocked.
- **Storage mode.** In-memory transactional storage only. **On-disk** storage
  (`--storage-mode=ON_DISK_TRANSACTIONAL`) is unaffected — the flag is inert there.
  **Analytical** in-memory mode is likewise unaffected (it keeps no version history to
  snapshot).

## Costs and trade-offs

Enabling the flag is a deliberate trade, honest about the following:

- **More write aborts under write contention (first-updater-wins).** Two transactions that
  race to write the same object are more likely to result in one of them getting a
  serialization error and having to retry, rather than one silently layering on top. This
  is standard Postgres-style first-updater-wins behavior. It never produces a wrong result
  — it converts some would-be conflicts into explicit, retryable serialization errors.
- **Slightly more version history retained.** While a long-running, old reader is active,
  garbage collection is a little more conservative, so a bit more version history is kept
  for the duration of that reader. It does not leak: retention returns to normal once the
  old reader finishes.
- **Edge-heavy workloads under slow commits pay more.** Concurrently modifying the same
  vertex's edges while a commit on it is mid-flight can push those writes onto a slower
  path (more aborts, and version history retained as a group until all contributors
  finish). This lands in exactly the edge-heavy + slow-commit combination the feature is
  meant to help, so the benefit and this cost should be weighed together for such
  workloads.

## Limitations and status

- **Experimental.** The flag is off by default and intended for evaluation, not yet for
  production reliance.
- **Replicated clusters are not yet cleared for flag-on.** The single-instance and
  main-side behavior is implemented and tested; enabling the flag on a **replicated
  cluster** (particularly STRICT_SYNC 2PC) still needs end-to-end validation before it
  should be turned on there.
- **Performance benefit is not yet quantified end-to-end.** The mechanism removes the
  read-blocking; the actual throughput improvement under slow-SYNC-commit workloads still
  needs a multi-machine A/B measurement to put numbers on it.

## Out of scope

- Making commits themselves faster (this feature only stops readers from waiting on them).
- Any change to on-disk or analytical storage.
- Any new user-facing query surface — there is no new Cypher syntax; the only surface is
  the startup flag.

## Product decisions and rationale

1. **Opt-in, off by default.** The change is behavior-preserving for correctness but alters
   commit/GC scheduling; keeping it off by default means upgrading never silently changes
   how an instance behaves. Operators choose it deliberately for read-latency-sensitive,
   slow-commit workloads.
2. **Startup-only / immutable.** A running instance has one consistent regime, which keeps
   the semantics and the internal bookkeeping simple and predictable, and avoids mixed
   behavior within a single process.
3. **Snapshot-semantics change limited to `SNAPSHOT` isolation.** That is the only level
   whose reads are anchored to a snapshot; applying the change elsewhere would be a no-op,
   so it is scoped to where it is meaningful while every level still gets the unblocking
   benefit.
4. **First-updater-wins accepted.** Turning some silent write races into explicit
   serialization errors is the correct, safe behavior under a snapshot boundary; clients
   already retry serialization errors.

## References

- Implementation and its correctness argument live on the PR that introduces the flag
  (branch `experiment/commit-lock-narrowing`); a design-level walk-through of the mechanism
  (the three-phase commit, the read snapshot, and the garbage-collection horizon split) and
  the deterministic interleaving tests accompany it there.
- Motivating problem: reads collapsing under slow SYNC/STRICT_SYNC commits because every
  `BEGIN` serializes behind the commit's durability/replication wait.
