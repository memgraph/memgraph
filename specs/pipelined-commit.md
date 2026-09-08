# Pipelined commit (`--experimental-enabled=pipelined-commit`)

**Status:** Experimental (opt-in, off by default; requires `lockfree-read-snapshot`)
**Area:** storage/v2 (commit path, durability), replication

One-line summary: an experimental commit mode that lets concurrent writers encode their
write-ahead-log payload in parallel, so the serialized part of a commit no longer contains
the encoding of every delta, and two to eight large-batch writers gain throughput instead
of queueing behind each other.

## Problem Statement

With `lockfree-read-snapshot` a main-side commit runs as: mint a commit timestamp (under
the commit serializer and, briefly, the engine lock), then write the transaction to the WAL
and ship it to replicas while still holding the commit serializer, then publish. The
serializer is held for the whole of durability and replication because unique-constraint
validation and the read watermark both need commits to become visible in mint order.

For a batch of a thousand rows the serial section is dominated by one piece of work that
does not need to be serial at all: encoding every delta into WAL bytes. On the reference
workload roughly 26 of the 41 milliseconds a commit spends are serialized, and the largest
single piece is the encoding of about 8,000 deltas. Adding writers therefore adds queueing,
not throughput: the commit lane has a ceiling that a single core's encoding speed sets.

## Solution

An opt-in, startup-only flag, `--experimental-enabled=pipelined-commit` (default off),
valid only together with `lockfree-read-snapshot`. With it on, an eligible main-side commit
runs in three stages:

1. **Mint.** Under the commit serializer and briefly the engine lock: mint the commit
   timestamp, register a ticket with the commit-order gate, release both locks.
2. **Encode.** With no serializer held: materialize the transaction's commands and encode
   them into a private, CRC-complete WAL buffer. Every byte this stage retains is charged to
   a per-database budget (`--storage-pipelined-commit-max-bytes`, default 256 MiB) before it
   is allocated; a refused charge never blocks, it converts the commit into the ordered
   legacy path.
3. **Ordered.** Enter the gate in commit-timestamp order: validate unique constraints,
   append the buffer verbatim to the WAL, replicate exactly as before, publish, retire the
   ticket.

Only the WAL encoding leaves the serial section. Replication, publication and two-phase
commit are unchanged code executed in ticket order. Commits that are not eligible (metadata
transactions, transactions on a storage without a WAL, transactions that turn out to need
two-phase commit, over-budget transactions) take the same ticket and run today's durability
code after entering the gate, so every main-side commit belongs to one ordering domain.

## Guarantees

- **WAL order and replica order are unchanged.** Every WAL file holds transactions in
  strictly increasing commit-timestamp order and replicas receive them in that order.
- **Publication order is unchanged.** Commit timestamps become visible in mint order, so the
  read watermark stays contiguous.
- **Unique constraints are validated against a settled predecessor set.** A commit
  validates only after every earlier-minted commit has published or fully aborted.
- **Off is identical to today.** With the flag off the commit path is the same code, and a
  deterministic harness compares its WAL output byte for byte against the base.
- **Durable data does not depend on the flag.** A transaction encoded to a private buffer
  and appended verbatim produces exactly the bytes and CRC the inline path produces.
- **Failure semantics unchanged.** Partial WAL writes and fsync failures stay fatal; a
  synchronous replica failure still reports the transaction as committed; aborts never
  advance the read watermark.

## Configuration

| | |
|---|---|
| Flag | `--experimental-enabled=lockfree-read-snapshot,pipelined-commit` |
| Default | off |
| Scope | per instance, set at startup, immutable while running |
| Budget | `--storage-pipelined-commit-max-bytes` (default 256 MiB) |

Enabling `pipelined-commit` without `lockfree-read-snapshot` is rejected at startup.

## Applicability

- In-memory transactional storage with `PERIODIC_SNAPSHOT_WITH_WAL` durability only. Without
  a WAL there is nothing to encode ahead of time, and analytical mode has no commit ordering
  to pipeline.
- Data transactions. A transaction carrying metadata deltas (index and constraint changes)
  takes the ordered legacy path.
- Main-side commits. Replica-side writes are applied by the replication server on one thread
  in main's order and are outside the ordering domain.

## Costs and trade-offs

- **A blocked worker at the gate.** A committer whose predecessor is still encoding,
  validating, replicating or publishing waits on its own worker thread. There is no latency
  bound on that wait and the interpreter's commit-lock parking does not cover it.
- **More unpublished commits in flight.** Several minted-but-unpublished transactions can
  coexist. Their deltas stay protected until they publish, so readers see nothing new, but
  memory for their materialized commands and WAL buffers is held for the duration.
- **Budget fallback.** When the retained bytes would exceed the budget, the commit is not
  refused and does not wait; it takes the ordered legacy path, which holds no more memory
  than today but re-serializes the encoding for that transaction.

## Deferred

- **Gate parking.** Parking a committer that waits at the gate instead of blocking its
  worker needs an owned commit continuation the interpreter's commit guards cannot provide.
- **A private replication transport.** Replica payloads are still encoded by the existing
  per-replica tasks in the ordered stage.

## Out of scope

- Making a single commit faster; the flag only stops concurrent commits from serializing
  their encoding.
- Any change to on-disk or analytical storage.
- Any new user-facing query surface beyond the startup flag and the new counters in
  `SHOW STORAGE INFO`.
