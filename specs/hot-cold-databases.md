# Hot/Cold Databases

**Status:** Enterprise feature (on by default; no flag)
**Author:** Andreja Tonev (https://github.com/andrejtonev)
**Last updated:** 2026-06-24

> Let an idle database in a multi-tenant instance be **suspended** — its in-memory
> storage dropped to reclaim RAM, leaving a durable on-disk shell — and later
> **resumed** back to full in-memory operation, with no data lost in between.

---

## 1. Motivation

A single Memgraph instance can host many databases (tenants). Today every database
that exists is *hot*: its entire graph lives in RAM for the lifetime of the instance.
On a multi-tenant deployment, most tenants are idle at any given moment, yet each one
still holds its full in-memory footprint. This caps how many tenants a node can host
to "how many fit in RAM **simultaneously**", even when only a handful are active.

Hot/cold databases break that cap. An operator can **suspend** an idle database: its
in-memory storage is torn down and the RAM is returned to the system, while a small
durable shell (metadata + on-disk snapshot/WAL) remains. When the database is needed
again, it is **resumed** — rebuilt from disk back to a fully hot, queryable state. This
lets one instance hold far more tenants than fit in memory at once, keeping only the
working set resident.

**The feature trades latency for memory.** A suspended database is cheap to keep around
but cannot be queried until it is resumed; resuming pays a one-time rebuild cost. The
operator decides which databases are worth that trade and when.

---

## 2. Core principle

> **Cold is a memory state, not a data state. Suspending a database never loses data.**

Everything else in this spec follows from that sentence. A suspended (cold) database
holds exactly the same data as it did when hot — it simply isn't materialized in memory.
Resuming reconstructs the identical graph from durable storage. If any decision in this
document seems surprising, check it against this principle: it is almost always the
reason.

The feature is part of enterprise multi-tenancy and is on by default in enterprise
builds — there is no opt-in flag. (It originally shipped behind an
`--experimental-enabled=hot-cold-databases` flag; once it stabilized the flag was
removed, which also deleted the cross-cluster flag-consistency safety machinery that
only existed to handle a flag being toggled while durable cold state existed.)

---

## 3. Concepts and terminology

- **Database / tenant** — a named storage in a multi-tenant instance. The default
  `memgraph` database is special (see §4) and is never suspendable.
- **HOT** — fully resident: in-memory storage present, queryable normally. The default
  and only state in a build without this feature.
- **COLD (suspended)** — in-memory storage dropped; a durable shell plus metadata remain
  on disk. Not queryable until resumed. RAM footprint is effectively zero (metadata only).
- **Suspend** — the HOT → COLD transition. Reclaims the database's RAM.
- **Resume** — the COLD → HOT transition. Rebuilds the in-memory storage from disk.

---

## 4. User-facing surface

### Commands

```cypher
SUSPEND DATABASE <name>;   -- HOT -> COLD
RESUME  DATABASE <name>;   -- COLD -> HOT
```

- Both require the **`MULTI_DATABASE_EDIT`** privilege — the same privilege as
  `CREATE` / `DROP` / `RENAME DATABASE`. Suspending or resuming a database is a
  multi-database administrative action, and is gated identically.
- Both are **administrative DDL**: they run only on a MAIN instance (rejected on a
  replica) and are ordered and replicated like `CREATE`/`DROP DATABASE` (see §7).
- **`SUSPEND DATABASE memgraph` is rejected.** The default database is a *system*
  database — it backs authentication, multi-tenancy metadata, and more, not just a user
  graph — and must always be available. Attempting to suspend it returns a clear error
  and leaves it untouched.
- Suspending or resuming a non-existent database returns a clean error.

### What it costs

- **Suspend** is fast: it tears down the in-memory storage and persists a marker.
- **Resume** is proportional to the database's size — it reloads the snapshot and
  replays the WAL. For the initial release, `RESUME` runs synchronously: the issuing
  query blocks until the database is hot. A large database can therefore take a while
  to resume. (Making resume asynchronous is a planned follow-up; see §10.)

### Behavior on a cold database

A cold database is **not transparently reheated**. Accessing one is an explicit error,
not an implicit resume:

| Action on a COLD database | Behavior |
|---|---|
| `USE DATABASE <cold>` then query, or any data query | **Error**: the database is suspended; `RESUME` it first. |
| `SHOW DATABASES` | The cold database is **listed**, with its state shown as `COLD`. |
| `SHOW STORAGE INFO ON DATABASE <cold>` | Returns the **as-of-suspend snapshot** of the database's stats (vertex/edge counts, last-hot memory footprint, indexes, constraints, etc.), clearly marked as a frozen snapshot — it does **not** reheat the database. |
| `SUSPEND` an already-cold database | Error (it is already cold). |
| `RESUME` a cold database | Reheats it to HOT. |

The deliberate choice here is that **reading a cold database's data is an error the
operator must resolve by resuming it** — Memgraph will not silently spend the resume
cost on their behalf. Metadata *about* the database (its existence, state, and last-hot
stats) remains visible without reheating, so an operator can see what they have and
decide what to resume.

---

## 5. Lifecycle

```
            SUSPEND DATABASE
   HOT ─────────────────────────▶ COLD
    ▲                              │
    │        RESUME DATABASE       │
    └──────────────────────────────┘
```

- A database can be suspended only when **nothing is actively using it**. If a client
  session is connected to it, or a query/transaction is in flight, `SUSPEND` **fails
  fast** with a clear "database is in use" error rather than waiting or forcibly killing
  the in-flight work (see §6, decision **D6**). The operator retries once the database is
  idle.
- `SUSPEND` requires the database to have durable storage (periodic snapshots + WAL) and
  to be an in-memory transactional database. Analytical/on-disk databases cannot be
  suspended. The reason is the resume contract: resume reconstructs a database losslessly
  by reloading its snapshot and replaying its WAL, and that is exactly what makes dropping
  the in-memory copy safe. Analytical mode keeps no WAL, so suspending it could drop every
  write made since the last snapshot — the data loss the core principle forbids. On-disk
  databases already hold their working set on disk rather than in RAM, so there is no
  in-memory footprint for suspend to reclaim, and they sit outside the snapshot+WAL
  recovery model the resume path is built on.
- States are surfaced to operators via the `status` column of `SHOW DATABASES` (present
  only when the feature is enabled).

---

## 6. Product decisions and rationale

The decisions below are the *why* behind the feature's shape: a small, explicit mechanism
(suspend/resume) with the eviction policy left to the operator rather than baked into the
engine.

### D1 — Only explicit `SUSPEND` / `RESUME` change state. No automatic logic.

Hot/cold state changes **only** when an operator runs `SUSPEND` or `RESUME`. There is no
automatic eviction under memory pressure, no idle-database reaper, no background policy.

*Rationale.* Automatic, memory-pressure-driven eviction sounds attractive, but a watermark
scheduler picking "the coldest idle tenant" brings thrash risk, hard-to-reason-about
timing, and a large concurrency surface (the idle-session reaper's claim protocol), and it
makes behavior non-deterministic — a database could vanish from memory without anyone
asking. The product decision is that **the operator knows their workload better than a
watermark heuristic does.** Memgraph provides the mechanism (suspend/resume) and the
visibility (`SHOW DATABASES`, stats, metrics); the policy belongs to the operator or an
external control plane. Keeping every state change explicit also keeps the feature small
enough to reason about and free of an entire class of eviction races.

### D2 — Suspending fully destroys the in-memory representation.

On suspend, *all* of the database's in-memory state and background activity is torn
down: the storage is dropped and every database-owned background thread (GC, snapshot,
async index builder, TTL, etc.) is stopped and joined.

*Rationale.* The entire point is to reclaim RAM. A partial teardown that left caches,
threads, or index structures resident would defeat the purpose. "Cold" means genuinely
cold — the only things that survive in memory are a lightweight handle and the
database's metadata.

### D3 — Suspend stops managed features; resume restores them.

Features attached to a database — **streams**, **triggers**, **TTL** — are stopped when
it is suspended and restored from their durable definitions when it is resumed. A stream
that was running before suspend comes back running after resume; a trigger that was
defined comes back defined and firing; the TTL scheduler restarts.

*Rationale.* These features are part of the database's behavior, and the core principle
says suspend/resume is data-preserving. An operator should be able to suspend a database
with a live Kafka stream and a TTL policy, resume it later, and find both working exactly
as before — without re-creating them. (An alternative where a live stream simply *pinned*
a database hot and blocked suspension was rejected: stop-and-restore is strictly more
useful and avoids a latent data race in the teardown path.)

### D4 — Hot/cold state is replicated. MAIN and all replicas hold the identical set.

`SUSPEND` / `RESUME` are system-replicated operations, ordered alongside
`CREATE` / `DROP` / `RENAME DATABASE`. When a MAIN suspends a database, each connected
replica tears down its own copy to cold; resume rebuilds it everywhere. A reconnecting
or lagging replica converges to the MAIN's authoritative `{hot ∪ cold}` set.

*Rationale.* In a replicated cluster, "which databases exist and in what state" is
cluster-wide truth, exactly like the set of databases itself. If hot/cold were a
node-local decision, a replica could disagree with its MAIN about whether a database is
queryable, which is confusing and operationally fragile. Treating it as replicated DDL
makes the behavior predictable: the cluster has one hot/cold map, and it is the MAIN's.

A consequence, accepted deliberately: suspending a database on the MAIN also tears it
down on replicas, evicting any reader connected to a replica's copy. That is the price of
a single coherent cluster-wide state, and it is consistent with how other DDL behaves.

This is also why hot/cold is the MAIN's decision rather than a per-node or consensus one.
A database that is idle on the MAIN but actively read on a replica still follows the MAIN
when the MAIN suspends it — the replica's readers are evicted. A node-local or quorum
decision would let the cluster hold divergent hot/cold maps (a replica believing a
database is queryable while the MAIN considers it cold), which is exactly the operational
ambiguity this design rules out. An operator who needs a tenant resident for replica-side
reads keeps it HOT.

### D5 — Hot/cold state is durable. On restart, only HOT databases are recovered.

The hot/cold state survives a restart. A database that was cold when the instance stopped
comes back **cold** — recovered as a metadata-only shell, with no in-memory rebuild — and
a database that was hot comes back hot.

*Rationale.* If a restart reheated every database, the feature would provide no benefit
across the most common operational event. Durability of the *state* (not just the data)
is what lets an operator suspend their idle long tail and keep it suspended through
restarts, deploys, and crashes. The data is always safe on disk either way; what's
durable here is the *decision* to keep it cold.

### D6 — `SUSPEND` fails fast on an active database; it never kills queries.

If a database is in use — a connected session, an in-flight transaction — `SUSPEND`
returns a "database is in use" error immediately rather than waiting for the work to
drain or forcibly terminating it.

*Rationale.* Suspending a database out from under a running query would be a surprising
and destructive thing to do silently. The safe, predictable contract is: suspend succeeds
only when the database is genuinely idle, and otherwise tells the operator why it
couldn't. The operator (or their control plane) drains the database and retries. This also
sidesteps an entire category of mid-transaction teardown hazards.

### D7 — On failover/promotion, cold databases stay cold.

When a replica is promoted to MAIN, databases that were cold stay cold. The new MAIN
records the new replication epoch into each cold database's durable metadata so that a
later `RESUME` picks up correctly where the cluster left off.

*Rationale.* A promotion is a control-plane event, not a request to materialize every
idle tenant. Force-reheating the entire cold set on every failover would cause a
memory spike at the worst possible moment and contradict D1 (no automatic reheat). Keeping
cold databases cold — while making sure their replication lineage stays correct so resume
is safe — preserves both the memory benefit and data safety across failover.

### D8 — Resume is an attempt-and-roll-back, not a pre-check.

Resuming a large database may fail to fit in available memory. Rather than trying to
predict that up front, `RESUME` attempts the rebuild and, if it runs out of memory (or
recovery otherwise fails), **rolls the database back to cold** and returns a retriable
error. The instance does not crash and no data is lost; the operator can free memory and
retry.

*Rationale.* A footprint pre-check would be both unreliable (memory is shared and moving)
and redundant with the rollback path that has to exist anyway for genuine recovery
failures. Attempt-and-roll-back is simpler, and it degrades gracefully: a failed resume
is a clean, retriable error, never a half-built database or a downed instance. The same
philosophy extends to startup: if recovering a hot database at boot would exhaust memory,
that database is left cold (with a clear marker in `SHOW DATABASES`) and the instance
comes up degraded-but-alive rather than failing to boot. (Whether a hot database that
fails to recover at boot should instead abort startup may become a configurable policy in
a future release; today the safe default is to keep the instance alive and surface the
per-database failure.)

---

## 7. Replication and high availability

- **Replicated DDL.** `SUSPEND`/`RESUME` stream to replicas as ordered system operations
  and apply in order, exactly like `CREATE`/`DROP DATABASE`. Sync replicas are awaited;
  async replicas converge.
- **Convergence.** A replica that missed a suspend/resume (it was down, or lagging)
  converges to the MAIN's authoritative hot/cold set on reconnect, including the
  as-of-suspend stats and the correct replication epoch for each cold database.
- **Failover.** Cold stays cold across promotion (D7); the new epoch is recorded durably
  so a later resume is consistent.
- A cold database has no live replication clients, so it is intentionally absent from
  `SHOW REPLICAS`.

---

## 8. Observability

Five **global** Prometheus metrics expose hot/cold activity (also visible via
`SHOW METRICS INFO`):

| Metric | Type | Meaning |
|---|---|---|
| `memgraph_database_suspends_total` | counter | Successful suspends (operator + replica-apply). |
| `memgraph_database_resumes_total` | counter | Successful resumes. |
| `memgraph_cold_databases` | gauge | Currently-cold database count. |
| `memgraph_database_boot_recovery_failures_total` | counter | Databases left cold at boot due to a recovery failure. |
| `memgraph_database_boot_recovery_oom_failures_total` | counter | Subset of the above where the cause was out-of-memory. |

Metrics are global rather than per-database because a cold database has no live storage to
attach per-database metrics to. Per-database visibility — so an operator can see exactly
which tenant is frequently cold or repeatedly fails to resume, and decide whether to move
it elsewhere — is a recognized gap; see §10.

In addition, `SHOW DATABASES` shows each database's `HOT`/`COLD` state, and
`SHOW STORAGE INFO ON DATABASE <cold>` shows its frozen as-of-suspend stats.

---

## 9. Guarantees and non-goals

**Guarantees**

- No data is lost by suspending and resuming (§2).
- The default `memgraph` database can never be suspended (§4).
- A failed resume rolls back to cold and is retriable; it never crashes the instance or
  leaves a half-built database (D8).
- Hot/cold state is consistent cluster-wide (D4) and durable across restart (D5).

**Non-goals (explicitly out of scope for this release)**

- **Automatic eviction / idle reaping** — out of scope; policy lives with the operator (D1).
- **Transparent reheat on access** — out of scope; accessing a cold database is an error (§4).
- **Asynchronous resume** — resume blocks the issuing query for now (§4, §10).
- **Killing in-flight queries to force a suspend** — never (D6).
- **Suspending analytical or on-disk databases** — rejected; could risk data (§5).

---

## 10. Known limitations and future direction

- **Synchronous resume.** `RESUME` currently blocks the issuing query until the database
  is hot; a very large database can take noticeable time, and resume on a sync replica can
  block the issuing client. Making resume asynchronous (return a retriable "resuming"
  response while the rebuild proceeds in the background) is the most likely next step.
- **Per-database metrics.** Hot/cold metrics are global today; per-database labelling
  would let an operator see exactly which database is cold or failed to resume.
- **Per-tenant non-default storage config** is reconstructed from instance defaults on
  restart (shared with the existing hot-restart path), so non-default per-tenant storage
  settings are not preserved across a restart. This is a pre-existing multi-tenancy
  limitation, not specific to hot/cold.
- **Cross-version safety.** The on-disk and replication wire formats were extended to
  carry hot/cold state; this is not downgrade-safe, which is an accepted trade-off.
- **Best-effort durable-marker persistence.** The hot/cold *state* marker is persisted
  best-effort: a storage write failure while writing it (at suspend, resume, or promotion)
  is logged but does not roll back or crash. No data is ever lost — the snapshot is written
  before teardown — and in the common cases the tenant simply recovers to its last
  durably-recorded hot/cold state on restart (a failed suspend marker recovers HOT; a failed
  resume marker recovers COLD and is resumable again). The one case that is **not**
  self-healing: if a promotion marker write fails and the newly-promoted MAIN then crashes
  before the marker is rewritten, the cold tenant recovers its *pre-promotion* epoch on
  restart. The MAIN reconciles its own copy on the next `RESUME`, but a **non-HA replica** of
  that tenant can latch `DIVERGED_FROM_MAIN` with no automatic recovery, and an operator must
  re-register replication for it to converge. The crash window is a single durable write wide.

---

## 11. Availability

The feature is part of enterprise multi-tenancy and is compiled only into enterprise
builds. In a community build the `SUSPEND`/`RESUME DATABASE` commands are unavailable
(enterprise-only), and no database can be suspended. There is no runtime flag to enable
or disable it within an enterprise build.
