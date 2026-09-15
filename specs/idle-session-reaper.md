# Idle-session accessor reaper

Enterprise-only. Always-on (no flag). Stacked on the non-blocking `DROP DATABASE … FORCE`
deferred-drop worker (#4573).

## Problem

A Bolt session holds a *connection-scoped* database accessor (`Interpreter::current_db_.db_acc_`,
a `Gatekeeper` accessor) for the whole life of the connection — acquired at connect / `USE`, not
per query. That accessor is a refcount on the tenant. A `DROP DATABASE` can only finish teardown
once the tenant's accessor count reaches zero.

Neo4j-style drivers keep a **connection pool** of open-but-idle sessions. Each idle pooled
connection keeps its last tenant pinned, so a tenant that is pinned *only* by idle connections can
never drain — the drop is issued, the tenant seals, but the husk sits in `DROPPING` forever. This
is the exact symptom reported by customers whose pooled connections "back up" and block drops.

Historically Memgraph had a per-listener inactivity watchdog (`communication/listener.hpp`,
`session->socket().Shutdown()` on `TimedOut()`) that closed such sockets. It was removed as
user-hostile: the client sees a **visible connection drop**. This feature must not reintroduce that.

## Goals / non-goals

- **Goal:** a tenant pinned only by *idle* sessions drains when it is dropped — without the client
  seeing a dropped connection.
- **Non-goal:** a general idle-timeout that closes connections (that is the removed watchdog).
- **Non-goal:** memory eviction / hot-cold state changes. Releasing an accessor changes no hot/cold
  state and frees no tenant memory on its own; its only effect is removing the *pin* so an operator
  action (drop) can proceed.

## Design decisions

### D1 — Reap the accessor, not the connection

The reaper releases the session's `db_acc_` and leaves the connection open. On its next query the
session transparently re-acquires the database by name (`EnsureDbAccessForQuery` → `Get(name)`), or
falls back to a **db-less** session if the tenant is gone. The client never observes a disconnect.
This is the deliberate contrast with the legacy epoll watchdog (accessor-releaser, not
session-killer).

### D2 — Drop-driven, inside the deferred-drop worker (no standalone sweep)

Reaping runs **inside #4573's deferred-drop worker tick**, not a separate periodic sweep. Each tick,
for each draining husk, the worker releases the accessor of any idle session still pinned to that
husk *before* it attempts `try_delete`. There is no idle-timeout and no background reaper thread of
its own; the only trigger is an in-progress drop. Rationale: releasing an accessor accomplishes
nothing until an operator drops the tenant, so the drop is the natural (and only) driver — this
deletes an entire background thread and its whole shutdown-lifetime hazard class.

Wiring: `Handler<T>` exposes a type-erased per-tick **drain hook** (`SetDrainHook`/`ClearDrainHook`);
`Tick_` invokes it per husk (by UUID) before `try_delete`. `memgraph.cpp` supplies the closure over
`InterpreterContext` — the generic `dbms::Handler` stays free of any query-layer dependency.

### D3 — Reap-only; FORCE aborts running transactions

The pin is the *accessor*, not the transaction, so the reaper only ever releases idle sessions.
Sessions with a live transaction on the tenant are handled by `DROP … FORCE`'s existing one-shot
`TerminateTransactions`; once aborted they park idle and are reaped by the worker on a later tick.
New transactions cannot start on a sealed tenant (`EnsureDbAccessForQuery` / `USE` go db-less on a
marked-for-deletion tenant), so no new pins appear after the seal.

### D4 — Match by UUID, not name

A drop+recreate can put a live, freshly-created tenant behind the same name while the old husk still
drains. The reaper therefore matches the session's tenant by **UUID** (`db->uuid() == husk uuid`),
never by name, so it never evicts the new same-name tenant's sessions.

### D5 — Connection-scoped accessor with transparent re-acquire

`CurrentDB` caches the tenant identity (`current_db_name_`, `current_db_uuid_`) so `name()` and
re-acquire survive an accessor release. `EnsureDbAccessForQuery` (run at query entry) re-acquires
`db_acc_` by name if it was released, with three guards that fall back to a db-less session instead
of re-pinning something dying: marked-for-deletion, UUID mismatch (name recycled), and
`UnknownDatabaseException` (gone/suspended).

### D6 — Always-on, enterprise-only

No experiment gate and no timeout flag. The machinery is compiled under `#ifdef MG_ENTERPRISE` and is
always active there. Flag-off behaviour does not exist; steady-state cost is one `message_in_flight_`
store per Bolt message and one cheap re-acquire check per query (a no-op when the accessor is held).

## Mechanism

### The IDLE→REAPING handshake (the concurrency crux)

The worker reaps on a background thread; the session's Bolt thread may start a query at any instant.
`db_acc_` stability during release rests on a seq_cst Dekker handshake:

- **Bolt side** (`SetMessageInFlight`): store `message_in_flight_ = true` (seq_cst), then spin while
  `transaction_status_ == REAPING`.
- **Reaper side** (`WithReapingLock`): reapable? → check `message_in_flight_` → CAS
  `transaction_status_` IDLE→REAPING → re-check `message_in_flight_` → run the release → restore IDLE.

The seq_cst total order guarantees exactly one side yields: either the reaper sees the gate and backs
out, or the Bolt thread sees REAPING and spins until IDLE is restored.

### Correctness contract (invariants)

1. **UUID, not name** — the release predicate compares `db->uuid()` to the husk UUID (non-allocating,
   safe inside the `noexcept` handshake).
2. **Clear the hook before the InterpreterContext is destroyed** — the defer worker (owned by
   `dbms_handler`) outlives the `InterpreterContext` under reverse-order destruction, so the hook
   (which closes over the context) is cleared via an `OnScopeExit` declared *after* the context, on
   every exit path including early returns.
3. **Destroy reaped accessors outside the interpreters lock, before `try_delete`** — an `Accessor`
   dtor can block on `GKInternals::mutex_`; the hook collects released accessors and destroys them
   outside `interpreters.WithLock` (so it never stalls other lock consumers) and before the same
   tick's `try_delete` (so the gatekeeper count is current).
4. **Never release under a live storage transaction** — the release predicate refuses while
   `db_transactional_accessor_` / `execution_db_accessor_` is set (a released last accessor would let
   the deferred `~Gatekeeper` destroy the storage under a held `ResourceLockGuard`).
5. **One-directional lock order** — the hook runs in `Tick_`'s lock-free per-node section; no path
   takes `interpreters` then `pending_mutex_`/`lock_`.

## Convergence

- **Idle pooled connections** (no transaction): reaped on the first drain tick.
- **Sessions in a transaction at drop time**: aborted by FORCE's one-shot `TerminateTransactions`,
  park idle, and are reaped on a later tick — no dependence on the client issuing another query.
- The husk stays visibly `DROPPING` in `SHOW DATABASES` until it drains.

## Testing

- Unit `idle_session_reaper` — UUID-keyed release, transparent re-acquire, db-less fallbacks,
  same-name-recreate UUID guard, the IDLE→REAPING handshake under concurrency, live-transaction guard.
- Unit `dbms_handler` — the `Handler` drain-hook mechanism (hook releases the pin → husk drains;
  `ClearDrainHook` stops invocations).

## Follow-ups (not in the initial change)

- A drop-driven **stress workload** (idle pooled connections + `DROP … FORCE` convergence) to replace
  the removed idle-timeout stress workload.
