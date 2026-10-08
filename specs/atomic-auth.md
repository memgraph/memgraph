# Atomic auth transactions

**Status:** Implemented (PR #4524), preview
**Author:** Colin Barry
**Last updated:** 2026-10-08

> Auth statements run inside `BEGIN` .. `COMMIT`. Everything the transaction
> changes becomes visible on this instance at once, or not at all. Each replica
> then applies it at once too, or is marked behind and catches up by snapshot.

---

## 1. Motivation

Each auth statement used to stand alone. `CREATE USER` took the auth lock,
wrote through to disk, replicated, and released; the next statement started
again. Setting a user up meant several of those in a row, and anything that
went wrong half way left the earlier statements applied.

That is a problem for the common case, which is not one statement but a
sequence:

```cypher
CREATE USER alice IDENTIFIED BY 'secret';
CREATE ROLE analyst;
GRANT MATCH TO analyst;
GRANT READ ON NODES CONTAINING LABELS :Person TO analyst;
SET ROLE FOR alice TO analyst;
```

Run as five statements, every pause between them is a state another session can
see and act on: alice exists with no role, the role exists with no privileges.
A failure at the fourth leaves the first three in place, and a replica can be
holding any prefix of the sequence.

The gaps are not only an inconvenience. Setting a user up often means granting
broadly and then narrowing, and the pause in between is a window where the user
holds more access than anyone intended:

```cypher
CREATE USER alice IDENTIFIED BY 'secret';
GRANT READ {*} ON NODES CONTAINING LABELS :Employee TO alice;
/* alice can read salary here */
DENY READ {salary} ON NODES CONTAINING LABELS :Employee TO alice;
```

A session that authenticates inside that window reads everything. Reordering
is not a general answer: a later `GRANT` clears an earlier `DENY` of the same
permission, so whether denying first helps depends on which permissions are
involved. It does nothing at all for a sequence whose statements are all
grants, where the user is simply unusable, and visible, until every one of them
has landed.

A transaction removes the question: no ordering within it is observable, so
there is no window to get wrong.

Wrapping them in a transaction makes the whole sequence one change.

## 2. Core principle

> An auth transaction is isolated until it commits, atomic when it does, and
> applied whole, or not at all, on every replica.

Nothing it writes is visible to another session, or to a replica, until
`COMMIT`. `ROLLBACK` discards it. Its reads are not isolated in the same way:
a record read once reads the same for the rest of the transaction, but a scan,
such as `SHOW USERS`, sees what other sessions have committed since. A
concurrent change by another transaction to anything the transaction read or
wrote fails the commit rather than overwriting it, and that check at `COMMIT`
is what makes the outcome serializable.

---

## 3. Using it

Open a transaction, run auth statements, commit:

```cypher
BEGIN;
CREATE USER alice IDENTIFIED BY 'secret';
CREATE ROLE analyst;
GRANT MATCH TO analyst;
SET ROLE FOR alice TO analyst;
COMMIT;
```

Until `COMMIT`, no other session sees alice or the role. After it, every other
session and every replica sees all of it.

`ROLLBACK` instead of `COMMIT` discards the whole transaction:

```cypher
BEGIN;
CREATE USER bob;
CREATE ROLE ops;
ROLLBACK;        /* neither bob nor ops exists */
```

A transaction reads its own writes, so later statements see earlier ones:

```cypher
BEGIN;
GRANT DATABASE sales TO alice;
SET MAIN DATABASE sales FOR alice;   /* sees the grant above */
COMMIT;
```

### 3.1 A transaction is either auth or data, not both

The first statement decides. An auth statement makes it an auth transaction and
every later data query is refused; a data query makes it a data transaction and
every later auth statement is refused. Either way the error is:

```
An explicit transaction cannot mix auth queries with data queries. Run them in
separate transactions.
```

The mixed statement fails. The server does not end the transaction itself: a
Bolt client resets the session after a failure, which ends the transaction and
discards everything in it, so a later `COMMIT` reports that there is no
transaction to commit.

### 3.2 What runs inside an auth transaction

Every auth statement: users, roles, privileges, fine-grained label, edge and
property permissions, database grants, impersonation, and the `SHOW` family
that reads them. A transaction adds no restriction of its own, so a statement
that needs an enterprise licence, or that is not permitted on a replica or a
coordinator, is refused inside a transaction exactly as it is outside one.
`BEGIN` itself still needs a current database, as it does for a data
transaction, so a session without one, such as on a coordinator, cannot open
an auth transaction.

Statements that are not auth statements are refused, including profile queries
and the rest of Cypher. A profile query is a data query, so inside an auth
transaction it is refused by the rule in 3.1 above.

Profiles are **not** transactional. A profile write, user or tenant, is refused
inside any transaction: both are durable the moment the statement runs, so a
transaction could neither isolate them nor roll them back.

---

## 4. Conflicts

Two auth transactions that touch unrelated records both commit. One that reads
or writes a record another transaction changed underneath it fails at `COMMIT`
with a serialization error. Nothing is applied, and the error is transient, so
a driver with retry logic replays the transaction without the application
having to handle it. That differs from the data path, whose commit-time
serialization error is not marked transient.

Conflicts are detected per record read, not per subsystem. Two sessions each
creating a different user both succeed; two sessions changing the same user do
not. One exception at bootstrap: on an instance with no users yet, two sessions
each creating their first user conflict, because each has to decide whether it
is creating the first one.

A statement that lists records depends on the whole list. This is worth
knowing, because it is the case that surprises people:

```cypher
BEGIN;
CREATE USER erin;
SHOW USERS;        /* reads the whole user set */
COMMIT;            /* fails if another session created a user meanwhile */
```

The listing is a serialisable read: the transaction saw a user set that another
session has since changed, so committing against it would mean acting on a
state that never existed. Retrying re-reads the set and succeeds.

Nothing blocks while a transaction is open. Conflict detection is optimistic
and the loser finds out at `COMMIT`.

---

## 5. Replication

An auth transaction replicates as a single batched request at `COMMIT`. A
replica applies all of it or none of it, so it cannot be left holding a user
without the grant that accompanied it.

The system lock is taken at `COMMIT` for the duration of the flush and
replication, not for the life of the transaction, and only when the transaction
has changes to replicate. An open auth transaction does not block other
sessions' system queries.

A replica that misses the commit is marked behind and recovers by full
snapshot. The main sends only the batched format, which a replica running an
older version cannot decode, so such a replica is snapshotted on every auth
commit until it is upgraded. An upgraded replica still accepts the earlier
per-record format from an older main, so upgrading replicas before the main,
as section 7 says, avoids that.

---

## 6. Limits

- **Profiles are outside the transaction.** Profile writes, user and tenant, are
  refused inside one. Profile reads answer from committed state, and inside a
  data transaction they are not isolated, so repeating one can give a different
  answer.
- **Per-user resource limits are outside the transaction.** They are
  process-wide and have no rollback. A user dropped inside a transaction keeps
  its limits until the transaction commits.
- **Auth transactions are not counted in a database's transaction metrics.**
  The commit and rollback counters and the active-transactions gauge count data
  transactions; an auth transaction never touches a database.
- **Privilege changes do not reach sessions that are already connected.** A
  session checks its statements against the privileges it had when it
  authenticated, so a `REVOKE` or `DROP USER` takes effect on that session when
  it reconnects, not at `COMMIT`. `COMMIT` checks no privilege of its own: each
  statement was checked when it ran. Triggers, streams, and transaction-management
  queries re-read privileges. This is existing behaviour, unchanged here.
- **A commit can fail for a reason other than a conflict.** If another session
  holds the system lock for more than 100ms, the commit is refused with
  "Multiple concurrent system queries are not supported." Unlike a conflict, that
  is not reported as retryable, so a driver's retry logic will not replay it.
  This applies to any system query, not only to a transaction.

---

## 7. Backwards compatibility

A single auth statement outside a transaction behaves as before on the instance
that runs it: it takes the lock, writes, replicates and releases, with no
transaction involved. Existing scripts are unaffected, with one exception: a
profile write, user or tenant, inside an explicit transaction was applied at
once before and is now refused.

Replication is the exception. Every auth change now goes out in the batched
format, so an older replica cannot decode it. Outside a transaction each record
a statement changes still goes out as its own batch of one, as before, so a
statement that changes several records is not applied atomically on a replica.
Upgrade replicas before the main; see section 5.
