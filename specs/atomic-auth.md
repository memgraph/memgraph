# Atomic auth transactions

**Status:** Implemented (PR #4524), preview
**Author:** Colin Barry
**Last updated:** 2026-10-02

> Auth statements run inside `BEGIN` .. `COMMIT`. Everything the transaction
> changes becomes visible at once, to this instance and to every replica, or
> not at all.

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
> atomic again on every replica.

Nothing it writes is visible to another session, or to a replica, until
`COMMIT`. `ROLLBACK` discards it. A concurrent change by another transaction to
anything the transaction read or wrote fails the commit rather than overwriting
it.

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

The mixed statement ends the transaction. Nothing in it is applied, and a
later `COMMIT` reports that there is no transaction to commit.

### 3.2 What runs inside an auth transaction

Every auth statement: users, roles, privileges, fine-grained label, edge and
property permissions, database grants, impersonation, and the `SHOW` family
that reads them. A transaction adds no restriction of its own, so a statement
that needs an enterprise licence, or that is not permitted on a replica or a
coordinator, is refused inside a transaction exactly as it is outside one.

Statements that are not auth statements are refused, including profile queries
and the rest of Cypher. A profile query is a data query, so inside an auth
transaction it is refused by the rule in 3.1 above.

User profiles are **not** transactional. A user profile write is refused inside
any transaction, because `UserProfiles` answers from an in-memory cache that the
transaction can neither isolate nor roll back.

Tenant profile writes are not transactional either, but they are **not
refused**: one made inside a transaction applies immediately and stays applied
after `ROLLBACK`. Keep them out of transactions.

---

## 4. Conflicts

Two auth transactions that touch unrelated records both commit. One that reads
or writes a record another transaction changed underneath it fails at `COMMIT`
with a serialization error, the same one the data path raises. Nothing is
applied, and the error is transient, so a driver with retry logic replays the
transaction without the application having to handle it.

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
replication, not for the life of the transaction. An open auth transaction does
not block other sessions' system queries.

A replica running an older version that cannot decode the batch is marked
behind and recovers by full snapshot, so a mixed-version cluster converges.
Upgrading replicas before the main avoids that cost.

---

## 6. Limits

- **Profiles are outside the transaction.** A user profile write is refused
  inside one. A tenant profile write is not refused, but is not transactional
  either: it applies immediately and survives `ROLLBACK`. Profile reads answer
  from committed state, and inside a data transaction they are not isolated, so
  repeating one can give a different answer.
- **Per-user resource limits are outside the transaction.** They are
  process-wide and have no rollback. A user dropped inside a transaction keeps
  its limits until the transaction commits.
- **Auth transactions are not counted in a database's commit or rollback
  metrics.** Those count data transactions; an auth transaction never touches a
  database.
- **A committed change does not reach sessions that are already connected.** A
  session authorises against the permissions it cached when it authenticated, so
  a `REVOKE` takes effect on that session when it reconnects, not at `COMMIT`.
  This is existing behaviour, unchanged here, but it bounds what the atomicity
  above buys you: the transaction closes the window for sessions that connect
  after it, not for sessions already holding a grant.
- **A commit can fail for a reason other than a conflict.** If another session
  holds the system lock for more than 100ms, the commit is refused with
  "Multiple concurrent system queries are not supported." Unlike a conflict, that
  is not reported as retryable, so a driver's retry logic will not replay it.
  This applies to any system query, not only to a transaction.

---

## 7. Backwards compatibility

A single auth statement outside a transaction behaves as before on the instance
that runs it: it takes the lock, writes, replicates and releases, with no
transaction involved. Existing scripts are unaffected.

Replication is the exception. Every auth change now goes out in the batched
format, a single statement as a batch of one, so an older replica cannot decode
it. Upgrade replicas before the main; see section 5.
