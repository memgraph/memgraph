# Durability

## Write-ahead Logging

Typically WAL denotes the process of writing a "log" of database
operations (state changes) to persistent storage before committing the
transaction, thus ensuring that the state can be recovered (in the case
of a crash) for all the transactions which the database committed.

The WAL is a fine-grained durability format. It's purpose is to store
database changes fast. It's primary purpose is not to provide
space-efficient storage, nor to support fast recovery. For that reason
it's often used in combination with a different persistence mechanism
(in Memgraph's case the "snapshot") that has complementary
characteristics.

### Guarantees

Ensuring that the log is written before the transaction is committed can
slow down the database. For that reason this guarantee is most often
configurable in databases.

Memgraph offers two options for the WAL. The default option, where the WAL is
flushed to the disk periodically and transactions do not wait for this to
complete, introduces the risk of database inconsistency because an operating
system or hardware crash might lead to missing transactions in the WAL. Memgraph
will handle this as if those transactions never happened. The second option,
called synchronous commit, will instruct Memgraph to wait for the WAL to be
flushed to the disk when a transactions completes and the transaction will wait
for this to complete. This option can be turned on with the
`--synchronous-commit` command line flag.

### Format

The WAL file contains a series of DB state changes called `StateDelta`s.
Each of them describes what the state change is and in which transaction
it happened. Also some kinds of meta-information needed to ensure proper
state recovery are recorded (transaction beginnings and commits/abort).

The following is guaranteed w.r.t. `StateDelta` ordering in
a single WAL file:
- For two ops in the same transaction, if op A happened before B in the
  database, that ordering is preserved in the log.
- Transaction begin/commit/abort messages also appear in exactly the
  same order as they were executed in the transactional engine.

### Recovery

The database can recover from the WAL on startup. This works in
conjunction with snapshot recovery. The database attempts to recover from
the latest snapshot and then apply as much as possible from the WAL
files. Only those transactions that were not recovered from the snapshot
are recovered from the WAL, for speed efficiency. It is possible (but
inefficient) to recover the database from WAL only, provided all the WAL
files created from DB start are available. It is not possible to recover
partial database state (i.e. from some suffix of WAL files, without the
preceding snapshot).

## Snapshots

A "snapshot" is a record of the current database state stored in permanent
storage. Note that the term "snapshot" is used also in the context of
the transaction engine to denote a set of running transactions.

A snapshot is written to the file by Memgraph periodically if so
configured. The snapshot creation  process is done within a transaction created
specifically for that purpose. The transaction is needed to ensure that
the stored state is internally consistent.

The database state can be recovered from the snapshot during startup, if
so configured. This recovery works in conjunction with write-ahead log
recovery.

A single snapshot contains all the data needed to recover a database. In
that sense snapshots are independent of each other and old snapshots can
be deleted once the new ones are safely stored, if it is not necessary
to revert the database to some older state.

The exact format of the snapshot file is defined inline in the snapshot
creation code.
