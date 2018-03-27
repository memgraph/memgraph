# Write-ahead logging

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
configurable in databases. In Memgraph it is at the moment not
guaranteed, nor configurable. The WAL is flushed to the disk
periodically and transactions do not wait for this to complete.

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
