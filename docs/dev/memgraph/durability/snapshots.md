# Snapshots

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
