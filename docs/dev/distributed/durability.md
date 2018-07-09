# Distributed durability

Durability in distributed is slightly different then in single-node as
the state itself is shared between multiple workers and none of those
states are independent.

Note that recovering from persistent storage must result in a stable
database state. This means that across the cluster the state
modification of every transaction that was running is either recovered
fully or not at all. Also, if transaction A committed before transaction B,
then if B is recovered so must A.

## Snapshots

It is possibly avoidable but highly desirable that the database can be
recovered from snapshot only, without relying on WAL files. For this to
be possible in distributed, it must be ensured that the same
transactions are recovered on all the workers (including master) in the
cluster. Since the snapshot does not contain information about which
state change happened in which transaction, the only way to achieve this
is to have synchronized snapshots. This means that the process of
creating a snapshot, which is in itself transactional (it happens within
a transaction and thus observes some consistent database state), must
happen in the same transaction. This is achieved by the master starting
a snapshot generating transaction and triggering the process on all
workers in the cluster.

## WAL

Unlike the snapshot, write-ahead logs contain the information on which
transaction made which state change. This makes it possible to include
or exclude transactions during the recovery process. What is necessary
however is a global consensus on which of the transactions should be
recovered and which not, to ensure recovery into a consistent state.

It would be possible to achieve this with some kind of synchronized
recovery process, but it would impose constraints on cluster startup and
would not be trivial.

A simpler alternative is that the consensus is achieved beforehand,
while the database (to be recovered) is still operational. What is
necessary is to keep track of which transactions are guaranteed to
have been flushed to the WAL files on all the workers in the cluster. It
makes sense to keep this record on the master, so a mechanism is
introduced which periodically pings all the workers, telling them to
flush their WALs, and writes some sort of a log indicating that this has
been confirmed. The downside of this is a periodic broadcast must be
done, and that potentially slightly less data can be recovered in the
case of a crash then if using a post-crash consensus. It is however much
simpler to implement.
