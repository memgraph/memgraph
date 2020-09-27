# Replication

## High Level Context

Replication is a method that ensures that multiple database instances are
storing the same data. To enable replication, there must be at least two
instances of Memgraph in a cluster. Each instance has one of either two roles:
master or slave. The master instance is the instance that accepts writes to the
database and replicates its state to the slaves. In a cluster, there can only
be one master. There can be one or more slaves. None of the slaves will accept
write queries, but they will always accept read queries (there is an exception
to this rule and is described below). Slaves can also be configured to be
replicas of slaves, not necessarily replicas of the master. Each instance will
always be reachable using the standard supported communication protocols. The
replication will replicate WAL data. All data is transported through a custom
binary protocol that will try remain backward compatible, so that replication
immediately allows for zero downtime upgrades.

Each slave can be configured to accept replicated data in one of the following
modes:
 - synchronous
 - asynchronous
 - semi-synchronous

### Synchronous Replication

When the data is replicated to a slave synchronously, all of the data of a
currently pending transaction must be sent to the synchronous slave before the
transaction is able to commit its changes.

This mode has a positive implication that all data that is committed to the
master will always be replicated to the synchronous slave.  It also has a
negative performance implication because non-responsive slaves could grind all
query execution to a halt.

This mode is good when you absolutely need to be sure that all data is always
consistent between the master and the slave.

### Asynchronous Replication

When the data is replicated to a slave asynchronously, all pending transactions
are immediately committed and their data is replicated to the asynchronous
slave in the background.

This mode has a positive performance implication in which it won't slow down
query execution.  It also has a negative implication that the data between the
master and the slave is almost never in a consistent state (when the data is
being changed).

This mode is good when you don't care about consistency and only need an
eventually consistent cluster, but you care about performance.

### Semi-synchronous Replication

When the data is replicated to a slave semi-synchronously, the data is
replicated using both the synchronous and asynchronous methodology. The data is
always replicated synchronously, but, if the slave for any reason doesn't
respond within a preset timeout, the pending transaction is committed and the
data is replicated to the slave asynchronously.

This mode has a positive implication that all data that is committed is
*mostly* replicated to the semi-synchronous slave. It also has a negative
performance implication as the synchronous replication mode.

This mode is useful when you want the replication to be synchronous to ensure
that the data within the cluster is consistent, but you don't want the master
to grind to a halt when you have a non-responsive slave.

### Addition of a New Slave

Each slave when added to the cluster (in any mode) will first start out as an
asynchronous slave. That will allow slaves that have fallen behind to first
catch-up to the current state of the database. When the slave is in a state
that it isn't lagging behind the master it will then be promoted (in a brief
stop-the-world operation) to a semi-synchronous or synchronous slave. Slaves
that are added as asynchronous slaves will remain asynchronous.

## User Facing Setup

### How to Setup a Memgraph Cluster with Replication?

Replication configuration is done primarily through openCypher commands. This
allows the cluster to be dynamically rearranged (new leader election, addition
of a new slave, etc.).

Each Memgraph instance when first started will be a master. You have to change
the mode of all slave nodes using the following openCypher query before you can
enable replication on the master:

```plaintext
SET REPLICATION MODE TO (MASTER|SLAVE);
```

After you have set your slave instance to the correct operating mode, you can
enable replication in the master instance by issuing the following openCypher
command:
```plaintext
CREATE REPLICA name (SYNC|ASYNC) [WITH TIMEOUT 0.5] TO <ip_address>;
```

Each Memgraph instance will remember that the configuration was set to and will
automatically resume with its role when restarted.

### How to Setup an Advanced Replication Scenario?

The configuration allows for a more advanced scenario like this:
```plaintext
master -[asyncrhonous]-> slave 1 -[semi-synchronous]-> slave 2
```

To configure the above scenario, issue the following commands:
```plaintext
SET REPLICATION MODE TO SLAVE;  # on slave 1
SET REPLICATION MODE TO SLAVE;  # on slave 2

CREATE REPLICA slave1 ASYNC TO <slave1_ip>;  # on master
CREATE REPLICA slave2 SYNC WITH TIMEOUT 0.5 TO <slave2_ip>;  # on slave 1
```

### How to See the Current Replication Status?

To see the replication mode of the current Memgraph instance, you can issue the
following query:

```plaintext
SHOW REPLICATION MODE;
```

To see the replicas of the current Memgraph instance, you can issue the
following query:

```plaintext
SHOW REPLICAS;
```

To delete a replica, issue the following query:

```plaintext
DELETE REPLICA 'name';
```

### How to Promote a New Master?

When you have an already set-up cluster, to promote a new master, just set the
slave that you want to be a master to the master role.

```plaintext
SET REPLICATION MODE TO MASTER;  # on desired slave
```

After the command is issued, if the original master is still alive, it won't be
able to replicate its data to the slave (the new master) anymore and will enter
an error state. You must ensure that at any given point in time there aren't
two masters in the cluster.

## Integration with Memgraph

WAL `Delta`s are replicated between the replication master and slave. With
`Delta`s, all `StorageGlobalOperation`s are also replicated.  Replication is
essentially the same as appending to the WAL.

Synchronous replication will occur in `Commit` and each
`StorageGlobalOperation` handler.  The storage itself guarantees that `Commit`
will be called single-threadedly and that no `StorageGlobalOperation` will be
executed during an active transaction.  Asynchronous replication will load its
data from already written WAL files and transmit the data to the slave.  All
data will be replicated using our RPC protocol (SLK encoded).

For each replica the replication master (or slave) will keep track of the
replica's state. That way, it will know which operations must be transmitted to
the replica and which operations can be skipped. When a replica is very stale,
a snapshot will be transmitted to it so that it can quickly synchronize with
the current state. All following operations will transmit WAL deltas.

## Reading materials

1. [PostgreSQL comparison of different solutions](https://www.postgresql.org/docs/12/different-replication-solutions.html)
2. [PostgreSQL docs](https://www.postgresql.org/docs/12/runtime-config-replication.html)
3. [MySQL reference manual](https://dev.mysql.com/doc/refman/8.0/en/replication.html)
4. [MySQL docs](https://dev.mysql.com/doc/refman/8.0/en/replication-setup-slaves.html)
5. [MySQL master switch](https://dev.mysql.com/doc/refman/8.0/en/replication-solutions-switch.html)
