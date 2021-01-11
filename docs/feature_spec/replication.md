# Replication

## High Level Context

Replication is a method that ensures that multiple database instances are
storing the same data. To enable replication, there must be at least two
instances of Memgraph in a cluster. Each instance has one of either two roles:
main or replica. The main instance is the instance that accepts writes to the
database and replicates its state to the replicas. In a cluster, there can only
be one main. There can be one or more replicas. None of the replicas will accept
write queries, but they will always accept read queries (there is an exception
to this rule and is described below). Replicas can also be configured to be
replicas of replicas, not necessarily replicas of the main. Each instance will
always be reachable using the standard supported communication protocols. The
replication will replicate WAL data. All data is transported through a custom
binary protocol that will try remain backward compatible, so that replication
immediately allows for zero downtime upgrades.

Each replica can be configured to accept replicated data in one of the following
modes:
 - synchronous
 - asynchronous
 - semi-synchronous

### Synchronous Replication

When the data is replicated to a replica synchronously, all of the data of a
currently pending transaction must be sent to the synchronous replica before the
transaction is able to commit its changes.

This mode has a positive implication that all data that is committed to the
main will always be replicated to the synchronous replica. It also has a
negative performance implication because non-responsive replicas could grind all
query execution to a halt.

This mode is good when you absolutely need to be sure that all data is always
consistent between the main and the replica.

### Asynchronous Replication

When the data is replicated to a replica asynchronously, all pending
transactions are immediately committed and their data is replicated to the
asynchronous replica in the background.

This mode has a positive performance implication in which it won't slow down
query execution. It also has a negative implication that the data between the
main and the replica is almost never in a consistent state (when the data is
being changed).

This mode is good when you don't care about consistency and only need an
eventually consistent cluster, but you care about performance.

### Semi-synchronous Replication

When the data is replicated to a replica semi-synchronously, the data is
replicated using both the synchronous and asynchronous methodology. The data is
always replicated synchronously, but, if the replica for any reason doesn't
respond within a preset timeout, the pending transaction is committed and the
data is replicated to the replica asynchronously.

This mode has a positive implication that all data that is committed is
*mostly* replicated to the semi-synchronous replica. It also has a negative
performance implication as the synchronous replication mode.

This mode is useful when you want the replication to be synchronous to ensure
that the data within the cluster is consistent, but you don't want the main
to grind to a halt when you have a non-responsive replica.

### Addition of a New Replica

Each replica, when added to the cluster (in any mode), will first start out as
an asynchronous replica. That will allow replicas that have fallen behind to
first catch-up to the current state of the database. When the replica is in a
state that it isn't lagging behind the main it will then be promoted (in a brief
stop-the-world operation) to a semi-synchronous or synchronous replica. Slaves
that are added as asynchronous replicas will remain asynchronous.

## User Facing Setup

### How to Setup a Memgraph Cluster with Replication?

Replication configuration is done primarily through openCypher commands. This
allows the cluster to be dynamically rearranged (new leader election, addition
of a new replica, etc.).

Each Memgraph instance when first started will be a main. You have to change
the role of all replica nodes using the following openCypher query before you
can enable replication on the main:

```plaintext
SET REPLICATION ROLE TO (MAIN|REPLICA) WITH PORT <port_number>;
```

Note that the "WITH PORT <port_number>" part of the query sets the replication port,
but it applies only to the replica. In other words, if you try to set the
replication port as the main, a semantic exception will be thrown.
After you have set your replica instance to the correct operating role, you can
enable replication in the main instance by issuing the following openCypher
command:
```plaintext
REGISTER REPLICA name (SYNC|ASYNC) [WITH TIMEOUT 0.5] TO <socket_address>;
```

The socket address must be a string of the following form:

```plaintext
"IP_ADDRESS:PORT_NUMBER"
```

where IP_ADDRESS is a valid IP address, and PORT_NUMBER is a valid port number,
both given in decimal notation.
Note that in this case they must be separated by a single colon.
Alternatively, one can give the socket address as:

```plaintext
"IP_ADDRESS"
```

where IP_ADDRESS must be a valid IP address, and the port number will be
assumed to be the default one (we specify it to be 10000).

Each Memgraph instance will remember what the configuration was set to and will
automatically resume with its role when restarted.


### How to See the Current Replication Status?

To see the replication ROLE of the current Memgraph instance, you can issue the
following query:

```plaintext
SHOW REPLICATION ROLE;
```

To see the replicas of the current Memgraph instance, you can issue the
following query:

```plaintext
SHOW REPLICAS;
```

To delete a replica, issue the following query:

```plaintext
DROP REPLICA 'name';
```

### How to Promote a New Main?

When you have an already set-up cluster, to promote a new main, just set the
replica that you want to be a main to the main role.

```plaintext
SET REPLICATION ROLE TO MAIN;  # on desired replica
```

After the command is issued, if the original main is still alive, it won't be
able to replicate its data to the replica (the new main) anymore and will enter
an error state. You must ensure that at any given point in time there aren't
two mains in the cluster.

## Limitations and Potential Features

Currently, we do not support chained replicas, i.e. a replica can't have its
own replica. When this feature becomes available, the user will be able to 
configure scenarios such as the following one:

```plaintext
main -[asynchronous]-> replica 1 -[semi-synchronous]-> replica 2
```

To configure the above scenario, the user will be able to issue the following
commands:
```plaintext
SET REPLICATION ROLE TO REPLICA WITH PORT <port1>;  # on replica 1
SET REPLICATION ROLE TO REPLICA WITH PORT <port2>;  # on replica 2

REGISTER REPLICA replica1 ASYNC TO "replica1_ip_address:port1";  # on main
REGISTER REPLICA replica2 SYNC WITH TIMEOUT 0.5
  TO "replica2_ip_address:port2";  # on replica 1
```

In addition, we do not yet support advanced recovery mechanisms. For example,
if a main crashes, a suitable replica will take its place as the new main. If
the crashed main goes back online, it will not be able to reclaim its previous
role, but will be forced to be a replica of the new main.
In the upcoming releases, we might be adding more advanced recovery mechanisms.
However, users are able to setup their own recovery policies using the basic
recovery mechanisms we currently provide, that can cover a wide range of
real-life scenarios.

## Integration with Memgraph

WAL `Delta`s are replicated between the replication main and replica. With
`Delta`s, all `StorageGlobalOperation`s are also replicated. Replication is
essentially the same as appending to the WAL.

Synchronous replication will occur in `Commit` and each
`StorageGlobalOperation` handler. The storage itself guarantees that `Commit`
will be called single-threadedly and that no `StorageGlobalOperation` will be
executed during an active transaction. Asynchronous replication will load its
data from already written WAL files and transmit the data to the replica. All
data will be replicated using our RPC protocol (SLK encoded).

For each replica the replication main (or replica) will keep track of the
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
