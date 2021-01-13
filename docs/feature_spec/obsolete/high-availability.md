# High Availability (abbr. HA)

## High Level Context

High availability is a characteristic of a system which aims to ensure a
certain level of operational performance for a higher-than-normal period.
Although there are multiple ways to design highly available systems, Memgraph
strives to achieve HA by elimination of single points of failure. In essence,
this implies adding redundancy to the system so that a failure of a component
does not imply the failure of the entire system. To ensure this, HA Memgraph
implements the [Raft consensus algorithm](https://raft.github.io/).

Correct implementation of the algorithm guarantees that the cluster will be
fully functional (available) as long as any strong majority of the servers are
operational and can communicate with each other and with clients. For example,
clusters of three or four machines can tolerate the failure of a single server,
clusters of five and six machines can tolerate the failure of any two servers,
and so on. Therefore, we strongly recommend a setup of an odd-sized cluster.

### Performance Implications

Internally, Raft achieves high availability by keeping a consistent replicated
log on each server within the cluster. Therefore, we must successfully replicate
a transaction on the majority of servers within the cluster before we actually
commit it and report the result back to the client. This operation represents
a significant performance hit when compared with single node version of
Memgraph.

Luckily, the algorithm can be tweaked in a way which allows read-only
transactions to perform significantly better than those which modify the
database state. That being said, the performance of read-only operations
is still not going to be on par with single node Memgraph.

This section will be updated with exact numbers once we integrate HA with
new storage.

With the old storage, write throughput was almost five times lower than read
throughput (~30000 reads per second vs ~6000 writes per second).

## User Facing Setup

### How to Setup HA Memgraph Cluster?

First, the user needs to install `memgraph_ha` package on each machine
in their cluster. HA Memgraph should be available as a Debian package,
so its installation on each machine should be as simple as:

```plaintext
dpkg -i /path/to/memgraph_ha_<version>.deb
```

After successful installation of the `memgraph_ha` package, the user should
finish its configuration before attempting to start the cluster.

There are two main things that need to be configured on every node in order for
the cluster to be able to run:

1. The user has to edit the main configuration file and specify the unique node
   ID to each server in the cluster
2. The user has to create a file that describes all IP addresses of all servers
   that will be used in the cluster

The `memgraph_ha` binary loads all main configuration parameters from
`/etc/memgraph/memgraph_ha.conf`. On each node of the cluster, the user should
uncomment the `--server-id=0` parameter and change its value to the `server_id`
of that node.

The last step before starting the server is to create a `coordination`
configuration file. That file is already present as an example in
`/etc/memgraph/coordination.json.example` and you have to copy it to
`/etc/memgraph/coordination.json` and edit it according to your cluster
configuration. The file contains coordination info consisting of a list of
`server_id`, `ip_address` and `rpc_port` lists. The assumed contents of the
`coordination.json` file are:

```plaintext
[
  [1, "192.168.0.1", 10000],
  [2, "192.168.0.2", 10000],
  [3, "192.168.0.3", 10000]
]
```
Here, each line corresponds to coordination of one server. The first entry is
that server's ID, the second is its IP address and the third is the RPC port it
listens to. This port should not be confused with the port used for client
interaction via the Bolt protocol.

The `ip_address` entered for each `server_id` *must* match the exact IP address
that belongs to that server and that will be used to communicate to other nodes
in the cluster. The coordination configuration file *must* be identical on all
nodes in the cluster.

After the user has set the `server_id` on each node in
`/etc/memgraph/memgraph_ha.conf` and provided the same
`/etc/memgraph/coordination.json` file to each node in the cluster, they can
start the Memgraph HA service by issuing the following command on each node in
the cluster:

```plaintext
systemctl start memgraph_ha
```

### How to Configure Raft Parameters?

All Raft configuration parameters can be controlled by modifying
`/etc/memgraph/raft.json`.  The assumed contents of the `raft.json` file are:

```plaintext
{
  "election_timeout_min": 750,
  "election_timeout_max": 1000,
  "heartbeat_interval": 100,
  "replication_timeout": 20000,
  "log_size_snapshot_threshold": 50000
}
```

The meaning behind each entry is demystified in the following table:

Flag                          | Description
------------------------------|------------
`election_timeout_min`        | Lower bound for the randomly sampled reelection timer given in milliseconds
`election_timeout_max`        | Upper bound for the randomly sampled reelection timer given in milliseconds
`heartbeat_interval`          | Time interval between consecutive heartbeats given in milliseconds
`replication_timeout`         | Time interval allowed for data replication given in milliseconds
`log_size_snapshot_threshold` | Allowed number of entries in Raft log before its compaction

### How to Query HA Memgraph via Proxy?

This chapter describes how to query HA Memgraph using our proxy server.
Note that this is not intended to be a long-term solution. Instead, we will
implement a proper Memgraph HA client which is capable of communicating with
the HA cluster. Once our own client is implemented, it will no longer be
possible to query HA Memgraph using other clients (such as neo4j client).

The Bolt protocol that is exposed by each Memgraph HA node is an extended
version of the standard Bolt protocol. In order to be able to communicate with
the highly available cluster of Memgraph HA nodes, the client must have some
logic implemented in itself so that it can communicate correctly with all nodes
in the cluster. To facilitate a faster start with the HA cluster we will build
the Memgraph HA proxy binary that communicates with all nodes in the HA cluster
using the extended Bolt protocol and itself exposes a standard Bolt protocol to
the user. All standard Bolt clients (libraries and custom systems) can
communicate with the Memgraph HA proxy without any code modifications.

The HA proxy should be deployed on each client machine that is used to
communicate with the cluster. It can't be deployed on the Memgraph HA nodes!

When using the Memgraph HA proxy, the communication flow is described in the
following diagram:

```plaintext
Memgraph HA node 1 -----+
                        |
Memgraph HA node 2 -----+ Memgraph HA proxy <---> any standard Bolt client (C, Java, PHP, Python, etc.)
                        |
Memgraph HA node 3 -----+
```

To setup the Memgraph HA proxy the user should install the `memgraph_ha_proxy`
package.

After its successful installation, the user should enter all endpoints of the
HA Memgraph cluster servers into the configuration before attempting to start
the HA Memgraph proxy server.

The HA Memgraph proxy server loads all of its configuration from
`/etc/memgraph/memgraph_ha_proxy.conf`. Assuming that the cluster is set up
like in the previous examples, the user should uncomment and enter the following
value into the `--endpoints` parameter:

```plaintext
--endpoints=192.168.0.1:7687,192.168.0.2:7687,192.168.0.3:7687
```

Note that the IP addresses used in the example match the individual cluster
nodes IP addresses, but the ports used are the Bolt server ports exposed by
each node (currently the default value of `7687`).

The user can now start the proxy by using the following command:

```plaintext
systemctl start memgraph_ha_proxy
```

After the proxy has been started, the user can query the HA cluster by
connecting to the HA Memgraph proxy IP address using their favorite Bolt
client.

## Integration with Memgraph

The first thing that should be defined is a single instruction within the
context of Raft (i.e. a single entry in a replicated log).
These instructions should be completely deterministic when applied
to the state machine. We have therefore decided that the appropriate level
of abstraction within Memgraph corresponds to `Delta`s (data structures
which describe a single change to the Memgraph state, used for durability
in WAL). Moreover, a single instruction in a replicated log will consist of a
batch of `Delta`s which correspond to a single transaction that's about
to be **committed**.

Apart from `Delta`s, there are certain operations within the storage called
`StorageGlobalOperations` which do not conform to usual transactional workflow
(e.g. Creating indices).  Since our storage engine implementation guarantees
that at the moment of their execution no other transactions are active, we can
safely replicate them as well. In other words, no additional logic needs to be
implemented because of them.

Therefore, we will introduce a new `RaftDelta` object which can be constructed
both from storage `Delta` and `StorageGlobalOperation`. Instead of appending
these to WAL (as we do in single node), we will start to replicate them across
our cluster. Once we have replicated the corresponding Raft log entry on
majority of the cluster, we are able to safely commit the transaction or execute
a global operation. If for any reason the replication fails (leadership change,
worker failures, etc.) the transaction will be aborted.

In the follower mode, we need to be able to apply `RaftDelta`s we got from
the leader when the protocol allows us to do so. In that case, we will use the
same concepts from durability in storage v2, i.e., applying deltas maps
completely to recovery from WAL in storage v2.

## Test and Benchmark Strategy

We have already implemented some integration and stress tests. These are:

1. leader election -- Tests whether leader election works properly.
2. basic test -- Tests basic leader election and log replication.
3. term updates test -- Tests a specific corner case (which used to fail)
                        regarding term updates.
4. log compaction test -- Tests whether log compaction works properly.
5. large log entries -- Tests whether we can successfully replicate relatively
                        large log entries.
6. index test -- Tests whether index creation works in HA.
7. normal operation stress test -- Long running concurrent stress test under
                                   normal conditions (no failures).
8. read benchmark -- Measures read throughput in HA.
9. write benchmark -- Measures write throughput in HA.

At the moment, our main goal is to pass existing tests and have a stable version
on our stress test. We should also implement a stress test which occasionally
introduces different types of failures in our cluster (we did this kind of
testing manually thus far). Passing these tests should convince us that we have
a "stable enough" version which we can start pushing to our customers.

Additional (proper) testing should probably involve some ideas from
[here](https://jepsen.io/analyses/dgraph-1-0-2)

## Possible Future Changes/Improvements/Extensions

There are two general directions in which we can alter HA Memgraph. The first
direction assumes we are going to stick with the Raft protocol. In that case
there are a few known ways to extend the basic algorithm in order to gain
better performance or achieve extra functionality. In no particular order,
these are:

1. Improving read performance using leader leases [Section 6.4 from Raft thesis]
2. Introducing cluster membership changes [Chapter 4 from Raft thesis]
3. Introducing a [learner mode](https://etcd.io/docs/v3.3.12/learning/learner/).
4. Consider different log compaction strategies [Chapter 5 from Raft thesis]
5. Removing HA proxy and implementing our own HA Memgraph client.

On the other hand, we might decide in the future to base our HA implementation
on a completely different protocol which might even offer different guarantees.
In that case we probably need to do a bit more of market research and weigh the
trade-offs of different solutions.
[This](https://www.postgresql.org/docs/9.5/different-replication-solutions.html)
might be a good starting point.

## Reading materials

1. [Raft paper](https://raft.github.io/raft.pdf)
2. [Raft thesis](https://github.com/ongardie/dissertation) (book.pdf)
3. [Raft playground](https://raft.github.io/)
4. [Leader Leases](https://blog.yugabyte.com/low-latency-reads-in-geo-distributed-sql-with-raft-leader-leases/)
5. [Improving Raft ETH](https://pub.tik.ee.ethz.ch/students/2017-FS/SA-2017-80.pdf)
