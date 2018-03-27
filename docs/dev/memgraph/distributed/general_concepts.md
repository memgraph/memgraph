# Memgraph distributed

This chapter describes some of the concepts used in distributed
Memgraph. By "distributed" here we mean the sharding of a single graph
onto multiple processing units (servers).

## Conceptual organization

There is a single master and multiple workers. The master contains all
the global sources of truth (transaction engine,
[label|edge-type|property] to name mappings). Also, in the current
organization it is the only one that contains a Bolt server (for
communication with the end client) and an interpretation engine. Workers
contain the data and means of subquery interpretation (query plans
recieved from the master) and means of communication with the master and
other workers.

In many query plans the load on the master is much larger then the load
on the workers. For that reason it might be beneficial to make the
master contain less data (or none at all), and/or having multiple
interpretation masters.

## Logic organization

Both the distributed and the single node Memgraph use the same codebase.
In cases where the behavior in single-node differs from that in
distributed, some kind of dynamic behavior change is implemented (either
through inheritance or conditional logic).

### GraphDb

The `database::GraphDb` is an "umbrella" object for parts of the
database such as storage, garbage collection, transaction engine etc.
There is a class heirarchy of `GraphDb` implementations, as well as a
base interface object. There are subclasses for single-node, master and
worker deplotyments. Which implementation is used depends on the
configuration processed in the `main` entry point of memgraph.

The `GraphDb` interface exposes getters to base classes of
other similar heirarchies (for example to `tx::Engine`). In that way
much of the code that uses those objects (for example query plan
interpretation) is agnostic to the type of deployment.

### RecordAccessors

The functionality of `RecordAccessors` and it's subclasses is already
documented. It's important to note that the same implementation of
accessors is used in all deployments, with internal changes of behavior
depending on the locality of the graph element (vertex or edge) the
accessor represents. For example, if the graph element is local, an
update operation on an accessor will make the necessary MVCC ops, update
local data, indexes, the write-ahead log etc. However, if the accessor
represents a remote graph element, an update will trigger an RPC message
to the owner about the update and a change in the local cache.
