# Distributed updates

Operations that modify the graph state are somewhat more complex in the
distributed system, as opposed to a single-node Memgraph deployment. The
complexity arises from two factors.

First, the data being modified is not necessarily owned by the worker
performing the modification. This situation is completely valid workers
execute parts of the query plan and parts must be executed by the
master.

Second, there are less guarantees regarding multi-threaded access. In
single-node Memgraph it was guaranteed that only one transaction will be
performing database work in a single transaction. This implied that
per-version storage could be thread-unsafe. In distributed Memgraph it
is possible that multiple threads could be performing work in the same
transaction as a consequence of the query being executed at the same
time on multiple workers and those executions interacting with the
globally partitioned database state.

## Deferred state modification

Making the per-version data storage thread-safe would most likely have a
performance impact very undesirable in a transactional database intended
for high throughput.

An alternative is that state modification over unsafe structures is not
performed immediately when requested, but postponed until it is safe to
do (there is is a guarantee of no concurrent access).

Since local query plan execution is done the same way on local data as
it is in single-node Memgraph, it is not possible to deffer that part of
the modification story. What can be deferred are modifications requested
by other workers. Since local query plan execution still is
single-threaded, this approach is safe.

At the same time those workers requesting the remote update can update
local copies (caches) of the not-owned data since that cache is only
being used by the single, local-execution thread.

### Visibility

Since updates are deferred the question arises: when do the updates
become visible? The above described process offers the following
visibility guarantees:
- updates done on the local state are visible to the owner
- updates done on the local state are NOT visible to anyone else during
  the same (transaction + command)
- updates done on remote state are deferred on the owner and not
  visible to the owner until applied
- updates done on the remote state are applied immediately to the local
  caches and thus visible locally

This implies an inconsistent view of the database state. In a concurrent
execution of a single query this can hardly be avoided and is accepted
as such. It does not change the Cypher query execution semantic in any
of the well-defined scenarios. It possibly changes some of the behaviors
in which the semantic is not well defined even in single-node execution.

### Synchronization, update application

In many queries it is mandatory to observe the latest global graph state
(typically when returning it to the client). That means that before that
happens all the deferred updates need to be applied, and all the caches
to remote data invalidated. Exactly this happens when executing queries
that modify the graph state. At some point a global synchronization
point is reached. First it is waited that all workers finish the
execution of query plan parts performing state modifications. After that
all the workers are told to apply the deferred updates they received to
their graph state. Since there is no concurrent query plan execution,
this is safe. Once that is done all the local caches are cleared and the
requested data can be returned to the client.

### Command advancement

In complex queries where a read part follows a state modification part
the synchronization process after the state modification part is
followed by command advancement, like in single-node execution.

## Creation

Graph element creation is not deferred. This is practical because the
response to a creation is the global ID of the newly created element. At
the same time it is safe because no other worker (including the owner)
will be using the newly added graph element.

## Updating

Updating is deferred, as described. Note that this also means that
record locking conflicts are deferred and serialization errors
(including lock timeouts) are postponed until the deferred update
application phase. In certain scenarios it might be beneficial to force
these errors to happen earlier, when the deferred update request is
processed.

## Deletion

Deletion is also deferred. Deleting an edge implies a modification of
it's endpoint vertices, which must be deferred as those data structures
are not thread-safe. Deleting a vertex is either with detaching, in
which case an arbitrary number of updates are implied in the vertex's
neighborhood, or without detaching which relies on checking the current
state of the graph which is generally impossible in distributed.
