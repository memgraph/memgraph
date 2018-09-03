# Distributed Memgraph specs
This document describes reasnonings behind Memgraphs distributed concepts.

## Distributed state machine
Memgraphs distributed mode introduces two states of the cluster, recovering and
working. The change between states shouldn't happen often, but when it happens
it can take a while to make a transition from one to another.

### Recovering
This state is the default state for Memgraph when the cluster starts with
recovery flags. If the recovery finishes successfully, the state changes to
working. If recovery fails, the user will be presented with a message that
explains what happened and what are the next steps.

Another way to enter this state is failure. If the cluster encounters a failure,
the master will enter the Recovering mode. This time, it will wait for all
workers to respond with a message saying they are alive and well, and making
sure they all have consistent state.

### Working
This state should be the default state of Memgraph most of the time. When in
this state, Memgraph accepts connections from Bolt clients and allows query
execution.

If distributed execution fails for a transaction, that transaction, and all
other active transactions will be aborted and the cluster will enter the
Recovering state.

## Durability
One of the important concepts in distributed Memgraph is durability.

### Cluster configuration
When running Memgraph in distributed mode, the master will store cluster
metadata in a persistent store. If fore some reason the cluster shuts down,
recovering Memgraph from durability files shouldn't require any additional
flags.

### Database ID
Each new and clean run of Memgraph should generate a new globally unique
database id.  This id will associate all files that have persisted with this
run. Adding the database id to snapshots, write-ahead logs and cluster metadata
files ties them a specific Memgraph run, and it makes recovery easier to reason
about.

When recovering, the cluster won't generate a new id, but will reuse the one
from the snapshot/wal that it was able to recover from.

### Durability files
Memgraph uses snapshots and write-ahead logs for durability.

When Memgraph recovers it has to make sure all machines in the cluster recover
to the same recovery point. This is done by finding a common snapshot and
finding common transactions in per-machine available write-ahead logs.

Since we can not be sure that each machine persisted durability files, we need
to be able to negotiate a common recovery point in the cluster. Possible
durability file failures could require to start the cluster from scratch,
purging everything from storage and recovering from existing durability files.

We need to ensure that we keep wal files containing information about
transactions between all existing snapshots. This will provide better durability
in the case of a random machine durability file failure, where the cluster can
find a common recovery point that all machines in the cluster have.

Also, we should suggest and make clear docs that anything less than two
snapshots isn't considered safe for recovery.

### Recovery
The recovery happens in following steps:
* Master enables worker registration.
* Master recovers cluster metadata from the persisted storage.
* Master waits all required workers to register.
* Master broadcasts a recovery request to all workers.
* Workers respond with with a set of possible recovery points.
* Master finds a common recovery point for the whole cluster.
* Master broadcasts a recovery request with the common recovery point.
* Master waits for the cluster to recover.
* After a successful cluster recovery, master can enter Working state.
