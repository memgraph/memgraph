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

## Dynamic Graph Partitioning (abbr. DGP)

### Implemented parameters

--dynamic-graph-partitioner-enabled (If the dynamic graph partitioner should be
  enabled.) type: bool default: false (start time)
--dgp-improvement-threshold (How much better should specific node score be
  to consider a migration to another worker. This represents the minimal
  difference between new score that the vertex will have when migrated
  and the old one such that it's migrated.) type: int32 default: 10
  (start time)
--dgp-max-batch-size (Maximal amount of vertices which should be migrated
  in one dynamic graph partitioner step.) type: int32 default: 2000
  (start time)

### Design decisions

* Each partitioning session has to be a new transaction.
* When and how does an instance perform the moves?
  * Periodically.
  * Token sharing (round robin, exactly one instance at a time has an
    opportunity to perform the moves).
* On server-side serialization error (when DGP receives an error).
  -> Quit partitioning and wait for the next turn.
* On client-side serialization error (when end client receives an error).
  -> The client should never receive an error because of any
     internal operation.
  -> For the first implementation, it's good enough to wait until data becomes
     available again.
  -> It would be nice to achieve that DGP has lower priority than end client
     operations.

### End-user parameters

* --dynamic-graph-partitioner-enabled (execution time)
* --dgp-improvement-threshold (execution time)
* --dgp-max-batch-size (execution time)
* --dgp-min-batch-size (execution time)
  -> Minimum number of nodes that will be moved in each step.
* --dgp-fitness-threshold (execution time)
  -> Do not perform moves if partitioning is good enough.
* --dgp-delta-turn-time (execution time)
  -> Time between each turn.
* --dgp-delta-step-time (execution time)
  -> Time between each step.
* --dgp-step-time (execution time)
  -> Time limit per each step.

### Testing

The implementation has to provide good enough results in terms of:
  * How good the partitioning is (numeric value), aka goodness.
  * Workload execution time.
  * Stress test correctness.

Test cases:
  * N not connected subgraphs
    -> shuffle nodes to N instances
    -> run partitioning
    -> test perfect partitioning.
  * N connected subgraph
    -> shuffle nodes to N instance
    -> run partitioning
    -> test partitioning.
  * Take realistic workload (Long Running, LDBC1, LDBC2, Card Fraud, BFS, WSP)
    -> measure exec time
    -> run partitioning
    -> test partitioning
    -> measure exec time (during and after partitioning).
