# Dynamic Graph Partitioning (abbr. DGP)

## Implementation

Take a look under `dev/memgraph/distributed/dynamic_graph_partitioning.md`.

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

## Planning

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
