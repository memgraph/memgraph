## Dynamic Graph Partitioner

Memgraph supports dynamic graph partitioning which dynamically improves
performance on badly partitioned dataset over workers. To enable it, the user
should use the following flag when firing up the *master* node:

```plaintext
--dynamic_graph_partitioner_enable
```

### Parameters

| Name | Default Value | Description | Range |
|------|---------------|-------------|-------|
|--dgp_improvement_threshold | 10 | How much better should specific node score
be to consider a migration to another worker. This represents the minimal
difference between new score that the vertex will have when migrated and the
old one such that it's migrated. | Min: 1, Max: 100
|--dgp_max_batch_size | 2000 | Maximal amount of vertices which should be
migrated in one dynamic graph partitioner step. | Min: 1, Max: MaxInt32 |
