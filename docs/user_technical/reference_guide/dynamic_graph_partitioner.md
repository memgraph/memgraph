## Dynamic Graph Partitioner

Memgraph supports dynamic graph partitioning which dynamically improves
performance on badly partitioned dataset over workers. To enable it, the user
should use the following flag when firing up the *master* node:

```plaintext
--dynamic_graph_partitioner_enable
```
