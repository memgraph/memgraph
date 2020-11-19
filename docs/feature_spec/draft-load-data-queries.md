# Load Data Queries

Loading data into Memgraph is a challenging task. We have to implement
something equivalent to the [Neo4j LOAD
CSV](https://neo4j.com/developer/guide-import-csv/#import-load-csv).

A more general concept is [SingleStore
Pipelines](https://docs.singlestore.com/v7.1/reference/sql-reference/pipelines-commands/create-pipeline).

We already tried with [Graph Streams](obsolete-kafka-integration.md). An option
is to migrate that code as a standalone product
[here](https://github.com/memgraph/mgtools).
