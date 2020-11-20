# Load Data Queries

Loading data into Memgraph is a challenging task. We have to implement
something equivalent to the [Neo4j LOAD
CSV](https://neo4j.com/developer/guide-import-csv/#import-load-csv). This
feature seems relatively straightforward to implement because `LoadCSV` could
be another operator that would yield row by row. By having the operator, the
operation would be composable with the rest of the `CREATE`|`MERGE` queries.

A more general concept is [SingleStore
Pipelines](https://docs.singlestore.com/v7.1/reference/sql-reference/pipelines-commands/create-pipeline).

We already tried with [Graph Streams](../obsolete/kafka-integration.md). An option
is to migrate that code as a standalone product
[here](https://github.com/memgraph/mgtools).
