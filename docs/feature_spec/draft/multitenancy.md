# Multitenancy

[Multitenancy](https://en.wikipedia.org/wiki/Multitenancy) is a feature mainly
in the domain of ease of use. Neo4j made a great move by introducing
[Fabric](https://neo4j.com/developer/multi-tenancy-worked-example).

Memgraph first step in a similar direction would be to add an abstraction layer
containing multiple `Storage` instances + the ability to specify a database
instance per client session or database transaction.

## Replication Context

Each transaction has to encode on top of which database it's getting executed.
Once a replica gets delta objects containing database info, the replica engine
could apply changes locally.
