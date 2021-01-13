# Date and Time Data Types

Neo4j offers the following functionality:

* https://neo4j.com/docs/cypher-manual/current/syntax/temporal/
* https://neo4j.com/docs/cypher-manual/current/functions/temporal/

The question is, how are we going to support equivalent capabilities? We need
something very similar because these are, in general, very well defined types.

A note about the storage is that Memgraph has a limit on the total number of
different data types, 16 at this point. We have to be mindful of that during
the design phase.
