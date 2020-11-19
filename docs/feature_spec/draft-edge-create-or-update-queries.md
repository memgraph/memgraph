# Edge Create or Update Queries

The old semantic of the `MERGE` clause is quite tricky. The new semantic of
`MERGE` is explained
[here](https://blog.acolyer.org/2019/09/18/updating-graph-databases-with-cypher/).

Similar to `MERGE`, but maybe simpler is to define clauses and semantics that
apply only to a single edge. In the case an edge between two nodes doesn't
exist, it should be created. On the other hand, if it exists, it should be
updated. The syntax should look similar to the following:

```
MERGE EDGE (a)-[e:Type {props}]->(b) [ON CREATE SET expression ON UPDATE SET expression] ...
```
