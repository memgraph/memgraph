## Writing New Data

For adding new data, you can use the following clauses.

  * `CREATE`, for creating new nodes and edges.
  * `SET`, for adding new or updating existing labels and properties.
  * `DELETE`, for deleting nodes and edges.
  * `REMOVE`, for removing labels and properties.

You can still use the `RETURN` clause to produce results after writing, but it
is not mandatory.

Details on which kind of data can be stored in *Memgraph* can be found in
[Data Storage](../concepts/storage.md) chapter.

### CREATE

This clause is used to add new nodes and edges to the database. The creation
is done by providing a pattern, similarly to `MATCH` clause.

For example, to create 2 new nodes connected with a new edge, use this query.

```opencypher
CREATE (node1)-[:edge_type]->(node2)
```

Labels and properties can be set during creation using the same syntax as in
`MATCH` patterns. For example, creating a node with a label and a
property:

```opencypher
CREATE (node :Label {property: "my property value"})
```

Additional information on `CREATE` is
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/create/).

### SET

The `SET` clause is used to update labels and properties of already existing
data.

Example. Incrementing everyone's age by 1.

```opencypher
MATCH (n :Person) SET n.age = n.age + 1
```

Click
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/create/)
for a more detailed explanation on what can be done with `SET`.

### DELETE

This clause is used to delete nodes and edges from the database.

Example. Removing all edges of a single type.

```opencypher
MATCH ()-[edge :type]-() DELETE edge
```

When testing the database, you want to often have a clean start by deleting
every node and edge in the database. It is reasonable that deleting each node
should delete all edges coming into or out of that node.

```opencypher
MATCH (node) DELETE node
```

But, openCypher prevents accidental deletion of edges. Therefore, the above
query will report an error. Instead, you need to use the `DETACH` keyword,
which will remove edges from a node you are deleting. The following should
work and *delete everything* in the database.

```opencypher
MATCH (node) DETACH DELETE node
```

More examples are
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/delete/).

### REMOVE

The `REMOVE` clause is used to remove labels and properties from nodes and
edges.

Example.

```opencypher
MATCH (n :WrongLabel) REMOVE n :WrongLabel, n.property
```
