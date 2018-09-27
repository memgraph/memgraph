## Reading and Writing

OpenCypher supports combining multiple reads and writes using the
`WITH` clause. In addition to combining, the `MERGE` clause is provided which
may create patterns if they do not exist.

### WITH

The write part of the query cannot be simply followed by another read part. In
order to combine them, `WITH` clause must be used. The names this clause
establishes are transferred from one part to another.

For example, creating a node and finding all nodes with the same property.

```opencypher
CREATE (node {property: 42}) WITH node.property AS propValue
MATCH (n {property: propValue}) RETURN n
```

Note that the `node` is not visible after `WITH`, since only `node.property`
was carried over.

This clause behaves very much like `RETURN`, so you should refer to features
of `RETURN`.

### MERGE

The `MERGE` clause is used to ensure that a pattern you are looking for exists
in the database. This means that if the pattern is not found, it will be
created. In a way, this clause is like a combination of `MATCH` and `CREATE`.


Example. Ensure that a person has at least one friend.

```opencypher
MATCH (n :Person) MERGE (n)-[:FriendOf]->(m)
```

The clause also provides additional features for updating the values depending
on whether the pattern was created or matched. This is achieved with `ON
CREATE` and `ON MATCH` sub clauses.

Example. Set a different properties depending on what `MERGE` did.

```opencypher
MATCH (n :Person) MERGE (n)-[:FriendOf]->(m)
ON CREATE SET m.prop = "created" ON MATCH SET m.prop = "existed"
```

For more details, click [this
link](https://neo4j.com/docs/developer-manual/current/cypher/clauses/merge/).
