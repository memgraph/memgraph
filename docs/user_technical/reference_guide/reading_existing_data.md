## Reading Existing Data

The simplest usage of the language is to find data stored in the
database. For that purpose, the following clauses are offered:

  * `MATCH`, which searches for patterns;
  * `WHERE`, for filtering the matched data and
  * `RETURN`, for defining what will be presented to the user in the result
    set.
  * `UNION` and `UNION ALL` for combining results from multiple queries.

### MATCH

This clause is used to obtain data from Memgraph by matching it to a given
pattern. For example, to find each node in the database, you can use the
following query.

```opencypher
MATCH (node) RETURN node
```

Finding connected nodes can be achieved by using the query:

```opencypher
MATCH (node1)-[connection]-(node2) RETURN node1, connection, node2
```

In addition to general pattern matching, you can narrow the search down by
specifying node labels and properties. Similarly, edge types and properties
can also be specified. For example, finding each node labeled as `Person` and
with property `age` being 42, is done with the following query.

```opencypher
MATCH (n :Person {age: 42}) RETURN n
```

While their friends can be found with the following.

```opencypher
MATCH (n :Person {age: 42})-[:FriendOf]-(friend) RETURN friend
```

There are cases when a user needs to find data which is connected by
traversing a path of connections, but the user doesn't know how many
connections need to be traversed. openCypher allows for designating patterns
with *variable path lengths*. Matching such a path is achieved by using the
`*` (*asterisk*) symbol inside the edge element of a pattern. For example,
traversing from `node1` to `node2` by following any number of connections in a
single direction can be achieved with:

```opencypher
MATCH (node1)-[r*]->(node2) RETURN node1, r, node2
```

If paths are very long, finding them could take a long time. To prevent that,
a user can provide the minimum and maximum length of the path. For example,
paths of length between 2 and 4 can be obtained with a query like:

```opencypher
MATCH (node1)-[r*2..4]->(node2) RETURN node1, r, node2
```

It is possible to name patterns in the query and return the resulting paths.
This is especially useful when matching variable length paths:

```opencypher
MATCH path = ()-[r*2..4]->() RETURN path
```

More details on how `MATCH` works can be found
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/match/).

The `MATCH` clause can be modified by prepending the `OPTIONAL` keyword.
`OPTIONAL MATCH` clause behaves the same as a regular `MATCH`, but when it
fails to find the pattern, missing parts of the pattern will be filled with
`null` values. Examples can be found
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/optional-match/).

### WHERE

You have already seen that simple filtering can be achieved by using labels
and properties in `MATCH` patterns. When more complex filtering is desired,
you can use `WHERE` paired with `MATCH` or `OPTIONAL MATCH`. For example,
finding each person older than 20 is done with the this query.

```opencypher
MATCH (n :Person) WHERE n.age > 20 RETURN n
```

Additional examples can be found
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/where/).

### RETURN

The `RETURN` clause defines which data should be included in the resulting
set. Basic usage was already shown in the examples for `MATCH` and `WHERE`
clauses. Another feature of `RETURN` is renaming the results using the `AS`
keyword.

Example.

```opencypher
MATCH (n :Person) RETURN n AS people
```

That query would display all nodes under the header named `people` instead of
`n`.

When you want to get everything that was matched, you can use the `*`
(*asterisk*) symbol.

This query:

```opencypher
MATCH (node1)-[connection]-(node2) RETURN *
```

is equivalent to:

```opencypher
MATCH (node1)-[connection]-(node2) RETURN node1, connection, node2
```

`RETURN` can be followed by the `DISTINCT` operator, which will remove
duplicate results. For example, getting unique names of people can be achieved
with:

```opencypher
MATCH (n :Person) RETURN DISTINCT n.name
```

Besides choosing what will be the result and how it will be named, the
`RETURN` clause can also be used to:

  * limit results with `LIMIT` sub-clause;
  * skip results with `SKIP` sub-clause;
  * order results with `ORDER BY` sub-clause and
  * perform aggregations (such as `count`).

More details on `RETURN` can be found
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/return/).

#### SKIP & LIMIT

These sub-clauses take a number of how many results to skip or limit.
For example, to get the first 3 results you can use this query.

```opencypher
MATCH (n :Person) RETURN n LIMIT 3
```

If you want to get all the results after the first 3, you can use the
following.

```opencypher
MATCH (n :Person) RETURN n SKIP 3
```

The `SKIP` and `LIMIT` can be combined. So for example, to get the 2nd result,
you can do:

```opencypher
MATCH (n :Person) RETURN n SKIP 1 LIMIT 1
```

#### ORDER BY

Since the patterns which are matched can come in any order, it is very useful
to be able to enforce some ordering among the results. In such cases, you can
use the `ORDER BY` sub-clause.

For example, the following query will get all `:Person` nodes and order them
by their names.

```opencypher
MATCH (n :Person) RETURN n ORDER BY n.name
```

By default, ordering will be in the ascending order. To change the order to be
descending, you should append `DESC`.

For example, to order people by their name descending, you can use this query.

```opencypher
MATCH (n :Person) RETURN n ORDER BY n.name DESC
```

You can also order by multiple variables. The results will be sorted by the
first variable listed. If the values are equal, the results are sorted by the
second variable, and so on.

Example. Ordering by first name descending and last name ascending.

```opencypher
MATCH (n :Person) RETURN n ORDER BY n.name DESC, n.lastName
```

Note that `ORDER BY` sees only the variable names as carried over by `RETURN`.
This means that the following will result in an error.

```opencypher
MATCH (n :Person) RETURN old AS new ORDER BY old.name
```

Instead, the `new` variable must be used:

```opencypher
MATCH (n: Person) RETURN old AS new ORDER BY new.name
```

The `ORDER BY` sub-clause may come in handy with `SKIP` and/or `LIMIT`
sub-clauses. For example, to get the oldest person you can use the following.

```opencypher
MATCH (n :Person) RETURN n ORDER BY n.age DESC LIMIT 1
```

##### Aggregating

openCypher has functions for aggregating data. Memgraph currently supports
the following aggregating functions.

  * `avg`, for calculating the average.
  * `collect`, for collecting multiple values into a single list or map. If
     given a single expression values are collected into a list. If given two
     expressions, values are collected into a map where the first expression
     denotes map keys (must be string values) and the second expression denotes
     map values.
  * `count`, for counting the resulting values.
  * `max`, for calculating the maximum result.
  * `min`, for calculating the minimum result.
  * `sum`, for getting the sum of numeric results.

Example, calculating the average age:

```opencypher
MATCH (n :Person) RETURN avg(n.age) AS averageAge
```

Collecting items into a list:

```opencypher
MATCH (n :Person) RETURN collect(n.name) AS list_of_names
```

Collecting items into a map:

```opencypher
MATCH (n :Person) RETURN collect(n.name, n.age) AS map_name_to_age
```

Click
[here](https://neo4j.com/docs/developer-manual/current/cypher/functions/aggregating/)
for additional details on how aggregations work.

### UNION and UNION ALL

openCypher supports combining results from multiple queries into a single result
set. That result will contain rows that belong to queries in the union
respecting the union type.

Using `UNION` will contain only distinct rows while `UNION ALL` will keep all
rows from all given queries.

Restrictions when using `UNION` or `UNION ALL`:
  * The number and the names of columns returned by queries must be the same
    for all of them.
  * There can be only one union type between single queries, i.e. a query can't
    contain both `UNION` and `UNION ALL`.

Example, get distinct names that are shared between persons and movies:

```opencypher
MATCH(n: Person) RETURN n.name AS name UNION MATCH(n: Movie) RETURN n.name AS name
```

Example, get all names that are shared between persons and movies (including duplicates):

```opencypher
MATCH(n: Person) RETURN n.name AS name UNION ALL MATCH(n: Movie) RETURN n.name AS name
