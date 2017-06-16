## openCypher Query Language

[*openCypher*](http://www.opencypher.org/) is a query language for querying
graph databases. It aims to be intuitive and easy to learn, while
providing a powerful interface for working with graph based data.

*Memgraph* supports most of the commonly used constructs of the language. This
chapter contains the details of features which are implemented. Additionally,
not yet supported features of the language are listed.

  * [Reading existing Data](#reading-existing-data)
  * [Writing new Data](#writing-new-data)
  * [Reading & Writing](#reading-amp-writing)
  * [Other Features](#other-features)

### Reading existing Data

The simplest usage of the language is to find data stored in the
database. For that purpose, the following clauses are offered:

  * `MATCH`, which searches for patterns;
  * `WHERE`, for filtering the matched data and
  * `RETURN`, for defining what will be presented to the user in the result
    set.

#### MATCH

This clause is used to obtain data from Memgraph by matching it to a given
pattern. For example, to find each node in the database, you can use the
following query.

    MATCH (node) RETURN node

Finding connected nodes can be achieved by using the query:

    MATCH (node1) -[connection]- (node2) RETURN node1, connection, node2

In addition to general pattern matching, you can narrow the search down by
specifying node labels and properties. Similarly, edge types and properties
can also be specified. For example, finding each node labeled as `Person` and
with property `age` being 42, is done with the following query.

    MATCH (n :Person {age: 42}) RETURN n.

While their friends can be found with the following.

    MATCH (n :Person {age: 42}) -[:FriendOf]- (friend) RETURN friend.

More details on how `MATCH` works can be found
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/match/).
Note that *variable length paths* and *named paths* are not yet supported.

The `MATCH` clause can be modified by prepending the `OPTIONAL` keyword.
`OPTIONAL MATCH` clause behaves the same as a regular `MATCH`, but when it
fails to find the pattern, missing parts of the pattern will be filled with
`null` values. Examples can be found
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/optional-match/).

#### WHERE

You have already seen that simple filtering can be achieved by using labels
and properties in `MATCH` patterns. When more complex filtering is desired,
you can use `WHERE` paired with `MATCH` or `OPTIONAL MATCH`. For example,
finding each person older than 20 is done with the this query.

    MATCH (n :Person) WHERE n.age > 20 RETURN n

Additional examples can be found
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/where/).

#### RETURN

The `RETURN` clause defines which data should be included in the resulting
set. Basic usage was already shown in the examples for `MATCH` and `WHERE`
clauses. Another feature of `RETURN` is renaming the results using the `AS`
keyword.

Example.

    MATCH (n :Person) RETURN n AS people

That query would display all nodes under the header named `people` instead of
`n`.

When you want to get everything that was matched, you can use the `*`
(*asterisk*) symbol.

This query:

    MATCH (node1) -[connection]- (node2) RETURN *

is equivalent to:

    MATCH (node1) -[connection]- (node2) RETURN node1, connection, node2

`RETURN` can be followed by the `DISTINCT` operator, which will remove
duplicate results. For example, getting unique names of people can be achieved
with:

    MATCH (n :Person) RETURN DISTINCT n.name

Besides choosing what will be the result and how it will be named, the
`RETURN` clause can also be used to:

  * limit results with `LIMIT` sub-clause;
  * skip results with `SKIP` sub-clause;
  * order results with `ORDER BY` sub-clause and
  * perform aggregations (such as `count`).

More details on `RETURN` can be found
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/return/).

##### SKIP & LIMIT

These sub-clauses take a number of how many results to skip or limit.
For example, to get the first 3 results you can use this query.

    MATCH (n :Person) RETURN n LIMIT 3

If you want to get all the results after the first 3, you can use the
following.

    MATCH (n :Person) RETURN n SKIP 3

The `SKIP` and `LIMIT` can be combined. So for example, to get the 2nd result,
you can do:

    MATCH (n :Person) RETURN n SKIP 1 LIMIT 1

##### ORDER BY

Since the patterns which are matched can come in any order, it is very useful
to be able to enforce some ordering among the results. In such cases, you can
use the `ORDER BY` sub-clause.

For example, the following query will get all `:Person` nodes and order them
by their names.

    MATCH (n :Person) RETURN n ORDER BY n.name

By default, ordering will be in the ascending order. To change the order to be
descending, you should append `DESC`.

For example, to order people by their name descending, you can use this query.

    MATCH (n :Person) RETURN n ORDER BY n.name DESC

You can also order by multiple variables. The results will be sorted by the
first variable listed. If the values are equal, the results are sorted by the
second variable, and so on.

Example. Ordering by first name descending and last name ascending.

    MATCH (n :Person) RETURN n ORDER BY n.name DESC, n.lastName

Note that `ORDER BY` sees only the variable names as carried over by `RETURN`.
This means that the following will result in an error.

    MATCH (n :Person) RETURN old AS new ORDER BY old.name

Instead, the `new` variable must be used:

    MATCH (n: Person) RETURN old AS new ORDER BY new.name

The `ORDER BY` sub-clause may come in handy with `SKIP` and/or `LIMIT`
sub-clauses. For example, to get the oldest person you can use the following.

    MATCH (n :Person) RETURN n ORDER BY n.age DESC LIMIT 1

##### Aggregating

openCypher has functions for aggregating data. Memgraph currently supports
the following aggregating functions.

  * `avg`, for calculating the average.
  * `collect`, for collecting multiple values into a single list.
  * `count`, for counting the resulting values.
  * `max`, for calculating the maximum result.
  * `min`, for calculating the minimum result.
  * `sum`, for getting the sum of numeric results.

Example, calculating the average age.

    MATCH (n :Person) RETURN avg(n.age) AS averageAge

Click
[here](https://neo4j.com/docs/developer-manual/current/cypher/functions/aggregating/)
for additional details on how aggregations work.

### Writing new Data

For adding new data, you can use the following clauses.

  * `CREATE`, for creating new nodes and edges.
  * `SET`, for adding new or updating existing labels and properties.
  * `DELETE`, for deleting nodes and edges.
  * `REMOVE`, for removing labels and properties.

You can still use the `RETURN` clause to produce results after writing, but it
is not mandatory.

#### CREATE

This clause is used to add new nodes and edges to the database. The creation
is done by providing a pattern, similarly to `MATCH` clause.

For example, to create 2 new nodes connected with a new edge, use this query.

    CREATE (node1) -[:edge_type]-> (node2)

Additional information on `CREATE` is
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/create/).

#### SET

The `SET` clause is used to update labels and properties of already existing
data.

Example. Incrementing everyone's age by 1.

    MATCH (n :Person) SET n.age = n.age + 1

Click
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/create/)
for a more detailed explanation on what can be done with `SET`.

#### DELETE

This clause is used to delete nodes and edges from the database.

Example. Removing all edges of a single type.

    MATCH () -[edge :type]- () DELETE edge

When testing the database, you want to often have a clean start by deleting
every node and edge in the database. It is reasonable that deleting each node
should delete all edges coming into or out of that node.

    MATCH (node) DELETE node

But, openCypher prevents accidental deletion of edges. Therefore, the above
query will report an error. Instead, you need to use the `DETACH` keyword,
which will remove edges from a node you are deleting. The following should
work and *delete everything* in the database.

    MATCH (node) DETACH DELETE node

More examples are
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/delete/).

#### REMOVE

The `REMOVE` clause is used to remove labels and properties from nodes and
edges.

Example.

    MATCH (n :WrongLabel) REMOVE n :WrongLabel, n.property

### Reading & Writing

OpenCypher supports combining multiple reads and writes using the
`WITH` clause. In addition to combining, the `MERGE` clause is provided which
may create patterns if they do not exist.

#### WITH

The write part of the query cannot be simply followed by another read part. In
order to combine them, `WITH` clause must be used. The names this clause
establishes are transferred from one part to another.

For example, creating a node and finding all nodes with the same property.

    CREATE (node {property: 42}) WITH node.property AS propValue
    MATCH (n {property: propValue}) RETURN n

Note that the `node` is not visible after `WITH`, since only `node.property`
was carried over.

This clause behaves very much like `RETURN`, so you should refer to features
of `RETURN`.

#### MERGE

The `MERGE` clause is used to ensure that a pattern you are looking for exists
in the database. This means that if the pattern is not found, it will be
created. In a way, this clause is like a combination of `MATCH` and `CREATE`.


Example. Ensure that a person has at least one friend.

    MATCH (n :Person) MERGE (n) -[:FriendOf]-> (m)

The clause also provides additional features for updating the values depending
on whether the pattern was created or matched. This is achieved with `ON
CREATE` and `ON MATCH` sub clauses.

Example. Set a different properties depending on what `MERGE` did.

    MATCH (n :Person) MERGE (n) -[:FriendOf]-> (m)
    ON CREATE SET m.prop = "created" ON MATCH SET m.prop = "existed"

For more details, click [this
link](https://neo4j.com/docs/developer-manual/current/cypher/clauses/merge/).


### Other Features

The following sections describe some of the other supported features.

#### UNWIND

The `UNWIND` clause is used to unwind a list of values as individual rows.

Example. Produce rows out of a single list.

    UNWIND [1,2,3] AS listElement RETURN listElement

More examples are
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/unwind/).

#### Functions

You have already been introduced to one type of functions, [aggregating
functions](#aggregating). This section contains the list of other supported
functions.

 Name         | Description
--------------|------------
 `coalesce`   | Returns the first non null argument.
 `startNode`  | Returns the starting node of an edge.
 `endNode`    | Returns the destination node of an edge.
 `head`       | Returns the first element of a list.
 `last`       | Returns the last element of a list.
 `properties` | Returns the properties of a node or an edge.
 `size`       | Returns the number of elements in a list.
 `toBoolean`  | Converts the argument to a boolean.
 `toFloat`    | Converts the argument to a floating point number.
 `toInteger`  | Converts the argument to an integer.
 `type`       | Returns the type of an edge as a character string.
 `keys`       | Returns a list keys of properties from an edge or a node. Each key is represented as a string of characters.
 `labels`     | Return a list of labels from a node. Each label is represented as a character string.
 `range`      | Constructs a list of value in given range.
 `tail`       | Returns all elements after the first of a given list.
 `abs`        | Returns the absolute value of a number.
 `ceil`       | Returns the smallest integer greater than or equal to given number.
 `floor`      | Returns the largest integer smaller than or equal to given number.
 `round`      | Returns the number, rounded to the nearest integer. Tie-breaking is done using the *commercial rounding*,  where -1.5 produces -2 and 1.5 produces 2.
 `exp`        | Calculates `e^n` where `e` is the base of the natural logarithm, and `n` is the given number.
 `log`        | Calculates the natural logarithm of a given number.
 `log10`      | Calculates the logarithm (base 10) of a given number.
 `sqrt`       | Calculates the square root of a given number.
 `acos`       | Calculates the arccosine of a given number.
 `asin`       | Calculates the arcsine of a given number.
 `atan`       | Calculates the arctangent of a given number.
 `atan2`      | Calculates the arctangent2 of a given number.
 `cos`        | Calculates the cosine of a given number.
 `sin`        | Calculates the sine of a given number.
 `tan`        | Calculates the tangent of a given number.
 `sign`       | Applies the signum function to a given number and returns the result. The signum of positive numbers is 1, of negative -1 and for 0 returns 0.
 `e`          | Returns the base of the natural logarithm.
 `pi`         | Returns the constant *pi*.
 `startsWith` | Check if the first argument starts with the second.
 `endsWith`   | Check if the first argument ends with the second.
 `contains`   | Check if the first argument has an element which is equal to the second argument.
