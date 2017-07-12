## Upcoming Features

This chapter describes some of the planned features, that we at Memgraph are
working on.

### Performance Improvements

Excellent database performance is one of Memgraph's long-standing goals. We
will be continually working on improving the performance. This includes:

  * query compilation;
  * query execution;
  * core engine performance;
  * memory usage and
  * other improvements.

### Label-Property Index Usage Improvements

Currently, indexing combinations of labels and properties can be created, but
cannot be deleted. We plan to add a new query language construct which will
allow deletion of created indices.

### Improving openCypher Support

Although we have implemented the most common features of the openCypher query
language, there are other useful features we are still working on.

#### Map Literals

This feature would enable using ad-hoc property maps. For example, updating
multiple properties on an existing node:

    MATCH (node) SET node = { name: "Lamp", price: 100, color: "blue" }

Note that the same can be achieved with the current support but in a longer
way:

    MATCH (node) SET node.name = "Lamp", node.price = 100, node.color = "blue"

#### Named Paths

It would be useful to store paths that match a pattern into a variable. This
enables the user to display the matched patterns or do some other operations
on the path, like calculating the length of the path.

The feature would be used by simply assigning the variable to a pattern. For
example:

    MATCH path = (node1) -[connection]-> (node2)

Path naming is especially useful with another upcoming feature, *variable
length paths*.

#### Variable Length Paths

There are cases when a user needs to find data which is connected by
traversing a path of connections, but the user doesn't know how many
connections need to be traversed. openCypher allows for designating patterns
with variable path lengths. Matching such a path is achieved by using the `*`
(*asterisk*) symbol inside the pattern for a connection. For example,
traversing from `node1` to `node2` by following any number of connections in a
single direction can be achieved with:

    MATCH (node1) -[*]-> (node2)

If paths are very long, finding them could take a long time. To prevent that,
a user can provide the minimum and maximum length of the path. For example,
paths of length between 2 and 4 can be obtained with a query like:

    MATCH (node1) -[*2..4]-> (node2)

#### Parameters

When automating the queries for Memgraph, it comes in handy to change only
some parts of the query. Usually, these parts are values which are used for
filtering results or similar, while the rest of the query remains the same.

Parameters will allow using the same query, but with different parameter
values. The syntax uses the `$` symbol to designate a parameter name. For
example, parameterizing filtering a node property:

    MATCH (node1 {property: $propertyValue}) RETURN node1

Other than helping users reuse similar queries, parameters should improve the
performance of running those queries.

#### Functions

Memgraph's openCypher implementation supports the most useful functions, but
there are more which openCypher provides. Some are related to not yet
implemented features like paths, while some may use the features Memgraph
already supports. Out of the remaining functions, some are more useful than
others and as such they will be supported sooner.

#### UNION

The `UNION` clause will offer joining the results from multiple queries. For
example, finding names of `:Person` and `:Car` names.

    MATCH (p :Person) RETURN p.name AS name
    UNION
    MATCH (c :Car) RETURN c.name AS name

#### List Comprehensions

List comprehensions are similar to the supported `collect` function, which
generates a list out of multiple values. But unlike `collect`, list
comprehensions offer a powerful mechanism for filtering or otherwise
manipulating values which are collected into a list.

For example, getting numbers between 0 and 10 and squaring them:

    RETURN [x IN range(0, 10) | x^2] AS squares

Another example, to collect `:Person` nodes with `age` less than 42, without
list comprehensions can be achieved with:

    MATCH (n :Person) WHERE n.age < 42 RETURN collect(n)

Using list comprehensions, the same can be done with the query:

    MATCH (n :Person) RETURN [n IN collect(n) WHERE n.age < 42]

