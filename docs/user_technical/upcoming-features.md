## Upcoming Features

This chapter describes some of the planned features, that we at Memgraph are
working on.

### Performance Improvements

Excellent database performance is one of Memgraph's long-standing goals. We
will be continually working on improving the performance. This includes:

  * query compilation;
  * query execution;
  * core engine performance;
  * algorithmic improvements (i.e. bidirectional breadth-first search);
  * memory usage and
  * other improvements.

### Label-Property Index Usage Improvements

Currently, indexing combinations of labels and properties can be created, but
cannot be deleted. We plan to add a new query language construct which will
allow deletion of created indices.

### Improving openCypher Support

Although we have implemented the most common features of the openCypher query
language, there are other useful features we are still working on.

#### Functions

Memgraph's openCypher implementation supports the most useful functions, but
there are more which openCypher provides. Some are related to not yet
implemented features like paths, while some may use the features Memgraph
already supports. Out of the remaining functions, some are more useful than
others and as such they will be supported sooner.

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

