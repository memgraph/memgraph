## Indexing {#indexing-concept}

### Introduction

A database index is a data structure used to improve the speed of data retrieval
within a database at the cost of additional writes and storage space for
maintaining the index data structure.

Armed with deep understanding of their data model and use-case, users can decide
which data to index and, by doing so, significantly improve their data retrieval
efficiency

### Index Types

At Memgraph, we support two types of indexes:

  * label index
  * label-property index

Label indexing is enabled by default in Memgraph, i.e., Memgraph automatically
indexes labeled data. By doing so we optimize queries which fetch nodes by
label:

```opencypher
MATCH (n: Label) ... RETURN n
```

Indexes can also be created on data with a specific combination of label and
property, hence the name label-property index. This operation needs to be
specified by the user and should be used with a specific data model and
use-case in mind.

For example, suppose we are storing information about certain people in our
database and we are often interested in retrieving their age. In that case,
it might be beneficial to create an index on nodes labeled as `:Person` which
have a property named `age`. We can do so by using the following language
construct:

```opencypher
CREATE INDEX ON :Person(age)
```

After the creation of that index, those queries will be more efficient due to
the fact that Memgraph's query engine will not have to fetch each `:Person` node
and check whether the property exists. Moreover, even if all nodes labeled as
`:Person` had an `age` property, creating such index might still prove to be
beneficial. The main reason is that entries within that index are kept sorted
by property value. Queries such as the following are therefore more efficient:

```opencypher
MATCH (n :Person {age: 42}) RETURN n
```

Index based retrieval can also be invoked on queries with `WHERE` statements.
For instance, the following query will have the same effect as the previous
one:

```opencypher
MATCH (n) WHERE n:Person AND n.age = 42 RETURN n
```

Naturally, indexes will also be used when filtering based on less than or
greater than comparisons. For example, filtering all minors (persons
under 18 years of age under Croatian law) using the following query will use
index based retrieval:

```opencypher
MATCH (n) WHERE n:PERSON and n.age < 18 RETURN n
```

Bear in mind that `WHERE` filters could contain arbitrarily complex expressions
and index based retrieval might not be used. Nevertheless, we are continually
improving our index usage recognition algorithms.

### Underlying Implementation

The central part of our index data structure is a highly-concurrent skip list.
Skip lists are probabilistic data structures that allow fast search within an
ordered sequence of elements. The structure itself is built in layers where the
bottom layer is an ordinary linked list that preserves the order. Each higher
level can be imagined as a highway for layers below.

The implementation details behind skip list operations are well documented
in the literature and are out of scope for this article. Nevertheless, we
believe that it is important for more advanced users to understand the following
implications of this data structure (`n` denotes the current number of elements
in a skip list):

  * Average insertion time is `O(log(n))`
  * Average deletion time is `O(log(n))`
  * Average search time is `O(log(n))`
  * Average memory consumption is `O(n)`

### Index Commands

  * [CREATE INDEX ON](reference__create_index.md)
