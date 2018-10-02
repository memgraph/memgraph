## Differences

Although we try to implement openCypher query language as closely to the
language reference as possible, we had to make some changes to enhance the
user experience.

### Symbolic Names

We don't allow symbolic names (variables, label names...) to be openCypher
keywords (WHERE, MATCH, COUNT, SUM...).

### Unicode Codepoints in String Literal

Use `\u` followed by 4 hex digits in string literal for UTF-16 codepoint and
`\U` with 8 hex digits for UTF-32 codepoint in Memgraph.


### Difference from Neo4j's Cypher Implementation

The openCypher initiative stems from Neo4j's Cypher query language. Following is a list
of most important differences between Neo's Cypher and Memgraph's openCypher implementation,
for users that are already familiar with Neo4j. There might be other differences not documented
here (especially subtle semantic ones).

#### Unsupported Constructs

* Data importing. Memgraph doesn't support Cypher's CSV importing capabilities.
* The `FOREACH` language construct for performing an operation on every list element.
* The `CALL` construct for a standalone function call. This can be expressed using
  `RETURN functioncall()`. For example, with Memgraph you can get information about
  the indexes present in the database using the `RETURN indexinfo()` openCypher query.
* Stored procedures.
* Regular expressions for string matching.
* `shortestPath` and `allShortestPaths` functions. `shortestPath` can be expressed using
  Memgraph's breadth-first expansion syntax already described in this document.
* Patterns in expressions. For example, Memgraph doesn't support `size((n)-->())`. Most of the time
  the same functionalities can be expressed differently in Memgraph using `OPTIONAL` expansions,
  function calls etc.
* Map projections such as `MATCH (n) RETURN n {.property1, .property2}`.

#### Unsupported Functions

General purpose functions:

* `exists(n.property)` - This can be expressed using `n.property IS NOT NULL`.
* `length()` is named `size()` in Memgraph.

Aggregation functions:

* `count(DISTINCT variable)` - This can be expressed using `WITH DISTINCT variable RETURN count(variable)`.

Mathematical functions:

* `percentileDisc()`
* `stDev()`
* `point()`
* `distance()`
* `degrees()`

List functions:

* `any()`
* `none()`
