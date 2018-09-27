## Other Features

The following sections describe some of the other supported features.

### UNWIND

The `UNWIND` clause is used to unwind a list of values as individual rows.

Example. Produce rows out of a single list.

```opencypher
UNWIND [1,2,3] AS listElement RETURN listElement
```

More examples are
[here](https://neo4j.com/docs/developer-manual/current/cypher/clauses/unwind/).

### Functions

This section contains the list of other supported functions.

 Name            | Description
-----------------|------------
 `coalesce`      | Returns the first non null argument.
 `startNode`     | Returns the starting node of an edge.
 `endNode`       | Returns the destination node of an edge.
 `degree`        | Returns the number of edges (both incoming and outgoing) of a node.
 `head`          | Returns the first element of a list.
 `last`          | Returns the last element of a list.
 `properties`    | Returns the properties of a node or an edge.
 `size`          | Returns the number of elements in a list or a map. When given a string it returns the number of characters. When given a path it returns the number of expansions (edges) in that path.
 `toBoolean`     | Converts the argument to a boolean.
 `toFloat`       | Converts the argument to a floating point number.
 `toInteger`     | Converts the argument to an integer.
 `type`          | Returns the type of an edge as a character string.
 `keys`          | Returns a list keys of properties from an edge or a node. Each key is represented as a string of characters.
 `labels`        | Returns a list of labels from a node. Each label is represented as a character string.
 `nodes`         | Returns a list of nodes from a path.
 `relationships` | Returns a list of relationships (edges) from a path.
 `range`         | Constructs a list of value in given range.
 `tail`          | Returns all elements after the first of a given list.
 `abs`           | Returns the absolute value of a number.
 `ceil`          | Returns the smallest integer greater than or equal to given number.
 `floor`         | Returns the largest integer smaller than or equal to given number.
 `round`         | Returns the number, rounded to the nearest integer. Tie-breaking is done using the *commercial rounding*,  where -1.5 produces -2 and 1.5 produces 2.
 `exp`           | Calculates `e^n` where `e` is the base of the natural logarithm, and `n` is the given number.
 `log`           | Calculates the natural logarithm of a given number.
 `log10`         | Calculates the logarithm (base 10) of a given number.
 `sqrt`          | Calculates the square root of a given number.
 `acos`          | Calculates the arccosine of a given number.
 `asin`          | Calculates the arcsine of a given number.
 `atan`          | Calculates the arctangent of a given number.
 `atan2`         | Calculates the arctangent2 of a given number.
 `cos`           | Calculates the cosine of a given number.
 `sin`           | Calculates the sine of a given number.
 `tan`           | Calculates the tangent of a given number.
 `sign`          | Applies the signum function to a given number and returns the result. The signum of positive numbers is 1, of negative -1 and for 0 returns 0.
 `e`             | Returns the base of the natural logarithm.
 `pi`            | Returns the constant *pi*.
 `rand`          | Returns a random floating point number between 0 (inclusive) and 1 (exclusive).
 `startsWith`    | Check if the first argument starts with the second.
 `endsWith`      | Check if the first argument ends with the second.
 `contains`      | Check if the first argument has an element which is equal to the second argument.
 `left`          | Returns a string containing the specified number of leftmost characters of the original string.
 `lTrim`         | Returns the original string with leading whitespace removed.
 `replace`       | Returns a string in which all occurrences of a specified string in the original string have been replaced by another (specified) string.
 `reverse`       | Returns a string in which the order of all characters in the original string have been reversed.
 `right`         | Returns a string containing the specified number of rightmost characters of the original string.
 `rTrim`         | Returns the original string with trailing whitespace removed.
 `split`         | Returns a list of strings resulting from the splitting of the original string around matches of the given delimiter.
 `substring`     | Returns a substring of the original string, beginning with a 0-based index start and length.
 `toLower`       | Returns the original string in lowercase.
 `toString`      | Converts an integer, float or boolean value to a string.
 `toUpper`       | Returns the original string in uppercase.
 `trim`          | Returns the original string with leading and trailing whitespace removed.
 `all`           | Check if all elements of a list satisfy a predicate.<br/>The syntax is: `all(variable IN list WHERE predicate)`.<br/> NOTE: Whenever possible, use Memgraph's lambda functions when matching instead.
 `single`        | Check if only one element of a list satisfies a predicate.<br/>The syntax is: `single(variable IN list WHERE predicate)`.
 `reduce`        | Accumulate list elements into a single result by applying an expression. The syntax is:<br/>`reduce(accumulator = initial_value, variable IN list | expression)`.
 `extract`       | A list of values obtained by evaluating an expression for each element in list. The syntax is:<br>`extract(variable IN list | expression)`.
 `assert`        | Raises an exception reported to the client if the given argument is not `true`.
 `counter`       | Generates integers that are guaranteed to be unique on the database level, for the given counter name.
 `counterSet`    | Sets the counter with the given name to the given value.
 `indexInfo`     | Returns a list of all the indexes available in the database. The list includes indexes that are not yet ready for use (they are concurrently being built by another transaction).
 `timestamp`     | Returns the difference, measured in milliseconds, between the current time and midnight, January 1, 1970 UTC.
 `id`            | Returns identifier for a given node or edge. The identifier is generated during the initialization of node or edge and will be persisted through the durability mechanism.

### String Operators

Apart from comparison and concatenation operators openCypher provides special
string operators for easier matching of substrings:

Operator           | Description
-------------------|------------
 `a STARTS WITH b` | Returns true if prefix of string a is equal to string b.
 `a ENDS WITH b`   | Returns true if suffix of string a is equal to string b.
 `a CONTAINS b`    | Returns true if some substring of string a is equal to string b.

### Parameters

When automating the queries for Memgraph, it comes in handy to change only
some parts of the query. Usually, these parts are values which are used for
filtering results or similar, while the rest of the query remains the same.

Parameters allow reusing the same query, but with different parameter values.
The syntax uses the `$` symbol to designate a parameter name. We don't allow
old Cypher parameter syntax using curly braces. For example, you can parameterize
filtering a node property:

```opencypher
MATCH (node1 {property: $propertyValue}) RETURN node1
```

You can use parameters instead of any literal in the query, but not instead of
property maps even though that is allowed in standard openCypher. Following
example is illegal in Memgraph:

```opencypher
MATCH (node1 $propertyValue) RETURN node1
```

To use parameters with Python driver use following syntax:

```python
session.run('CREATE (alice:Person {name: $name, age: $ageValue}',
            name='Alice', ageValue=22)).consume()
```

To use parameters which names are integers you will need to wrap parameters in
a dictionary and convert them to strings before running a query:

```python
session.run('CREATE (alice:Person {name: $0, age: $1}',
            {'0': "Alice", '1': 22})).consume()
```

To use parameters with some other driver please consult appropriate
documentation.

### CASE

Conditional expressions can be expressed in openCypher language by simple and
generic form of `CASE` expression. A simple form is used to compare an expression
against multiple predicates. For the first matched predicate result of the
expression provided after the `THEN` keyword is returned.  If no expression is
matched value following `ELSE` is returned is provided, or `null` if `ELSE` is not
used:

```opencypher
MATCH (n)
RETURN CASE n.currency WHEN "DOLLAR" THEN "$" WHEN "EURO" THEN "€" ELSE "UNKNOWN" END
```

In generic form, you don't need to provide an expression whose value is compared to
predicates, but you can list multiple predicates and the first one that evaluates
to true is matched:

```opencypher
MATCH (n)
RETURN CASE WHEN n.height < 30 THEN "short" WHEN n.height > 300 THEN "tall" END
```
