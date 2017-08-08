## Storable Data Types

Since *Memgraph* is a *graph* database management system, data is stored in
the form of graph elements: nodes and edges. Each graph element can also
contain various types of data. This chapter describes which data types are
supported in *Memgraph*.

### Node Labels & Edge Types

Each node can have any number of labels. A label is a text value, which can be
used to *label* or group nodes according to users' desires. A user can change
labels at any time. Similarly to labels, each edge can have a type,
represented as text. Unlike nodes, which can have multiple labels or none at
all, edges *must* have exactly one edge type. Another difference to labels, is
that the edge types are set upon creation and never modified again.

### Properties

Nodes and edges can store various properties. These are like mappings or
tables containing property names and their accompanying values. Property names
are represented as text, while values can be of different types. Each property
name can store a single value, it is not possible to have multiple properties
with the same name on a single graph element. Naturally, the same property
names can be found across multiple graph elements. Also, there are no
restrictions on the number of properties that can be stored in a single graph
element. The only restriction is that the values must be of the supported
types. Following is a table of supported data types.

 Type      | Description
-----------|------------
 `Null`    | Denotes that the property has no value. This is the same as if the property does not exist.
 `String`  | A character string, i.e. text.
 `Boolean` | A Boolean value, either `true` or `false`.
 `Integer` | An integer number.
 `Float`   | A floating-point number, i.e. a real number.
 `List`    | A list containing any number of property values of any supported type. It can be used to store multiple values under a single property name.

Note that even though map literals are supported in openCypher queries, they can't be stored in the graph. For example, the following query is legal:

     MATCH (n:Person) RETURN {name: n.name, age: n.age}

However, the next query is not:

     MATCH (n:Person {name: "John"}) SET n.address = {city: "London", street: "Fleet St."}
