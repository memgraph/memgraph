## Durability and Data Recovery

*Memgraph* uses two mechanisms to ensure the durability of the stored data:

  * write-ahead logging (WAL) and
  * taking periodic snapshots.

Write-ahead logging works by logging all database modifications to a file.
This ensures that all operations are done atomically and provides a trace of
steps needed to reconstruct the database state.

Snapshots are taken periodically during the entire runtime of *Memgraph*. When
a snapshot is triggered, the whole data storage is written to disk. The
snapshot file provides a quicker way to restore the database state.

Database recovery is done on startup from the most recently found snapshot
file. Since the snapshot may be older than the most recent update logged in
the WAL file, the recovery process will apply the remaining state changes
found in the said WAL file.

NOTE: Snapshot and WAL files are not (currently) compatible between *Memgraph*
versions.

Behaviour of the above mechanisms can be tweaked in the configuration file,
usually found in `/etc/memgraph/memgraph.conf`.

In addition to the above mentioned data durability and recovery, a
snapshot file may be generated using *Memgraph's* import tools. For more
information, take a look at **Import Tools** chapter.

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
 `Boolean` | A boolean value, either `true` or `false`.
 `Integer` | An integer number.
 `Float`   | A floating-point number, i.e. a real number.
 `List`    | A list containing any number of property values of any supported type. It can be used to store multiple values under a single property name.
 `Map`     | A mapping of string keys to values of any supported type.

 Note that even though it's possible to store `List` and `Map` property values, it is not possible to modify them. It is however possible to replace them completely. So, the following queries are legal:

    CREATE (:Node {property: [1, 2, 3]})
    CREATE (:Node {property: {key: "value"}})

However, these queries are not:

    MATCH (n:Node) SET n.property[0] = 0
    MATCH (n:Node) SET n.property.key = "other value"
