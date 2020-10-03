# DatabaseAccessor (Obsolete)

A `DatabaseAccessor` actually wraps a transactional access to database
data, for a single transaction. In that sense the naming is bad. It
encapsulates references to the database and the transaction object.

It contains logic for working with database content (graph element
data) in the context of a single transaction. All CRUD operations are
performed within a single transaction (as Memgraph is a transactional
database), and therefore iteration over data, finding a specific graph
element etc are all functionalities of a `GraphDbAccessor`.

In single-node Memgraph the database accessor also defined the lifetime
of a transaction. Even though a `Transaction` object was owned by the
transactional engine, it was `GraphDbAccessor`'s lifetime that object
was bound to (the transaction was implicitly aborted in
`GraphDbAccessor`'s destructor, if it was not explicitly ended before
that).

# RecordAccessor

It is important to understand data organization and access in the
storage layer. This discussion pertains to vertices and edges as graph
elements that the end client works with.

Memgraph uses MVCC (documented on it's own page). This means that for
each graph element there could be different versions visible to
different currently executing transactions. When we talk about a
`Vertex` or `Edge` as a data structure we typically mean one of those
versions. In code this semantic is implemented so that both those classes
inherit `mvcc::Record`, which in turn inherits `mvcc::Version`.

Handling MVCC and visibility is not in itself trivial. Next to that,
there is other book-keeping to be performed when working with data. For
that reason, Memgraph uses "accessors" to define an API of working with
data in a safe way. Most of the code in Memgraph (for example the
interpretation code) should work with accessors. There is a
`RecordAccessor` as a base class for `VertexAccessor` and
`EdgeAccessor`. Following is an enumeration of their purpose.

### Data access

The client interacts with Memgraph using the Cypher query language. That
language has certain semantics which imply that multiple versions of the
data need to be visible during the execution of a single query. For
example: expansion over the graph is always done over the graph state as
it was at the beginning of the transaction.

The `RecordAccessor` exposes functions to switch between the old and the new
versions of the same graph element (intelligently named `SwitchOld` and
`SwitchNew`) within a single transaction. In that way the client code
(mostly the interpreter) can avoid dealing with the underlying MVCC
version concepts.

### Updates

Data updates are also done through accessors. Meaning: there are methods
on the accessors that modify data, the client code should almost never
interact directly with `Vertex` or `Edge` objects.

The accessor layer takes care of creating version in the MVCC layer and
performing updates on appropriate versions.

Next, for many kinds of updates it is necessary to update the relevant
indexes. There are implicit indexes for vertex labels, as
well as user-created indexes for (label, property) pairs. The accessor
layer takes care of updating the indexes when these values are changed.

Each update also triggers a log statement in the write-ahead log. This
is also handled by the accessor layer.

### Distributed

In distributed Memgraph accessors also contain a lot of the remote graph
element handling logic. More info on that is available in the
documentation for distributed.

### Deferred MVCC data lookup for Edges

Vertices and edges are versioned using MVCC. This means that for each
transaction an MVCC lookup needs to be done to determine which version
is visible to that transaction. This tends to slow things down due to
cache invalidations (version lists and versions are stored in arbitrary
locations on the heap).

However, for edges, only the properties are mutable. The edge endpoints
and type are fixed once the edge is created. For that reason both edge
endpoints and type are available in vertex data, so that when expanding
it is not mandatory to do MVCC lookups of versioned, mutable data. This
logic is implemented in `RecordAccessor` and `EdgeAccessor`.

### Exposure

The original idea and implementation of graph element accessors was that
they'd prevent client code from ever interacting with raw `Vertex` or
`Edge` data. This however turned out to be impractical when implementing
distributed Memgraph and the raw data members have since been exposed
(through getters to old and new version pointers). However, refrain from
working with that data directly whenever possible! Always consider the
accessors to be the first go-to for interacting with data, especially
when in the context of a transaction.

# Skiplist accessor

The term "accessor" is also used in the context of a skiplist. Every
operation on a skiplist must be performed within on an
accessor. The skiplist ensures that there will be no physical deletions
of an object during the lifetime of an accessor. This mechanism is used
to ensure deletion correctness in a highly concurrent container.
We only mention that here to avoid confusion regarding terminology.
