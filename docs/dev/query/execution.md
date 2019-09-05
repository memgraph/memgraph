# Logical Plan Execution

We implement classical iterator style operators. Logical operators define
operations on database. They encapsulate the following info: what the input is
(another `LogicalOperator`), what to do with the data, and how to do it.

Currently logical operators can have zero or more input operations, and thus a
`LogicalOperator` tree is formed. Most `LogicalOperator` types have only one
input, so we are mostly working with chains instead of full fledged trees.
You can find information on each operator in `src/query/plan/operator.lcp`.

## Cursor

Logical operators do not perform database work themselves. Instead they create
`Cursor` objects that do the actual work, based on the info in the operator.
Cursors expose a `Pull` method that gets called by the cursor's consumer. The
consumer keeps pulling as long as the `Pull` returns `true` (indicating it
successfully performed some work and might be eligible for another `Pull`).
Most cursors will call the `Pull` function of their input provided cursor, so
typically a cursor chain is created that is analogue to the logical operator
chain it's created from.

## Frame

The `Frame` object contains all the data of the current `Pull` chain. It
serves for communicating data between cursors.

For example, in a `MATCH (n) RETURN n` query the `ScanAllCursor` places a
vertex on the `Frame` for each `Pull`. It places it on the place reserved for
the `n` symbol. Then the `ProduceCursor` can take that same value from the
`Frame` because it knows the appropriate symbol. `Frame` positions are indexed
by `Symbol` objects.

## ExpressionEvaluator

Expressions results are not placed on the `Frame` since they do not need to be
communicated between different `Cursors`. Instead, expressions are evaluated
using an instance of `ExpressionEvaluator`. Since generally speaking an
expression can be defined by a tree of subexpressions, the
`ExpressionEvaluator` is implemented as a tree visitor. There is a performance
sub-optimality here because a stack is used to communicate intermediary
expression results between elements of the tree. This is one of the reasons
why it's planned to use `Frame` for intermediary expression results as well.
The other reason is that it might facilitate compilation later on.

## Cypher Execution Semantics

Cypher query execution has *mostly* well-defined semantics. Some are
explicitly defined by openCypher and its TCK, while others are implicitly
defined by Neo4j's implementation of Cypher that we want to be generally
compatible with.

These semantics can in short be described as follows: a Cypher query consists
of multiple clauses some of which modify it. Generally, every clause in the
query, when reading it left to right, operates on a consistent state of the
property graph, untouched by subsequent clauses. This means that a `MATCH`
clause in the beginning operates on a graph-state in which modifications by
the subsequent `SET` are not visible.

The stated semantics feel very natural to the end-user, and Neo seems to
implement them well. For Memgraph the situation is complex because
`LogicalOperator` execution (through a `Cursor`) happens one `Pull` at a time
(generally meaning all the query clauses get executed for every top-level
`Pull`). This is not inherently consistent with Cypher semantics because a
`SET` clause can modify data, and the `MATCH` clause that precedes it might
see the modification in a subsequent `Pull`. Also, the `RETURN` clause might
want to stream results to the user before all `SET` clauses have been
executed, so the user might see some intermediate graph state. There are many
edge-cases that Memgraph does its best to avoid to stay true to Cypher
semantics, while at the same time using a high-performance streaming approach.
The edge-cases are enumerated in this document along with the implementation
details they imply.

## Implementation Peculiarities

### Once

An operator that does nothing but whose `Cursor::Pull` returns `true` on the
first `Pull` and `false` on subsequent ones. This operator is used when
another operator has an optional input, because in Cypher a clause will
typically execute once for every input from the preceding clauses, or just
once if there was no preceding input. For example, consider the `CREATE`
clause. In the query `CREATE (n)` only one node is created, while in the query
`MATCH (n) CREATE (m)` a node is created for each existing node. Thus in our
`CreateNode` logical operator the input is either a `ScanAll` operator, or a
`Once` operator.

### storage::View

In the previous section, [Cypher Execution
Semantics](#cypher-execution-semantics), we mentioned how the preceding
clauses should not see changes made in subsequent ones. For that reason, some
operators take a `storage::View` enum value. This value determines which state of
the graph an operator sees.

Consider the query `MATCH (n)--(m) WHERE n.x = 0 SET m.x = 1`. Naive streaming
could match a vertex `n` on the given criteria, expand to `m`, update it's
property, and in the next iteration consider the vertex previously matched to
`m` and skip it because it's newly set property value does not qualify. This
is not how Cypher works. To handle this issue properly, Memgraph designed the
`VertexAccessor` class that tracks two versions of data: one that was visible
before the current transaction+command, and the optional other that was
created in the current transaction+command. The `MATCH` clause will be planned
as `ScanAll` and `Expand` operations using `storage::View::OLD` value. This
will ensure modifications performed in the same query do not affect it. The
same applies to edges and the `EdgeAccessor` class.

### Existing Record Detection

It's possible that a pattern element has already been declared in the same
pattern, or a preceding pattern. For example `MATCH (n)--(m), (n)--(l)` or a
cycle-detection match `MATCH (n)-->(n) RETURN n`. Implementation-wise,
existing record detection just checks that the expanded record is equal to the
one already on the frame.

### Why Not Use Separate Expansion Ops for Edges and Vertices?

Expanding an edge and a vertex in separate ops is not feasible when matching a
cycle in bi-directional expansions. Consider the query `MATCH (n)--(n) RETURN
n`. Let's try to expand first the edge in one op, and vertex in the next. The
vertex expansion consumes the edge expansion input. It takes the expanded edge
from the frame. It needs to detect a cycle by comparing the vertex existing on
the frame with one of the edge vertices (`from` or `to`). But which one? It
doesn't know, and can't ensure correct cycle detection.

### Data Visibility During and After SET

In Cypher, setting values always works on the latest version of data (from
preceding or current clause). That means that within a `SET` clause all the
changes from previous clauses must be visible, as well as changes done by the
current `SET` clause. Also, if there is a clause after `SET` it must see *all*
the changes performed by the preceding `SET`. Both these things are best
illustrated with the following queries executed on an empty database:

    CREATE (n:A {x:0})-[:EdgeType]->(m:B {x:0})
    MATCH (n)--(m) SET m.x = n.x + 1 RETURN labels(n), n.x, labels(m), m.x

This returns:

+---------+---+---------+---+
|labels(n)|n.x|labels(m)|m.x|
+:=======:+:=:+:=======:+:=:+
|[A]      |2  |[B]      |1  |
+---------+---+---------+---+
|[B]      |1  |[A]      |2  |
+---------+---+---------+---+

The obtained result implies the following operations:

  1. In the first iteration set the value of the `B.x` to 1
  2. In the second iteration the we observe `B.x` with the value of 1 and set
     `A.x` to 2
  3. In `RETURN` we see all the changes made in both iterations

To implement the desired behavior Memgraph utilizes two techniques. First is
the already mentioned tracking of two versions of data in vertex accessors.
Using this approach ensures that the second iteration in the example query
sees the data modification performed by the preceding iteration. The second
technique is the `Accumulate` operation that accumulates all the iterations
from the preceding logical op before passing them to the next logical op. In
the example query, `Accumulate` ensures that the results returned to the user
reflect changes performed in all iterations of the query (naive streaming
could stream results at the end of first iteration producing inconsistent
results). Note that `Accumulate` is demanding regarding memory and slows down
query execution. For that reason it should be used only when necessary, for
example it does not have to be used in a query that has `MATCH` and `SET` but
no `RETURN`.

### Neo4j Inconsistency on Multiple SET Clauses

Considering the preceding example it could be expected that when a query has
multiple `SET` clauses all the changes from those preceding one are visible.
This is not the case in Neo4j's implementation. Consider the following queries
executed on an empty database:

    CREATE (n:A {x:0})-[:EdgeType]->(m:B {x:0})
    MATCH (n)--(m) SET n.x = n.x + 1 SET m.x = m.x * 2
    RETURN labels(n), n.x, labels(m), m.x

This returns:

+---------+---+---------+---+
|labels(n)|n.x|labels(m)|m.x|
+:=======:+:=:+:=======:+:=:+
|[A]      |2  |[B]      |1  |
+---------+---+---------+---+
|[B]      |1  |[A]      |2  |
+---------+---+---------+---+

If all the iterations of the first `SET` clause were executed before executing
the second, all the resulting values would be 2. This not being the case, we
conclude that Neo4j does not use a barrier-like mechanism between `SET`
clauses.  It is Memgraph's current vision that this is inconsistent and we
plan to reduce Neo4j compliance in favour of operation consistency.

### Double Deletion

It's possible to match the same graph element multiple times in a single query
and delete it. Neo supports this, and so do we. The relevant implementation
detail is in the `GraphDbAccessor` class, where the record deletion functions
reside, and not in the logical plan execution. It comes down to checking if a
record has already been deleted in the current transaction+command and not
attempting to do it again (results in a crash).

### Set + Delete Edge-case

It's legal for a query to combine `SET` and `DELETE` clauses. Consider the
following queries executed on an empty database:


    CREATE ()-[:T]->()
    MATCH (n)--(m) SET n.x = 42 DETACH DELETE m

Due to the `MATCH` being undirected the second pull will attempt to set data
on a deleted vertex. This is not a legal operation in Memgraph storage
implementation. For that reason the logical operator for `SET` must check if
the record it's trying to set something on has been deleted by the current
transaction+command. If so, the modification is not executed.

### Deletion Accumulation

Sometimes it's necessary to accumulate deletions of all the matches before
attempting to execute them. Consider this the following. Start with an empty
database and execute queries:

    CREATE ()-[:T]->()-[:T]->()
    MATCH (a)-[r1]-(b)-[r2]-(c) DELETE r1, b, c

Note that the `DELETE` clause attempts to delete node `c`, but it does not
detach it by deleting edge `r2`. However, due to undirected edge in the
`MATCH`, both edges get pulled and deleted.

Currently Memgraph does not support this behavior, Neo does. There are a few
ways that we could do this.

 * Accumulate on deletion (that sucks because we have to keep track of
   everything that gets returned after the deletion).
 * Maybe we could stream through the deletion op, but defer actual deletion
   until plan-execution end.
 * Ignore this because it's very edgy (this is the currently selected option).

### Aggregation Without Input

It is necessary to define what aggregation ops return when they receive no
input. Following is a table that shows what Neo4j's Cypher implementation and
SQL produce.


+-------------+------------------------+---------------------+---------------------+------------------+
|    \<OP\>   | 1. Cypher, no group-by | 2. Cypher, group-by | 3. SQL, no group-by | 4. SQL, group-by |
+=============+:======================:+:===================:+:===================:+:================:+
| Count(\*)   | 0                      | \<NO\_ROWS>         | 0                   | \<NO\_ROWS>      |
+-------------+------------------------+---------------------+---------------------+------------------+
| Count(prop) | 0                      | \<NO\_ROWS>         | 0                   | \<NO\_ROWS>      |
+-------------+------------------------+---------------------+---------------------+------------------+
| Sum         | 0                      | \<NO\_ROWS>         | NULL                | \<NO\_ROWS>      |
+-------------+------------------------+---------------------+---------------------+------------------+
| Avg         | NULL                   | \<NO\_ROWS>         | NULL                | \<NO\_ROWS>      |
+-------------+------------------------+---------------------+---------------------+------------------+
| Min         | NULL                   | \<NO\_ROWS>         | NULL                | \<NO\_ROWS>      |
+-------------+------------------------+---------------------+---------------------+------------------+
| Max         | NULL                   | \<NO\_ROWS>         | NULL                | \<NO\_ROWS>      |
+-------------+------------------------+---------------------+---------------------+------------------+
| Collect     | []                     | \<NO\_ROWS>         | N/A                 | N/A              |
+-------------+------------------------+---------------------+---------------------+------------------+

Where:

    1. `MATCH (n) RETURN <OP>(n.prop)`
    2. `MATCH (n) RETURN <OP>(n.prop), (n.prop2)`
    3. `SELECT <OP>(prop) FROM Table`
    4. `SELECT <OP>(prop), prop2 FROM Table GROUP BY prop2`

Neo's Cypher implementation diverges from SQL only when performing `SUM`.
Memgraph implements SQL-like behavior. It is considered that `SUM` of
arbitrary elements should not be implicitly 0, especially in a property graph
without a strict schema (the property in question can contain values of
arbitrary types, or no values at all).

### OrderBy

The `OrderBy` logical operator sorts the results in the desired order. It
occurs in Cypher as part of a `WITH` or `RETURN` clause. Both the concept and
the implementation are straightforward. It's necessary for the logical op to
`Pull` everything from its input so it can be sorted. It's not necessary to
keep the whole `Frame` state of each input, it is sufficient to keep a list of
`TypedValues` on which the results will be sorted, and another list of values
that need to be remembered and recreated on the `Frame` when yielding.

The sorting itself is made to reflect that of Neo's implementation which comes
down to these points.

  * `Null` comes last (as if it's greater than anything).
  * Primitive types compare naturally, with no implicit casting except from
    `int` to `double`.
  * Complex types are not comparable.
  * Every unsupported comparison results in an exception that gets propagated
    to the end user.

### Limit in Write Queries

`Limit` can be used as part of a write query, in which case it will *not*
reduce the amount of performed updates. For example, consider a database that
has 10 vertices. The query `MATCH (n) SET n.x = 1 RETURN n LIMIT 3` will
result in all vertices having their property value changed, while returning
only the first to the client. This makes sense from the implementation
standpoint, because `Accumulate` is planned after `SetProperty` but before
`Produce` and `Limit` operations. Note that this behavior can be
non-deterministic in some queries, since it relies on the order of iteration
over nodes which is undefined when not explicitly specified.

### Merge

`MERGE` in Cypher attempts to match a pattern. If it already exists, it does
nothing and subsequent clauses like `RETURN` can use the matched pattern
elements. If the pattern can't match to any data, it creates it. For detailed
information see Neo4j's [merge
documentation.](https://neo4j.com/docs/developer-manual/current/cypher/clauses/merge/)

An important thing about `MERGE` is visibility of modified data. `MERGE` takes
an input (typically a `MATCH`) and has two additional *phases*: the merging
part, and the subsequent set parts (`ON MATCH SET` and `ON CREATE SET`).
Analysis of Neo4j's behavior indicates that each of these three phases (input,
merge, set) does not see changes to the graph state done by subsequent phase.
The input phase does not see data created by the merge phase, nor the set
phase. This is consistent with what seems like the general Cypher philosophy
that query clause effects aren't visible in the preceding clauses.

We define the `Merge` logical operator as a *routing* operator that uses three
logical operator branches.

  1. The input from a preceding clause.

     For example in `MATCH (n), (m) MERGE (n)-[:T]-(m)`. This input is
     optional because `MERGE` is allowed to be the first clause in a query.

  2. The `merge_match` branch.

     This logical operator branch is `Pull`-ed from until exhausted for each
     successful `Pull` from the input branch.

  3. The `merge_create` branch.

     This branch is `Pull`ed when the `merge_match` branch does not match
     anything (no successful `Pull`s) for an input `Pull`. It is `Pull`ed only
     once in such a situation, since only one creation needs to occur for a
     failed match.

The `ON MATCH SET` and `ON CREATE SET` parts of the `MERGE` clause are
included in the `merge_match` and `merge_create` branches respectively. They
are placed on the end of their branches so that they execute only when those
branches succeed.

Memgraph strives to be consistent with Neo in its `MERGE` implementation,
while at the same time keeping performance as good as possible. Consistency
with Neo w.r.t. graph state visibility is not trivial. Documentation for
`Expand` and `Set` describe how Memgraph keeps track of both the updated
version of an edge/vertex and the old one, as it was before the current
transaction+command. This technique is also used in `Merge`. The input
phase/branch of `Merge` always looks at the old data. The merge phase needs to
see the new data so it doesn't create more data then necessary.

For example, consider the query.

    MATCH (p:Person) MERGE (c:City {name: p.lives_in})

This query needs to create a city node only once for each unique `p.lives_in`.
Finally the set phase of a `MERGE` clause should not affect the merge phase.
To achieve this the `merge_match` branch of the `Merge` operator should see
the latest created nodes, but filter them on their old state (if those nodes
were not created by the `create_branch`).  Implementation-wise that means that
`ScanAll` and `Expand` operators in the `merge_branch` need to look at the new
graph state, while `Filter` operators the old, if available.
