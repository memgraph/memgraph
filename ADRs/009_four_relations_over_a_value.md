# Four Relations Over A Value ADR

**Author**
Gareth Andrew Lloyd (github.com/Ignition)

**Status**
PROPOSED

**Date**
August 29, 2026

**Problem**

openCypher defines four relations over values, and a query engine has to
implement all four because different parts of a query ask different ones:

- *Equality* is what `=`, `<>`, `IN` and a `CASE` read. It is three-valued: a
  comparison turning on a null answers null and decides nothing.
- *Equivalence* is what `DISTINCT`, grouping and a hash table's probe read. It
  is two-valued, because a set has no third answer to give, and it holds a null
  the same value as a null.
- *Comparability* is what `<`, `<=`, `>` and `>=` read. It is partial: two
  values may have no order between them, which is what a NaN is, and all four
  comparisons then answer false.
- *Orderability* is what `ORDER BY`, `min` and `max` read. Alone among the four
  it places every pair of values, which is what makes a sort possible at all.

Left unnamed, these collapse into whichever comparison a given call site
happened to reach for, and the collapse is silent. A structure keyed by
equivalence answering an equality question returns rows a user did not ask for.
A sort reading a relation that leaves a pair unplaced is undefined behaviour
rather than a wrong answer, because `std::sort` requires a strict weak ordering
and a NaN gives it none.

Three further decisions have to be made that openCypher does not make for us,
and each of them is the kind that is argued once and then has to stay argued.

**Criteria**

- *A sort must terminate and must place every value* (highest weight). This is
  not a preference: without a total order the sort is undefined behaviour, so
  any value the language leaves unplaced still has to be given a place.
- *The two layers must agree wherever a plan relies on them agreeing* (highest
  weight). An index holds its entries in the order the storage layer gives
  values; a sort reads the order the query layer gives them. A plan that drops
  a sort because an index already walked the column is only sound where the two
  orders coincide, and a wrong answer here is silent.
- *Agreement with the reference implementation* (medium). Divergence is
  invisible to a user: both engines return rows, and only the order differs.
- *One decision, one place* (medium). A relation written twice drifts, and the
  drift shows up as a lookup that misses rather than as a failure.

**Decision**

*The four relations are named where they are defined.* Each lives in
`src/query/relations/`, in a file that states what it answers, who reads it,
and how it differs from its neighbours. The operators a query writes are the
presentation surface over them, not the definition.

*Every type is given a place in one ordering, declared once for both layers.*
The ranks live in the storage layer, in an enumeration the query layer maps
onto rather than repeats. This puts a query-language decision in the lower
layer on purpose: it is the only arrangement in which neither layer can
renumber the order alone. The alternative, a rank per layer with a test holding
them together, was rejected because the test can only compare the types both
layers can build, and the query layer has several the storage layer cannot
store.

*The order of types follows the reference implementation, and where a type sits
is taken whole.* Cypher fixes this order and we take it, including the place the
temporal types are given and the place the point types are given as a group.
Memgraph's own types are inserted where they fit: an enum between duration and
the points, and the virtual node and edge beside the node and relationship they
stand in for. A query ordering a mixed column then answers alike on both engines
for every type but one.

*A two-dimensional point sorts before a three-dimensional one, rather than the
two being interleaved by their reference systems.* This is the one place the
order departs from the reference implementation, which gives points a single
place and sorts them within it by the identifier of the system a point is
stated in: a two-dimensional Cartesian point sorts after a three-dimensional
WGS-84 one there, and before it here. The two are separate types everywhere else
in the engine, a query tells them apart, and a stored one is told apart by the
same enumeration, so giving them one place would mean the order was the only
thing in the system that did not. The visible consequence is confined to a
column holding points of both shapes and more than one reference system, which
is the only query the two orders answer apart on.

*A NaN sorts after every number and before null.* Cypher pins a NaN only for
comparability, where every comparison against one is false, and says nothing
about where it sorts. Something had to be chosen, because a sort cannot be
handed a pair it has no answer for, and this is the choice that keeps a NaN
adjacent to the numbers it is one of. Two NaNs sort alongside one another, so
equivalence holds them the same value and a hash of one reaches the other's
bucket. The visible consequence is that `max` over a column holding a NaN
returns the NaN while `min` ignores it.

*`min` and `max` read orderability rather than a relation of their own.* This is
what the language asks for: the specification defines both as the smallest and
the largest of the aggregated values under orderability, over every value they
are given. The reference implementation's own manual describes an ordering for
these two over lists, strings and numbers alone, and places those three in the
relative order orderability places them in, so the two accounts agree wherever
both speak. Raising for the types the manual does not name was the previous
behaviour and answered neither: a column that happens to hold a date is not a
query the user got wrong.

This holds for a node, a relationship and a path as much as for the rest. The
order places those three by their identity, which is an arbitrary answer, but a
relation that places every pair has no way to decline one and still be the
relation these two read. Refusing them would put back exactly what naming the
four relations set out to remove: an aggregation whose behaviour depends on
which types the column it was handed happened to hold. Only the three types the
order has no place for at all are refused.

*How far a range may trust an index is named per type, in one place.* An index
places pairs the comparison operators refuse, so a scan reading a range can
return rows the filter it stands in for would drop. Each stored type says which
of four things a range over it means: the operators carry no order over the
type, so a scan raises; the bounds are the whole answer; the bounds have to be
cut down to the band of the one kind the bound holds, which is what a stored
temporal needs because a single type carries all four kinds a query tells
apart; or the bounds give candidates that must each be put to the comparison as
well, which is what a list needs because two lists are ordered where the
comparison still declines. Both the vertex and the edge scans read their bounds
through that one statement. Asking the question of a type instead of asking
whether a bound happens to be a list is what keeps the two paths from drifting,
which they had: the vertex path learned to refuse a NaN bound and the edge path
did not.

*A structure keyed by equivalence may answer an equality question only where
the two relations agree.* They agree over every value carrying neither a null
nor a NaN, and that condition is named once and asked at each such structure:
the index lookups, the hash join's probe, and the cached list behind `IN`.
Carrying either, the caller asks equality directly.

*A query module is offered both relations rather than one.* A module holds the
same values a query does and has the same two questions to ask about them, so
naming the relations inside the engine and leaving one of them unreachable from
outside would only move the collapse across the boundary. `mgp_value_equal`
answers the three-valued equality and `mgp_value_equivalent` the two-valued
equivalence. Equality needs a third answer that no boolean carries, so it
returns a ternary rather than the integer the rest of the boolean surface uses,
and that is stated where it is declared. The `==` a module writes on a value
reads equivalence, because a C++ operator returning a boolean cannot carry the
third answer and a value has to be the same value as itself for a container to
hold it.

**Consequences**

A map is the one type whose ordering the two layers cannot be made to share. A
stored map is kept by the identifiers its keys were given, which is the order
they were first seen in, and a sort orders them by name. Nothing reconciles
that while one layer cannot read the other's keys, so a sort over a map column
is never dropped. The same holds for the temporal types, which storage carries
under a single type and tells apart by an enumeration that counts in a
different order than a sort places them.

The list types are refused for a weaker reason and it is worth stating: a list
may hold a value of any type, including the ones above, and what a given
column's lists hold is not something a plan can see. The refusal is
conservative rather than forced, and a sort is kept that might not need
keeping.

A stored vector takes a list's place, because that is what a query is handed
when it reads the property back: the list of its coordinates. This is the one
place where a stored type's representation is deliberately not its own type in
the order. Ranking it anywhere else would have an index walk such a column in
an order no sort produces, and would report a vector and a list alike whenever
they happened to share a rank without anything comparing what they hold.

Reading one relation for `min` and `max` means both answer for maps, nodes,
relationships, paths, points and temporals. The language defines the answer, so
it is not ours to choose, but the reference implementation's manual does not
describe it and a later decision to raise for these instead would be a breaking
change. For a node, a relationship and a path the answer is settled by identity,
so which one comes back is arbitrary and only the fact that one does is
promised.
