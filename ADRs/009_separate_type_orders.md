# Storage And Query Keep Separate Type Orders ADR

**Author**
Gareth Andrew Lloyd (github.com/Ignition)

**Status**
ACCEPTED

**Date**
September 14, 2026

**Problem**

Values of unlike types have to be put in some order, and that happens twice over,
once in each layer.

Storage keeps an order so that an index can hold every value of a property in a
sorted container. It has to be total, so that a search finds an entry again, and
cheap, because it is read on every comparison the container makes. Nothing
outside storage observes it.

The query layer needs the order the specification fixes for `ORDER BY`: map,
node, relationship, list, path, string, boolean, number with a NaN last, then
null. This one a user reads directly.

The two differ structurally rather than in detail. A null is the lowest value
storage keeps and the highest the specification names, and the run from boolean
through map goes in nearly opposite directions in the two. Storage also has to
place types the specification never names: the four temporal kinds it tells
apart, enumerations, points, and vector identifiers.

Naming where each type sits once, in a single declaration both layers read, is
therefore not a restatement of something already agreed. It would move one layer
onto the other's order, and there is a reason to think the numbering is worth
more than that.

**Criteria**

- *What each order answers to* (highest weight). Storage's answers to a
  container and may be anything total; the query layer's answers to a
  specification and to what a user sees. An order serving both answers to
  whichever constrains it more, which is the specification, and storage then
  carries a user-visible semantic it has no other reason to hold.
- *Whether the numbering is spent* (high). The order a type sits in and the
  value its enumerator takes are the same number today. Spending it on semantics
  forecloses spending it on anything else.
- *Cost of deferring* (medium). Two orders mean a plan cannot always substitute
  an index walk for a sort, and mean a predicate needed on both sides of the
  layering is written twice.
- *Cost of being wrong* (medium). Reversing a merge is a second reordering of
  every index, which rebuilds on restart and so costs time rather than data.

**Decision**

The two orders stay separate, and **no decision is recorded for or against
merging them later**.

This is a deferral with a reason, not a rejection. The enumerator's value is a
performance lever that nobody has priced. The destructor for a query-layer value
is an out-of-line switch over every enumerator and costs a measurable share of
every comparison the engine makes; the types needing no destructor at all sit in
four separate runs with the others between them. Grouping those below a
threshold would reduce the whole switch to one comparison. Separately, a shared
order may have a benefit of its own, in that a scan handing values to a sort
would not have to reorder them.

Neither has been measured. Merging the orders now would settle the numbering on
semantics before either alternative has been costed, and the numbering can only
be settled once.

**Consequences**

A sort expresses where each type sits as a table keyed by the type rather than as
arithmetic on the enumerator, and does not renumber anything. A lookup costs less
than the switch it replaces, and it leaves all three claimants live: whichever is
eventually wanted is one edit away.

A plan may drop a sort only where both layers order that column alike, and the
sort stays everywhere else. That refusal is what makes two orders safe rather
than merely tolerated, and it is load-bearing: without it, the same query returns
rows in one order with an index and another without.

Storage must not depend on the query layer, so a question both need answering,
such as whether a value is equal to itself, is written twice, once over each
layer's value type. That duplication is accepted as the price of the layering.
If a shared value layer is ever built, these pairs are the first thing that
should move into it, and this decision should be revisited at the same time
rather than before.
