# Cypher Execution over Subgraphs and Projections ADR

**Author**
Gareth Lloyd (https://github.com/Ignition)

**Status**
ACCEPTED (target model; implementation staged, see Consequences)

**Date**
July 15, 2026

**Problem**

Memgraph can hand a derived view of the graph to an algorithm procedure, but
users want more: to run declarative Cypher over a view, and to write computed
values back onto its nodes. Two kinds of derived view exist. A **subgraph**
(`project(path)`) is a membership-filtered set of real vertices and edges; its
elements carry real accessors, so a write persists. A **projection**
(`derive(path, config)`, or `virtualGraph(nodes, edges)` assembled from lists) is
built from nodes and edges that are not real vertices.

Three forces shape the design. First, the original projection copied every
property of every node into compute, which is untenable for embedding-heavy
nodes. Second, a projection's nodes must still map back to a real node where one
exists, so a written value lands on the right vertex. Third, adding derived-view
nodes as a second element kind forces every consumer (built-in functions, the
`SET` operator, Bolt serialization, every scan and expand operator) to branch on
kind, and each new capability multiplies the branching.

**Criteria**

- **Real-graph path is not regressed** (highest weight). The overwhelming
  majority of queries touch no derived view; they must pay nothing.
- **One code path, not per-kind forks.** Adding a derived view must not fork
  every operator and function.
- **No eager data copy.** A projection over real nodes must not materialise
  properties it never reads.
- **Declarative expressiveness.** The whole query engine, not a fixed set of
  procedures, should run over a view.

**Decision**

Seven decisions, each addressing part of the problem above.

## 1. One projected-node type

Every node in a derived view is one in-memory type (`query::VirtualNode`) holding
an **optional origin** real vertex and its own **overlay** property store. An
*overlay node* has an origin: reads fall through to it lazily (an origin property
is never copied unless read), and declared overlay keys shadow it. A *synthetic
node* has no origin: reads and writes hit its overlay only. These are two
configurations of one type, not two types, which collapses the per-kind branching
to a single path. This directly serves "no eager data copy" and "one code path".

## 2. Per-property binding with a static schema

Each property is bound to one store: `origin` (read-through and write-through,
persisted), `overlay` (read and write hit the overlay, compute-only), or `hidden`
(not visible). Read source and write target are coupled to the same store, so a
read after a write cannot return a stale shadowed value. The set of overlay and
hidden keys is fixed at construction, so the schema is static and can be shipped
before the result stream. A single shared schema object is referenced by every
node in a projection rather than copied per node.

## 3. Synthetic-edge endpoints carry unresolved import handles

A synthetic edge's endpoint is settled per endpoint: either a resolved node, or
an unresolved integer **handle** (an import key the user passed to
`virtualEdge`). The handle is stored on the node, is not the node's identity, and
is bound to a node at list assembly by matching handles. This lets a user wire an
edge to a node that does not exist yet, and lets a standalone edge serialize with
its handles as endpoints. A real vertex is rejected as an endpoint; it is wired by
passing its id as a handle.

## 4. List assembly is a scalar constructor

`virtualGraph(nodes, edges, config)` assembles a projection from lists in one
query, so an external graph (nodes and edges tables from another system) imports
in a single statement. It is a scalar function, not an aggregation, which avoids a
plan-time accumulator type and the two-expression argument limit. A dangling edge
(an endpoint resolving to no node in the list) aborts assembly by default, or is
dropped per config; a duplicate import handle is always an error.

## 5. A projection is a bindable graph scope, not a value

Querying a view happens by binding it as the ambient graph for a `CALL { ... }`
block with `USE`, not by threading a value through functions:

```cypher
WITH derive(path, {...}) AS g
CALL { USE g MATCH (n:Person)-[:KNOWS]->(m) WHERE m.churned RETURN n }
SET n.at_risk = true
```

Inside the scope, `MATCH`, built-in functions, and reads resolve against the
bound view; write-back happens outside the scope through the binding. The first
version of the scope is read-only, single-nesting, and full-scan.

## 6. GraphView: one seam for real, subgraph, and projected graphs

The execution engine runs against one abstraction, `query::GraphView`, that
answers only what an operator needs of "the graph": scan vertices, expand a
vertex's edges, and map names to ids. **The real accessor is the identity view.**
A subgraph is that view filtered by membership; a projection composes its own
topology and delegates origin reads to a base view. `USE` rebinds which view is
ambient for a scope, so nesting a view over a view is composition, not a special
case.

The virtual boundary is **coarse**: one call yields a vertex range or a vertex's
edge range, and iteration then runs over concrete elements with direct reads. The
boundary is crossed once per scan, not once per row, so the real-graph path is not
regressed. Scan and expand are graph operations on the view; property read and
write stay on the element, which already encapsulates binding and read-through.

## 7. Identity on the wire

Virtual elements are given synthetic gids counted down from the maximum, so they
never collide with real gids (which count up from zero). At the query boundary
these are mapped to small, query-local external ids (`-1, -2, ...`) that `id()`
reports and that Bolt serialization matches. An overlay node serializes at its
origin's real id so a client maps it back to the real node. Provenance travels as
a non-breaking channel: each projected node carries a small reserved tag
referencing a schema table sent once in the result header, so a streaming client
resolves provenance as nodes arrive, and property values are never wrapped.

**Consequences**

- The GraphView seam is the foundation; the element-kind unification, the USE
  scope, and function support over a view are its payoff. Implementation is
  staged: single-hop scan and expand run through the seam, and the
  variable-length and shortest-path family runs over a per-view traversal policy.
  Completing the seam for every remaining per-operator branch (edge expansion, the
  ambient-view dispatch) is follow-up work, tracked outside this ADR.
- Version-one boundaries surface as clear query errors, not silent wrong results:
  no writes inside a USE scope, no nested USE, no index-backed scans inside the
  scope (full scan only), no filter or accumulated-path lambda over a projection,
  and no named path over a projection (a path value holds real elements only).
  Each has a documented error; over a subgraph, whose elements are real, these
  restrictions do not apply.
- Supporting cross-tenant virtual elements and rendering an arbitrary virtual
  graph in external clients are enabled by this model but are not built here.
