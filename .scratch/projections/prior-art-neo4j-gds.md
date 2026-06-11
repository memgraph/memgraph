# Prior art: Neo4j GDS projections and APOC virtual graphs

Comparison note for issue 13. What Neo4j has already explored for projecting a
graph to compute over, measured against what this branch built (`project()`,
`derive()`, `virtualNode()`, `virtualEdge()`, `virtualGraph()`).

## What Neo4j offers

**GDS native projection** - `CALL gds.graph.project(name, nodeProjection,
relationshipProjection, config)`. Projects *all* nodes with the given labels and
*all* relationships of the given types into a named, in-memory graph held in a
**catalog**. Materializes the data (a copy); **only numeric property types** can
be projected. Relationship `orientation` (NATURAL/UNDIRECTED/REVERSE) and
parallel-edge `aggregation` (NONE/SINGLE/COUNT/MIN/MAX/SUM) are config options.

**GDS Cypher projection** - `RETURN gds.graph.project(name, sourceNode,
targetNode, dataConfig, configuration)`, an **aggregation function** over the
rows a normal `MATCH` produces. `sourceNode`/`targetNode` accept **a node or an
integer id** (target may be null -> the source is projected unconnected).
`dataConfig` carries `sourceNodeProperties`/`targetNodeProperties`,
`sourceNodeLabels`/`targetNodeLabels`, `relationshipType`,
`relationshipProperties`; `configuration` carries `undirectedRelationshipTypes`
(`'*'` or a list). Still produces a named catalog graph; still materializes.

**APOC virtual graphs** - `apoc.create.vNode(labels, props)`,
`apoc.create.vRelationship(from, type, props, to)`,
`apoc.graph.fromData(nodes, rels, ...)`, `apoc.graph.fromPath(s)`. Transitory
in-query virtual elements for shaping/visualization.

## What aged badly (the lessons)

1. **Cypher-as-strings was deprecated.** The original
   `gds.graph.project.cypher(name, nodeQuery, relQuery)` took two Cypher query
   *strings* that GDS parsed and ran itself. It was replaced by the
   aggregation-function form, where the user writes ordinary Cypher and GDS only
   aggregates the rows - "GDS is no longer responsible or in control of the
   execution of the Cypher queries." Passing a sub-language as opaque strings is
   the anti-pattern.
2. **A bolt-on for undirected, then again for aggregation.** The legacy form
   could not project undirected relationships, so "Cypher Aggregation" was added
   beside it, then the two were unified. Retrofitting core projection semantics
   onto a surface that did not plan for them cost two migrations.
3. **Parallel-edge aggregation moved out of config and into the query.** The new
   form has no `aggregation: SUM`; you `count()`/`sum()` parallel edges yourself
   in Cypher. Neo4j concluded the host language, not a config enum, is the right
   place for that.
4. **APOC virtual nodes break the rest of Cypher.** "Virtual Nodes cannot be
   queried ... some built-in Cypher functions may not work" - `labels()` on a
   virtual node returns `[]`, and you cannot `MATCH` them. Virtual elements that
   are second-class to the function and query layer are a persistent footgun.
5. **Numeric-only, materialized.** Both GDS projection forms copy data into
   memory and accept only a limited (numeric) property set - the exact cost this
   branch set out to avoid for embedding-heavy nodes.

## How our branch compares

### Convergent (independently arrived at the same good shape)

- **Aggregation function over matched rows.** `derive(path, config)` and
  `project(path|lists)` are aggregations, matching GDS's *new* (non-deprecated)
  Cypher projection - not the string-query form. We never built the deprecated
  shape.
- **Node-or-id endpoints.** Our edge endpoint is a virtual node or an integer
  **handle** (ADR 0002) - the same "Node or Integer" choice GDS landed on.
- **Config vocabulary.** `derive()`'s `sourceNodeProperties`,
  `targetNodeLabels`, `relationshipProperties`, `undirectedEdgeTypes` are the
  GDS `dataConfig`/`configuration` keys almost verbatim - convergent naming, low
  surprise for GDS users.
- **Virtual constructors.** `virtualNode`/`virtualEdge`/`virtualGraph` mirror
  APOC's `vNode`/`vRelationship`/`fromData`.

### Deliberately better (avoiding their mistakes)

- **Lazy read-through, any property type.** `derive()` never copies origin
  properties unless read and imposes no numeric-only restriction. This is the
  headline divergence from GDS materialization - the embedding-memory win.
- **Functions work over virtual nodes.** Where APOC's `labels()` returns `[]`,
  ours returns the real labels; issue 15 finishes this for the rest of the
  built-ins. We are fixing the footgun APOC left open.
- **Loud failure on ambiguity.** GDS silently keeps "the first occurrence's
  properties/labels" on a repeated node; we make a duplicate import handle and a
  conflicting binding a construction error.
- **Inline values, no catalog lifecycle.** Our projection is a value threaded
  through `WITH` in one query; theirs is a named catalog graph you must `drop`.
  We avoid the leak-prone create/drop lifecycle.

### Gaps and open divergences (feed the design issues)

- **No named-graph reuse.** The flip side of inline values: GDS can project once
  and run many algorithms against the named graph; we recompute per query. If
  reuse matters, an *optional* catalog is the missing piece - but it reintroduces
  stateful lifecycle. (Feeds issue 14 and issue 11.)
- **No native label/type projection.** GDS `project('g', 'Person', 'KNOWS')`
  grabs a whole-graph slice by label/type in one call. We are path/list-driven;
  the same intent is `MATCH ... project(...)`, which is more verbose. A
  label/type convenience could close the ergonomic gap. (Feeds issue 14.)
- **Parallel-edge aggregation.** We dedup edges by `(from, to, type)` (their
  `SINGLE`). Following lesson 3, weighting parallel edges should stay in user
  Cypher rather than become config - worth stating explicitly. (Feeds issue 14.)

## Recommendations folded into design issues

- **Issue 11 (`USE projection`)**: do *not* resurrect a Cypher-as-strings
  surface (GDS lesson 1). Inner Cypher must be real nested Cypher the planner
  sees, not a string GDS-style.
- **Issue 14 (concise surface)**: the declarative win over GDS is already ours
  (inline, aggregation-based, lazy). The two ergonomic gaps to weigh are
  named-graph reuse and a native label/type projection shorthand - both must stay
  declarative and standard-client-safe. Keep parallel-edge weighting in Cypher.
- **Issue 15 (functions over non-native nodes)**: directly fixes APOC's
  best-known footgun; high value, already partly done.
