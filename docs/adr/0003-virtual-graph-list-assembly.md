# ADR 0003: Assembling a projection from lists is a scalar constructor

Status: accepted

## Context

A user importing an external graph has two tables - nodes and edges - and wants
to assemble them into a projection in one query: collect the rows into a node
list and an edge list, then build a `VirtualGraph` an algorithm procedure can
run over. The edge rows reference node rows by an id; that id is the import
**handle** carried on each synthetic node (ADR 0002).

The existing graph constructors are aggregations. `project(path)` and
`derive(path, config)` accumulate one path per row across the whole input;
`project(nodes, edges)` reuses that aggregation to fold per-row lists of **real**
vertices and edges into a real-accessor `Graph`. Reaching for the same machinery
for synthetic list-import runs into three walls:

- An aggregation's result type is fixed before any input is seen
  (`DefaultAggregationOpValue` picks `Graph` for `PROJECT_LISTS`), so
  `project(a, b)` cannot decide real-vs-virtual from list contents at runtime;
  producing a `VirtualGraph` would need a separate aggregation op.
- The `Aggregation` AST node carries exactly two expression slots, leaving no
  room for nodes, edges, and a validation config together.
- The call shape `project(nodes, edges)` is already shipped for real subgraphs
  and must not change meaning.

But list-import is not cross-row aggregation. The data arrives as two lists in a
single row (the collect already happened upstream); assembly is a one-shot
function of those two lists, not an accumulation over many rows.

## Decision

Assemble a projection from lists with a **scalar constructor**, not an
aggregation:

```cypher
virtualGraph(nodes, edges)                              // dangling edge => error (default)
virtualGraph(nodes, edges, {onDanglingEdge: 'drop'})   // dangling edge => omit that edge
```

`virtualGraph` returns the existing `VirtualGraph` value, consumable by a
procedure exactly as a `derive()` graph is. Modelling it as a scalar function
sidesteps all three walls: arguments are positional with an optional third config
map (no two-expression limit), there is no plan-time accumulator type to fix, and
`project`/`derive` are untouched. It also completes the `virtualNode` /
`virtualEdge` family - a synthetic-graph constructor alongside the synthetic-node
and synthetic-edge ones. It is not `derive`: there is no path and no origin
read-through, only synthetic construction.

Binding and validation:

- Insert every node from the node list into the graph (deduplicated by synthetic
  gid, as the graph already does), building a `handle -> node` map.
- Resolve each edge endpoint to a node in the graph: a **handle** endpoint via
  the map; a **resolved-node** endpoint by its synthetic-gid membership. Rebind
  the edge to the canonical graph nodes and insert it (edges deduplicate by
  `(from, to, type)`).
- A **dangling edge** - an endpoint resolving to no node in the list - aborts the
  construction under `onDanglingEdge: 'error'` (the default) or is silently
  omitted under `'drop'`.
- **Duplicate import handles** among the nodes make a handle reference ambiguous
  and are always a hard error, independent of `onDanglingEdge`.
- **Nulls** in either list are skipped. A **real** vertex or edge is rejected
  with a message pointing at `project()`/`derive()`; the lists hold virtual kinds
  only.

## Consequences

- A second `VirtualGraph` producer exists alongside `derive()`, reached as a
  plain function call - no new aggregation op, grammar trigger, or AST field.
- Validation config has a natural home (the third argument), so `onDanglingEdge`
  and any future option ride a normal map without touching the aggregation AST.
- Synthetic nodes assembled this way carry no origin and no projection-schema
  reference, so the Bolt provenance channel (ADR 0001/0002 territory) has nothing
  to emit for them - they serialize as ordinary synthetic nodes.
- List-import is per-row: if a query somehow produces several rows each with two
  lists, each row yields its own graph. Cross-row merging is not offered; the
  upstream `collect` is where rows are combined.

## Alternatives considered

- **A new aggregation op (`PROJECT_VIRTUAL_LISTS`).** Rejected: it pays the full
  cost of the aggregation framework (plan-time accumulator type, cross-row state,
  parallel-merge paths) for data that is already a single row, and still needs a
  third AST slot for the config.
- **A list form of `derive` (`derive({nodes, edges}, config)`).** Rejected:
  `derive` means lazy overlay-projection from a path with origin read-through;
  list-import shares none of that. Overloading it blurs a precise term.
- **Content-sniffing `project(a, b)` to branch real-vs-virtual.** Rejected: the
  aggregation result type is fixed before input is seen, and it would risk the
  shipped real-subgraph behaviour.
