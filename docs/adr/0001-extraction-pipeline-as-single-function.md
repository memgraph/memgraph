# ADR 0001: Extraction pipeline as a single function with stateless adapters

**Status**: Accepted

**Date**: 2026-04-29

## Context

The planner v2 e-graph extraction pipeline started as four free functions in
`memgraph::planner::core::extract` (`ComputeFrontiers`, `CollectDependencies`,
`TopologicalSort`) plus a struct (`PlanResolver` in `query::plan::v2`),
composed by `ConvertToLogicalOperator`. The shape created friction:

- The pipeline order lived only in the single caller; a reader had to
  reconstruct the dataflow from `ConvertToLogicalOperator` to understand
  what the four `extract::*` symbols were for.
- Implicit invariants were spread across asserts in different stages
  (`TopologicalSort` requires acyclicity from upstream, `ComputeFrontiers`
  may return `nullopt` for cycles, `PlanResolver` assumes the frontier map
  is fully populated). No single place documented the contract.
- The test surface was four entry points; the per-stage tests had to set
  up the upstream stages by hand.
- The `PlanResolver` struct held const-refs to `egraph` + `frontier_map`,
  with `&&`-qualified `resolve()` to enforce single-use. The const-ref
  members required NOLINT suppressions; the test resolver
  (`extract::testing::ResolveSelection`) had a *different* shape (free
  function, no captured state) and shared no contract with `PlanResolver`.
- Other parts of the codebase will leverage this e-graph infrastructure;
  the current shape would force each new consumer to either re-implement
  resolution or copy-paste the orchestration.

## Decision

Reify the pipeline as a single deep entry point:

```cpp
namespace memgraph::planner::core::extract {

  template <CostResultType CR>     struct ExtractionContext;   // owns stage state
  template <CostResultType CR>     struct ExtractView;         // span<order> + root_cost
  template <CostResultType CR>     struct ExtractResult;       // owned variant

  template <typename R, /*...*/>   concept Resolver = ...;     // see below

  // Primary entry: caller owns ctx; output is a view valid until the next call.
  auto Extract(EGraph const &g, EClassId root,
               CostModel const &cm, Resolver r,
               ExtractionContext<CR> &ctx) -> ExtractView<CR>;

  // Convenience: returns owned data, allocates a fresh context internally.
  auto Extract(EGraph const &g, EClassId root,
               CostModel const &cm, Resolver r) -> ExtractResult<CR>;

  // Public adapters (two of each = real seams, not hypothetical):
  template <typename T>            struct DefaultCostResult;   // scalar
  struct DefaultResolver;                                      // walks all children
  // (CostFrontier and PlanResolver live in query::plan::v2 — Bind-aware adapters.)

  namespace detail {
    auto ComputeFrontiers(...);    // moved from public
    auto CollectDependencies(...);
    auto TopologicalSort(...);
  }
}
```

Two customisation points:

- **`CostResultType` concept** — what costs look like (scalar, Pareto, etc.).
- **`Resolver` concept** — stateless functor `r(egraph, frontier_map, root)`
  returning a selection map. Contract: the returned map's domain is exactly
  the set of eclasses transitively reachable from `root` via the resolver's
  chosen child set; children absent from the map are deliberately excluded
  ("dead").

`PlanResolver` reshapes to a default-constructible struct whose
`operator()(g, fm, root)` constructs an internal `Impl` (holding the
const-refs and the recursion-shared `resolved` map) for the call duration,
then discards it. The `&&` qualifier and the const-ref-member NOLINTs go
away.

## Alternatives considered

- **Stateful `Extraction` class with `compute_frontiers()` /
  `resolve()` / etc. methods.** Rejected: adds a second
  `&&`-qualified lifetime ceremony for no win; the pipeline is genuinely
  one-shot and linear, so persistent intermediate state earns its keep
  only for debugging — and we can do that with a separate debug-output
  variant when a real diagnostic tool needs it.

- **Pluggable stages (caller-supplied stage list).** Rejected: YAGNI.
  No future stage is concretely on the roadmap; the two real
  customisation points (`CostResultType`, `Resolver`) cover the
  variations we know about.

- **Strict-coverage resolver contract** (every child of every chosen enode
  must be in the selection map). Rejected: incompatible with Bind dead,
  which deliberately excludes `sym` and `expr` from the resolved tree.
  The "chosen-coverage" contract names the existing skip semantics
  (`if (!enode_selection.contains(child)) continue` in `CollectDependencies`
  and `TopologicalSort`).

- **Owned output (`ExtractResult` only, no view).** Rejected as the primary
  shape because it doesn't permit cross-call buffer reuse — the
  `ExtractionContext` is the buffer-reuse seam, and the view shape is what
  makes it work. The owned variant survives as a convenience overload.

- **Per-stage cost-model context (`CostModelContext` carrying scratch
  buffers across `Extract` calls).** Rejected for now: the hot scratch
  (`CombineAltsFn::scratch`, the Bind alive-branch scratch) is already
  reused intra-`combine`, and cross-`Extract` reuse only helps when
  consecutive queries have similar shapes — measurable but bounded gain,
  and the cost is growing the cost-model concept's surface (associated
  context type rippling through `CostResultType`, `PlanCostModel`, and
  test cost models). Revisit if profiling shows allocation churn.

- **PMR (`monotonic_buffer_resource`) for all internal allocations.**
  Rejected for this refactor: heavier plumbing than warranted by current
  evidence. Worth considering once `Extract` is on a hot path measured
  against many query-compilation cycles.

- **Keep `DefaultResolver` and `DefaultCostResult` test-only.** Rejected:
  with future leverage points incoming, shipping the concepts without
  reference adapters means each new consumer reinvents the contract.
  Two-adapters-per-concept makes the seams real rather than hypothetical.

## Consequences

- Future consumers of the e-graph infrastructure start from
  `Extract(egraph, root, DefaultCostResult-based-cost-model, DefaultResolver{}, ctx)`
  and replace adapters as their semantics demand.
- The contract for "what does a Resolver promise" is stated once in the
  concept's doc-comment; downstream stages reference the contract instead
  of asserting it from below.
- `ConvertToLogicalOperator` shrinks to ~15 lines (build cost model,
  call `Extract`, hand the order to the Builder).
- Production callers cannot accidentally compose stages in a wrong order:
  the stages are in `detail::`. Tests reach `detail::` deliberately, with
  the namespace name documenting "I know I'm poking internals."
- `PlanResolver`'s reshape removes one of the lint smells flagged in
  reviews (const-ref data members + NOLINT). The shape is now
  symmetrical with `DefaultResolver`: both are values, both have an
  `operator()(g, fm, root)`.
