// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/plan_v2/frontend/egraph_converter.hpp"

#include <algorithm>
#include <ranges>
#include <utility>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "planner/extract/extract.hpp"
#include "planner/extract/extractor.hpp"
#include "query/plan_v2/cost/builtin_estimator.hpp"
#include "query/plan_v2/cost/cost_model.hpp"
#include "query/plan_v2/egraph/alternative.hpp"
#include "query/plan_v2/egraph/child_layout.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"
#include "query/plan_v2/frontend/builder.hpp"
#include "query/plan_v2/resolve/pre_extraction.hpp"
#include "query/plan_v2/resolve/resolver.hpp"
#include "query/plan_v2/resolve/variable_index.hpp"
#include "query/plan_v2/resolve/variable_set.hpp"
#include "utils/on_scope_exit.hpp"

namespace memgraph::query::plan::v2 {

// ============================================================================
// Plan extraction entry point. Stitches the cost model, resolver, and builder
// together: pre-pass builds the VariableIndex and demand filter; cost model
// computes pareto frontiers; resolver picks one alt per (eclass, scope) key;
// builder turns the resolved order into a LogicalOperator tree.
// ============================================================================

struct QueryPlannerContext::Impl
    : planner::core::extract::ExtractContext<CostFrontier, ResolverKey, std::hash<ResolverKey>> {};

QueryPlannerContext::QueryPlannerContext() : impl_(std::make_unique<Impl>()) {}

QueryPlannerContext::~QueryPlannerContext() = default;
QueryPlannerContext::QueryPlannerContext(QueryPlannerContext &&) noexcept = default;
QueryPlannerContext &QueryPlannerContext::operator=(QueryPlannerContext &&) noexcept = default;

namespace {

using enum symbol;

/// Singleton invariant: each Symbol e-class corresponds to one variable.
/// Compile-time guard on Symbol's shape + debug-build check that no rewrite
/// merged two Symbol e-nodes.
void CheckSymbolSingletonInvariant([[maybe_unused]] EGraph const &core) {
  static_assert(is_leaf_v<Symbol>);
#ifndef NDEBUG
  for (auto eclass_id : core.canonical_eclass_ids()) {
    auto const &cls = core.eclass(eclass_id);
    bool const has_symbol =
        std::ranges::any_of(cls.nodes(), [&](auto enode_id) { return core.get_enode(enode_id).symbol() == Symbol; });
    DMG_ASSERT(!has_symbol || cls.nodes().size() == 1, "planner bug: Symbol e-class merged with another e-node");
  }
#endif
}

/// Compute pareto frontiers; throw if root has no self-contained alternative.
/// Returns a reference to the root frontier for cost/cardinality extraction.
auto ComputeFrontiersAndCheckRoot(EGraph const &core, CostCtx const &cost_ctx, planner::core::EClassId true_root,
                                  QueryPlannerContext::Impl &ctx) -> CostFrontier const & {
  namespace extract = planner::core::extract;
  (void)extract::ComputeFrontiers(core, cost_ctx, true_root, ctx.frontier_context);

  auto const root_it = ctx.frontier_context.frontier_map.find(true_root);
  if (root_it == ctx.frontier_context.frontier_map.end() || !root_it->second.has_value()) {
    ThrowPlannerBug("root eclass has no frontier");
  }
  auto const &root_frontier = *root_it->second;
  bool const root_satisfiable =
      std::ranges::any_of(root_frontier.alts(), [](Alternative const &a) { return a.required.empty(); });
  if (!root_satisfiable) {
    ThrowPlannerBug(
        "root frontier has no self-contained alternative; "
        "ensure all Identifier references are resolved by rewrites before extraction");
  }
  return root_frontier;
}

/// Post-order resolver: writes ResolvedEntries + CSR child indices into
/// `ctx.build`. Per-node policy (alt selection + child threading + error
/// classification) lives in `ResolvePlanNode`.
void ResolveSelection(EGraph const &core, SymbolContext const &syms, planner::core::EClassId true_root,
                      QueryPlannerContext::Impl &ctx) {
  namespace extract = planner::core::extract;
  extract::Resolve<symbol, analysis, CostFrontier>(
      core,
      ResolverKey{.eclass = true_root, .in_scope = VariableSet{}, .must_introduce = VariableSet{}},
      [&](ResolverKey const &key,
          extract::FrontierMap<CostFrontier> const &frontier_map,
          auto const & /*egraph_ref*/,
          auto record_child) { return ResolvePlanNode(syms, frontier_map, key, record_child); },
      ctx);
}

/// Walk the resolver's post-order, calling per-symbol build traits.
/// Returns the built BuildState (with ast_storage / symbol_table owned)
/// and the root LogicalOperatorPtr; post-order emits the root last.
struct BuildOutput {
  BuildState state;
  LogicalOperatorPtr root;
};

auto BuildOperatorTree(auto const &impl, QueryPlannerContext::Impl &ctx) -> BuildOutput {
  // Dense build cache indexed by `build_order` position. The resolver's
  // `seen` map guarantees each ResolverKey is emitted exactly once.
  auto build_state = BuildState{
      .literal_info = impl.graph.template storage<Literal>().info,
      .named_output_info = impl.graph.template storage<NamedOutput>().info,
      .symbol_store = impl.graph.template storage<Symbol>().store,
      .function_info = impl.graph.template storage<Function>().info,
      .egraph = impl.graph.core(),
  };
  auto const resolved_entries = ctx.build.resolved_entries();
  auto built = std::vector<BuildResult>{};
  built.reserve(resolved_entries.size());

  auto children_refs = std::vector<ChildRef>{};
  for (auto const &entry : resolved_entries) {
    auto const &enode = impl.graph.core().get_enode(entry.enode_id);

    auto const child_slots = ctx.build.children_of(entry);
    auto const built_children = child_slots | std::views::transform([&](auto slot) { return std::cref(built[slot]); });
    children_refs.assign_range(built_children);
    built.push_back(build_state.Build(enode, children_refs));
  }

  DMG_ASSERT(!built.empty(), "build order must contain at least the root entry");
  auto *ptr = std::get_if<LogicalOperatorPtr>(&built.back());
  if (!ptr) ThrowPlannerBug("root build result is not a LogicalOperator");
  return BuildOutput{.state = std::move(build_state), .root = *ptr};
}

}  // namespace

auto ConvertToLogicalOperator(egraph const &e, eclass root, QueryPlannerContext &planner_context) -> ExtractionResult {
  auto const &impl = impl_of(e);
  auto const true_root = to_core(root);

  // Context buffers are cleared on scope exit (success or throw) so the next
  // call sees an empty context; Resolve's empty-on-entry guards stay real.
  auto &ctx = planner_context.impl();
  utils::OnScopeExit const clear_ctx_on_exit{[&ctx] { ctx.clear(); }};

  CheckSymbolSingletonInvariant(impl.graph.core());

  auto pre = BuildPreExtractionData(impl.graph.core());

  // Top-level query: outer_scope is empty. Importing CALL / Apply / Cartesian
  // inner blocks (future work) will seed it with what the outer pipeline binds.
  SymbolContext const syms{.egraph = impl.graph.core(),
                           .variable_index = pre.variable_index,
                           .outer_scope = VariableSet{},
                           .referenced_syms = std::move(pre.referenced_syms)};

  auto const active_estimator = BuiltinEstimator{e};
  auto const &root_frontier = ComputeFrontiersAndCheckRoot(
      impl.graph.core(), CostCtx{.estimator = active_estimator, .syms = syms}, true_root, ctx);

  ResolveSelection(impl.graph.core(), syms, true_root, ctx);

  auto build_out = BuildOperatorTree(impl, ctx);

  // NOTE: this is a SILLY copy because we are going from shared_ptr to unique_ptr
  //       fix if this becomes a bottleneck
  auto unique_result = build_out.root->Clone(&build_out.state.ast_storage);

  // NOTE: this is already known during resolve, but because of internal efficiency we lose that information
  //       we do a cheap search of root frontier only to get the alternative we just resolved and built for
  auto root_satisfiable =
      root_frontier.alts() | std::views::filter([](Alternative const &a) { return a.required.empty(); });
  auto const &best = *std::ranges::min_element(root_satisfiable, std::less<>{}, &Alternative::cost);

  return ExtractionResult{.plan = std::move(unique_result),
                          .cost = best.cost,
                          .cardinality = best.cardinality,
                          .ast_storage = std::move(build_out.state.ast_storage),
                          .symbol_table = std::move(build_out.state.symbol_table)};
}

}  // namespace memgraph::query::plan::v2
