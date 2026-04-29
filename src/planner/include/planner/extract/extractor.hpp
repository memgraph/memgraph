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

#pragma once

#include <concepts>
#include <functional>
#include <queue>
#include <span>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include <cassert>

#include "planner/extract/pareto_frontier.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::extract {

// ============================================================================
// CostResult contract
// ============================================================================
//
// Every cost model defines:
//   using CostResult = ...;
//   operator()(ENode const &, ENodeId, span<CostResult const>) -> CostResult
//
// CostResult must satisfy CostResultType (defined below).  ParetoFrontier-based
// cost models derive from CostResultBase (planner/extract/pareto_frontier.hpp).
// DefaultCostResult<T> is the scalar reference adapter.

/// CostResult contract — enforced at compile time.
/// Every CostResult type must provide:
///   cost_t                 — the scalar cost type (must be totally_ordered)
///   merge(a, b)            — combine frontiers
///   resolve(r)             — pick the best enode
///   min_cost(r)            — extract the comparable cost (asserts non-empty for frontiers)
///   resolve_with_cost(r)   — paired (enode_id, cost) so callers needing both
///                            avoid scanning the frontier twice
template <typename CR>
concept CostResultType = std::copyable<CR> && requires(CR const &a, CR const &b) {
  typename CR::cost_t;
  requires std::totally_ordered<typename CR::cost_t>;
  { CR::merge(a, b) } -> std::same_as<CR>;
  { CR::resolve(a) } noexcept -> std::same_as<ENodeId>;
  { CR::min_cost(a) } noexcept -> std::same_as<typename CR::cost_t>;
  { CR::resolve_with_cost(a) } -> std::same_as<std::pair<ENodeId, typename CR::cost_t>>;
};

/// Default scalar CostResult — wraps a cost value with enode metadata.
/// Reference adapter for cost models that don't need Pareto frontiers.
template <std::totally_ordered T>
struct DefaultCostResult {
  using cost_t = T;

  T cost;
  ENodeId enode_id;

  static auto merge(DefaultCostResult const &a, DefaultCostResult const &b) -> DefaultCostResult {
    return a.cost <= b.cost ? a : b;
  }

  static auto resolve_with_cost(DefaultCostResult const &r) -> std::pair<ENodeId, cost_t> {
    return {r.enode_id, r.cost};
  }

  static auto resolve(DefaultCostResult const &r) noexcept -> ENodeId { return r.enode_id; }

  static auto min_cost(DefaultCostResult const &r) noexcept -> cost_t { return r.cost; }
};

// ============================================================================
// Extraction pipeline types
// ============================================================================

/// Per-eclass frontier during cost propagation.
/// nullopt means "in progress" (cycle detection).
template <typename CostResult>
using EClassFrontier = std::optional<CostResult>;

/// Map from EClassId to its computed frontier.  Part of the Resolver contract:
/// resolvers receive `FrontierMap<CR> const &` after ComputeFrontiers populates it.
template <typename CostResult>
using FrontierMap = std::unordered_map<EClassId, EClassFrontier<CostResult>>;

/// Selection: one enode chosen per eclass, with its cost.
template <typename CostType>
struct Selection {
  ENodeId enode_id;
  CostType cost;
};

/// Map from EClassId to the enode chosen by a Resolver, with its cost.
template <typename CostType>
using SelectionMap = std::unordered_map<EClassId, Selection<CostType>>;

// ============================================================================
// Resolver contract
// ============================================================================
//
// A Resolver is a stateless functor `r(egraph, frontier_map, root)` that
// returns a SelectionMap.  It chooses one enode per eclass (typically by cost),
// and decides which children of that enode are part of the extracted tree.
//
// Contract on the returned SelectionMap:
//   - root is in the map.
//   - For each (id, sel) in the map, sel.enode_id is a valid enode in eclass id.
//   - For each (id, sel) in the map, every child of sel.enode_id that the
//     resolver wishes to be part of the extracted tree is also in the map.
//   - Children absent from the map are deliberately excluded ("dead").
//
// Downstream stages (CollectDependencies, TopologicalSort) skip absent children
// — that is how the contract surfaces in the rest of the pipeline.
//
// Two production adapters:
//   * DefaultResolver       — walks all children of the chosen enode.
//   * PlanResolver (in query::plan::v2) — Bind-aware; honours alive/dead and
//                                          re-resolves shared eclasses on
//                                          incompatible re-visits.

template <typename R, typename Symbol, typename Analysis, typename CostResult>
concept Resolver =
    CostResultType<CostResult> &&
    std::invocable<R, EGraph<Symbol, Analysis> const &, FrontierMap<CostResult> const &, EClassId> &&
    std::convertible_to<
        std::invoke_result_t<R, EGraph<Symbol, Analysis> const &, FrontierMap<CostResult> const &, EClassId>,
        SelectionMap<typename CostResult::cost_t>>;

/// Generic Resolver that selects each eclass via CostResult::resolve_with_cost
/// and walks every child of the chosen enode.  Safe for any cost model whose
/// children are unconditionally part of the extracted tree.
///
/// NOT safe for cost models with conditional child semantics (e.g., Bind
/// alive/dead in query::plan::v2).  Those need a context-aware resolver such
/// as PlanResolver.
struct DefaultResolver {
  template <typename Symbol, typename Analysis, CostResultType CostResult>
  auto operator()(EGraph<Symbol, Analysis> const &egraph, FrontierMap<CostResult> const &frontier_map,
                  EClassId root) const -> SelectionMap<typename CostResult::cost_t> {
    using CostType = CostResult::cost_t;

    auto resolved = SelectionMap<CostType>{};
    auto to_visit = std::vector{root};
    auto visited = std::unordered_set{root};

    while (!to_visit.empty()) {
      auto current = to_visit.back();
      to_visit.pop_back();

      auto it = frontier_map.find(current);
      assert(it != frontier_map.end() && it->second.has_value());

      auto const &frontier = *it->second;
      auto [enode_id, cost] = CostResult::resolve_with_cost(frontier);
      resolved[current] = Selection<CostType>{enode_id, cost};

      auto const &enode = egraph.get_enode(enode_id);
      for (auto child : enode.children()) {
        if (visited.insert(child).second) {
          to_visit.push_back(child);
        }
      }
    }

    return resolved;
  }
};

// ============================================================================
// Extraction stages — internal, called by Extract().
// ============================================================================
// Production callers should use Extract().  The stages survive in detail:: so
// existing per-stage tests and ConvertToLogicalOperator (which has a
// planner-v2-specific validation step between ComputeFrontiers and resolve)
// can compose them directly.
// TODO: frontier_map and other std::unordered_map parameters could be switched
// to boost::unordered_flat_map for better cache locality, but the type change
// propagates through template signatures to all callers.

namespace detail {

/// In-degree map for topological sorting.
using InDegreeMap = std::unordered_map<EClassId, int>;

/// Bottom-up cost propagation. Calls cost_model(enode, enode_id, children) for each enode,
/// merges results via CostResult::merge across enodes in the same eclass.
template <typename Symbol, typename Analysis, typename CostModel>
  requires CostResultType<typename CostModel::CostResult>
[[nodiscard]] auto ComputeFrontiers(EGraph<Symbol, Analysis> const &egraph, CostModel const &cost_model,
                                    EClassId eclass_id, FrontierMap<typename CostModel::CostResult> &frontier_map)
    -> std::optional<typename CostModel::CostResult> {
  using CostResult = CostModel::CostResult;

  assert(!egraph.needs_rebuild() && "egraph must be rebuilt before extraction");

  if (auto const it = frontier_map.find(eclass_id); it != frontier_map.end()) {
    return it->second;
  }

  auto const &eclass = egraph.eclass(eclass_id);

  // Mark this e-class as "in progress" with nullopt frontier to detect cycles
  frontier_map.emplace(eclass_id, std::nullopt);

  auto merged_frontier = std::optional<CostResult>{};

  auto children_frontiers = std::vector<CostResult>{};
  for (auto const &enode_id : eclass.nodes()) {
    auto const &enode = egraph.get_enode(enode_id);
    children_frontiers.clear();
    auto has_cyclic_child = false;
    for (auto child : enode.children()) {
      auto frontier = ComputeFrontiers(egraph, cost_model, child, frontier_map);
      if (!frontier) {
        has_cyclic_child = true;
        // N.B. intentionally no break — continue processing remaining children
        // so their costs are computed and cached for other extraction paths
      } else {
        children_frontiers.emplace_back(std::move(*frontier));
      }
    }
    if (has_cyclic_child) continue;

    auto enode_frontier = cost_model(enode, enode_id, children_frontiers);

    if (!merged_frontier) {
      merged_frontier = std::move(enode_frontier);
    } else {
      merged_frontier = CostResult::merge(std::move(*merged_frontier), std::move(enode_frontier));
    }
  }

  if (merged_frontier) {
    frontier_map[eclass_id] = *merged_frontier;
    return merged_frontier;
  }

  // All enodes cyclic — remove sentinel
  frontier_map.erase(eclass_id);
  return std::nullopt;
}

template <typename Symbol, typename Analysis, typename CostResult>
[[nodiscard]] auto CollectDependencies(EGraph<Symbol, Analysis> const &egraph,
                                       SelectionMap<CostResult> const &enode_selection, EClassId root) -> InDegreeMap {
  auto in_degree = InDegreeMap{{root, 0}};
  auto bfs = std::vector{root};
  auto visited = std::unordered_set{root};
  bfs.reserve(enode_selection.size());
  visited.reserve(enode_selection.size());

  // Non-recursive BFS search
  while (!bfs.empty()) {
    auto curr = bfs.back();
    bfs.pop_back();

    auto enode_it = enode_selection.find(curr);
    assert(enode_it != enode_selection.end() && "all reachable EClasses should have selected ENode");

    auto const &enode = egraph.get_enode(enode_it->second.enode_id);
    for (auto child : enode.children()) {
      // Only walk children that were resolved — dead Bind's sym/expr are skipped
      // (Resolver contract: absent children are deliberately excluded).
      if (!enode_selection.contains(child)) continue;
      ++in_degree[child];
      if (visited.insert(child).second) {
        bfs.emplace_back(child);
      }
    }
  }
  return in_degree;
}

template <typename Symbol, typename Analysis, typename CostResult>
[[nodiscard]] auto TopologicalSort(EGraph<Symbol, Analysis> const &egraph,
                                   SelectionMap<CostResult> const &enode_selection, InDegreeMap in_degree)
    -> std::vector<std::pair<EClassId, ENodeId>> {
  auto result = std::vector<std::pair<EClassId, ENodeId>>{};
  result.reserve(in_degree.size());

  auto queue = std::queue<EClassId>{};
  for (auto const &[eclass, degree] : in_degree)
    if (degree == 0) queue.emplace(eclass);

  while (!queue.empty()) {
    auto current = queue.front();
    queue.pop();

    auto it = enode_selection.find(current);
    assert(it != enode_selection.end() && "all reachable EClasses should have selected ENode");

    auto enode_id = it->second.enode_id;
    result.emplace_back(current, enode_id);

    auto const &enode = egraph.get_enode(enode_id);
    for (EClassId child : enode.children()) {
      auto deg_it = in_degree.find(child);
      if (deg_it == in_degree.end()) continue;  // resolver excluded child — see Resolver contract
      if (--deg_it->second == 0) {
        queue.emplace(child);
      }
    }
  }

  // Post-condition: all nodes must have been emitted. If not, the input contained a cycle,
  // which means an upstream stage (ComputeFrontiers or the Resolver) admitted a cyclic
  // dependency into the resolved selection — a bug in that stage.
  assert(result.size() == in_degree.size() &&
         "TopologicalSort: cycle detected — resolved selection is not a DAG; "
         "check ComputeFrontiers and the Resolver for upstream bug");

  return result;
}

}  // namespace detail

// ============================================================================
// Extract — single deep entry point
// ============================================================================
//
// The pipeline (frontier-build → resolve → collect-deps → topo-sort) lives
// here as one function so the order, the invariants, and the contract between
// stages are all in one place.  Two customisation points: the `cost_model`
// (CostResultType) and the `resolver` (Resolver).

/// Caller-owned buffer for stage state, reused across Extract() calls.
template <CostResultType CostResult>
struct ExtractionContext {
  FrontierMap<CostResult> frontier_map;
  SelectionMap<typename CostResult::cost_t> selection;
  detail::InDegreeMap in_degree;
  std::vector<std::pair<EClassId, ENodeId>> order;

  void clear() {
    frontier_map.clear();
    selection.clear();
    in_degree.clear();
    order.clear();
  }
};

/// View over ExtractionContext-owned storage.  Valid until the next Extract()
/// call on the same context.
template <CostResultType CostResult>
struct ExtractView {
  std::span<std::pair<EClassId, ENodeId> const> order;
  CostResult::cost_t root_cost;
};

/// Owned extraction result — independent of any ExtractionContext.
/// Returned by the convenience overload that doesn't take a ctx.
template <CostResultType CostResult>
struct ExtractResult {
  std::vector<std::pair<EClassId, ENodeId>> order;
  CostResult::cost_t root_cost;
};

/// Primary entry point.  Caller owns `ctx`; the returned view points into
/// ctx-owned storage and is valid until the next Extract() call on `ctx`.
template <typename Symbol, typename Analysis, typename CostModel, typename ResolverFn>
  requires CostResultType<typename CostModel::CostResult> &&
           Resolver<ResolverFn, Symbol, Analysis, typename CostModel::CostResult>
[[nodiscard]] auto Extract(EGraph<Symbol, Analysis> const &egraph, EClassId root, CostModel const &cost_model,
                           ResolverFn resolver, ExtractionContext<typename CostModel::CostResult> &ctx)
    -> ExtractView<typename CostModel::CostResult> {
  using CostResult = CostModel::CostResult;

  ctx.clear();

  // Stage 1: bottom-up cost propagation.
  (void)detail::ComputeFrontiers(egraph, cost_model, root, ctx.frontier_map);

  // Stage 2: top-down resolution.  Resolver is responsible for the contract
  // documented above (chosen-coverage selection map).
  ctx.selection = resolver(egraph, ctx.frontier_map, root);

  // Stage 3: count in-degrees over the resolver-chosen child set.
  ctx.in_degree = detail::CollectDependencies(egraph, ctx.selection, root);

  // Stage 4: topological sort.
  ctx.order = detail::TopologicalSort(egraph, ctx.selection, std::move(ctx.in_degree));

  auto root_cost = typename CostResult::cost_t{};
  if (auto it = ctx.selection.find(root); it != ctx.selection.end()) {
    root_cost = it->second.cost;
  }

  return ExtractView<CostResult>{ctx.order, root_cost};
}

/// Convenience overload — single-shot, returns owned data.  Suitable for tests
/// and one-shot callers that don't want to manage an ExtractionContext.
template <typename Symbol, typename Analysis, typename CostModel, typename ResolverFn>
  requires CostResultType<typename CostModel::CostResult> &&
           Resolver<ResolverFn, Symbol, Analysis, typename CostModel::CostResult>
[[nodiscard]] auto Extract(EGraph<Symbol, Analysis> const &egraph, EClassId root, CostModel const &cost_model,
                           ResolverFn resolver) -> ExtractResult<typename CostModel::CostResult> {
  using CostResult = CostModel::CostResult;
  ExtractionContext<CostResult> ctx;
  auto view = Extract(egraph, root, cost_model, std::move(resolver), ctx);
  return ExtractResult<CostResult>{std::move(ctx.order), view.root_cost};
}

}  // namespace memgraph::planner::core::extract
