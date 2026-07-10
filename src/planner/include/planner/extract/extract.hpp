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

#include <cassert>
#include <concepts>
#include <cstdint>
#include <ranges>
#include <span>
#include <utility>
#include <vector>

#include <boost/container/small_vector.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

#include "planner/extract/extractor.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::extract {

/// A resolver key uniquely identifies a position to resolve in the e-graph.
/// Every position has a current `eclass`; the rest of the key (scope, demand,
/// etc.) is planner-specific.  The library reads `.eclass` to drive
/// `ComputeFrontiers` and the per-node frontier lookup; everything else is
/// opaque.
template <typename K>
concept ResolverKeyType = requires(K const &k) {
  { k.eclass } -> std::convertible_to<EClassId>;
};

/// Window into `ResolvedBuildContext::child_indices` as offset + count, in the
/// shape `std::span::subspan` consumes directly.  Constructed by
/// `ResolvedBuildContext::append_children`; resolved into a span by
/// `children_of`.
struct ChildSlice {
  std::uint32_t begin = 0;
  std::uint32_t count = 0;
};

/// One entry of the resolver output: the chosen enode plus the CSR slice
/// covering its children in `ResolvedBuildContext::child_indices`.  Each slot
/// in that slice holds the `build_order` index of one child the resolver
/// visited, in resolver-visit order.
struct ResolvedEntry {
  ENodeId enode_id;
  ChildSlice children;
};

/// Non-template half of the resolver's output bundle: the visit-ordered entries
/// plus the CSR child-index buffer.  Downstream consumers (e.g. a builder that
/// walks `build_order` children-before-parents) only need this view of the
/// resolver's result and don't depend on the cost-model or resolver-key types.
///
/// `append_children` and `children_of` are the only intended way to write into
/// or read from `child_indices`; they own the half-open-window invariant.
struct ResolvedBuildContext {
  std::vector<ResolvedEntry> build_order;
  std::vector<std::uint32_t> child_indices;

  void clear() {
    build_order.clear();
    child_indices.clear();
  }

  /// Append `slots` to `child_indices` and return the window they occupy.
  template <std::ranges::input_range R>
    requires std::convertible_to<std::ranges::range_reference_t<R>, std::uint32_t>
  auto append_children(R const &slots) -> ChildSlice {
    auto const begin = static_cast<std::uint32_t>(child_indices.size());
    child_indices.append_range(slots);
    auto const count = static_cast<std::uint32_t>(child_indices.size() - begin);
    return ChildSlice{.begin = begin, .count = count};
  }

  /// View `entry`'s child slots as a span into `child_indices`.
  [[nodiscard]] auto children_of(ResolvedEntry const &entry) const -> std::span<std::uint32_t const> {
    assert(entry.children.begin + std::size_t{entry.children.count} <= child_indices.size() &&
           "CSR child slice out of range");
    return std::span(child_indices).subspan(entry.children.begin, entry.children.count);
  }

  /// The resolver's entries in children-before-parents order.  A `children_of`
  /// slot is an index into this sequence.
  [[nodiscard]] auto resolved_entries() const -> std::span<ResolvedEntry const> { return build_order; }
};

/// Reusable scratch for the `Extract` pipeline.  `clear()` preserves
/// capacity across calls so a long-lived owner amortises allocations.
template <CostResultType CostResult, typename Key, typename KeyHash>
  requires ResolverKeyType<Key>
struct ExtractContext {
  FrontierContext<CostResult> frontier_context;
  ResolvedBuildContext build;
  boost::unordered_flat_map<Key, std::uint32_t, KeyHash> resolver_seen;

  void clear() {
    frontier_context.clear();
    build.clear();
    resolver_seen.clear();
  }
};

/// Run the resolver stage of the extraction pipeline.  Walks the frontier
/// graph in children-before-parents order from `root_key`, emitting one
/// `ResolvedEntry` per visited key into `ctx.build.build_order`.
///
/// Precondition: `ctx.frontier_context` populated by a prior `ComputeFrontiers`;
/// `ctx`'s resolver-side buffers empty on entry.
///
/// `resolve_node(key, frontier_map, egraph, record_child) -> ENodeId` picks
/// the alt and dispatches per-operator child threading by invoking
/// `record_child(child_key)` for each child the entry depends on.

/// Child-key sink handed to a `resolve_node`: `f(child_key)` records one child
/// the entry depends on.  The harness swallows the child's post-order index, so
/// the return is unobservable here and left unconstrained.  Any `VisitChildFn`
/// therefore satisfies `ChildSink`.
template <typename F, typename Key>
concept ChildSink = requires(F &&f, Key key) { std::forward<F>(f)(std::move(key)); };

/// Per-node resolver contract for `Resolve`.  `f(key, frontier_map, egraph,
/// record_child)` must accept a `record_child` **ChildSink** and return the
/// chosen `ENodeId`.
template <typename F, typename Symbol, typename Analysis, typename CostResult, typename Key>
concept ResolveNodeFnType = requires(F &&f, Key const &key, FrontierMap<CostResult> const &frontier_map,
                                     EGraph<Symbol, Analysis> const &egraph) {
  {
    std::forward<F>(f)(
        key, frontier_map, egraph, [](Key const &) { /* no-op sink: only the call's return type is probed */ })
  } -> std::convertible_to<ENodeId>;
};

template <typename Symbol, typename Analysis, typename CostResult, typename Key, typename KeyHash,
          typename ResolveNodeFn>
  requires CostResultType<CostResult> && ResolverKeyType<Key> &&
           ResolveNodeFnType<ResolveNodeFn, Symbol, Analysis, CostResult, Key>
void Resolve(EGraph<Symbol, Analysis> const &egraph, Key const &root_key, ResolveNodeFn &&resolve_node,
             ExtractContext<CostResult, Key, KeyHash> &ctx) {
  assert(ctx.build.build_order.empty() && "ExtractContext::build.build_order must be empty on entry");
  assert(ctx.build.child_indices.empty() && "ExtractContext::build.child_indices must be empty on entry");
  assert(ctx.resolver_seen.empty() && "ExtractContext::resolver_seen must be empty on entry");

  DfsPostOrder(root_key,
               ctx.resolver_seen,
               ctx.build.build_order,
               [&](Key const &key, VisitChildFn<Key> auto visit_child) -> ResolvedEntry {
                 // Per-frame scratch: collect this entry's child indices contiguously
                 // here, then bulk-append to the shared CSR at emit time.  Direct
                 // append-on-visit would interleave with grandchildren's appends.
                 boost::container::small_vector<std::uint32_t, 4> scratch;
                 auto const collect_child = [&](Key child_key) {
                   scratch.push_back(visit_child(std::move(child_key)));
                 };
                 auto const enode_id = resolve_node(key, ctx.frontier_context.frontier_map, egraph, collect_child);
                 return ResolvedEntry{.enode_id = enode_id, .children = ctx.build.append_children(scratch)};
               });
}

}  // namespace memgraph::planner::core::extract
