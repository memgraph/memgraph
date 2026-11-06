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

#include "query/vertex_accessor.hpp"

#include <ranges>
#include <utility>

#include "query/edge_accessor.hpp"
#ifdef MG_ENTERPRISE
#include <algorithm>
#include <cstdint>
#include <map>
#include <tuple>
#include <vector>

#include "storage/v2/indices/active_indices.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "versioning/branch_engine.hpp"
#endif

namespace memgraph::query {

static_assert(std::ranges::forward_range<ExpandedEdges>);
static_assert(std::ranges::sized_range<ExpandedEdges>);

namespace {

/// The same expansion as the accessors this layer deals in. The edges move across whole, so this
/// changes their type and not what was found.
auto AsQueryResult(storage::EdgesVertexAccessorResult &&result) -> ExpandedEdgesResult {
  return ExpandedEdgesResult{.edges = ExpandedEdges{std::move(result.edges)}, .expanded_count = result.expanded_count};
}

}  // namespace

#ifdef MG_ENTERPRISE
storage::Result<storage::VertexKey> VertexAccessor::BranchLabels(storage::View view) const {
  return branch_ctx()->Labels(impl_, view);
}

storage::Result<bool> VertexAccessor::BranchHasLabel(storage::View view, storage::LabelId label) const {
  return branch_ctx()->HasLabel(impl_, label, view);
}

storage::Result<bool> VertexAccessor::BranchHasAnyLabel(storage::View view) const {
  return branch_ctx()->HasAnyLabel(impl_, view);
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> VertexAccessor::BranchProperties(
    storage::View view) const {
  return branch_ctx()->Properties(impl_, view);
}

storage::Result<storage::PropertyValue> VertexAccessor::BranchGetProperty(storage::View view,
                                                                          storage::PropertyId key) const {
  return branch_ctx()->GetProperty(impl_, key, view);
}

storage::Result<uint64_t> VertexAccessor::BranchGetPropertySize(storage::PropertyId key, storage::View view) const {
  return branch_ctx()->GetPropertySize(impl_, key, view);
}

storage::Result<void> VertexAccessor::BranchReadPropertyValues(storage::OrderedPropertyPaths const &ordered_properties,
                                                               std::span<storage::PropertyValue> out,
                                                               storage::View view) const {
  return branch_ctx()->ReadPropertyValues(impl_, ordered_properties, out, view);
}

storage::Result<bool> VertexAccessor::BranchAddLabel(storage::LabelId label) {
  return branch_ctx()->AddLabel(impl_, label);
}

storage::Result<bool> VertexAccessor::BranchRemoveLabel(storage::LabelId label) {
  return branch_ctx()->RemoveLabel(impl_, label);
}

storage::Result<storage::PropertyValue> VertexAccessor::BranchSetProperty(storage::PropertyId key,
                                                                          const storage::PropertyValue &value) {
  return branch_ctx()->SetProperty(impl_, key, value);
}

storage::Result<bool> VertexAccessor::BranchInitProperties(
    std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  return branch_ctx()->InitProperties(impl_, properties);
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
VertexAccessor::BranchUpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) const {
  return branch_ctx()->UpdateProperties(impl_, properties);
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> VertexAccessor::BranchClearProperties() {
  return branch_ctx()->ClearProperties(impl_);
}

// A native vertex is the diff engine's own object, so its flag says; a fork vertex's flag is main's, not the branch's.
bool VertexAccessor::BranchIsDeleted() const {
  if (impl_.storage_ == &branch_ctx()->diff_engine()) return impl_.vertex_->deleted();
  return branch_ctx()->IsForkVertexDeleted(impl_, storage::View::NEW);
}

std::vector<storage::PropertyId> VertexAccessor::BranchVectorIndexedProperties(
    std::span<storage::LabelId const> labels) const {
  if (impl_.storage_ != &branch_ctx()->diff_engine()) return impl_.VectorIndexedProperties(labels);
  return branch_ctx()->historical().GetTransaction()->active_indices_->vector_->IndexedProperties(labels);
}

// The union of the fork and diff-engine edges, not a cheaper count.
storage::Result<size_t> VertexAccessor::BranchDegree(storage::EdgeDirection direction, storage::View view) const {
  auto edges = direction == storage::EdgeDirection::IN ? branch_ctx()->InEdges(impl_, view)
                                                       : branch_ctx()->OutEdges(impl_, view);
  return edges.transform([](const auto &resolved) { return resolved.size(); });
}

// Never pass `dest` to impl_.{In,Out}Edges: a stub dest.impl_ belongs to another transaction (storage DMG_ASSERT), so
// the destination is compared by gid. Where impl_ already holds the vertex's whole branch adjacency, its own typed,
// hops-limited expansion runs (charged as on main); otherwise the branch's edges are unfiltered, so hops are charged
// before the edge_types/dest filters.
storage::Result<ExpandedEdgesResult> VertexAccessor::BranchEdges(storage::EdgeDirection direction, storage::View view,
                                                                 const std::vector<storage::EdgeTypeId> &edge_types,
                                                                 const VertexAccessor *dest,
                                                                 storage::HopsLimit *hops_limit) const {
  auto const dest_gid = dest != nullptr ? dest->Gid() : storage::kInvalidGid;
  auto const far_end = [direction](auto const &edge) {
    return direction == storage::EdgeDirection::IN ? edge.FromVertex() : edge.ToVertex();
  };

  std::vector<EdgeAccessor> edges;
  if (auto const direct = branch_ctx()->DirectExpansionView(impl_, direction, view)) {
    auto expanded = direction == storage::EdgeDirection::IN ? impl_.InEdges(*direct, edge_types, nullptr, hops_limit)
                                                            : impl_.OutEdges(*direct, edge_types, nullptr, hops_limit);
    if (!expanded) return std::unexpected{expanded.error()};
    // Wrapped as it is looked at, like main's expansion, unless a destination filter has to look at every edge now.
    if (dest == nullptr) {
      return ExpandedEdgesResult{.edges = ExpandedEdges{std::move(expanded->edges)},
                                 .expanded_count = expanded->expanded_count};
    }
    for (auto const &edge : expanded->edges) {
      if (far_end(edge).Gid() != dest_gid) continue;
      edges.emplace_back(edge);
    }
    return ExpandedEdgesResult{.edges = ExpandedEdges{std::move(edges)}, .expanded_count = expanded->expanded_count};
  }

  int64_t expanded_count = 0;
  auto visited = branch_ctx()->VisitEdges(
      impl_,
      direction,
      view,
      [&](size_t count) { edges.reserve(count); },
      [&](const storage::EdgeAccessor &edge) {
        if (hops_limit != nullptr && hops_limit->IsUsed()) {
          if (hops_limit->IncrementHopsCount() == 0) return false;
        }
        ++expanded_count;
        if (dest != nullptr && far_end(edge).Gid() != dest_gid) return true;
        if (!edge_types.empty() && !std::ranges::contains(edge_types, edge.EdgeType())) return true;
        edges.emplace_back(edge);
        return true;
      });
  if (!visited) return std::unexpected{visited.error()};
  return ExpandedEdgesResult{.edges = ExpandedEdges{std::move(edges)}, .expanded_count = expanded_count};
}

#endif
bool VertexAccessor::IsDeleted() const {
#ifdef MG_ENTERPRISE
  if (branch_ctx() != nullptr) [[unlikely]]
    return BranchIsDeleted();
#endif
  return impl_.vertex_->deleted();
}

storage::Result<ExpandedEdgesResult> VertexAccessor::InEdges(storage::View view) const { return InEdges(view, {}); }

storage::Result<ExpandedEdgesResult> VertexAccessor::InEdges(storage::View view,
                                                             const std::vector<storage::EdgeTypeId> &edge_types,
                                                             storage::HopsLimit *hops_limit) const {
#ifdef MG_ENTERPRISE
  if (branch_ctx() != nullptr) [[unlikely]]
    return BranchEdges(storage::EdgeDirection::IN, view, edge_types, nullptr, hops_limit);
#endif
  return impl_.InEdges(view, edge_types, nullptr, hops_limit).transform(AsQueryResult);
}

storage::Result<ExpandedEdgesResult> VertexAccessor::InEdges(storage::View view,
                                                             const std::vector<storage::EdgeTypeId> &edge_types,
                                                             const VertexAccessor &dest,
                                                             storage::HopsLimit *hops_limit) const {
#ifdef MG_ENTERPRISE
  if (branch_ctx() != nullptr) [[unlikely]]
    return BranchEdges(storage::EdgeDirection::IN, view, edge_types, &dest, hops_limit);
#endif
  return impl_.InEdges(view, edge_types, &dest.impl_, hops_limit).transform(AsQueryResult);
}

storage::Result<ExpandedEdgesResult> VertexAccessor::OutEdges(storage::View view,
                                                              const std::vector<storage::EdgeTypeId> &edge_types,
                                                              storage::HopsLimit *hops_limit) const {
#ifdef MG_ENTERPRISE
  if (branch_ctx() != nullptr) [[unlikely]]
    return BranchEdges(storage::EdgeDirection::OUT, view, edge_types, nullptr, hops_limit);
#endif
  return impl_.OutEdges(view, edge_types, nullptr, hops_limit).transform(AsQueryResult);
}

storage::Result<ExpandedEdgesResult> VertexAccessor::OutEdges(storage::View view,
                                                              const std::vector<storage::EdgeTypeId> &edge_types,
                                                              const VertexAccessor &dest,
                                                              storage::HopsLimit *hops_limit) const {
#ifdef MG_ENTERPRISE
  if (branch_ctx() != nullptr) [[unlikely]]
    return BranchEdges(storage::EdgeDirection::OUT, view, edge_types, &dest, hops_limit);
#endif
  return impl_.OutEdges(view, edge_types, &dest.impl_, hops_limit).transform(AsQueryResult);
}

storage::Result<ExpandedEdgesResult> VertexAccessor::OutEdges(storage::View view) const { return OutEdges(view, {}); }

}  // namespace memgraph::query
