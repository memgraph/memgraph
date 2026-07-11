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

#include "query/edge_accessor.hpp"
#include "query/exceptions.hpp"
#include "versioning/branch_engine.hpp"

namespace memgraph::query {

// Graph Versioning v1 (lazy diff-context, slice E-1) HIGH-2 FIX (adversarial-review): see each
// declaration's own doc-comment in vertex_accessor.hpp for WHY these are self-correcting (re-
// resolve by gid before reading) and WHY they're out-of-line (BranchContext is only
// forward-declared in the header). Not a mutation -- ResolveVertex never COWs -- so this cannot
// double-apply a COW; it's a plain diff-engine-first FindVertex, same cost class as any other
// point lookup already on this hot path.
auto VertexAccessor::Labels(storage::View view) const -> decltype(impl_.Labels(view)) {
  if (branch_ctx_ != nullptr) {
    if (auto resolved = branch_ctx_->ResolveVertex(impl_.Gid(), view)) {
      return resolved->Labels(view);
    }
  }
  return impl_.Labels(view);
}

storage::Result<bool> VertexAccessor::HasLabel(storage::View view, storage::LabelId label) const {
  if (branch_ctx_ != nullptr) {
    if (auto resolved = branch_ctx_->ResolveVertex(impl_.Gid(), view)) {
      return resolved->HasLabel(label, view);
    }
  }
  return impl_.HasLabel(label, view);
}

auto VertexAccessor::Properties(storage::View view) const -> decltype(impl_.Properties(view)) {
  if (branch_ctx_ != nullptr) {
    if (auto resolved = branch_ctx_->ResolveVertex(impl_.Gid(), view)) {
      return resolved->Properties(view);
    }
  }
  return impl_.Properties(view);
}

storage::Result<storage::PropertyValue> VertexAccessor::GetProperty(storage::View view, storage::PropertyId key) const {
  if (branch_ctx_ != nullptr) {
    if (auto resolved = branch_ctx_->ResolveVertex(impl_.Gid(), view)) {
      return resolved->GetProperty(key, view);
    }
  }
  return impl_.GetProperty(key, view);
}

storage::Result<uint64_t> VertexAccessor::GetPropertySize(storage::PropertyId key, storage::View view) const {
  if (branch_ctx_ != nullptr) {
    if (auto resolved = branch_ctx_->ResolveVertex(impl_.Gid(), view)) {
      return resolved->GetPropertySize(key, view);
    }
  }
  return impl_.GetPropertySize(key, view);
}

// Graph Versioning v1 (lazy diff-context, slice E-1) -- see the header's own doc-comment. Every
// call site (SetProperty/InitProperties/AddLabel/RemoveLabel/UpdateProperties/ClearProperties)
// already checks branch_ctx_ != nullptr before calling this, so branch_ctx_ is guaranteed non-null
// here.
void VertexAccessor::CowIfNeeded() {
  // CowVertex reads the diff-engine accessor from branch_ctx_->current_diff_txn() itself (a single
  // per-query slot, branch_engine.hpp) rather than taking one as a parameter here.
  auto cowed = branch_ctx_->CowVertex(impl_.Gid());
  if (!cowed) {
    throw QueryRuntimeException(cowed.error().message);
  }
  impl_ = *cowed;
}

// Graph Versioning v1, slice E-2a: in branch mode, InEdges/OutEdges must return the UNION of
// historical_'s fork-state incident edges with the diff engine's own (branch-native creates this
// slice) -- `impl_.InEdges`/`impl_.OutEdges` alone only ever sees ONE side (whichever engine `impl_`
// happens to be resident in), silently missing the other -- exactly the same class of gap
// DbAccessor::Vertices(view, label)'s HIGH-3(b) fix closed on the vertex side. Delegates to
// `BranchContext::ResolveEdges` (branch_engine.hpp) for the actual historical-vs-diff merge.
//
// FLAGGED, not fixed (out of scope for this slice): `hops_limit` is NOT wired through
// ResolveEdges/consulted here on the branch path -- ordinary (unlimited) expansion is correct and
// is this slice's whole point, but a hop-limited traversal (`context.hops_limit`, plan/operator.cpp)
// against a checked-out branch would not have its quota decremented/enforced. Mirrors
// DbAccessor::Vertices(view, label, properties, ...)'s own "NOTE (flagged, not fixed)" on IndexOrder
// -- a follow-up slice's problem, not silently miscounted here.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  query::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::IN, view, edge_types);
    std::vector<EdgeAccessor> edges;
    edges.reserve(resolved.size());
    std::ranges::transform(
        resolved, std::back_inserter(edges), [this](auto const &edge) { return EdgeAccessor(edge, branch_ctx_); });
    return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = static_cast<int64_t>(edges.size())};
  }

  auto maybe_result = impl_.InEdges(view, edge_types, nullptr, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  std::ranges::transform(
      (*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

// Branch mode: ResolveEdges has no destination filter (it returns the whole incident set) -- filter
// client-side by gid, comparing against the edge's FROM vertex (the "other side" of an in-edge; see
// storage::vertex_info_helpers.hpp's Edges_ActionMethod, whose destination-match predicate compares
// `delta.vertex_edge.vertex` -- the FROM vertex for an ADD_IN_EDGE delta). Gid is stable across a
// COW (a copy keeps its source's gid, see CowVertex/CowVertex-equivalent-for-edges), so comparing
// raw storage gids here (no re-Resolve needed) is correct regardless of which side either endpoint
// currently resolves through.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  const VertexAccessor &dest,
                                                                  query::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::IN, view, edge_types);
    auto const dest_gid = dest.Gid();
    std::vector<EdgeAccessor> edges;
    for (auto const &edge : resolved) {
      if (edge.FromVertex().Gid() == dest_gid) {
        edges.emplace_back(edge, branch_ctx_);
      }
    }
    return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = static_cast<int64_t>(edges.size())};
  }

  auto maybe_result = impl_.InEdges(view, edge_types, &dest.impl_, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  std::ranges::transform(
      (*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view) const {
  return InEdges(view, {});
}

// Branch-aware -- see InEdges' own doc-comment above (identical rationale, OUT direction).
storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   const std::vector<storage::EdgeTypeId> &edge_types,
                                                                   query::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::OUT, view, edge_types);
    std::vector<EdgeAccessor> edges;
    edges.reserve(resolved.size());
    std::ranges::transform(
        resolved, std::back_inserter(edges), [this](auto const &edge) { return EdgeAccessor(edge, branch_ctx_); });
    return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = static_cast<int64_t>(edges.size())};
  }

  auto maybe_result = impl_.OutEdges(view, edge_types, nullptr, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  std::ranges::transform(
      (*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

// Branch mode: destination filter compares against the edge's TO vertex -- see InEdges(dest)'s own
// doc-comment above (identical rationale, mirrored for OUT: `delta.vertex_edge.vertex` is the TO
// vertex for an ADD_OUT_EDGE delta).
storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   std::vector<storage::EdgeTypeId> const &edge_types,
                                                                   VertexAccessor const &dest,
                                                                   query::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::OUT, view, edge_types);
    auto const dest_gid = dest.Gid();
    std::vector<EdgeAccessor> edges;
    for (auto const &edge : resolved) {
      if (edge.ToVertex().Gid() == dest_gid) {
        edges.emplace_back(edge, branch_ctx_);
      }
    }
    return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = static_cast<int64_t>(edges.size())};
  }

  auto maybe_result = impl_.OutEdges(view, edge_types, &dest.impl_, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  std::ranges::transform(
      (*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view) const {
  return OutEdges(view, {});
}

// Graph Versioning v1, slice E-2a: branch mode counts the resolved historical-vs-diff UNION rather
// than trusting `impl_.InDegree`/`impl_.OutDegree` (which, like the pre-fix InEdges/OutEdges above,
// only ever sees whichever ONE engine `impl_` currently resolves through). Correctness over speed --
// see the header's own doc-comment (this is a full ResolveEdges call, not a dedicated O(1)/O(log)
// counter; a real branch-aware degree index is out of scope here, E-2d).
storage::Result<size_t> VertexAccessor::InDegree(storage::View view) const {
  if (branch_ctx_ != nullptr) {
    return branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::IN, view, {}).size();
  }
  return impl_.InDegree(view);
}

storage::Result<size_t> VertexAccessor::OutDegree(storage::View view) const {
  if (branch_ctx_ != nullptr) {
    return branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::OUT, view, {}).size();
  }
  return impl_.OutDegree(view);
}

}  // namespace memgraph::query
