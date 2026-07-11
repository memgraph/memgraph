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

storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  query::HopsLimit *hops_limit) const {
  auto maybe_result = impl_.InEdges(view, edge_types, nullptr, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  std::ranges::transform(
      (*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  const VertexAccessor &dest,
                                                                  query::HopsLimit *hops_limit) const {
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

storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   const std::vector<storage::EdgeTypeId> &edge_types,
                                                                   query::HopsLimit *hops_limit) const {
  auto maybe_result = impl_.OutEdges(view, edge_types, nullptr, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  std::ranges::transform(
      (*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   std::vector<storage::EdgeTypeId> const &edge_types,
                                                                   VertexAccessor const &dest,
                                                                   query::HopsLimit *hops_limit) const {
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

}  // namespace memgraph::query
