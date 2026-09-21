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

// Out-of-line because BranchContext is only forward-declared in the header. ResolveVertex never
// COWs, so a read cannot double-apply a COW -- it is a plain diff-engine-first FindVertex.
auto VertexAccessor::Labels(storage::View view) const -> decltype(impl_.Labels(view)) {
  if (branch_ctx_ != nullptr) {
    // (0) Diff-resident (storage_ == diff_engine): own MVCC is authoritative; skip re-resolve.
    // (1) branched() relaxed-load hint: un-branched vertices are fork-identical by phase-1 invariant.
    // (2) Tombstone gate runs before filter/resolve: View::NEW -> DELETED_OBJECT; View::OLD -> impl_
    //     (preserves pre-delete value for subqueries that read OLD after a tombstone).
    // (3) Clear change-filter bit -> definitive skip (monotonic, no false negatives); set -> resolve.
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->IsVertexTombstoned(gid)) {
        if (view == storage::View::NEW) return std::unexpected{storage::Error::DELETED_OBJECT};
        return impl_.Labels(view);
      }
      if (branch_ctx_->MayHaveLabelChange(gid)) {
        if (auto resolved = branch_ctx_->ResolveVertex(gid, view)) {
          return resolved->Labels(view);
        }
      }
    }
  }
  return impl_.Labels(view);
}

storage::Result<bool> VertexAccessor::HasLabel(storage::View view, storage::LabelId label) const {
  if (branch_ctx_ != nullptr) {
    // Phase-2 read fast path -- see Labels() above for the full rationale (LABEL change filter).
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->IsVertexTombstoned(gid)) {
        if (view == storage::View::NEW) return std::unexpected{storage::Error::DELETED_OBJECT};
        return impl_.HasLabel(label, view);
      }
      if (branch_ctx_->MayHaveLabelChange(gid)) {
        if (auto resolved = branch_ctx_->ResolveVertex(gid, view)) {
          return resolved->HasLabel(label, view);
        }
      }
    }
  }
  return impl_.HasLabel(label, view);
}

auto VertexAccessor::Properties(storage::View view) const -> decltype(impl_.Properties(view)) {
  if (branch_ctx_ != nullptr) {
    // Phase-2 read fast path -- see Labels() above for the full rationale (PROPERTY change filter).
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->IsVertexTombstoned(gid)) {
        if (view == storage::View::NEW) return std::unexpected{storage::Error::DELETED_OBJECT};
        return impl_.Properties(view);
      }
      if (branch_ctx_->MayHavePropertyChange(gid)) {
        if (auto resolved = branch_ctx_->ResolveVertex(gid, view)) {
          return resolved->Properties(view);
        }
      }
    }
  }
  return impl_.Properties(view);
}

storage::Result<storage::PropertyValue> VertexAccessor::GetProperty(storage::View view, storage::PropertyId key) const {
  if (branch_ctx_ != nullptr) {
    // Phase-2 read fast path -- see Labels() above. Fine per-property filter: resolves only when
    // THIS property changed on the branch (vs the coarse per-gid filter used by Properties()).
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->IsVertexTombstoned(gid)) {
        if (view == storage::View::NEW) return std::unexpected{storage::Error::DELETED_OBJECT};
        return impl_.GetProperty(key, view);
      }
      if (branch_ctx_->MayHavePropertyFieldChange(gid, key)) {
        if (auto resolved = branch_ctx_->ResolveVertex(gid, view)) {
          return resolved->GetProperty(key, view);
        }
      }
    }
  }
  return impl_.GetProperty(key, view);
}

storage::Result<uint64_t> VertexAccessor::GetPropertySize(storage::PropertyId key, storage::View view) const {
  if (branch_ctx_ != nullptr) {
    // Phase-2 read fast path -- same per-property filter as GetProperty() above.
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->IsVertexTombstoned(gid)) {
        if (view == storage::View::NEW) return std::unexpected{storage::Error::DELETED_OBJECT};
        return impl_.GetPropertySize(key, view);
      }
      if (branch_ctx_->MayHavePropertyFieldChange(gid, key)) {
        if (auto resolved = branch_ctx_->ResolveVertex(gid, view)) {
          return resolved->GetPropertySize(key, view);
        }
      }
    }
  }
  return impl_.GetPropertySize(key, view);
}

// Pre-condition: branch_ctx_ is non-null (all call sites guard on branch_ctx_ != nullptr).
// Out-of-line because BranchContext is only forward-declared in the header.
void VertexAccessor::CowIfNeeded(versioning::BranchChangeKind kind) {
  auto cowed = branch_ctx_->CowVertex(impl_.Gid());
  if (!cowed) {
    throw QueryRuntimeException(cowed.error().message);
  }
  impl_ = *cowed;
  // Always fire after COW: even a vertex already COW'd for a different kind needs this call to flag
  // the new kind. gid is stable across the COW; monotonic+single-writer => happens-before any read.
  branch_ctx_->RecordVertexChange(impl_.Gid(), kind);
}

// Must be called after CowIfNeeded by property mutators. Records pid so GetProperty(pid) can skip
// the resolve when only other pids changed on this vertex. gid is stable across the COW.
void VertexAccessor::RecordPropertyFieldChange(storage::PropertyId pid) {
  branch_ctx_->RecordPropertyFieldChange(impl_.Gid(), pid);
}

void VertexAccessor::RecordPropertyFieldChanges(
    const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  const auto gid = impl_.Gid();
  for (const auto &[pid, _] : properties) {
    branch_ctx_->RecordPropertyFieldChange(gid, pid);
  }
}

// Branch mode returns the union of historical_ (fork-state) and diff-engine edges; impl_.InEdges
// alone sees only one side. ResolveEdges is unfiltered; hops charged before the edge_types filter.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // View::OLD mirrors ResolveEdges' convention for the frozen historical_ snapshot.
    // TOMBSTONE SAFETY: detach-delete flags both endpoints' edge bit via CowEdge -> MayHaveEdgeChange
    // -> this fast path is skipped -> ResolveEdges (which filters tombstoned_edges_) runs instead.
    if (impl_.storage_ != &branch_ctx_->diff_engine() &&
        (!impl_.vertex_->branched() || !branch_ctx_->MayHaveEdgeChange(impl_.Gid()))) {
      auto maybe_result = impl_.InEdges(storage::View::OLD, edge_types, nullptr, hops_limit);
      if (!maybe_result) return std::unexpected{maybe_result.error()};

      std::vector<EdgeAccessor> edges;
      edges.reserve((*maybe_result).edges.size());
      std::ranges::transform((*maybe_result).edges, std::back_inserter(edges), [this](auto const &edge) {
        return EdgeAccessor(edge, branch_ctx_);
      });

      return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = (*maybe_result).expanded_count};
    }

    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::IN, view, {});
    std::vector<EdgeAccessor> edges;
    int64_t expanded_count = 0;
    for (auto const &edge : resolved) {
      if (hops_limit != nullptr && hops_limit->IsUsed()) {
        if (hops_limit->IncrementHopsCount() == 0) break;  // quota exhausted -> truncate
      }
      ++expanded_count;
      if (!edge_types.empty() && !std::ranges::contains(edge_types, edge.EdgeType())) continue;
      edges.emplace_back(edge, branch_ctx_);
    }
    return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = expanded_count};
  }

  auto maybe_result = impl_.InEdges(view, edge_types, nullptr, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  std::ranges::transform(
      (*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

// Destination filter is client-side by gid (FromVertex for an IN edge); gid is stable across COW
// so no re-resolve is needed. ResolveEdges is unfiltered; hops charged before the dest/type filters.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  const VertexAccessor &dest,
                                                                  storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Phase 2 fast path -- see non-dest InEdges above. NEVER pass `dest` to impl_.InEdges:
    // a COW'd dest.impl_ lives in a different transaction, tripping storage's DMG_ASSERT.
    if (impl_.storage_ != &branch_ctx_->diff_engine() &&
        (!impl_.vertex_->branched() || !branch_ctx_->MayHaveEdgeChange(impl_.Gid()))) {
      auto maybe_result = impl_.InEdges(storage::View::OLD, edge_types, nullptr, hops_limit);
      if (!maybe_result) return std::unexpected{maybe_result.error()};

      auto const dest_gid = dest.Gid();
      std::vector<EdgeAccessor> edges;
      edges.reserve((*maybe_result).edges.size());
      for (auto const &edge : (*maybe_result).edges) {
        if (edge.FromVertex().Gid() != dest_gid) continue;
        edges.emplace_back(edge, branch_ctx_);
      }
      return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = (*maybe_result).expanded_count};
    }

    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::IN, view, {});
    auto const dest_gid = dest.Gid();
    std::vector<EdgeAccessor> edges;
    int64_t expanded_count = 0;
    for (auto const &edge : resolved) {
      if (hops_limit != nullptr && hops_limit->IsUsed()) {
        if (hops_limit->IncrementHopsCount() == 0) break;  // quota exhausted -> truncate
      }
      ++expanded_count;
      if (edge.FromVertex().Gid() != dest_gid) continue;
      if (!edge_types.empty() && !std::ranges::contains(edge_types, edge.EdgeType())) continue;
      edges.emplace_back(edge, branch_ctx_);
    }
    return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = expanded_count};
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

// Branch-aware -- see InEdges above (identical rationale, OUT direction).
storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   const std::vector<storage::EdgeTypeId> &edge_types,
                                                                   storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Phase 2 fast path -- see InEdges above (identical rationale, OUT direction).
    if (impl_.storage_ != &branch_ctx_->diff_engine() &&
        (!impl_.vertex_->branched() || !branch_ctx_->MayHaveEdgeChange(impl_.Gid()))) {
      auto maybe_result = impl_.OutEdges(storage::View::OLD, edge_types, nullptr, hops_limit);
      if (!maybe_result) return std::unexpected{maybe_result.error()};

      std::vector<EdgeAccessor> edges;
      edges.reserve((*maybe_result).edges.size());
      std::ranges::transform((*maybe_result).edges, std::back_inserter(edges), [this](auto const &edge) {
        return EdgeAccessor(edge, branch_ctx_);
      });

      return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = (*maybe_result).expanded_count};
    }

    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::OUT, view, {});
    std::vector<EdgeAccessor> edges;
    int64_t expanded_count = 0;
    for (auto const &edge : resolved) {
      if (hops_limit != nullptr && hops_limit->IsUsed()) {
        if (hops_limit->IncrementHopsCount() == 0) break;  // quota exhausted -> truncate
      }
      ++expanded_count;
      if (!edge_types.empty() && !std::ranges::contains(edge_types, edge.EdgeType())) continue;
      edges.emplace_back(edge, branch_ctx_);
    }
    return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = expanded_count};
  }

  auto maybe_result = impl_.OutEdges(view, edge_types, nullptr, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  std::ranges::transform(
      (*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

// Destination filter uses ToVertex().Gid() (TO vertex for ADD_OUT_EDGE) -- mirrors InEdges(dest)
// above with FromVertex(). ResolveEdges unfiltered; hops charged before dest/type filters.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   std::vector<storage::EdgeTypeId> const &edge_types,
                                                                   VertexAccessor const &dest,
                                                                   storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Phase 2 fast path -- see InEdges(dest) above. Client-side filter uses ToVertex() (not FromVertex()).
    if (impl_.storage_ != &branch_ctx_->diff_engine() &&
        (!impl_.vertex_->branched() || !branch_ctx_->MayHaveEdgeChange(impl_.Gid()))) {
      auto maybe_result = impl_.OutEdges(storage::View::OLD, edge_types, nullptr, hops_limit);
      if (!maybe_result) return std::unexpected{maybe_result.error()};

      auto const dest_gid = dest.Gid();
      std::vector<EdgeAccessor> edges;
      edges.reserve((*maybe_result).edges.size());
      for (auto const &edge : (*maybe_result).edges) {
        if (edge.ToVertex().Gid() != dest_gid) continue;
        edges.emplace_back(edge, branch_ctx_);
      }
      return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = (*maybe_result).expanded_count};
    }

    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::OUT, view, {});
    auto const dest_gid = dest.Gid();
    std::vector<EdgeAccessor> edges;
    int64_t expanded_count = 0;
    for (auto const &edge : resolved) {
      if (hops_limit != nullptr && hops_limit->IsUsed()) {
        if (hops_limit->IncrementHopsCount() == 0) break;  // quota exhausted -> truncate
      }
      ++expanded_count;
      if (edge.ToVertex().Gid() != dest_gid) continue;
      if (!edge_types.empty() && !std::ranges::contains(edge_types, edge.EdgeType())) continue;
      edges.emplace_back(edge, branch_ctx_);
    }
    return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = expanded_count};
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

// Branch mode uses a full ResolveEdges call for correctness: impl_.InDegree/OutDegree only sees
// one engine side. A dedicated branch-aware degree counter is a future optimization.
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
