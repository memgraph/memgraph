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
    // Phase-2 read fast path (branched-bit) -- mirror of InEdges/OutEdges' own fast path in this
    // file. An un-COW'd, un-tombstoned MAIN-resident vertex's state is, by phase 1's invariant,
    // identical to what ResolveVertex would resolve on the historical side: impl_ already IS that
    // historical accessor, so read it directly and skip the diff-engine skiplist probe. Byte-
    // identical to the slow path's return (same Vertex*, same view). SAFE: mutators never consult
    // this bit (they always CowIfNeeded); a COW or tombstone flips branched()->true, disabling this
    // fast path so the re-resolve / View::NEW tombstone guard below runs.
    if (impl_.storage_ != &branch_ctx_->diff_engine() && !impl_.vertex_->branched()) {
      return impl_.Labels(view);
    }
    if (auto resolved = branch_ctx_->ResolveVertex(impl_.Gid(), view)) {
      return resolved->Labels(view);
    }
    // S2 FIX (view-gated tombstone guard): a resolve miss on a tombstoned gid is ambiguous by
    // itself -- it also happens for subqueries.feature:424's incidental View::OLD re-resolution of
    // an object that's alive in the pre-current-command state (main does NOT error there). What
    // discriminates the two is the caller's own `view`: View::NEW is the query's current-command
    // state, where a tombstoned object IS deleted -- matching main's MVCC read-after-delete-in-
    // command error. View::OLD falls through to `impl_` unchanged (still the alive, pre-delete
    // value), preserving the non-erroring OLD case.
    if (view == storage::View::NEW && branch_ctx_->IsVertexTombstoned(impl_.Gid())) {
      return std::unexpected{storage::Error::DELETED_OBJECT};
    }
  }
  return impl_.Labels(view);
}

storage::Result<bool> VertexAccessor::HasLabel(storage::View view, storage::LabelId label) const {
  if (branch_ctx_ != nullptr) {
    // Phase-2 read fast path (branched-bit) -- mirror of InEdges/OutEdges' own fast path in this
    // file. An un-COW'd, un-tombstoned MAIN-resident vertex's state is, by phase 1's invariant,
    // identical to what ResolveVertex would resolve on the historical side: impl_ already IS that
    // historical accessor, so read it directly and skip the diff-engine skiplist probe. Byte-
    // identical to the slow path's return (same Vertex*, same view). SAFE: mutators never consult
    // this bit (they always CowIfNeeded); a COW or tombstone flips branched()->true, disabling this
    // fast path so the re-resolve / View::NEW tombstone guard below runs.
    if (impl_.storage_ != &branch_ctx_->diff_engine() && !impl_.vertex_->branched()) {
      return impl_.HasLabel(label, view);
    }
    if (auto resolved = branch_ctx_->ResolveVertex(impl_.Gid(), view)) {
      return resolved->HasLabel(label, view);
    }
    // S2 FIX (view-gated tombstone guard) -- see Labels() above for the full rationale.
    if (view == storage::View::NEW && branch_ctx_->IsVertexTombstoned(impl_.Gid())) {
      return std::unexpected{storage::Error::DELETED_OBJECT};
    }
  }
  return impl_.HasLabel(label, view);
}

auto VertexAccessor::Properties(storage::View view) const -> decltype(impl_.Properties(view)) {
  if (branch_ctx_ != nullptr) {
    // Phase-2 read fast path (branched-bit) -- mirror of InEdges/OutEdges' own fast path in this
    // file. An un-COW'd, un-tombstoned MAIN-resident vertex's state is, by phase 1's invariant,
    // identical to what ResolveVertex would resolve on the historical side: impl_ already IS that
    // historical accessor, so read it directly and skip the diff-engine skiplist probe. Byte-
    // identical to the slow path's return (same Vertex*, same view). SAFE: mutators never consult
    // this bit (they always CowIfNeeded); a COW or tombstone flips branched()->true, disabling this
    // fast path so the re-resolve / View::NEW tombstone guard below runs.
    if (impl_.storage_ != &branch_ctx_->diff_engine() && !impl_.vertex_->branched()) {
      return impl_.Properties(view);
    }
    if (auto resolved = branch_ctx_->ResolveVertex(impl_.Gid(), view)) {
      return resolved->Properties(view);
    }
    // S2 FIX (view-gated tombstone guard) -- see Labels() above for the full rationale.
    if (view == storage::View::NEW && branch_ctx_->IsVertexTombstoned(impl_.Gid())) {
      return std::unexpected{storage::Error::DELETED_OBJECT};
    }
  }
  return impl_.Properties(view);
}

storage::Result<storage::PropertyValue> VertexAccessor::GetProperty(storage::View view, storage::PropertyId key) const {
  if (branch_ctx_ != nullptr) {
    // Phase-2 read fast path (branched-bit) -- mirror of InEdges/OutEdges' own fast path in this
    // file. An un-COW'd, un-tombstoned MAIN-resident vertex's state is, by phase 1's invariant,
    // identical to what ResolveVertex would resolve on the historical side: impl_ already IS that
    // historical accessor, so read it directly and skip the diff-engine skiplist probe. Byte-
    // identical to the slow path's return (same Vertex*, same view). SAFE: mutators never consult
    // this bit (they always CowIfNeeded); a COW or tombstone flips branched()->true, disabling this
    // fast path so the re-resolve / View::NEW tombstone guard below runs.
    if (impl_.storage_ != &branch_ctx_->diff_engine() && !impl_.vertex_->branched()) {
      return impl_.GetProperty(key, view);
    }
    if (auto resolved = branch_ctx_->ResolveVertex(impl_.Gid(), view)) {
      return resolved->GetProperty(key, view);
    }
    // S2 FIX (view-gated tombstone guard) -- see Labels() above for the full rationale.
    if (view == storage::View::NEW && branch_ctx_->IsVertexTombstoned(impl_.Gid())) {
      return std::unexpected{storage::Error::DELETED_OBJECT};
    }
  }
  return impl_.GetProperty(key, view);
}

storage::Result<uint64_t> VertexAccessor::GetPropertySize(storage::PropertyId key, storage::View view) const {
  if (branch_ctx_ != nullptr) {
    // Phase-2 read fast path (branched-bit) -- mirror of InEdges/OutEdges' own fast path in this
    // file. An un-COW'd, un-tombstoned MAIN-resident vertex's state is, by phase 1's invariant,
    // identical to what ResolveVertex would resolve on the historical side: impl_ already IS that
    // historical accessor, so read it directly and skip the diff-engine skiplist probe. Byte-
    // identical to the slow path's return (same Vertex*, same view). SAFE: mutators never consult
    // this bit (they always CowIfNeeded); a COW or tombstone flips branched()->true, disabling this
    // fast path so the re-resolve / View::NEW tombstone guard below runs.
    if (impl_.storage_ != &branch_ctx_->diff_engine() && !impl_.vertex_->branched()) {
      return impl_.GetPropertySize(key, view);
    }
    if (auto resolved = branch_ctx_->ResolveVertex(impl_.Gid(), view)) {
      return resolved->GetPropertySize(key, view);
    }
    // S2 FIX (view-gated tombstone guard) -- see Labels() above for the full rationale.
    if (view == storage::View::NEW && branch_ctx_->IsVertexTombstoned(impl_.Gid())) {
      return std::unexpected{storage::Error::DELETED_OBJECT};
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
// S3 FIX: `hops_limit` is now threaded through and enforced, mirroring storage's
// VertexAccessor::HandleExpansionsWithEdgeTypes (storage/v2/vertex_accessor.cpp) exactly: ResolveEdges
// is called with an EMPTY edge_types filter so it returns every incident edge, unfiltered; the loop
// then charges one hop per incident edge (IncrementHopsCount) and increments expanded_count BEFORE
// applying the edge_types filter client-side, breaking out once the quota is exhausted. This matches
// main's accounting for getHopsCounter()/USING HOPS LIMIT -- a non-matching-type edge still costs a
// hop, and the returned expanded_count reflects edges visited, not edges kept.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  query::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Phase 2 (read side) of the branched-bit fast path (storage::Vertex::branched(), vertex.hpp):
    // an un-COW'd, un-tombstoned MAIN-resident vertex's incident-edge set is, by phase 1's own
    // invariant, IDENTICAL to what this branch would see via the historical-vs-diff union below --
    // so its edges can be read straight off main's own adjacency list, skipping ResolveEdges'
    // dual lookup (historical_ + current_diff_txn_) and gid seen-map entirely.
    //   - `impl_.storage_ != &branch_ctx_->diff_engine()` tests "not already diff-resident": a
    //     diff-engine-resident impl_ (branch-native create, or a prior COW) is exactly the case the
    //     bit does NOT cover -- the diff engine is the authoritative side for it, so it must still
    //     go through the union below.
    //   - `!impl_.vertex_->branched()` tests the hint itself (a relaxed-atomic, lock-free read --
    //     see branched()'s own doc-comment for why this is safe without the vertex's lock).
    // Reads at View::OLD, mirroring ResolveEdges' own fixed-View::OLD convention for the historical_
    // side (historical_ is a frozen, self-pinned snapshot -- it has no "NEW" relative to this
    // transaction). `hops_limit`/`edge_types` are handed straight to storage's own
    // HandleExpansionsWithEdgeTypes/HandleExpansionsWithoutEdgeTypes (storage/v2/vertex_accessor.cpp),
    // which already charges one hop per incident edge BEFORE the edge_types filter -- byte-identical
    // accounting to the manual loop below, so this cannot regress the S3 hops-limit fix.
    if (impl_.storage_ != &branch_ctx_->diff_engine() && !impl_.vertex_->branched()) {
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

// Branch mode: ResolveEdges has no destination filter (it returns the whole incident set) -- filter
// client-side by gid, comparing against the edge's FROM vertex (the "other side" of an in-edge; see
// storage::vertex_info_helpers.hpp's Edges_ActionMethod, whose destination-match predicate compares
// `delta.vertex_edge.vertex` -- the FROM vertex for an ADD_IN_EDGE delta). Gid is stable across a
// COW (a copy keeps its source's gid, see CowVertex/CowVertex-equivalent-for-edges), so comparing
// raw storage gids here (no re-Resolve needed) is correct regardless of which side either endpoint
// currently resolves through.
//
// S3 FIX: same hops-budget threading as the non-dest InEdges above -- ResolveEdges is called
// unfiltered ({}) so every incident edge is visited and charged a hop; the dest-gid filter and the
// edge_types filter both apply AFTER the hop is counted (post-count `continue`s, not the loop's
// emission gate), matching storage's HandleExpansionsWithEdgeTypes ordering: hop charge, then
// expanded_count++, then destination check, then edge_types check.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  const VertexAccessor &dest,
                                                                  query::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Phase 2 fast path -- see the non-dest InEdges' own doc-comment above for the full rationale
    // (identical main-resident + unbranched test). IMPORTANT DIFFERENCE from that overload: `dest`
    // is NOT handed to `impl_.InEdges` as a destination pointer here, even though storage's own
    // InEdges accepts one -- `dest.impl_` may be DIFF-engine-resident (a branched/COW'd neighbor),
    // in which case its `.transaction_` differs from `impl_.transaction_` (historical_'s own), which
    // would trip storage's own `DMG_ASSERT(!destination || destination->transaction_ ==
    // transaction_, "Invalid accessor!")` (storage/v2/vertex_accessor.cpp) -- and even a same-
    // transaction Vertex* comparison would be wrong regardless, since a COW'd dest is a physically
    // DIFFERENT Vertex object from main's own adjacency-list pointer to it (COW copies props/labels,
    // it never rewrites main's adjacency). So the destination filter is done client-side by GID
    // (matching the slow path's own `edge.FromVertex().Gid() != dest_gid` comparison below) over the
    // type-filtered, hop-truncated list storage already produced -- `edge_types`/`hops_limit` are
    // still safe to hand straight to storage (no pointer-identity hazard for a type id or a hop
    // counter), so hop accounting is still storage's, not re-derived here.
    if (impl_.storage_ != &branch_ctx_->diff_engine() && !impl_.vertex_->branched()) {
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

// Branch-aware -- see InEdges' own doc-comment above (identical rationale, OUT direction, including
// the S3 hops-budget fix: ResolveEdges called unfiltered, hop charged per incident edge before the
// edge_types filter, expanded_count reflecting edges visited not edges kept).
storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   const std::vector<storage::EdgeTypeId> &edge_types,
                                                                   query::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Phase 2 fast path -- see InEdges' own doc-comment above (identical rationale, OUT direction).
    if (impl_.storage_ != &branch_ctx_->diff_engine() && !impl_.vertex_->branched()) {
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

// Branch mode: destination filter compares against the edge's TO vertex -- see InEdges(dest)'s own
// doc-comment above (identical rationale, mirrored for OUT: `delta.vertex_edge.vertex` is the TO
// vertex for an ADD_OUT_EDGE delta).
//
// S3 FIX: same hops-budget threading as InEdges(dest) above -- ResolveEdges called unfiltered ({}),
// hop charged per incident edge before expanded_count++, dest-gid and edge_types filters both applied
// as post-count `continue`s (not the emission gate), matching storage's HandleExpansionsWithEdgeTypes.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   std::vector<storage::EdgeTypeId> const &edge_types,
                                                                   VertexAccessor const &dest,
                                                                   query::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Phase 2 fast path -- see InEdges(dest)'s own doc-comment above (identical rationale + the
    // same "never hand `dest` to storage as a destination pointer" caveat, mirrored for OUT: the
    // client-side gid filter compares against `edge.ToVertex().Gid()`, the TO vertex for an OUT
    // edge, instead of FromVertex()).
    if (impl_.storage_ != &branch_ctx_->diff_engine() && !impl_.vertex_->branched()) {
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
