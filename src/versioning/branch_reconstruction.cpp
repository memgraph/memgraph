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

#include "versioning/branch_reconstruction.hpp"

#include "storage/v2/view.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::versioning {

namespace {

// Free helpers (not BranchReconstruction members) so the doubly-nested UnionVerticesIterable::
// Iterator can call them without relying on nested-class access-through-two-levels-of-nesting --
// they only ever touch plain VertexAccessor/Vertex/EdgeAccessor/Edge/BranchOverlay objects passed
// in explicitly, nothing BranchReconstruction-private.

BranchReconstruction::ReconstructedVertex MaterializeMainVertex(const storage::VertexAccessor &vertex) {
  auto labels_res = vertex.Labels(storage::View::OLD);
  auto props_res = vertex.Properties(storage::View::OLD);
  if (!labels_res.has_value() || !props_res.has_value()) {
    throw utils::BasicException(
        "Failed to read fork-state labels/properties for vertex {} while reconstructing branch view.",
        vertex.Gid().AsUint());
  }
  return BranchReconstruction::ReconstructedVertex{
      .gid = vertex.Gid(),
      .labels = std::vector<storage::LabelId>(labels_res->begin(), labels_res->end()),
      .properties = std::move(*props_res),
      .source = BranchReconstruction::Source::kMain};
}

BranchReconstruction::ReconstructedVertex MaterializeOverlayVertex(const storage::Vertex &vertex) {
  return BranchReconstruction::ReconstructedVertex{
      .gid = vertex.gid,
      .labels = std::vector<storage::LabelId>(vertex.labels.begin(), vertex.labels.end()),
      .properties = vertex.properties.Properties(),
      .source = BranchReconstruction::Source::kOverlay};
}

BranchReconstruction::ReconstructedEdge MaterializeMainEdge(const storage::EdgeAccessor &edge) {
  auto props_res = edge.Properties(storage::View::OLD);
  if (!props_res.has_value()) {
    throw utils::BasicException("Failed to read fork-state properties for edge {} while reconstructing branch view.",
                                edge.Gid().AsUint());
  }
  return BranchReconstruction::ReconstructedEdge{.gid = edge.Gid(),
                                                 .edge_type = edge.EdgeType(),
                                                 .from_vertex = edge.FromVertex().Gid(),
                                                 .to_vertex = edge.ToVertex().Gid(),
                                                 .properties = std::move(*props_res),
                                                 .source = BranchReconstruction::Source::kMain};
}

BranchReconstruction::ReconstructedEdge MaterializeOverlayEdge(BranchOverlay &overlay, storage::Gid gid,
                                                               const storage::Edge &edge) {
  const auto *endpoints = overlay.LookupEdgeEndpoints(gid);
  if (endpoints == nullptr) {
    throw utils::BasicException("Overlay edge {} is missing its endpoint/type side-table entry -- corrupt overlay.",
                                gid.AsUint());
  }
  return BranchReconstruction::ReconstructedEdge{.gid = gid,
                                                 .edge_type = endpoints->edge_type,
                                                 .from_vertex = endpoints->from_vertex,
                                                 .to_vertex = endpoints->to_vertex,
                                                 .properties = edge.properties.Properties(),
                                                 .source = BranchReconstruction::Source::kOverlay};
}

}  // namespace

BranchReconstruction::BranchReconstruction(std::unique_ptr<storage::Storage::Accessor> historical,
                                           storage::NameIdMapper *name_id_mapper)
    : historical_(std::move(historical)), overlay_(name_id_mapper) {}

std::expected<std::unique_ptr<BranchReconstruction>, storage::InMemoryStorage::HistoricalAccessError>
BranchReconstruction::Open(storage::InMemoryStorage &main, uint64_t fork_ts,
                           const std::vector<storage::durability::WalDeltaData> &changelog,
                           storage::NameIdMapper *name_id_mapper) {
  auto historical = main.HistoricalAccess(fork_ts);
  if (!historical.has_value()) {
    return std::unexpected(historical.error());
  }

  // Private ctor -- constructed via `new` rather than std::make_unique (which needs public ctor
  // access), from inside this static member function which does have that access.
  auto reconstruction =
      std::unique_ptr<BranchReconstruction>(new BranchReconstruction(std::move(*historical), name_id_mapper));
  // Reuses chunk 5a's Materialize unchanged, against the SAME historical accessor this
  // reconstruction now owns -- see BranchOverlay::Materialize's own doc-comment for the COW
  // contract (R35: fork_base is read-only).
  reconstruction->overlay_.Materialize(changelog, *reconstruction->historical_);
  return reconstruction;
}

BranchReconstruction::UnionVerticesIterable::UnionVerticesIterable(
    storage::VerticesIterable main_vertices, utils::SkipListDb<storage::Vertex>::Accessor overlay_accessor,
    BranchOverlay *overlay)
    : main_vertices_(std::move(main_vertices)), overlay_accessor_(std::move(overlay_accessor)), overlay_(overlay) {}

BranchReconstruction::UnionVerticesIterable::Iterator BranchReconstruction::UnionVerticesIterable::begin() {
  return Iterator(
      main_vertices_.begin(), main_vertices_.end(), overlay_accessor_.begin(), overlay_accessor_.end(), overlay_);
}

BranchReconstruction::UnionVerticesIterable::Iterator::Iterator(
    storage::VerticesIterable::Iterator main_it, storage::VerticesIterable::Iterator main_end,
    utils::SkipListDb<storage::Vertex>::Iterator overlay_it, utils::SkipListDb<storage::Vertex>::Iterator overlay_end,
    BranchOverlay *overlay)
    : main_it_(std::move(main_it)),
      main_end_(std::move(main_end)),
      overlay_it_(overlay_it),
      overlay_end_(overlay_end),
      overlay_(overlay) {
  SeekNext();
}

BranchReconstruction::UnionVerticesIterable::Iterator &
BranchReconstruction::UnionVerticesIterable::Iterator::operator++() {
  SeekNext();
  return *this;
}

// The streaming gid-ordered merge -- see the class comment in branch_reconstruction.hpp for the
// full O(N+O) argument and the reconciliation rules. Every branch of this function advances at
// least one of the two cursors, so the loop terminates after at most N+O total iterations
// (including the skipped-tombstone continues, since each of those also strictly advances
// `main_it_`).
void BranchReconstruction::UnionVerticesIterable::Iterator::SeekNext() {
  while (true) {
    const bool main_has = !(main_it_ == main_end_);
    const bool overlay_has = !(overlay_it_ == overlay_end_);

    if (!main_has && !overlay_has) {
      done_ = true;
      return;
    }

    if (main_has && overlay_has) {
      const auto main_gid = (*main_it_).Gid();
      const auto overlay_gid = overlay_it_->gid;

      if (main_gid < overlay_gid) {
        // Main-only for this gid (overlay hasn't reached it, and per the class comment an
        // overlay-resident entry can never share a gid with a still-pending tombstone -- so this
        // status check is the only thing that can turn a main-side row into a skip).
        const bool tombstoned = overlay_->LookupVertex(main_gid) == BranchOverlay::Status::kTombstoned;
        auto vertex_copy = *main_it_;  // VertexAccessor is a cheap (few pointers) value type --
                                       // copying it off the iterable's internal cursor before
                                       // advancing is required, not optional (see AllVerticesIterable::
                                       // Iterator::operator*, which returns a reference into a
                                       // member re-used by the NEXT ++).
        ++main_it_;
        if (tombstoned) continue;
        current_ = MaterializeMainVertex(vertex_copy);
        return;
      }

      if (overlay_gid < main_gid) {
        current_ = MaterializeOverlayVertex(*overlay_it_);
        ++overlay_it_;
        return;
      }

      // Tie: same gid on both sides. The overlay's copy (created-here or COW'd-and-modified) IS
      // the branch's authoritative view of this gid -- it wins outright; main's fork-state copy
      // is superseded wholesale, never field-merged.
      current_ = MaterializeOverlayVertex(*overlay_it_);
      ++main_it_;
      ++overlay_it_;
      return;
    }

    if (main_has) {
      const auto main_gid = (*main_it_).Gid();
      const bool tombstoned = overlay_->LookupVertex(main_gid) == BranchOverlay::Status::kTombstoned;
      auto vertex_copy = *main_it_;
      ++main_it_;
      if (tombstoned) continue;
      current_ = MaterializeMainVertex(vertex_copy);
      return;
    }

    // overlay_has only.
    current_ = MaterializeOverlayVertex(*overlay_it_);
    ++overlay_it_;
    return;
  }
}

BranchReconstruction::UnionVerticesIterable BranchReconstruction::UnionVertices() {
  return UnionVerticesIterable(historical_->Vertices(storage::View::OLD), overlay_.VerticesAccessor(), &overlay_);
}

std::optional<BranchReconstruction::ReconstructedVertex> BranchReconstruction::FindVertex(storage::Gid gid) {
  storage::Vertex *overlay_vertex = nullptr;
  switch (overlay_.LookupVertex(gid, &overlay_vertex)) {
    case BranchOverlay::Status::kPresent:
      return MaterializeOverlayVertex(*overlay_vertex);
    case BranchOverlay::Status::kTombstoned:
      return std::nullopt;
    case BranchOverlay::Status::kAbsent:
      break;
  }

  auto vertex = historical_->FindVertex(gid, storage::View::OLD);
  if (!vertex) return std::nullopt;
  return MaterializeMainVertex(*vertex);
}

std::optional<BranchReconstruction::ReconstructedEdge> BranchReconstruction::FindEdge(storage::Gid gid) {
  storage::Edge *overlay_edge = nullptr;
  switch (overlay_.LookupEdge(gid, &overlay_edge)) {
    case BranchOverlay::Status::kPresent:
      return MaterializeOverlayEdge(overlay_, gid, *overlay_edge);
    case BranchOverlay::Status::kTombstoned:
      return std::nullopt;
    case BranchOverlay::Status::kAbsent:
      break;
  }

  auto edge = historical_->FindEdge(gid, storage::View::OLD);
  if (!edge) return std::nullopt;
  return MaterializeMainEdge(*edge);
}

storage::durability::WalTxnEndPos CaptureBranchCommit(BranchLog &branch_log, const storage::Transaction &transaction,
                                                      storage::Storage *target_storage, uint64_t commit_timestamp) {
  using storage::Delta;
  using storage::PreviousPtr;

  for (const auto &delta : transaction.deltas) {
    // ADD_IN_EDGE/REMOVE_IN_EDGE are the vertex-side mirror of an edge's ADD_OUT_EDGE/
    // REMOVE_OUT_EDGE half and carry no independent WAL record of their own -- mirrors main's own
    // commit-time owner-resolution (inmemory/storage.cpp's `append_deltas`).
    if (delta.action == Delta::Action::ADD_IN_EDGE || delta.action == Delta::Action::REMOVE_IN_EDGE) {
      continue;
    }

    // Resolve the delta's true owner by walking `prev` past any intermediate DELTA links -- the
    // same walk main's real commit-time WAL append performs.
    auto owner = delta.prev.Get();
    while (owner.type == PreviousPtr::Type::DELTA) {
      owner = owner.delta->prev.Get();
    }

    if (owner.type == PreviousPtr::Type::VERTEX) {
      branch_log.AppendDelta(delta, owner.vertex, commit_timestamp, target_storage);
    } else if (owner.type == PreviousPtr::Type::EDGE) {
      // Only SET_PROPERTY is WAL-encoded for edges -- edge create/delete is captured entirely
      // through the endpoint vertices' ADD_OUT_EDGE/RECREATE_OBJECT/DELETE_OBJECT deltas instead.
      if (delta.action != Delta::Action::SET_PROPERTY) continue;
      auto *edge = owner.edge;
      // Same cache main's own commit path reads (transaction.hpp's EdgeSetPropertyInfo) -- not a
      // test-only invention.
      auto info = transaction.GetEdgeSetPropertyInfo(edge->gid);
      branch_log.AppendDelta(delta, edge, commit_timestamp, target_storage, info.in_vertex_gid, info.edge_type_id);
    }
    // DELTA/NULL_PTR owners: allocation failed or otherwise unresolved -- nothing to capture,
    // mirrors main's own handling of the same cases.
  }

  return branch_log.AppendTransactionEnd(commit_timestamp);
}

}  // namespace memgraph::versioning
