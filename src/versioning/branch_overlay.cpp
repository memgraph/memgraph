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

#include "versioning/branch_overlay.hpp"

#include <algorithm>

#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::versioning {

namespace {
using storage::durability::WalDeltaData;
}  // namespace

storage::Vertex *BranchOverlay::EnsureOverlayVertex(storage::Gid gid, storage::Storage::Accessor &fork_base) {
  auto acc = vertices_.access();
  auto it = acc.find(gid);
  if (it != acc.end()) {
    return &*it;
  }

  // A mutation record (SetProperty/AddLabel/RemoveLabel) targeting a gid the change-log has
  // already deleted -- and not since recreated, which would have erased the tombstone (see
  // WalVertexCreate below) -- is a change-log anomaly: there is no live object left to mutate.
  // Treated the same as the duplicate-create case below: throw rather than silently re-COW a
  // now-unreachable object.
  if (deleted_vertices_.contains(gid)) {
    throw utils::BasicException(
        "Branch change-log mutates vertex {} after it was deleted (and not recreated) -- corrupt change-log.",
        gid.AsUint());
  }

  // First touch of this vertex by the branch: COW its fork-state OUT of main (read-only, R35) --
  // main's own Vertex object is never written.
  auto v_acc = fork_base.FindVertex(gid, storage::View::OLD);
  if (!v_acc) {
    throw utils::BasicException(
        "Branch change-log references vertex {} that is neither overlay-resident nor present in the fork-state "
        "base -- corrupt change-log or wrong fork_base.",
        gid.AsUint());
  }
  auto labels_res = v_acc->Labels(storage::View::OLD);
  auto props_res = v_acc->Properties(storage::View::OLD);
  if (!labels_res.has_value() || !props_res.has_value()) {
    throw utils::BasicException("Failed to read fork-state labels/properties for vertex {} while COW'ing into overlay.",
                                gid.AsUint());
  }

  auto [inserted_it, inserted] = acc.insert(storage::Vertex{gid, nullptr});
  MG_ASSERT(inserted, "Overlay vertex {} must not already exist -- find() above said it didn't.", gid.AsUint());
  storage::Vertex *overlay_vertex = &*inserted_it;
  // Direct field copy: an overlay Vertex is a plain materialized object with no live MVCC delta
  // chain to go through (see class comment), so this bypasses VertexAccessor entirely.
  overlay_vertex->labels = *labels_res;
  const bool init_ok = overlay_vertex->properties.InitProperties(*props_res);
  DMG_ASSERT(init_ok,
             "InitProperties must succeed on a fresh overlay vertex {} (PropertyStore is guaranteed empty).",
             gid.AsUint());
  return overlay_vertex;
}

storage::Edge *BranchOverlay::EnsureOverlayEdge(storage::Gid gid, storage::Storage::Accessor &fork_base,
                                                const std::optional<storage::Gid> &from_gid,
                                                const std::optional<storage::Gid> &to_gid,
                                                const std::optional<std::string> &edge_type_name) {
  auto acc = edges_.access();
  auto it = acc.find(gid);
  if (it != acc.end()) {
    return &*it;
  }

  // Same anomaly as EnsureOverlayVertex's: a mutation targeting an already-deleted (and not
  // recreated) edge has nothing left to mutate.
  if (deleted_edges_.contains(gid)) {
    throw utils::BasicException(
        "Branch change-log mutates edge {} after it was deleted (and not recreated) -- corrupt change-log.",
        gid.AsUint());
  }

  // First touch of this edge by the branch: resolve it in fork_base using the best available
  // hint data, mirroring dbms/inmemory/replication_handlers.cpp's WalEdgeSetProperty resolution
  // strategy (newest-to-oldest WAL format), then COW its properties out (read-only, R35).
  //
  // The *to_gid != kInvalidGid / !edge_type_name->empty() guards mirror the same tier-1 gate
  // replication_handlers.cpp:1381-1411 / wal.cpp:1612-1619 apply before trusting the hint triple:
  // without them, a sentinel to_gid (kInvalidGid) would make FindVertex(*to_gid) fail, leaving
  // e_acc unset and this function throwing a false "corrupt change-log" instead of correctly
  // falling through to the tier-2 (from_gid-only) / tier-3 (gid-only) resolution below.
  std::optional<storage::EdgeAccessor> e_acc;
  if (from_gid.has_value() && to_gid.has_value() && edge_type_name.has_value() && *to_gid != storage::kInvalidGid &&
      !edge_type_name->empty()) {
    auto from_v = fork_base.FindVertex(*from_gid, storage::View::OLD);
    auto to_v = fork_base.FindVertex(*to_gid, storage::View::OLD);
    if (from_v && to_v) {
      e_acc = fork_base.FindEdge(gid, storage::View::OLD, NameToEdgeType(*edge_type_name), &*from_v, &*to_v);
    }
  } else if (from_gid.has_value()) {
    e_acc = fork_base.FindEdge(gid, *from_gid, storage::View::OLD);
  } else {
    e_acc = fork_base.FindEdge(gid, storage::View::OLD);
  }

  if (!e_acc) {
    throw utils::BasicException(
        "Branch change-log references edge {} that is neither overlay-resident nor present in the fork-state "
        "base -- corrupt change-log or wrong fork_base.",
        gid.AsUint());
  }
  auto props_res = e_acc->Properties(storage::View::OLD);
  if (!props_res.has_value()) {
    throw utils::BasicException("Failed to read fork-state properties for edge {} while COW'ing into overlay.",
                                gid.AsUint());
  }

  auto [inserted_it, inserted] = acc.insert(storage::Edge{gid, nullptr});
  MG_ASSERT(inserted, "Overlay edge {} must not already exist -- find() above said it didn't.", gid.AsUint());
  storage::Edge *overlay_edge = &*inserted_it;
  const bool init_ok = overlay_edge->properties.InitProperties(*props_res);
  DMG_ASSERT(init_ok,
             "InitProperties must succeed on a fresh overlay edge {} (PropertyStore is guaranteed empty).",
             gid.AsUint());

  // Resolved from the fork-state accessor itself (authoritative), regardless of which of the
  // three lookup strategies above actually found it.
  edge_endpoints_[gid] = EdgeEndpoints{
      .edge_type = e_acc->EdgeType(), .from_vertex = e_acc->FromVertex().Gid(), .to_vertex = e_acc->ToVertex().Gid()};
  return overlay_edge;
}

void BranchOverlay::Materialize(const std::vector<WalDeltaData> &changelog, storage::Storage::Accessor &fork_base) {
  auto apply = utils::Overloaded{
      [&](storage::durability::WalVertexCreate const &data) {
        auto acc = vertices_.access();
        auto [it, inserted] = acc.insert(storage::Vertex{data.gid, nullptr});
        if (!inserted) {
          throw utils::BasicException("Branch change-log creates vertex {} more than once.", data.gid.AsUint());
        }
        // A gid can legitimately be reused within one Materialize() pass (delete then recreate --
        // unreachable via ordinary monotonic gid allocation today, but later chunks like
        // merge/rebase/undo may replay a delete+recreate pair for the same gid). Clear any stale
        // tombstone so LookupVertex reports the recreated object as kPresent, not kTombstoned.
        deleted_vertices_.erase(data.gid);
      },
      [&](storage::durability::WalVertexDelete const &data) {
        auto acc = vertices_.access();
        acc.remove(data.gid);  // No-op if it was never COW'd/created into the overlay -- fine.
        deleted_vertices_.insert(data.gid);
      },
      [&](storage::durability::WalVertexAddLabel const &data) {
        auto *v = EnsureOverlayVertex(data.gid, fork_base);
        auto label = NameToLabel(data.label);
        if (!std::ranges::contains(v->labels, label)) {
          v->labels.push_back(label);
        }
      },
      [&](storage::durability::WalVertexRemoveLabel const &data) {
        auto *v = EnsureOverlayVertex(data.gid, fork_base);
        auto label = NameToLabel(data.label);
        auto it = std::ranges::find(v->labels, label);
        if (it != v->labels.end()) {
          *it = v->labels.back();
          v->labels.pop_back();
        }
      },
      [&](storage::durability::WalVertexSetProperty const &data) {
        auto *v = EnsureOverlayVertex(data.gid, fork_base);
        v->properties.SetProperty(NameToProperty(data.property), storage::ToPropertyValue(data.value, name_id_mapper_));
      },
      [&](storage::durability::WalEdgeCreate const &data) {
        auto acc = edges_.access();
        auto [it, inserted] = acc.insert(storage::Edge{data.gid, nullptr});
        if (!inserted) {
          throw utils::BasicException("Branch change-log creates edge {} more than once.", data.gid.AsUint());
        }
        // See the matching comment on WalVertexCreate: clear any stale tombstone from a prior
        // delete of this same gid earlier in this pass.
        deleted_edges_.erase(data.gid);
        edge_endpoints_[data.gid] = EdgeEndpoints{
            .edge_type = NameToEdgeType(data.edge_type), .from_vertex = data.from_vertex, .to_vertex = data.to_vertex};
      },
      [&](storage::durability::WalEdgeDelete const &data) {
        auto acc = edges_.access();
        acc.remove(data.gid);  // No-op if it was never COW'd/created into the overlay -- fine.
        edge_endpoints_.erase(data.gid);
        deleted_edges_.insert(data.gid);
      },
      [&](storage::durability::WalEdgeSetProperty const &data) {
        auto *e = EnsureOverlayEdge(data.gid, fork_base, data.from_gid, data.to_gid, data.edge_type);
        e->properties.SetProperty(NameToProperty(data.property), storage::ToPropertyValue(data.value, name_id_mapper_));
      },
      [&](storage::durability::WalTransactionStart const &) { /* transaction bracketing only */ },
      [&](storage::durability::WalTransactionEnd const &) { /* transaction bracketing only */ },
      // Non-graph-data records (indices, constraints, enums, text/vector indices, TTL, ...) are
      // out of a branch overlay's scope -- it materializes graph data only.
      [&](auto const &) {}};

  for (const auto &delta : changelog) {
    std::visit(apply, delta.data_);
  }
}

BranchOverlay::Status BranchOverlay::LookupVertex(storage::Gid gid, storage::Vertex **out) {
  if (deleted_vertices_.contains(gid)) {
    if (out) *out = nullptr;
    return Status::kTombstoned;
  }
  auto acc = vertices_.access();
  auto it = acc.find(gid);
  if (it == acc.end()) {
    if (out) *out = nullptr;
    return Status::kAbsent;
  }
  if (out) *out = &*it;
  return Status::kPresent;
}

BranchOverlay::Status BranchOverlay::LookupEdge(storage::Gid gid, storage::Edge **out) {
  if (deleted_edges_.contains(gid)) {
    if (out) *out = nullptr;
    return Status::kTombstoned;
  }
  auto acc = edges_.access();
  auto it = acc.find(gid);
  if (it == acc.end()) {
    if (out) *out = nullptr;
    return Status::kAbsent;
  }
  if (out) *out = &*it;
  return Status::kPresent;
}

const BranchOverlay::EdgeEndpoints *BranchOverlay::LookupEdgeEndpoints(storage::Gid gid) const {
  auto it = edge_endpoints_.find(gid);
  return it == edge_endpoints_.end() ? nullptr : &it->second;
}

}  // namespace memgraph::versioning
