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

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
#include "storage/v2/view.hpp"

#include <optional>
#include <shared_mutex>

namespace memgraph::storage {

inline bool IsEdgeVisible(Edge *edge, const Transaction *transaction, View view) {
  bool exists = true;
  bool deleted = true;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{edge->lock};
    deleted = edge->deleted();
    delta = edge->delta();
  }
  ApplyDeltasForRead(transaction, delta, view, [&](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT: {
        exists = false;
        break;
      }
    }
  });
  return exists && !deleted;
}

namespace detail {
template <typename IsEdge>
std::optional<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>> ResolveVisibleOutEdge(
    Vertex *from_vertex, IsEdge const &is_edge, bool properties_on_edges, const Transaction *transaction, View view) {
  std::optional<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>> link;
  std::shared_lock const guard{from_vertex->lock};
  for (auto const &[edge_type, opposing_vertex, edge_ref] : from_vertex->out_edges) {
    if (is_edge(edge_ref)) {
      link.emplace(edge_ref, edge_type, from_vertex, opposing_vertex);
      break;
    }
  }
  if (transaction == nullptr) return link;
  if (auto *delta = from_vertex->delta(); delta != nullptr) {
    using enum DeltaAction;
    ApplyDeltasForRead(transaction, delta, view, [&](const Delta &delta) {
      DeltaDispatch(delta,
                    utils::ChainedOverloaded{
                        ActionMethod<ADD_OUT_EDGE>([&](Delta const &delta) {
                          auto const &link_delta = delta.vertex_edge;
                          if (is_edge(link_delta.edge)) {
                            link.emplace(link_delta.edge, link_delta.edge_type, from_vertex, link_delta.vertex.Get());
                          }
                        }),
                        ActionMethod<REMOVE_OUT_EDGE>([&](Delta const &delta) {
                          if (is_edge(delta.vertex_edge.edge)) link.reset();
                        }),
                    });
    });
  }
  // Still under the vertex lock (vertex before edge, as writers take them): an edge another transaction is
  // creating stays in adjacency, and so allocated, until its writer takes this lock to unlink it.
  if (link && properties_on_edges && !IsEdgeVisible(std::get<EdgeRef>(*link).ptr, transaction, view)) {
    return std::nullopt;
  }
  return link;
}
}  // namespace detail

/// Resolves a by-gid lookup from `from_vertex`'s out-edges as `OutEdges` sees them in `view`; with
/// `transaction == nullptr` (replica replay, WAL recovery) it stays current-state and adjacency-only.
/// Whether one link is in the replayed adjacency depends only on the deltas that mention it, so the link is looked up
/// in place and only its own ADD/REMOVE_OUT_EDGE deltas are replayed, as `Edges_ActionMethod` would replay them.
/// `edge_ptr` is the edge's object when the caller already has it (heavy edges), so links compare by pointer.
inline std::optional<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>> ResolveVisibleOutEdge(
    Vertex *from_vertex, Gid edge_gid, Edge const *edge_ptr, bool properties_on_edges, const Transaction *transaction,
    View view) {
  // One matcher per EdgeRef form, so the adjacency scan carries no per-link branch on it.
  if (edge_ptr != nullptr) {
    return detail::ResolveVisibleOutEdge(
        from_vertex, [edge_ptr](EdgeRef ref) { return ref.ptr == edge_ptr; }, true, transaction, view);
  }
  if (properties_on_edges) {
    return detail::ResolveVisibleOutEdge(
        from_vertex, [edge_gid](EdgeRef ref) { return ref.ptr->gid == edge_gid; }, true, transaction, view);
  }
  return detail::ResolveVisibleOutEdge(
      from_vertex, [edge_gid](EdgeRef ref) { return ref.gid == edge_gid; }, false, transaction, view);
}

/// Returns true if the edge was created in the transaction with the given commit timestamp
/// (i.e. the edge's delta chain contains RECREATE_OBJECT from that transaction).
/// If there is no delta, or all deltas are from before this tx, the edge was created before → returns false.
inline bool EdgeWasCreatedThisTransaction(Edge *edge, uint64_t current_commit_timestamp) {
  for (Delta *delta = edge->delta(); delta != nullptr; delta = delta->next.load(std::memory_order_acquire)) {
    const auto ts = delta->commit_info->timestamp.load(std::memory_order_acquire);
    if (ts != current_commit_timestamp) break;
    if (delta->action == Delta::Action::DELETE_OBJECT) {
      return true;
    }
  }
  return false;
}

}  // namespace memgraph::storage
