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

#ifndef MG_ENTERPRISE
#error "versioning/overlay_mark_deleted.hpp is enterprise-only"
#endif

#include <algorithm>
#include <expected>
#include <mutex>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/atomic_memory_block.hpp"
#include "versioning/overlay_state.hpp"

// An overlay's delete mark is the stock `deleted` flag with the stock RECREATE_OBJECT undo, so the diff engine's MVCC
// versions, aborts and collects it as for any deleted object. Collection keeps it alive
// (Config::retained_gid_floors). Stock DeleteVertex / MarkEdgeAsDeleted do not fit an overlay: the first
// refuses on the Mark(T) adjacency entries, the second decrements an edge count the overlay never incremented.
namespace memgraph::versioning::overlay {

// Deletes `v` as stock vertex deletion does (RECREATE_OBJECT delta, deleted flag, label counts), but lets Mark(T)
// entries stay: they are the endpoint's hidden-edge set. Refuses with VERTEX_HAS_EDGES while a real edge is attached,
// and with SERIALIZATION_ERROR on a conflicting writer. Returns false when `v` is already deleted.
// @throw utils::OutOfMemoryException
inline storage::Result<bool> MarkVertexDeleted(storage::Transaction *transaction, storage::Storage &engine,
                                               storage::Vertex *v) {
  auto guard = std::unique_lock{v->lock};
  if (!storage::PrepareForWrite(transaction, v)) return std::unexpected{storage::Error::SERIALIZATION_ERROR};
  if (v->deleted()) return false;
  auto const is_real = [](auto const &entry) { return !storage::IsReservedId(std::get<storage::EdgeTypeId>(entry)); };
  if (std::ranges::any_of(v->in_edges, is_real) || std::ranges::any_of(v->out_edges, is_real)) {
    return std::unexpected{storage::Error::VERTEX_HAS_EDGES};
  }
  utils::AtomicMemoryBlock([&] {
    storage::CreateAndLinkDelta(transaction, v, storage::Delta::RecreateObjectTag());
    v->SetDeleted(true);
    transaction->UpdateOnVertexDelete(v);
  });
  // The abort's RECREATE_OBJECT re-adds one per label.
  for (auto const label : v->labels) engine.UpdateLabelCount(label, -1);
  return true;
}

// Deletes `e` as stock MarkEdgeAsDeleted does, without its edge_count_ decrement: a base edge never incremented the
// diff engine's count. Returns false when `e` is already deleted.
//
// Writes only the edge overlay, which is the sole conflict against a concurrent edge property write and against a
// DETACH of an endpoint. Stock deletion also writes both endpoints; a caller deleting a BASE edge must call
// DeleteBaseEdge, which adds the endpoint markers a concurrent edge creation conflicts with.
// @throw utils::OutOfMemoryException
inline storage::Result<bool> MarkEdgeDeleted(storage::Transaction *transaction, storage::Edge *e) {
  auto guard = std::unique_lock{e->lock};
  if (!storage::PrepareForWrite(transaction, e)) return std::unexpected{storage::Error::SERIALIZATION_ERROR};
  if (e->deleted()) return false;
  utils::AtomicMemoryBlock([&] {
    storage::CreateAndLinkDelta(transaction, e, storage::Delta::RecreateObjectTag());
    e->SetDeleted(true);
  });
  return true;
}

// Deletes the base edge `edge_overlay` (of type `base_edge_type`, from `from_overlay` to `to_overlay`):
// MarkEdgeDeleted, then a Mark(`base_edge_type`) entry on each endpoint (OUT on `from_overlay`, IN on `to_overlay`; a
// self-loop gets one in each list). Returns false, touching nothing else, when the edge is already deleted.
// SERIALIZATION_ERROR means a conflicting writer on the edge or an endpoint. Not atomic: a failure after the edge
// delete mark leaves earlier deltas in the transaction, which is poisoned (as in stock) and must abort, which undoes
// them all.
// @throw utils::OutOfMemoryException
inline storage::Result<bool> DeleteBaseEdge(storage::Transaction *transaction, storage::Storage &engine,
                                            storage::Edge *edge_overlay, storage::Vertex *from_overlay,
                                            storage::Vertex *to_overlay, storage::EdgeTypeId base_edge_type) {
  auto const marked_deleted = MarkEdgeDeleted(transaction, edge_overlay);
  if (!marked_deleted.has_value() || !*marked_deleted) return marked_deleted;
  if (!LinkEdgeMarker(
          transaction, engine, from_overlay, storage::EdgeDirection::OUT, base_edge_type, to_overlay, edge_overlay) ||
      !LinkEdgeMarker(
          transaction, engine, to_overlay, storage::EdgeDirection::IN, base_edge_type, from_overlay, edge_overlay)) {
    return std::unexpected{storage::Error::SERIALIZATION_ERROR};
  }
  return true;
}

}  // namespace memgraph::versioning::overlay
