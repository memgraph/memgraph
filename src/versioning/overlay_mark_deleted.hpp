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
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/atomic_memory_block.hpp"

// An overlay's delete mark is the stock `deleted` flag with the stock RECREATE_OBJECT undo, so the diff engine's MVCC
// versions, aborts and collects it as for any deleted object. Collection keeps it alive
// (InMemoryStorage::SetRetainedGidFloors). Stock DeleteVertex / MarkEdgeAsDeleted do not fit an overlay: the first
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

}  // namespace memgraph::versioning::overlay
