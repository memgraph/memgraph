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

#include <expected>
#include <mutex>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/atomic_memory_block.hpp"
#include "versioning/overlay_state.hpp"

// An overlay's delete mark is the stock `deleted` flag with the stock RECREATE_OBJECT undo. An overlay vertex is
// deleted by stock non-detach DeleteVertex, which is valid because an overlay's adjacency holds only native edges.
namespace memgraph::versioning::overlay {

// Stock MarkEdgeAsDeleted minus its edge_count_ decrement (a base edge never incremented it). False if already deleted.
// The whole base-edge delete at this layer: it writes only the edge overlay; endpoint bookkeeping comes later.
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
