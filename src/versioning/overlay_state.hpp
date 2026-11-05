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

#include <cstdint>
#include <expected>
#include <mutex>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/atomic_memory_block.hpp"

// A branch keeps what it changed on a base object as an overlay: a sparse stock Vertex or Edge in the branch's diff
// engine that holds only the difference to the object as of the fork. The diff engine's own MVCC then versions, aborts
// and collects the overlay; this header is the overlay's encoding in labels and properties:
//
//   label added by the branch       -> label L
//   base label removed              -> label Mark(L)
//   property set by the branch      -> property p = value
//   base property removed           -> property Mark(p) = true
//   object deleted                  -> label kDeleteLabel (vertex) / property kEdgeDeletePid (edge)
//   base edge deleted               -> entry of type Mark(T) in both endpoint overlays' adjacency (LinkEdgeMarker)
//
// A marked id has the reserved bit set (id_types.hpp), so it never collides with a user id.
namespace memgraph::versioning::overlay {

// Largest real id the encoding supports; real ids come from a counter starting at 0, far below it.
inline constexpr uint32_t kLastRealId = storage::kReservedIdBit - 3;

// Labels and properties share one name mapper, so the one delete id serves both.
inline constexpr uint32_t kDeleteId = 0xFFFFFFFEU;
inline constexpr storage::LabelId kDeleteLabel = storage::LabelId::FromUint(kDeleteId);
inline constexpr storage::PropertyId kEdgeDeletePid = storage::PropertyId::FromUint(kDeleteId);

inline storage::LabelId Mark(storage::LabelId l) noexcept {
  return storage::LabelId::FromUint(l.AsUint() | storage::kReservedIdBit);
}

inline storage::PropertyId Mark(storage::PropertyId p) noexcept {
  return storage::PropertyId::FromUint(p.AsUint() | storage::kReservedIdBit);
}

inline storage::EdgeTypeId Mark(storage::EdgeTypeId t) noexcept {
  return storage::EdgeTypeId::FromUint(t.AsUint() | storage::kReservedIdBit);
}

inline storage::LabelId Strip(storage::LabelId l) noexcept {
  return storage::LabelId::FromUint(l.AsUint() & ~storage::kReservedIdBit);
}

inline storage::PropertyId Strip(storage::PropertyId p) noexcept {
  return storage::PropertyId::FromUint(p.AsUint() & ~storage::kReservedIdBit);
}

inline storage::EdgeTypeId Strip(storage::EdgeTypeId t) noexcept {
  return storage::EdgeTypeId::FromUint(t.AsUint() & ~storage::kReservedIdBit);
}

static_assert(storage::IsReservedId(kDeleteId));
static_assert((kLastRealId | storage::kReservedIdBit) < kDeleteId,
              "no real id's mark is a delete marker, nor the invalid id 0xFFFFFFFF");

inline storage::Result<bool> IsDeleted(const storage::VertexAccessor &overlay, storage::View view) {
  return overlay.HasLabel(kDeleteLabel, view);
}

inline storage::Result<bool> IsEdgeDeleted(const storage::EdgeAccessor &overlay, storage::View view) {
  auto r = overlay.GetProperty(kEdgeDeletePid, view);
  if (!r) return std::unexpected(r.error());
  return !r->IsNull();
}

// Links one end of a deleted base edge into `overlay`'s adjacency as a stock-created Mark(`base_edge_type`) entry
// holding `edge_overlay` (not the base's EdgeRef: walkers read the gid through every entry). Its FORCED_SEQUENTIAL
// delta makes a concurrent edge creation conflict; each OUT entry adds the edge_count_ increment that balances its
// abort's decrement. Returns false, with the transaction poisoned, when another transaction wrote the overlay.
// @throw utils::OutOfMemoryException
inline bool LinkEdgeMarker(storage::Transaction *transaction, storage::Storage &engine, storage::Vertex *overlay,
                           storage::EdgeDirection direction, storage::EdgeTypeId base_edge_type,
                           storage::Vertex *other_overlay, storage::Edge *edge_overlay) {
  auto const marked = Mark(base_edge_type);
  auto const edge = storage::EdgeRef{edge_overlay};
  auto guard = std::unique_lock{overlay->lock};
  if (!storage::PrepareForWrite(transaction, overlay)) return false;
  utils::AtomicMemoryBlock([&] {
    constexpr auto kState = storage::DeltaChainState::FORCED_SEQUENTIAL;
    if (direction == storage::EdgeDirection::OUT) {
      storage::CreateAndLinkDelta(
          transaction, overlay, storage::Delta::RemoveOutEdgeTag(), marked, other_overlay, edge, kState);
      overlay->out_edges.emplace_back(marked, other_overlay, edge);
      transaction->abort_callbacks_.Add(
          [&count = engine.edge_count_] { count.fetch_add(1, std::memory_order_acq_rel); });
    } else {
      storage::CreateAndLinkDelta(
          transaction, overlay, storage::Delta::RemoveInEdgeTag(), marked, other_overlay, edge, kState);
      overlay->in_edges.emplace_back(marked, other_overlay, edge);
    }
    transaction->manyDeltasCache.Invalidate(overlay, marked, direction);
  });
  return true;
}

}  // namespace memgraph::versioning::overlay
