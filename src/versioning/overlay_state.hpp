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
#error "versioning/overlay_state.hpp is enterprise-only"
#endif

#include <algorithm>
#include <cstdint>
#include <expected>
#include <mutex>
#include <span>

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
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/logging.hpp"

// An overlay is a sparse stock Vertex or Edge in a branch's diff engine holding only a branch's difference to the base
// object, so the diff engine's own MVCC versions, aborts and collects it. This header is the encoding in labels and
// properties; a marked id has the reserved bit set (id_types.hpp), so it never collides with a user id:
//
//   label added by the branch       -> label L
//   base label removed              -> label Mark(L)
//   property set by the branch      -> property p = value
//   base property removed           -> property Mark(p) = true
//   object deleted                  -> label kDeleteLabel (vertex) / property kEdgeDeletePid (edge)
//   base edge deleted               -> entry of type Mark(T) in both endpoint overlays' adjacency (LinkEdgeMarker)
namespace memgraph::versioning::overlay {

// Largest real id the encoding supports; real ids come from a counter starting at 0, far below it.
inline constexpr uint32_t kLastRealId = storage::kReservedIdBit - 3;

// Labels and properties share one name mapper, so the one delete id serves both.
inline constexpr storage::LabelId kDeleteLabel = storage::LabelId::FromUint(storage::kDeleteId);
inline constexpr storage::PropertyId kEdgeDeletePid = storage::PropertyId::FromUint(storage::kDeleteId);

static_assert(storage::IsReservedId(storage::kDeleteId));
static_assert((kLastRealId | storage::kReservedIdBit) < storage::kDeleteId,
              "no real id's mark is a delete marker, nor the invalid id 0xFFFFFFFF");

inline storage::LabelId Mark(storage::LabelId l) noexcept {
  return storage::LabelId::FromUint(l.AsUint() | storage::kReservedIdBit);
}

inline storage::PropertyId Mark(storage::PropertyId p) noexcept {
  return storage::PropertyId::FromUint(p.AsUint() | storage::kReservedIdBit);
}

inline storage::EdgeTypeId Mark(storage::EdgeTypeId t) noexcept {
  return storage::EdgeTypeId::FromUint(t.AsUint() | storage::kReservedIdBit);
}

// Whether an overlay vertex's labels carry the delete marker.
inline bool HasDeleteLabel(std::span<const storage::LabelId> labels) noexcept {
  return std::ranges::contains(labels, kDeleteLabel);
}

inline storage::Result<bool> IsDeleted(const storage::VertexAccessor &overlay, storage::View view) {
  return overlay.HasLabel(kDeleteLabel, view);
}

inline storage::Result<bool> IsEdgeDeleted(const storage::EdgeAccessor &overlay, storage::View view) {
  auto r = overlay.GetProperty(kEdgeDeletePid, view);
  if (!r) return std::unexpected(r.error());
  return !r->IsNull();
}

// Writes `value` (null removes) to `key` on a vertex or edge overlay in the per-key order readers rely on: the removal
// marker goes on before the value is dropped and off after the new value is set. `in_base`: the base object has the
// key; `marked`: the overlay already holds Mark(key). Not atomic across the writes; the caller guards a partial write.
template <typename OverlayAccessor>
storage::Result<void> WriteKey(OverlayAccessor &overlay, storage::PropertyId key, const storage::PropertyValue &value,
                               bool in_base, bool marked) {
  if (value.IsNull()) {
    if (in_base) {
      if (auto on = overlay.SetProperty(Mark(key), storage::PropertyValue{true}); !on)
        return std::unexpected(on.error());
    }
    if (auto dropped = overlay.SetProperty(key, storage::PropertyValue{}); !dropped) {
      return std::unexpected(dropped.error());
    }
    return {};
  }
  if (auto set = overlay.SetProperty(key, value); !set) return std::unexpected(set.error());
  if (marked) {
    if (auto off = overlay.SetProperty(Mark(key), storage::PropertyValue{}); !off) return std::unexpected(off.error());
  }
  return {};
}

// Links one end of a deleted base edge into `overlay`'s adjacency as a stock-created Mark(`base_edge_type`) entry
// holding `edge_overlay` (not the base's EdgeRef: walkers read the gid through every entry). Its FORCED_SEQUENTIAL
// delta makes a concurrent edge creation conflict; each OUT entry adds the edge_count_ increment that balances its
// abort's decrement. Returns false, with the transaction poisoned, when another transaction wrote the overlay.
// @throw utils::OutOfMemoryException
inline bool LinkEdgeMarker(storage::Transaction *transaction, storage::Storage &engine, storage::Vertex *overlay,
                           storage::EdgeDirection direction, storage::EdgeTypeId base_edge_type,
                           storage::Vertex *other_overlay, storage::Edge *edge_overlay) {
  // Analytical mode creates no delta, so the entry would be permanent and its count compensation unbalanced.
  DMG_ASSERT(transaction->storage_mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL, "overlays need MVCC deltas");
  auto const marked = Mark(base_edge_type);
  auto const edge = storage::EdgeRef{edge_overlay};
  auto guard = std::unique_lock{overlay->lock};
  if (!storage::PrepareForWrite(transaction, overlay)) return false;
  utils::AtomicMemoryBlock([&] {
    constexpr auto kState = storage::DeltaChainState::FORCED_SEQUENTIAL;
    if (direction == storage::EdgeDirection::OUT) {
      // First, so a failure after the delta exists cannot leave its abort's decrement uncompensated.
      transaction->abort_callbacks_.Add(
          [&count = engine.edge_count_] { count.fetch_add(1, std::memory_order_acq_rel); });
      storage::CreateAndLinkDelta(
          transaction, overlay, storage::Delta::RemoveOutEdgeTag(), marked, other_overlay, edge, kState);
      overlay->out_edges.emplace_back(marked, other_overlay, edge);
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
