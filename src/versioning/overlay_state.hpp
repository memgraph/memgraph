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

#include <cstdint>
#include <expected>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <type_traits>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
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
// object, so the diff engine's own MVCC versions, aborts and collects it. The encoding uses stock fields:
//
//   labels            the overlay's FULL label set, copied from the base at overlay creation (CopyBaseLabels);
//   afterwards
//                     stock AddLabel / RemoveLabel
//   property set      stock value
//   property removed  an explicit null entry (PropertyStore::SetExplicitNull), written by WriteProperty
//   object deleted    the stock deleted flag (MarkVertexDeleted / MarkEdgeDeleted, overlay_mark_deleted.hpp), read by
//   IsDeleted base edge deleted an entry of type Mark(T) in both endpoint overlays' adjacency (LinkEdgeMarker); the
//   only reserved
//                     id left: an edge type with the reserved bit set (id_types.hpp) never collides with a user type
//
// A property key is absent (the base's value stands), an explicit null (the base's value is removed) or a value.
// An MVCC reader reconstructing an older snapshot of a key through its SET_PROPERTY deltas must read a before-image
// equal to &storage::kExplicitNullBeforeImage as an explicit null, not as absent; every other before-image Null is
// absent. WriteProperty is the only writer that may put an explicit null on an overlay or write over one: a stock
// SetProperty over an explicit null records a plain Null before-image, and its abort would erase the key.
namespace memgraph::versioning::overlay {

inline storage::EdgeTypeId Mark(storage::EdgeTypeId t) noexcept {
  return storage::EdgeTypeId::FromUint(t.AsUint() | storage::kReservedIdBit);
}

// Gives an unpublished overlay the base vertex's labels as the delta-less base copy, counted as stock AddLabel would so
// MarkVertexDeleted's decrement balances. Must run before the overlay is published to any other thread or transaction:
// it takes no lock and records no delta.
// @throw utils::OutOfMemoryException
inline void CopyBaseLabels(storage::Storage &engine, storage::Vertex *overlay,
                           std::span<const storage::LabelId> base_labels) {
  DMG_ASSERT(overlay->labels.empty() && overlay->delta() == nullptr, "labels are copied once, onto a fresh overlay");
  overlay->labels.reserve(base_labels.size());
  for (auto const label : base_labels) {
    overlay->labels.push_back(label);
    engine.UpdateLabelCount(label, 1);
  }
}

// Whether the overlay is deleted in `view`: the stock deleted flag with the overlay's deltas applied.
inline bool IsDeleted(const storage::Transaction *transaction, const storage::Vertex *overlay, storage::View view) {
  return !storage::VertexAccessor::IsVisible(overlay, transaction, view);
}

inline bool IsDeleted(const storage::Transaction *transaction, const storage::Edge *overlay, storage::View view) {
  bool deleted = false;
  const storage::Delta *delta = nullptr;
  {
    auto const guard = std::shared_lock{overlay->lock};
    deleted = overlay->deleted();
    delta = overlay->delta();
  }
  storage::ApplyDeltasForRead(transaction, delta, view, [&deleted](const storage::Delta &d) {
    if (d.action == storage::Delta::Action::RECREATE_OBJECT) deleted = false;
  });
  return deleted;
}

// The overlay's latest stored state of `key` (this transaction's writes and everything committed): nullopt when absent,
// a Null value for an explicit null, otherwise the value. Older snapshots go through the stock MVCC readers.
template <typename Overlay>
  requires(std::is_same_v<Overlay, storage::Vertex> || std::is_same_v<Overlay, storage::Edge>)
std::optional<storage::PropertyValue> FindProperty(const Overlay &overlay, storage::PropertyId key) {
  auto const guard = std::shared_lock{overlay.lock};
  return overlay.properties.FindProperty(key);
}

// Writes `value` to `key` on a vertex or edge overlay; a null value removes the base's value by storing an explicit
// null. The undo delta records kExplicitNullBeforeImage when the old state was an explicit null, so an abort restores
// it. Not index- or schema-tracked: an overlay is in no index of its diff engine. Returns SERIALIZATION_ERROR on a
// conflicting writer, DELETED_OBJECT on a marked-deleted overlay.
// @throw utils::OutOfMemoryException
template <typename Overlay>
  requires(std::is_same_v<Overlay, storage::Vertex> || std::is_same_v<Overlay, storage::Edge>)
storage::Result<void> WriteProperty(storage::Transaction *transaction, Overlay *overlay, storage::PropertyId key,
                                    const storage::PropertyValue &value) {
  DMG_ASSERT(transaction->storage_mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL, "overlays need MVCC deltas");
  auto guard = std::unique_lock{overlay->lock};
  if (!storage::PrepareForWrite(transaction, overlay)) return std::unexpected{storage::Error::SERIALIZATION_ERROR};
  if (overlay->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  auto const old = overlay->properties.FindProperty(key);
  auto const old_is_explicit_null = old.has_value() && old->IsNull();
  utils::AtomicMemoryBlock([&] {
    storage::CreateAndLinkDelta(
        transaction, overlay, storage::Delta::SetPropertyTag(), key, old.value_or(storage::PropertyValue{}));
    if (old_is_explicit_null) overlay->delta()->property.value = &storage::kExplicitNullBeforeImage;
    if (value.IsNull()) {
      overlay->properties.SetExplicitNull(key);
    } else {
      overlay->properties.SetProperty(key, value);
    }
  });
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
