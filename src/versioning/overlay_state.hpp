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

#include "storage/v2/config.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/edge_info_helpers.hpp"
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
#include "utils/memory_tracker.hpp"

// An overlay is a sparse stock Vertex or Edge in a branch's diff engine holding only a branch's difference to the base
// object, so the diff engine's own MVCC versions, aborts and collects it.
//
// Creation is delta-less: it changes nothing visible, the overlay is shared by every branch transaction and is never
// removed. Labels are the FULL base label set copied at creation (CopyBaseLabels), so the STOCK AddLabel/RemoveLabel
// apply. A vertex overlay is deleted by the STOCK non-detach DeleteVertex (overlay_mark_deleted.hpp); its has-edges
// check skips Mark(T) entries. Never DetachDelete an overlay.
//
// OVERLAY-SPECIFIC operations, where stock cannot express the encoding:
//   a) WriteProperty. A key is absent (the base's value stands), an explicit null (PropertyStore::SetExplicitNull; the
//      base's value is removed) or a value. Stock erases a null, and its abort cannot restore an explicit null: a stock
//      SetProperty over one records a plain Null before-image. WriteProperty records
//      &storage::kExplicitNullBeforeImage instead; an MVCC reader reads that before-image as an explicit null, every
//      other before-image Null as absent.
//   b) Base-edge delete. The base edge is not in the overlay's adjacency, so deleting it adds Mark(T) entries to both
//      endpoint overlays (LinkEdgeMarker) and leaves edge_count_ alone. Mark(T) sets the reserved bit of the edge type
//      (id_types.hpp), so it never collides with a user type.
//   c) LinkEdgeMarker itself, the only writer of Mark(T) entries.
namespace memgraph::versioning::overlay {

inline storage::EdgeTypeId Mark(storage::EdgeTypeId t) noexcept {
  return storage::EdgeTypeId::FromUint(t.AsUint() | storage::kReservedIdBit);
}

// Gives an unpublished overlay the base vertex's labels as the delta-less base copy. The diff engine keeps no label
// counts (Config::track_label_counts is off). Must run before the overlay is published to any other thread or
// transaction: it takes no lock and records no delta.
// @throw utils::OutOfMemoryException
inline void CopyBaseLabels(storage::Vertex *overlay, std::span<const storage::LabelId> base_labels) {
  DMG_ASSERT(overlay->labels.empty() && overlay->delta() == nullptr, "labels are copied once, onto a fresh overlay");
  overlay->labels.reserve(base_labels.size());
  for (auto const label : base_labels) overlay->labels.push_back(label);
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

namespace detail {

// The overlay's state of `key` before a write: Null for absent and for an explicit null alike, as stock reads it.
struct PriorProperty {
  storage::PropertyValue value;
  bool explicit_null;
};

template <typename Overlay>
PriorProperty ReadPrior(const Overlay *overlay, storage::PropertyId key) {
  auto old = overlay->properties.FindProperty(key);
  auto const explicit_null = old.has_value() && old->IsNull();
  return {old.value_or(storage::PropertyValue{}), explicit_null};
}

// The stored half of a write, to run in the caller's AtomicMemoryBlock right after its delta is linked.
template <typename Overlay>
void StoreProperty(Overlay *overlay, storage::PropertyId key, const storage::PropertyValue &value,
                   bool old_explicit_null) {
  if (old_explicit_null) overlay->delta()->property.value = &storage::kExplicitNullBeforeImage;
  if (value.IsNull()) {
    overlay->properties.SetExplicitNull(key);
  } else {
    overlay->properties.SetProperty(key, value);
  }
}

// The diff engine has no constraints; overlay writes do not run the stock constraint checks.
inline bool NoConstraints(const storage::Transaction &transaction) {
  return !transaction.active_constraints_ || transaction.active_constraints_->empty();
}

}  // namespace detail

// Writes `value` to `key` on a vertex overlay; a null value removes the base's value by storing an explicit null. The
// undo delta records kExplicitNullBeforeImage when the old state was an explicit null, so an abort restores it.
// Keeps the stock SetProperty index upkeep (an overlay is indexed in its diff engine; a Null is never indexed) but not
// its schema-info or constraint upkeep, which the diff engine does not have. Returns SERIALIZATION_ERROR on a
// conflicting writer, DELETED_OBJECT on a marked-deleted overlay.
// @throw utils::OutOfMemoryException
inline storage::Result<void> WriteProperty(storage::Transaction *transaction, storage::Storage &engine,
                                           storage::Vertex *overlay, storage::PropertyId key,
                                           const storage::PropertyValue &value) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  DMG_ASSERT(transaction->storage_mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL, "overlays need MVCC deltas");
  DMG_ASSERT(engine.indices_.vector_index_.Empty() && detail::NoConstraints(*transaction),
             "overlay writes skip the vector conversion and constraint checks, which the diff engine must not have");
  auto guard = std::unique_lock{overlay->lock};
  if (!storage::PrepareForWrite(transaction, overlay)) return std::unexpected{storage::Error::SERIALIZATION_ERROR};
  if (overlay->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  auto const prior = detail::ReadPrior(overlay, key);
  utils::AtomicMemoryBlock([&] {
    storage::CreateAndLinkDelta(transaction, overlay, storage::Delta::SetPropertyTag(), key, prior.value);
    detail::StoreProperty(overlay, key, value, prior.explicit_null);
  });
  engine.indices_.UpdateOnSetProperty(key, prior.value, value, overlay, *transaction);
  transaction->UpdateOnSetProperty(key, prior.value, value, overlay);
  return {};
}

// As above for an edge overlay. `from` and `to` are the overlay's endpoint vertices in the diff engine and `type` its
// real edge type: the delta records `from` as its out vertex (abort and the WAL read it) and the edge indexes key on
// them.
// @throw utils::OutOfMemoryException
inline storage::Result<void> WriteProperty(storage::Transaction *transaction, storage::Storage &engine,
                                           storage::Edge *overlay, storage::Vertex *from, storage::Vertex *to,
                                           storage::EdgeTypeId type, storage::PropertyId key,
                                           const storage::PropertyValue &value) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  DMG_ASSERT(transaction->storage_mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL, "overlays need MVCC deltas");
  DMG_ASSERT(engine.indices_.vector_edge_index_.Empty() && detail::NoConstraints(*transaction),
             "overlay writes skip the vector conversion and constraint checks, which the diff engine must not have");
  DMG_ASSERT(from && to, "an edge overlay write needs its endpoints");
  auto guard = std::unique_lock{overlay->lock};
  if (!storage::PrepareForWrite(transaction, overlay)) return std::unexpected{storage::Error::SERIALIZATION_ERROR};
  if (overlay->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  auto const prior = detail::ReadPrior(overlay, key);
  utils::AtomicMemoryBlock([&] {
    storage::CreateAndLinkDelta(transaction, overlay, storage::Delta::SetPropertyTag(), from, key, prior.value);
    // Stock records the in-vertex so a replica finds the edge without a scan; not for an edge made by this transaction.
    if (engine.config_.durability.snapshot_wal_mode ==
            storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL &&
        transaction->commit_info &&
        !storage::EdgeWasCreatedThisTransaction(overlay,
                                                transaction->commit_info->timestamp.load(std::memory_order_acquire))) {
      transaction->RecordEdgeSetPropertyInfo(overlay->gid, to->gid, type);
    }
    detail::StoreProperty(overlay, key, value, prior.explicit_null);
  });
  engine.indices_.UpdateOnSetProperty(type, key, value, from, to, overlay, *transaction);
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
