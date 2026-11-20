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
#include "storage/v2/edge_info_helpers.hpp"
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

// An overlay is a sparse stock Vertex or Edge in a branch's diff engine holding only the branch's difference to the
// base object, so the diff engine's own MVCC versions, aborts and collects it. Creation is delta-less and shared by all
// branch transactions.
//
// STOCK: labels (CopyBaseLabels copies the full base label set, so AddLabel/RemoveLabel apply), vertex delete
// (non-detach DeleteVertex is valid because an overlay's adjacency holds only native edges; never DetachDelete an
// overlay) and edge delete (stock MarkEdgeAsDeleted minus the edge_count_ decrement, see MarkEdgeDeleted).
//
// OVERLAY-SPECIFIC: only WriteProperty, because a key is absent (the base's value stands), an explicit null (the base's
// value is removed) or a value. Stock erases a null and its abort restores a plain Null, so WriteProperty sets
// kExplicitNullBeforeImage as the before-image of an overwritten explicit null. Stock readers see a plain Null; abort
// and the branch's overlay reader read it as an explicit null.
namespace memgraph::versioning::overlay {

// Takes no lock and records no delta, so it must run before the overlay is published. Label counts are not updated: the
// diff engine leaves Config::track_label_counts off.
// @throw utils::OutOfMemoryException
inline void CopyBaseLabels(storage::Vertex *overlay, std::span<const storage::LabelId> base_labels) {
  DMG_ASSERT(overlay->labels.empty() && overlay->delta() == nullptr, "labels are copied once, onto a fresh overlay");
  overlay->labels.reserve(base_labels.size());
  for (auto const label : base_labels) overlay->labels.push_back(label);
}

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

inline bool NoConstraints(const storage::Transaction &transaction) {
  return !transaction.active_constraints_ || transaction.active_constraints_->empty();
}

}  // namespace detail

// Writes `value` to `key` on a vertex overlay; a null value stores an explicit null. Keeps stock index upkeep but skips
// schema-info and constraint upkeep. Returns SERIALIZATION_ERROR on a conflicting writer, DELETED_OBJECT on a
// marked-deleted overlay.
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
  auto const old = overlay->properties.FindProperty(key);  // nullopt: absent, the base's value stands
  auto const before = old.value_or(storage::PropertyValue{});
  utils::AtomicMemoryBlock([&] {
    storage::CreateAndLinkDelta(transaction, overlay, storage::Delta::SetPropertyTag(), key, before);
    // Abort and stock readers would erase a plain Null; the sentinel restores an explicit null instead.
    if (old && old->IsNull()) overlay->delta()->property.value = &storage::kExplicitNullBeforeImage;
    if (value.IsNull()) {
      overlay->properties.SetExplicitNull(key);
    } else {
      overlay->properties.SetProperty(key, value);
    }
  });
  engine.indices_.UpdateOnSetProperty(key, before, value, overlay, *transaction);
  transaction->UpdateOnSetProperty(key, before, value, overlay);
  return {};
}

// As above for an edge overlay. `from`, `to` and `type` are the overlay's endpoints and real edge type in the diff
// engine; the delta records `from` as its out vertex, which abort reads.
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
  auto const old = overlay->properties.FindProperty(key);  // nullopt: absent, the base's value stands
  auto const before = old.value_or(storage::PropertyValue{});
  utils::AtomicMemoryBlock([&] {
    storage::CreateAndLinkDelta(transaction, overlay, storage::Delta::SetPropertyTag(), from, key, before);
    // Mirrors stock CreateAndLinkDeltaForEdgeSetProperty (file-local in edge_accessor.cpp).
    if (engine.config_.durability.snapshot_wal_mode ==
            storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL &&
        transaction->commit_info &&
        !storage::EdgeWasCreatedThisTransaction(overlay,
                                                transaction->commit_info->timestamp.load(std::memory_order_acquire))) {
      transaction->RecordEdgeSetPropertyInfo(overlay->gid, to->gid, type);
    }
    // Abort and stock readers would erase a plain Null; the sentinel restores an explicit null instead.
    if (old && old->IsNull()) overlay->delta()->property.value = &storage::kExplicitNullBeforeImage;
    if (value.IsNull()) {
      overlay->properties.SetExplicitNull(key);
    } else {
      overlay->properties.SetProperty(key, value);
    }
  });
  engine.indices_.UpdateOnSetProperty(type, key, value, from, to, overlay, *transaction);
  return {};
}

}  // namespace memgraph::versioning::overlay
