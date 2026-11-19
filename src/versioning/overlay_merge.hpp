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
#include <map>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <type_traits>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/view.hpp"
#include "versioning/overlay_state.hpp"

// How a branch reads the properties of a base object: the overlay's state of a key, merged over the object as of the
// fork (read at View::OLD). Per key the overlay holds nothing (the base's value stands), an explicit null (the base's
// value is removed) or a value (see overlay_state.hpp).
//
// The stock property readers cannot serve an overlay at an older snapshot: they turn the explicit-null before-image
// into plain Null, which reads as an absent key, so the base's value would reappear. These readers walk the overlay's
// deltas themselves and read kExplicitNullBeforeImage as an explicit null. Each is one lock-protected copy of the
// stored state plus one delta walk, so a read is a whole snapshot of what it asks for. Callers check IsDeleted
// separately.
namespace memgraph::versioning::overlay {

namespace detail {

template <typename Overlay>
concept OverlayObject = std::is_same_v<Overlay, storage::Vertex> || std::is_same_v<Overlay, storage::Edge>;

// The key's state before a SET_PROPERTY delta's write: absent, an explicit null, or the old value.
inline std::optional<storage::PropertyValue> BeforeImage(const storage::Delta &delta) {
  if (storage::IsExplicitNullBeforeImage(delta.property.value)) return storage::PropertyValue{};
  if (delta.property.value->IsNull()) return std::nullopt;
  return storage::PropertyValue(*delta.property.value);
}

}  // namespace detail

// The overlay's state of `key` at `view` of `transaction`: nullopt when the overlay holds no entry, a Null value for an
// explicit null, otherwise the value.
template <detail::OverlayObject Overlay>
std::optional<storage::PropertyValue> ReadProperty(const storage::Transaction *transaction, const Overlay &overlay,
                                                   storage::PropertyId key, storage::View view) {
  std::optional<storage::PropertyValue> state;
  const storage::Delta *delta = nullptr;
  {
    auto const guard = std::shared_lock{overlay.lock};
    state = overlay.properties.FindProperty(key);
    delta = overlay.delta();
  }
  storage::ApplyDeltasForRead(transaction, delta, view, [&](const storage::Delta &d) {
    if (d.action == storage::Delta::Action::SET_PROPERTY && d.property.key == key) state = detail::BeforeImage(d);
  });
  return state;
}

// Every key the overlay holds an entry for at `view`; an explicit null is a Null entry.
template <detail::OverlayObject Overlay>
std::map<storage::PropertyId, storage::PropertyValue> ReadProperties(const storage::Transaction *transaction,
                                                                     const Overlay &overlay, storage::View view) {
  std::map<storage::PropertyId, storage::PropertyValue> state;
  const storage::Delta *delta = nullptr;
  {
    auto const guard = std::shared_lock{overlay.lock};
    state = overlay.properties.Properties();
    delta = overlay.delta();
  }
  storage::ApplyDeltasForRead(transaction, delta, view, [&](const storage::Delta &d) {
    if (d.action != storage::Delta::Action::SET_PROPERTY) return;
    if (auto before = detail::BeforeImage(d)) {
      state.insert_or_assign(d.property.key, std::move(*before));
    } else {
      state.erase(d.property.key);
    }
  });
  return state;
}

// The base object's properties with the overlay's entries over them: values win, explicit nulls remove.
template <typename BaseObject>
storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> MergeProperties(
    const std::map<storage::PropertyId, storage::PropertyValue> &overlay_properties, const BaseObject &base) {
  auto merged = base.Properties(storage::View::OLD);
  if (!merged) return merged;
  for (auto const &[pid, value] : overlay_properties) {
    if (value.IsNull()) {
      merged->erase(pid);
    } else {
      merged->insert_or_assign(pid, value);
    }
  }
  return merged;
}

// `own` is ReadProperty's result: the overlay's entry, else the base's value.
template <typename BaseObject>
storage::Result<storage::PropertyValue> MergeGetProperty(const std::optional<storage::PropertyValue> &own,
                                                         const BaseObject &base, storage::PropertyId key) {
  if (own) return *own;
  return base.GetProperty(key, storage::View::OLD);
}

template <typename BaseObject>
storage::Result<uint64_t> MergeGetPropertySize(const std::optional<storage::PropertyValue> &own, const BaseObject &base,
                                               storage::PropertyId key) {
  if (!own) return base.GetPropertySize(key, storage::View::OLD);
  if (own->IsNull()) return uint64_t{0};
  storage::PropertyStore store;
  store.SetProperty(key, *own);
  return uint64_t{store.PropertySize(key)};
}

}  // namespace memgraph::versioning::overlay
