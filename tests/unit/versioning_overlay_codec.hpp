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

#include <algorithm>
#include <expected>
#include <optional>
#include <vector>

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/logging.hpp"
#include "versioning/overlay_state.hpp"

// Test-only codec: OverlayState <-> an overlay's labels and properties (encoding in overlay_state.hpp), written through
// the product WriteKey.
namespace memgraph::versioning {

// What an overlay holds, unpacked and canonical: nothing both added and removed, a removed property has no value, a
// deleted object nothing else.
struct OverlayState {
  bool deleted{false};
  std::vector<storage::LabelId> added_labels;     // sorted
  std::vector<storage::LabelId> removed_labels;   // sorted
  storage::PropertyStore properties;              // changed values only
  std::vector<storage::PropertyId> removed_pids;  // sorted

  OverlayState Copy() const {
    OverlayState copy;
    copy.deleted = deleted;
    copy.added_labels = added_labels;
    copy.removed_labels = removed_labels;
    copy.properties = storage::PropertyStore::CreateFromBuffer(properties.StringBuffer());
    copy.removed_pids = removed_pids;
    return copy;
  }

  void AddLabel(storage::LabelId label) {
    Erase(removed_labels, label);
    Insert(added_labels, label);
  }

  void RemoveLabel(storage::LabelId label) {
    Erase(added_labels, label);
    Insert(removed_labels, label);
  }

  void UnhideLabel(storage::LabelId label) { Erase(removed_labels, label); }

  void DropAddedLabel(storage::LabelId label) { Erase(added_labels, label); }

  void SetProperty(storage::PropertyId pid, const storage::PropertyValue &value) {
    if (value.IsNull()) {
      RemoveProperty(pid);
      return;
    }
    properties.SetProperty(pid, value);
    Erase(removed_pids, pid);
  }

  void RemoveProperty(storage::PropertyId pid) {
    properties.SetProperty(pid, storage::PropertyValue{});
    Insert(removed_pids, pid);
  }

  void MarkDeleted() {
    deleted = true;
    added_labels.clear();
    removed_labels.clear();
    properties.ClearProperties();
    removed_pids.clear();
  }

 private:
  template <typename T>
  static void Insert(std::vector<T> &sorted, T value) {
    auto const it = std::ranges::lower_bound(sorted, value);
    if (it == sorted.end() || *it != value) sorted.insert(it, value);
  }

  template <typename T>
  static void Erase(std::vector<T> &sorted, T value) {
    auto const it = std::ranges::lower_bound(sorted, value);
    if (it != sorted.end() && *it == value) sorted.erase(it);
  }
};

}  // namespace memgraph::versioning

namespace memgraph::versioning::overlay {

namespace detail {

template <typename Accessor>
std::optional<storage::Error> ApplyProperties(Accessor &overlay, const OverlayState &before,
                                              const OverlayState &after) {
  auto const old_props = before.properties.Properties();
  auto const new_props = after.properties.Properties();
  auto const write = [&](storage::PropertyId pid, const storage::PropertyValue &value, bool in_base, bool marked) {
    auto r = WriteKey(overlay, pid, value, in_base, marked);
    return r ? std::nullopt : std::optional{r.error()};
  };
  for (auto const &[pid, value] : new_props) {
    auto const it = old_props.find(pid);
    if (it != old_props.end() && it->second == value) continue;
    if (auto e = write(pid, value, false, std::ranges::binary_search(before.removed_pids, pid))) return e;
  }
  for (auto const &entry : old_props) {
    if (new_props.contains(entry.first) || std::ranges::binary_search(after.removed_pids, entry.first)) continue;
    if (auto e = write(entry.first, storage::PropertyValue{}, false, false)) return e;
  }
  for (auto const pid : after.removed_pids) {
    if (std::ranges::binary_search(before.removed_pids, pid)) continue;
    if (auto e = write(pid, storage::PropertyValue{}, true, false)) return e;
  }
  return std::nullopt;
}

inline void DecodeProperty(OverlayState &state, storage::PropertyId pid, const storage::PropertyValue &value) {
  if (storage::IsReservedId(pid)) {
    state.removed_pids.push_back(storage::StripReservedId(pid));
  } else {
    state.properties.SetProperty(pid, value);
  }
}

}  // namespace detail

// The overlay's state in `view`. A deleted overlay decodes to the delete alone, whatever else it still holds.
inline storage::Result<OverlayState> Decode(const storage::VertexAccessor &overlay, storage::View view) {
  auto labels = overlay.Labels(view);
  if (!labels) return std::unexpected(labels.error());
  auto props = overlay.Properties(view);
  if (!props) return std::unexpected(props.error());
  OverlayState state;
  if (HasDeleteLabel(*labels)) return OverlayState{.deleted = true};
  for (auto const l : *labels) {
    if (storage::IsReservedId(l)) {
      state.removed_labels.push_back(storage::StripReservedId(l));
    } else {
      state.added_labels.push_back(l);
    }
  }
  std::ranges::sort(state.added_labels);
  std::ranges::sort(state.removed_labels);
  for (auto const &[pid, value] : *props) {
    detail::DecodeProperty(state, pid, value);
  }
  std::ranges::sort(state.removed_pids);
  return state;
}

// Brings the overlay from `before` (its decoded state) to `after` with stock single-key writes. nullopt on success.
// Not atomic: an error after the first write leaves the overlay half-applied. A deleted object stays deleted.
inline std::optional<storage::Error> Apply(storage::VertexAccessor &overlay, const OverlayState &before,
                                           const OverlayState &after) {
  DMG_ASSERT(!before.deleted || after.deleted, "a deleted object cannot be revived");
  if (before.deleted) return std::nullopt;

  auto const sync_labels = [&](const std::vector<storage::LabelId> &b,
                               const std::vector<storage::LabelId> &a,
                               bool marked) -> std::optional<storage::Error> {
    for (auto const l : a) {
      if (std::ranges::binary_search(b, l)) continue;
      if (auto r = overlay.AddLabel(marked ? Mark(l) : l); !r) return r.error();
    }
    for (auto const l : b) {
      if (std::ranges::binary_search(a, l)) continue;
      if (auto r = overlay.RemoveLabel(marked ? Mark(l) : l); !r) return r.error();
    }
    return std::nullopt;
  };
  if (after.deleted) {
    if (auto r = overlay.AddLabel(kDeleteLabel); !r) return r.error();
    return std::nullopt;
  }
  if (auto e = sync_labels(before.added_labels, after.added_labels, false)) return e;
  if (auto e = sync_labels(before.removed_labels, after.removed_labels, true)) return e;
  return detail::ApplyProperties(overlay, before, after);
}

// Edge overlays have no labels.
inline storage::Result<OverlayState> DecodeEdge(const storage::EdgeAccessor &overlay, storage::View view) {
  auto props = overlay.Properties(view);
  if (!props) return std::unexpected(props.error());
  OverlayState state;
  for (auto const &[pid, value] : *props) {
    if (pid == kEdgeDeletePid) return OverlayState{.deleted = true};
    detail::DecodeProperty(state, pid, value);
  }
  std::ranges::sort(state.removed_pids);
  return state;
}

inline std::optional<storage::Error> ApplyEdge(storage::EdgeAccessor &overlay, const OverlayState &before,
                                               const OverlayState &after) {
  DMG_ASSERT(!before.deleted || after.deleted, "a deleted object cannot be revived");
  DMG_ASSERT(before.added_labels.empty() && before.removed_labels.empty() && after.added_labels.empty() &&
                 after.removed_labels.empty(),
             "an edge has no labels");
  if (before.deleted) return std::nullopt;
  if (after.deleted) {
    if (auto r = overlay.SetProperty(kEdgeDeletePid, storage::PropertyValue{true}); !r) return r.error();
    return std::nullopt;
  }
  return detail::ApplyProperties(overlay, before, after);
}

}  // namespace memgraph::versioning::overlay
