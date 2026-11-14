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
#include "versioning/stub_state.hpp"

// The unit tests' codec for a stub's whole state: StubState <-> the stub's labels and properties in the encoding of
// stub_state.hpp. Production code does not use it.
namespace memgraph::versioning {

// What a stub holds, unpacked. The mutators keep it canonical: a label or property is never both added and removed,
// a removed property carries no value, and a deleted object carries nothing else.
struct StubState {
  bool deleted{false};
  std::vector<storage::LabelId> added_labels;     // sorted
  std::vector<storage::LabelId> removed_labels;   // sorted
  storage::PropertyStore properties;              // changed values only
  std::vector<storage::PropertyId> removed_pids;  // sorted

  StubState Copy() const {
    StubState copy;
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

namespace memgraph::versioning::stub {

namespace detail {

template <typename Accessor>
std::optional<storage::Error> ApplyProperties(Accessor &stub, const StubState &before, const StubState &after) {
  auto const old_props = before.properties.Properties();
  auto const new_props = after.properties.Properties();
  for (auto const &[pid, value] : new_props) {
    auto const it = old_props.find(pid);
    if (it != old_props.end() && it->second == value) continue;
    if (auto r = stub.SetProperty(pid, value); !r) return r.error();
  }
  for (auto const &entry : old_props) {
    if (new_props.contains(entry.first)) continue;
    if (auto r = stub.SetProperty(entry.first, storage::PropertyValue{}); !r) return r.error();
  }
  for (auto const pid : after.removed_pids) {
    if (std::ranges::binary_search(before.removed_pids, pid)) continue;
    if (auto r = stub.SetProperty(Mark(pid), storage::PropertyValue{true}); !r) return r.error();
  }
  for (auto const pid : before.removed_pids) {
    if (std::ranges::binary_search(after.removed_pids, pid)) continue;
    if (auto r = stub.SetProperty(Mark(pid), storage::PropertyValue{}); !r) return r.error();
  }
  return std::nullopt;
}

inline void DecodeProperty(StubState &state, storage::PropertyId pid, const storage::PropertyValue &value) {
  if (storage::IsReservedId(pid)) {
    state.removed_pids.push_back(Strip(pid));
  } else {
    state.properties.SetProperty(pid, value);
  }
}

}  // namespace detail

// The stub's state in `view`. A deleted stub decodes to the delete alone, whatever else it still holds.
inline storage::Result<StubState> Decode(const storage::VertexAccessor &stub, storage::View view) {
  auto labels = stub.Labels(view);
  if (!labels) return std::unexpected(labels.error());
  auto props = stub.Properties(view);
  if (!props) return std::unexpected(props.error());
  StubState state;
  for (auto const l : *labels) {
    if (l == kDeleteLabel) {
      return StubState{.deleted = true};
    }
    if (storage::IsReservedId(l)) {
      state.removed_labels.push_back(Strip(l));
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

// Brings the stub from `before` (its decoded state) to `after` with stock single-key writes. nullopt on success.
// Not atomic: an error after the first write leaves the stub half-applied. A deleted object stays deleted.
inline std::optional<storage::Error> Apply(storage::VertexAccessor &stub, const StubState &before,
                                           const StubState &after) {
  DMG_ASSERT(!before.deleted || after.deleted, "a deleted object cannot be revived");
  if (before.deleted) return std::nullopt;

  auto const sync_labels = [&](const std::vector<storage::LabelId> &b,
                               const std::vector<storage::LabelId> &a,
                               bool marked) -> std::optional<storage::Error> {
    for (auto const l : a) {
      if (std::ranges::binary_search(b, l)) continue;
      if (auto r = stub.AddLabel(marked ? Mark(l) : l); !r) return r.error();
    }
    for (auto const l : b) {
      if (std::ranges::binary_search(a, l)) continue;
      if (auto r = stub.RemoveLabel(marked ? Mark(l) : l); !r) return r.error();
    }
    return std::nullopt;
  };
  if (after.deleted) {
    if (auto r = stub.AddLabel(kDeleteLabel); !r) return r.error();
    return std::nullopt;
  }
  if (auto e = sync_labels(before.added_labels, after.added_labels, false)) return e;
  if (auto e = sync_labels(before.removed_labels, after.removed_labels, true)) return e;
  return detail::ApplyProperties(stub, before, after);
}

// Edge stubs have no labels.
inline storage::Result<StubState> DecodeEdge(const storage::EdgeAccessor &stub, storage::View view) {
  auto props = stub.Properties(view);
  if (!props) return std::unexpected(props.error());
  StubState state;
  for (auto const &[pid, value] : *props) {
    if (pid == kEdgeDeletePid) return StubState{.deleted = true};
    detail::DecodeProperty(state, pid, value);
  }
  std::ranges::sort(state.removed_pids);
  return state;
}

inline std::optional<storage::Error> ApplyEdge(storage::EdgeAccessor &stub, const StubState &before,
                                               const StubState &after) {
  DMG_ASSERT(!before.deleted || after.deleted, "a deleted object cannot be revived");
  DMG_ASSERT(before.added_labels.empty() && before.removed_labels.empty() && after.added_labels.empty() &&
                 after.removed_labels.empty(),
             "an edge has no labels");
  if (before.deleted) return std::nullopt;
  if (after.deleted) {
    if (auto r = stub.SetProperty(kEdgeDeletePid, storage::PropertyValue{true}); !r) return r.error();
    return std::nullopt;
  }
  return detail::ApplyProperties(stub, before, after);
}

}  // namespace memgraph::versioning::stub
