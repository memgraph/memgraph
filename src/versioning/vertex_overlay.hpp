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
#include <map>
#include <optional>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/schema_info_types.hpp"
#include "storage/v2/view.hpp"
#include "versioning/vertex_overlay_store.hpp"

// Merges an OverlayVersion over the fork vertex/edge state read at View::OLD. The version is read in place and must not
// outlive the branch transaction that pinned it.
namespace memgraph::versioning {

inline storage::VertexKey MergeLabels(const OverlayVersion &version, storage::VertexKey fork_labels) {
  auto const removed = version.removed_labels();
  if (!removed.empty()) {
    auto const gone = [&](storage::LabelId label) { return std::ranges::binary_search(removed, label); };
    fork_labels.erase(std::remove_if(fork_labels.begin(), fork_labels.end(), gone), fork_labels.end());
  }
  for (auto const label : version.added_labels()) {
    if (!std::ranges::contains(fork_labels, label)) fork_labels.push_back(label);
  }
  return fork_labels;
}

// Added and removed labels are disjoint, so either set decides; nullopt defers to the fork vertex.
inline std::optional<bool> OverlayDecidesLabel(const OverlayVersion &version, storage::LabelId label) {
  if (std::ranges::binary_search(version.added_labels(), label)) return true;
  if (std::ranges::binary_search(version.removed_labels(), label)) return false;
  return std::nullopt;
}

template <typename Fork>
storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> MergeProperties(const OverlayVersion &version,
                                                                                       const Fork &fork) {
  auto merged = fork.Properties(storage::View::OLD);
  if (!merged) return merged;
  for (auto const pid : version.removed_pids()) merged->erase(pid);
  for (auto &[key, value] : version.properties().Properties()) merged->insert_or_assign(key, std::move(value));
  return merged;
}

template <typename Fork>
storage::Result<storage::PropertyValue> MergeGetProperty(const OverlayVersion &version, const Fork &fork,
                                                         storage::PropertyId key) {
  // A property the overlay holds is never null: a null write is stored as a removal.
  if (auto own = version.properties().GetProperty(key); !own.IsNull()) return own;
  if (std::ranges::binary_search(version.removed_pids(), key)) return storage::PropertyValue{};
  return fork.GetProperty(key, storage::View::OLD);
}

template <typename Fork>
storage::Result<uint64_t> MergeGetPropertySize(const OverlayVersion &version, const Fork &fork,
                                               storage::PropertyId key) {
  // Overlay properties are never null, so a non-zero size means the overlay owns `key`.
  if (auto const own = version.properties().PropertySize(key); own != 0) return uint64_t{own};
  auto in_fork = fork.GetPropertySize(key, storage::View::OLD);
  if (!in_fork) return in_fork;
  if (*in_fork == 0 || std::ranges::binary_search(version.removed_pids(), key)) return uint64_t{0};
  return in_fork;
}

}  // namespace memgraph::versioning
