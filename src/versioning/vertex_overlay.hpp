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
#include <cstdint>
#include <map>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/schema_info_types.hpp"
#include "storage/v2/vertex_accessor.hpp"

// A branch's changes to one fork vertex, as BranchContext::ReadVertexOverlay hands them out, and the pure merge of
// those changes over the fork vertex's own state. Nothing here knows how the changes are stored.
namespace memgraph::versioning {

// Parts of the overlay a read decodes; the delete flag is always produced.
struct OverlayRead {
  bool labels{false};
  bool properties{false};
  bool deleted_edges{false};
};

inline constexpr OverlayRead kOverlayReadAll{.labels = true, .properties = true, .deleted_edges = true};

// Parts that were not requested stay empty. When `deleted` is set the other parts are empty too.
struct VertexOverlayView {
  using PropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;

  bool deleted{false};
  std::vector<storage::LabelId> added_labels;
  std::vector<storage::LabelId> removed_labels;
  PropertyMap properties;  // changed values only
  std::vector<storage::PropertyId> removed_pids;
  std::vector<uint64_t> deleted_edge_gids;  // ascending
};

inline storage::VertexKey MergeLabels(const VertexOverlayView &overlay, storage::VertexKey fork_labels) {
  std::vector<storage::LabelId> merged;
  for (auto const label : fork_labels) {
    if (!std::ranges::contains(overlay.removed_labels, label)) merged.push_back(label);
  }
  for (auto const label : overlay.added_labels) {
    if (!std::ranges::contains(merged, label)) merged.push_back(label);
  }
  fork_labels.clear();
  for (auto const label : merged) fork_labels.push_back(label);
  return fork_labels;
}

inline bool MergeHasLabel(const VertexOverlayView &overlay, bool in_fork, storage::LabelId label) {
  if (in_fork) return !std::ranges::contains(overlay.removed_labels, label);
  return std::ranges::contains(overlay.added_labels, label);
}

// Moves `overlay.properties` out.
inline storage::Result<VertexOverlayView::PropertyMap> MergeProperties(VertexOverlayView &overlay,
                                                                       const storage::VertexAccessor &fork) {
  auto merged = fork.Properties(storage::View::OLD);
  if (!merged) return merged;
  for (auto const pid : overlay.removed_pids) merged->erase(pid);
  for (auto &[key, value] : overlay.properties) merged->insert_or_assign(key, std::move(value));
  return merged;
}

inline storage::Result<storage::PropertyValue> MergeGetProperty(const VertexOverlayView &overlay,
                                                                const storage::VertexAccessor &fork,
                                                                storage::PropertyId key) {
  auto in_fork = fork.GetProperty(key, storage::View::OLD);
  if (!in_fork) return in_fork;
  if (auto const it = overlay.properties.find(key); it != overlay.properties.end()) return it->second;
  if (in_fork->IsNull() || std::ranges::contains(overlay.removed_pids, key)) return storage::PropertyValue{};
  return in_fork;
}

}  // namespace memgraph::versioning
