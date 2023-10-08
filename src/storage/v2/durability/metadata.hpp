// Copyright 2023 Memgraph Ltd.
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
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"

namespace memgraph::storage::durability {

/// Structure used to hold metadata about the recovered snapshot/WAL.
struct RecoveryInfo {
  uint64_t next_vertex_id{0};
  uint64_t next_edge_id{0};
  uint64_t next_timestamp{0};

  // last timestamp read from a WAL file
  std::optional<uint64_t> last_commit_timestamp;

  std::vector<std::pair<Gid /*first vertex gid*/, uint64_t /*batch size*/>> vertex_batches;
};

/// Structure used to track indices and constraints during recovery.
struct RecoveredIndicesAndConstraints {
  struct {
    std::vector<LabelId> label;
    std::vector<std::pair<LabelId, PropertyId>> label_property;
    std::vector<std::pair<LabelId, LabelIndexStats>> label_stats;
    std::vector<std::pair<LabelId, std::pair<PropertyId, LabelPropertyIndexStats>>> label_property_stats;
  } indices;

  struct {
    std::vector<std::pair<LabelId, PropertyId>> existence;
    std::vector<std::pair<LabelId, std::set<PropertyId>>> unique;
  } constraints;
};

// Helper function used to insert indices/constraints into the recovered
// indices/constraints object.
// @throw RecoveryFailure
template <typename TObj>
void AddRecoveredIndexConstraint(std::vector<TObj> *list, TObj obj, const char *error_message) {
  auto it = std::find(list->begin(), list->end(), obj);
  if (it == list->end()) {
    list->push_back(obj);
  } else {
    throw RecoveryFailure(error_message);
  }
}

// Helper function used to remove indices/constraints from the recovered
// indices/constraints object.
// @throw RecoveryFailure
template <typename TObj>
void RemoveRecoveredIndexConstraint(std::vector<TObj> *list, TObj obj, const char *error_message) {
  auto it = std::find(list->begin(), list->end(), obj);
  if (it != list->end()) {
    std::swap(*it, list->back());
    list->pop_back();
  } else {
    throw RecoveryFailure(error_message);
  }
}

// Helper function used to remove indices stats from the recovered
// indices/constraints object.
// @note multiple stats can be pushed one after the other; when removing, remove from the back
// @throw RecoveryFailure
template <typename TObj, typename K>
void RemoveRecoveredIndexStats(std::vector<TObj> *list, K label, const char *error_message) {
  for (auto it = list->rbegin(); it != list->rend(); ++it) {
    if (it->first == label) {
      list->erase(std::next(it).base());  // erase using a reverse iterator
      return;
    }
  }
  throw RecoveryFailure(error_message);
}

}  // namespace memgraph::storage::durability
