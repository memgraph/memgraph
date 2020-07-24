#pragma once

#include <algorithm>
#include <set>
#include <utility>
#include <vector>

#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/id_types.hpp"

namespace storage::durability {

/// Structure used to hold metadata about the recovered snapshot/WAL.
struct RecoveryInfo {
  uint64_t next_vertex_id{0};
  uint64_t next_edge_id{0};
  uint64_t next_timestamp{0};
};

/// Structure used to track indices and constraints during recovery.
struct RecoveredIndicesAndConstraints {
  struct {
    std::vector<LabelId> label;
    std::vector<std::pair<LabelId, PropertyId>> label_property;
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
void AddRecoveredIndexConstraint(std::vector<TObj> *list, TObj obj,
                                 const char *error_message) {
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
void RemoveRecoveredIndexConstraint(std::vector<TObj> *list, TObj obj,
                                    const char *error_message) {
  auto it = std::find(list->begin(), list->end(), obj);
  if (it != list->end()) {
    std::swap(*it, list->back());
    list->pop_back();
  } else {
    throw RecoveryFailure(error_message);
  }
}

}  // namespace storage::durability
