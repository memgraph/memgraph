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

module;

#include <algorithm>
#include <cassert>

#include <boost/container/small_vector.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

module memgraph.planner.core.egraph;

namespace memgraph::planner::core::detail {

EClassBase::EClassBase(ENodeId initial_enode_id) {
  parents_.reserve(16);
  nodes_.push_back(initial_enode_id);
}

void EClassBase::remove_node(ENodeId enode_id) {
  auto it = std::ranges::find(nodes_, enode_id);
  if (it != nodes_.end()) {
    nodes_.erase(it);
  }
}

void EClassBase::merge_with(EClassBase &other) {
  // Simple optimization: swap if other is significantly larger
  if (other.nodes_.size() > nodes_.size()) {
    nodes_.swap(other.nodes_);
    parents_.swap(other.parents_);
  }

  // Append other's nodes. Duplicates are structurally impossible:
  // each e-node belongs to exactly one e-class, and merge transfers ownership.
  nodes_.insert(nodes_.end(), other.nodes_.begin(), other.nodes_.end());

  // TODO: should parents ever be made canonical to reduce the set of equivalent parents
  parents_.insert(other.parents_.begin(), other.parents_.end());
}

}  // namespace memgraph::planner::core::detail
