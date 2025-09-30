// Copyright 2025 Memgraph Ltd.
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

#include "planner/core/eids.hpp"
#include "planner/core/enode.hpp"

#include <boost/unordered/unordered_flat_set.hpp>

namespace memgraph::planner::core {

namespace detail {
struct EClassBase {
  explicit EClassBase(ENodeId initial_enode_id);

  /**
   * @brief Add a parent reference (needed for congruence maintenance)
   */
  void add_parent(ENodeId parent_enode_id) { parents_.insert(parent_enode_id); }

  /**
   * @brief Get the number of e-nodes in this class
   */
  [[nodiscard]] auto size() const -> size_t { return nodes_.size(); }

  auto nodes() const -> boost::unordered_flat_set<ENodeId> const & { return nodes_; }

  // TODO: does this need to be a set? do we use O(1) contains/lookup?
  //       maybe needed for ematching later, leave for now
  auto parents() const -> boost::unordered_flat_set<ENodeId> const & { return parents_; }

  void merge_with(EClassBase &other);

 private:
  boost::unordered_flat_set<ENodeId> nodes_;
  boost::unordered_flat_set<EClassId> parents_;
};
}  // namespace detail

/**
 * @brief Equivalence class containing semantically equivalent e-nodes
 *
 * @details
 * An E-class represents a set of e-nodes that are known to be semantically
 * equivalent.
 */
template <typename Analysis>
struct EClass : private detail::EClassBase {
  explicit EClass(ENodeId initial_enode_id) : EClassBase(initial_enode_id) {}

  using EClassBase::add_parent;
  using EClassBase::nodes;
  using EClassBase::parents;
  using EClassBase::size;

  /**
   * @brief Merge of another e-class into this one
   */
  void merge_with(EClass &&other) {
    EClassBase::merge_with(other);

    // Merge analysis data if present
    if constexpr (!std::is_same_v<Analysis, void>) {
      // This would need to be implemented based on the analysis type
      // For now, just a placeholder
    }
  }

 private:
  Analysis analysis_data;
};

}  // namespace memgraph::planner::core
