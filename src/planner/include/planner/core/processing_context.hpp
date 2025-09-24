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

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/container/flat_set.hpp>

#include "planner/core/constants.hpp"
#include "planner/core/eids.hpp"
#include "planner/core/enode.hpp"
#include "planner/core/union_find.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::planner::core {

/**
 * Contains reusable buffers and state that don't depend on Symbol type.
 */
struct BaseProcessingContext {
  utils::small_vector<EClassId> canonical_children_buffer;
  UnionFindContext union_find_context;
  std::vector<EClassId> canonical_eclass_ids;
  boost::container::flat_set<EClassId> canonicalized_chunk;
};

/**
 * Contains reusable buffers and state
 */
template <typename Symbol>
struct ProcessingContext : BaseProcessingContext {
  ProcessingContext() { canonicalized_chunk.reserve(REBUILD_BATCH_SIZE); }

  auto rebuild_canonicalized_chunk_container() -> boost::container::flat_set<EClassId> & {
    canonicalized_chunk.clear();
    return canonicalized_chunk;
  }

  auto rebuild_enode_to_parents_container() -> std::unordered_map<ENode<Symbol>, std::vector<ENodeId>> & {
    enode_to_parents.clear();
    return enode_to_parents;
  }

 private:
  std::unordered_map<ENode<Symbol>, std::vector<ENodeId>> enode_to_parents;
};

}  // namespace memgraph::planner::core
