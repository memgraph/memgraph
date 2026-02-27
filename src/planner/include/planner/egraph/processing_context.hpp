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

import memgraph.planner.core.eids;
import memgraph.planner.core.constants;
import memgraph.planner.core.union_find;

#include "planner/egraph/enode.hpp"

#include <boost/container/flat_set.hpp>

namespace memgraph::planner::core {

/**
 * Contains reusable buffers and state that don't depend on Symbol type.
 */
struct BaseProcessingContext {
  ENodeContext enode_context;
  UnionFindContext union_find_context;
  std::vector<strong::underlying_type_t<EClassId>> canonical_eclass_ids;
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
