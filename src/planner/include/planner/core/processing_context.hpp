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
#include <vector>

#include "planner/core/eids.hpp"
#include "planner/core/enode.hpp"
#include "planner/core/union_find.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::planner::core {

/**
 * @brief Base context with non-templated shared components
 *
 * Contains reusable buffers and state that don't depend on Symbol type.
 */
struct BaseProcessingContext {
  /**
   * @brief Reusable buffer for canonicalization operations
   *
   * Temporary storage for canonical children during e-node canonicalization.
   * Reusing this buffer eliminates repeated allocations in hot paths.
   */
  mutable utils::small_vector<EClassId> canonical_children_buffer;

  /**
   * @brief Context for bulk union-find operations
   *
   * Provides temporary storage for efficient bulk merging of e-classes
   * during congruence closure.
   */
  UnionFindContext union_find_context;
};

struct Bla {
  std::vector<EClassId> parent_eclass_ids;
  std::vector<ENodeId> parent_enode_ids;
};

/**
 * @brief External context for e-graph operations
 *
 * ProcessingContext provides temporary storage for e-graph algorithms,
 * particularly during rebuilding and congruence closure operations.
 * By passing this context externally, we avoid mutable state in the e-graph
 * and give users control over memory allocation.
 *
 * @tparam Symbol The symbol type used in e-nodes (must satisfy ENodeSymbol concept)
 *
 * @par Usage:
 * Create a ProcessingContext and pass it to operations like merge() and rebuild():
 * @code
 * ProcessingContext<MySymbol> ctx;
 * egraph.merge(id1, id2, ctx);
 * egraph.rebuild(ctx);
 * @endcode
 *
 * @par Performance:
 * The context can be reused across multiple operations. Internal data structures
 * are cleared but capacity is retained, avoiding repeated allocations.
 */
template <typename Symbol>
struct ProcessingContext : BaseProcessingContext {
  /**
   * @brief Map from canonical e-nodes to their parent e-classes
   *
   * Used during rebuilding to group parents by their canonical form,
   * enabling efficient detection of congruent parents that should be merged.
   */
  std::unordered_map<ENode<Symbol>, Bla> enode_to_parents;

  /**
   * @brief Reserve capacity for expected number of unique e-nodes
   *
   * Pre-allocates space in the enode_to_parents map to avoid rehashing
   * during rebuild operations.
   *
   * @param capacity Expected number of unique canonical e-nodes
   */
  void reserve(size_t capacity) { enode_to_parents.reserve(capacity); }

  /**
   * @brief Clear all temporary data
   *
   * Clears the contents while preserving allocated capacity for reuse.
   * Called automatically at the start of rebuild operations.
   */
  void clear() {
    enode_to_parents.clear();
    // union_find_context is cleared internally when used
  }
};

}  // namespace memgraph::planner::core
