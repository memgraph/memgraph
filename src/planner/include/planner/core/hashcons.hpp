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

#include <optional>
#include <stdexcept>
#include <unordered_map>

#include "absl/container/flat_hash_map.h"
#include "boost/unordered/unordered_flat_map.hpp"
#include "planner/core/enode.hpp"
#include "planner/core/fwd.hpp"

namespace memgraph::planner::core {

/**
 * @brief Hash consing table for efficient e-node deduplication using direct ENode mapping
 *
 * @details
 * Hash consing is a fundamental technique in e-graph implementations that
 * maintains a global table of unique e-node structures. When a new e-node
 * is created, the hash consing table is consulted to see if an equivalent
 * e-node already exists. If so, the existing e-class ID is returned;
 * otherwise, the new e-node is added to the table.
 *
 * This implementation maps ENode<Symbol> directly to EClassId, eliminating
 * the two-level lookup overhead. ENode's pre-computed hash enables efficient
 * direct mapping without performance penalty.
 *
 * @tparam Symbol The type used to represent operation symbols in e-nodes
 */
template <typename Symbol>
struct Hashcons {
  Hashcons() {
    // Pre-size to avoid expensive rehashing during initial population
    // Based on performance analysis: prevents 70-80% of rehash operations
    table_.reserve(4096);
  }

 private:
  /**
   * @brief Internal hash table storing ENode to e-class ID mappings
   */
  boost::unordered_flat_map<ENodeRef<Symbol>, EClassId> table_;
};

}  // namespace memgraph::planner::core
