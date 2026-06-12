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

/// @file
/// Lowering for an indexed disjunction over a single node: fold N index scans
/// into a balanced `Union` tree topped by a single `Distinct`.

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "query/frontend/semantic/symbol.hpp"
#include "query/plan/operator.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::plan {

/// Fold per-symbol index `scans` into a balanced binary `Union` tree topped by a
/// single deduplicating `Distinct`, and return the root.
///
/// Preconditions (all hold for an indexed label or edge-type disjunction over
/// one node, and are exactly what make the balancing + single `Distinct` valid):
///   - every scan produces the same single output symbol `node_symbol`, so the
///     symbol mapping is identical at every level of the tree;
///   - the branches combine by pure set-union (`Union` concatenates), so one
///     global `Distinct` is equivalent to deduplicating at every level;
///   - the combine operator (`Union`) is order-independent in cost --
///     `Union(A, B)` costs the same as `Union(B, A)`, and the total cardinality
///     is the sum of the branches regardless of how they nest -- so the pairing
///     and nesting are free to optimize for depth. Note this does NOT require
///     the branches to be equal-cost: they routinely are not (one label may hit
///     a handful of vertices and another millions), and they are paired by
///     index order, not by cost.
///
/// Do NOT reuse this for query-level `UNION` (per-combinator symbols, `UNION
/// ALL`) or `Cartesian` (left/right order is the cost decision): those violate
/// the preconditions, and balancing would change semantics or cost.
///
/// Balancing keeps the operator-tree depth at O(log N) in the number of scans
/// instead of the O(N) of a left-deep chain, which otherwise overflows the
/// executor thread stack at large N. Balancing trades nothing in logical
/// (cardinality) cost; what it bounds is the executor stack depth. The single
/// top `Distinct` removes the duplicates a vertex matching several labels would
/// otherwise produce.
///
/// `scans` must be non-empty. With a single scan the result is that scan
/// unchanged (no `Union`, no `Distinct`).
inline std::unique_ptr<LogicalOperator> BalancedDisjunctionUnion(std::vector<std::unique_ptr<LogicalOperator>> scans,
                                                                 const Symbol &node_symbol) {
  MG_ASSERT(!scans.empty(), "BalancedDisjunctionUnion requires at least one scan");
  // More than one branch means a vertex can match several of them, so the
  // combined output needs deduplicating exactly once at the top.
  const bool needs_distinct = scans.size() > 1;
  // Combine adjacent pairs each round; an odd one out is carried over unchanged.
  while (scans.size() > 1) {
    std::vector<std::unique_ptr<LogicalOperator>> combined;
    combined.reserve((scans.size() + 1) / 2);
    for (std::size_t i = 0; i < scans.size(); i += 2) {
      if (i + 1 < scans.size()) {
        combined.push_back(std::make_unique<Union>(std::move(scans[i]),
                                                   std::move(scans[i + 1]),
                                                   std::vector<Symbol>{node_symbol},
                                                   std::vector<Symbol>{node_symbol},
                                                   std::vector<Symbol>{node_symbol}));
      } else {
        combined.push_back(std::move(scans[i]));
      }
    }
    scans = std::move(combined);
  }
  auto root = std::move(scans.front());
  if (needs_distinct) {
    root = std::make_unique<Distinct>(std::move(root), std::vector<Symbol>{node_symbol});
  }
  return root;
}

}  // namespace memgraph::query::plan
