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

// Compatibility shim.  DefaultCostResult and the generic resolver were
// previously test-only; both are now promoted to the production extract::
// namespace as reference adapters for CostResultType / Resolver.
//
// New code should refer to extract::DefaultCostResult and extract::DefaultResolver
// directly.  This file remains so that existing tests continue to compile.

#include "planner/extract/extractor.hpp"

namespace memgraph::planner::core::extract::testing {

template <typename T>
using DefaultCostResult = ::memgraph::planner::core::extract::DefaultCostResult<T>;

/// Free-function wrapper around DefaultResolver{} so old test call sites
/// `ResolveSelection<S, A, CR>(g, fm, root)` keep working.  Prefer
/// `extract::DefaultResolver{}(g, fm, root)` in new code.
template <typename Symbol, typename Analysis, typename CostResult>
  requires CostResultType<CostResult>
[[nodiscard]] auto ResolveSelection(EGraph<Symbol, Analysis> const &egraph, FrontierMap<CostResult> const &frontier_map,
                                    EClassId root) -> SelectionMap<typename CostResult::cost_t> {
  return DefaultResolver{}(egraph, frontier_map, root);
}

}  // namespace memgraph::planner::core::extract::testing
