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

#include <memory>

namespace memgraph::query {
class SymbolTable;
}  // namespace memgraph::query

namespace memgraph::query::plan {

class LogicalOperator;

/// Marks the variable-length expansions a pruning BFS may replace. Which of the
/// two runs is settled by the cursor, the only one that can read the bound, so a
/// mark is not a promise that pruning happens and nothing may read it as one.
///
/// The deduplication a mark depends on has to stay in the plan whatever the
/// cursor settles on, and earns its place besides: it spans input rows, where
/// pruning reaches only within one.
std::unique_ptr<LogicalOperator> RewriteWithPruningBFS(std::unique_ptr<LogicalOperator> root_op,
                                                       SymbolTable const *symbol_table);

}  // namespace memgraph::query::plan
